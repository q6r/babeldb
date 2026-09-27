//! Read coalescing and a small write-through cache of recent pages.
//!
//! [`SingleFlight`]: concurrent identical reads share one execution. The first
//! caller (the leader) performs the read; callers that arrive while it runs
//! (followers) wait and receive an `Arc` of the same result. Nothing is kept
//! after the read completes: a cache would add staleness, and because nothing
//! is retained a writer never has to invalidate anything.
//!
//! Freshness: joining a read that is already running could hand a caller a
//! snapshot taken before a write the caller already saw acknowledged. To keep
//! read-your-writes, writers bump a [`WriteEpochs`] counter after their write
//! is visible and before acknowledging it; a caller joins a running read only
//! if that read started at or after the caller's current epoch, i.e. after
//! every write the caller could have observed. Writes that bypass the epochs
//! (not reported with `note_write`) do not get this guarantee.
//!
//! [`RecentCache`]: newest page of messages per channel, bounded in bytes,
//! write-through (every write made through `ChatStore` updates or invalidates
//! the channel's page in the writer's thread before the write returns). A page
//! is installed only if no write to its channel was in flight and no write
//! completed since the read started, so the cache never serves anything older
//! than the latest acknowledged write of that channel.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};

use super::sharded::Router;
use super::{ReadSource, lock, mix64, replicate_error, wait};
use crate::engine::{Revision, ScanItem, ScanOptions};
use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Write epochs
// ---------------------------------------------------------------------------

#[repr(align(64))]
#[derive(Default)]
struct PaddedU64(AtomicU64);

/// Which epoch a read depends on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochScope {
    /// Writes to one partition (identified by its hash).
    Partition(u64),
    /// Any write.
    Global,
}

/// Striped, monotonic write counters. Partitions sharing a stripe only make
/// sharing more conservative, never incorrect.
pub struct WriteEpochs {
    slots: Box<[PaddedU64]>,
    mask: u64,
    global: PaddedU64,
}

impl Default for WriteEpochs {
    fn default() -> Self {
        WriteEpochs::new(1024)
    }
}

impl WriteEpochs {
    /// `slots` is rounded up to a power of two.
    pub fn new(slots: usize) -> WriteEpochs {
        let n = slots.clamp(1, 1 << 20).next_power_of_two();
        WriteEpochs {
            slots: (0..n).map(|_| PaddedU64::default()).collect(),
            mask: (n - 1) as u64,
            global: PaddedU64::default(),
        }
    }

    fn slot(&self, partition: u64) -> &AtomicU64 {
        &self.slots[(mix64(partition) & self.mask) as usize].0
    }

    pub fn current(&self, scope: EpochScope) -> u64 {
        match scope {
            EpochScope::Partition(p) => self.slot(p).load(Ordering::Acquire),
            EpochScope::Global => self.global.0.load(Ordering::Acquire),
        }
    }

    /// Record a write to `partition`. Call after the write is visible and
    /// before acknowledging it (the release pairs with `current`'s acquire).
    pub fn bump(&self, partition: u64) {
        self.slot(partition).fetch_add(1, Ordering::AcqRel);
        self.global.0.fetch_add(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------
// Single flight
// ---------------------------------------------------------------------------

type Outcome<V> = std::result::Result<Arc<V>, Arc<Error>>;

struct Flight<V> {
    epoch: u64,
    leader: ThreadId,
    outcome: Mutex<Option<Outcome<V>>>,
    done: Condvar,
}

impl<V> Flight<V> {
    fn wait(&self) -> Outcome<V> {
        let mut g = lock(&self.outcome);
        loop {
            if let Some(o) = g.as_ref() {
                return o.clone();
            }
            g = wait(&self.done, g);
        }
    }

    fn finish(&self, o: Outcome<V>) {
        *lock(&self.outcome) = Some(o);
        self.done.notify_all();
    }
}

/// A read result shared by every caller of one flight.
#[derive(Debug)]
pub struct SharedRead<V> {
    pub value: Arc<V>,
    /// Epoch observed before the read started.
    pub epoch: u64,
    /// Whether this caller performed the read.
    pub leader: bool,
}

type FlightMap<K, V> = Mutex<HashMap<K, Arc<Flight<V>>>>;

/// Deduplicates concurrent identical reads (see the module docs).
pub struct SingleFlight<K, V> {
    stripes: Box<[FlightMap<K, V>]>,
    hasher: RandomState,
    leaders: AtomicU64,
    followers: AtomicU64,
}

impl<K: Hash + Eq + Clone, V> Default for SingleFlight<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Hash + Eq + Clone, V> SingleFlight<K, V> {
    pub fn new() -> Self {
        Self::with_stripes(64)
    }

    /// `stripes` (rounded up to a power of two) independent maps.
    pub fn with_stripes(stripes: usize) -> Self {
        let n = stripes.clamp(1, 4096).next_power_of_two();
        SingleFlight {
            stripes: (0..n).map(|_| Mutex::new(HashMap::new())).collect(),
            hasher: RandomState::new(),
            leaders: AtomicU64::new(0),
            followers: AtomicU64::new(0),
        }
    }

    fn stripe(&self, key: &K) -> &FlightMap<K, V> {
        let h = self.hasher.hash_one(key) as usize;
        &self.stripes[h & (self.stripes.len() - 1)]
    }

    /// Run `load` for `key`, or join a running load of the same key that
    /// started at an epoch >= `epoch` (read from [`WriteEpochs::current`]
    /// BEFORE calling). Errors are shared too. `load` must not call `run`
    /// for the same key (that returns `InvalidArgument` instead of hanging).
    pub fn run<F>(&self, key: K, epoch: u64, load: F) -> Result<SharedRead<V>>
    where
        F: FnOnce() -> Result<V>,
    {
        let stripe = self.stripe(&key);
        let me = thread::current().id();
        let flight = {
            let mut map = lock(stripe);
            if let Some(running) = map.get(&key)
                && running.epoch >= epoch
            {
                if running.leader == me {
                    return Err(Error::InvalidArgument(
                        "singleflight: re-entrant read of a key this thread is loading".into(),
                    ));
                }
                let running = running.clone();
                drop(map);
                self.followers.fetch_add(1, Ordering::Relaxed);
                return match running.wait() {
                    Ok(value) => Ok(SharedRead {
                        value,
                        epoch: running.epoch,
                        leader: false,
                    }),
                    Err(e) => Err(replicate_error(&e)),
                };
            }
            // No flight, or one that may predate a write this caller saw:
            // start a new one (the old one finishes for its own followers).
            let flight = Arc::new(Flight {
                epoch,
                leader: me,
                outcome: Mutex::new(None),
                done: Condvar::new(),
            });
            map.insert(key.clone(), flight.clone());
            flight
        };
        self.leaders.fetch_add(1, Ordering::Relaxed);
        let mut guard = LeaderGuard {
            stripe,
            key: Some(key),
            flight: &flight,
        };
        let result = load();
        guard.retire();
        match result {
            Ok(v) => {
                let value = Arc::new(v);
                flight.finish(Ok(value.clone()));
                Ok(SharedRead {
                    value,
                    epoch,
                    leader: true,
                })
            }
            Err(e) => {
                flight.finish(Err(Arc::new(replicate_error(&e))));
                Err(e)
            }
        }
    }

    /// (reads performed, reads that joined another caller's read).
    pub fn counts(&self) -> (u64, u64) {
        (
            self.leaders.load(Ordering::Relaxed),
            self.followers.load(Ordering::Relaxed),
        )
    }
}

/// Unpublishes the flight when the leader finishes; if `load` panics, also
/// fails the followers so none of them hangs.
struct LeaderGuard<'a, K: Hash + Eq, V> {
    stripe: &'a FlightMap<K, V>,
    key: Option<K>,
    flight: &'a Arc<Flight<V>>,
}

impl<K: Hash + Eq, V> LeaderGuard<'_, K, V> {
    fn retire(&mut self) {
        if let Some(key) = self.key.take() {
            let mut map = lock(self.stripe);
            if map.get(&key).is_some_and(|f| Arc::ptr_eq(f, self.flight)) {
                map.remove(&key);
            }
        }
    }
}

impl<K: Hash + Eq, V> Drop for LeaderGuard<'_, K, V> {
    fn drop(&mut self) {
        if self.key.is_some() {
            self.retire();
            self.flight.finish(Err(Arc::new(Error::Backend(
                "singleflight: the leading read panicked".into(),
            ))));
        }
    }
}

// ---------------------------------------------------------------------------
// Coalescing reader
// ---------------------------------------------------------------------------

/// Identity of a coalesced prefix scan.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScanKey {
    pub prefix: Vec<u8>,
    pub limit: usize,
    pub reverse: bool,
}

/// Counters of a [`Coalescer`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CoalesceStats {
    /// Reads actually sent to the source.
    pub source_reads: u64,
    /// Reads served by joining another caller's read.
    pub joined: u64,
}

/// Single-flight front end of a [`ReadSource`] for point reads and prefix
/// scans. Write epochs are partitioned with `router` (use the same router as
/// the data, e.g. `FirstBytes(8)` for channel-prefixed keys).
pub struct Coalescer<R> {
    source: R,
    router: Router,
    epochs: WriteEpochs,
    gets: SingleFlight<Vec<u8>, Option<Vec<u8>>>,
    scans: SingleFlight<ScanKey, Vec<ScanItem>>,
}

impl<R: ReadSource> Coalescer<R> {
    pub fn new(source: R, router: Router) -> Self {
        Coalescer {
            source,
            router,
            epochs: WriteEpochs::default(),
            gets: SingleFlight::new(),
            scans: SingleFlight::new(),
        }
    }

    pub fn source(&self) -> &R {
        &self.source
    }

    /// Coalesced point read.
    pub fn get(&self, key: &[u8]) -> Result<Arc<Option<Vec<u8>>>> {
        let epoch = self
            .epochs
            .current(EpochScope::Partition(self.router.key_hash(key)));
        Ok(self
            .gets
            .run(key.to_vec(), epoch, || self.source.get(key))?
            .value)
    }

    /// Coalesced prefix scan with values (e.g. `reverse` + `limit` = latest N).
    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<Arc<Vec<ScanItem>>> {
        let scope = self
            .router
            .prefix_hash(prefix)
            .map_or(EpochScope::Global, EpochScope::Partition);
        let epoch = self.epochs.current(scope);
        let key = ScanKey {
            prefix: prefix.to_vec(),
            limit,
            reverse,
        };
        let opts = ScanOptions::prefix(prefix)
            .reverse(reverse)
            .limit(limit)
            .with_values(true);
        Ok(self
            .scans
            .run(key, epoch, || self.source.scan(&opts))?
            .value)
    }

    /// Report a write of `key` (after it is visible, before acknowledging it)
    /// so later reads never join a read that could miss it.
    pub fn note_write(&self, key: &[u8]) {
        self.epochs.bump(self.router.key_hash(key));
    }

    pub fn stats(&self) -> CoalesceStats {
        let (gl, gf) = self.gets.counts();
        let (sl, sf) = self.scans.counts();
        CoalesceStats {
            source_reads: gl + sl,
            joined: gf + sf,
        }
    }
}

// ---------------------------------------------------------------------------
// Recent-page cache
// ---------------------------------------------------------------------------

/// One cached message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedMessage {
    pub id: u64,
    pub revision: Revision,
    pub payload: Vec<u8>,
}

/// Counters and gauges of a [`RecentCache`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecentCacheStats {
    pub capacity_bytes: usize,
    pub used_bytes: usize,
    pub pages: usize,
    pub hits: u64,
    pub misses: u64,
    pub installs: u64,
    /// Installs refused because a write raced with the read.
    pub rejected_installs: u64,
    pub evictions: u64,
    pub invalidations: u64,
}

const ENTRY_OVERHEAD: usize = 48;
const PAGE_OVERHEAD: usize = 128;
const CACHE_STRIPES: usize = 16;

enum Change {
    Applied,
    Ignored,
    /// The page cannot be kept consistent: drop it.
    Invalidate,
}

/// Newest messages of one channel, id-descending. Invariant: `msgs` holds the
/// newest `msgs.len()` live messages; `complete` = no older message exists.
/// `msgs` is a copy-on-write snapshot: a hit clones the `Arc` under the stripe
/// lock and copies payloads after releasing it, so readers of a hot channel
/// do not serialize on the copy; writers use `Arc::make_mut`.
struct Page {
    msgs: Arc<Vec<CachedMessage>>,
    complete: bool,
    bytes: usize,
    tick: u64,
}

fn entry_bytes(m: &CachedMessage) -> usize {
    m.payload.len() + ENTRY_OVERHEAD
}

impl Page {
    fn position(&self, id: u64) -> std::result::Result<usize, usize> {
        self.msgs.binary_search_by(|probe| id.cmp(&probe.id))
    }

    /// Whether a message with this id belongs inside the cached window.
    fn in_window(&self, id: u64) -> bool {
        self.complete || self.msgs.last().is_some_and(|oldest| id > oldest.id)
    }

    fn insert(&mut self, id: u64, revision: Revision, payload: &[u8], page_len: usize) -> Change {
        match self.position(id) {
            Ok(i) => self.replace(i, revision, payload),
            Err(i) => {
                if !self.in_window(id) {
                    return Change::Ignored;
                }
                let m = CachedMessage {
                    id,
                    revision,
                    payload: payload.to_vec(),
                };
                self.bytes += entry_bytes(&m);
                let msgs = Arc::make_mut(&mut self.msgs);
                msgs.insert(i, m);
                while msgs.len() > page_len {
                    if let Some(old) = msgs.pop() {
                        self.bytes -= entry_bytes(&old);
                    }
                    self.complete = false;
                }
                Change::Applied
            }
        }
    }

    fn replace(&mut self, i: usize, revision: Revision, payload: &[u8]) -> Change {
        if revision <= self.msgs[i].revision {
            return Change::Ignored;
        }
        let m = &mut Arc::make_mut(&mut self.msgs)[i];
        self.bytes = self.bytes - m.payload.len() + payload.len();
        m.payload = payload.to_vec();
        m.revision = revision;
        Change::Applied
    }

    /// An update of a message missing from its window means an insert whose
    /// cache update is still in flight: invalidate rather than guess.
    fn update(&mut self, id: u64, revision: Revision, payload: &[u8]) -> Change {
        match self.position(id) {
            Ok(i) => self.replace(i, revision, payload),
            Err(_) if self.in_window(id) => Change::Invalidate,
            Err(_) => Change::Ignored,
        }
    }

    /// `revision` is the revision of the record the delete removed (what
    /// `Db::write_batch_each` reports for a delete), so a cached entry at that
    /// revision or older is the deleted one; a newer cached entry wins.
    fn delete(&mut self, id: u64, revision: Revision) -> Change {
        match self.position(id) {
            Ok(i) if self.msgs[i].revision <= revision => {
                let old = Arc::make_mut(&mut self.msgs).remove(i);
                self.bytes -= entry_bytes(&old);
                Change::Applied
            }
            Ok(_) => Change::Ignored,
            Err(_) if self.in_window(id) => Change::Invalidate,
            Err(_) => Change::Ignored,
        }
    }
}

#[derive(Default)]
struct CacheStripe {
    pages: HashMap<u64, Page>,
    lru: BTreeMap<u64, u64>,
    tick: u64,
    bytes: usize,
    /// Writes in flight per channel (between `begin_write` and its outcome).
    pending: HashMap<u64, u32>,
}

impl CacheStripe {
    fn touch(&mut self, channel: u64) {
        self.tick += 1;
        let tick = self.tick;
        if let Some(page) = self.pages.get_mut(&channel) {
            self.lru.remove(&page.tick);
            page.tick = tick;
            self.lru.insert(tick, channel);
        }
    }

    fn remove(&mut self, channel: u64) -> bool {
        match self.pages.remove(&channel) {
            Some(page) => {
                self.lru.remove(&page.tick);
                self.bytes -= page.bytes;
                true
            }
            None => false,
        }
    }

    fn evict_to(&mut self, budget: usize) -> u64 {
        let mut evicted = 0;
        while self.bytes > budget {
            let Some((_, channel)) = self.lru.pop_first() else {
                break;
            };
            if let Some(page) = self.pages.remove(&channel) {
                self.bytes -= page.bytes;
                evicted += 1;
            }
        }
        evicted
    }
}

#[derive(Default)]
struct CacheCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    installs: AtomicU64,
    rejected: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
}

/// Byte-bounded, write-through cache of the newest page of each channel. With
/// `capacity_bytes == 0` it keeps no pages but still provides the channel
/// write epochs used for coalescing.
pub struct RecentCache {
    stripes: Box<[Mutex<CacheStripe>]>,
    epochs: WriteEpochs,
    page_len: usize,
    capacity: usize,
    stripe_budget: usize,
    counters: CacheCounters,
}

/// A write in progress on one channel, from before it is submitted until its
/// outcome is applied to the cache. Dropping it without an outcome (error,
/// panic) conservatively invalidates the channel's page.
pub struct ChannelWrite<'a> {
    cache: &'a RecentCache,
    channel: u64,
    done: bool,
}

impl ChannelWrite<'_> {
    pub fn inserted(mut self, id: u64, revision: Revision, payload: &[u8]) {
        self.finish(|p, len| p.insert(id, revision, payload, len));
    }

    pub fn updated(mut self, id: u64, revision: Revision, payload: &[u8]) {
        self.finish(|p, _| p.update(id, revision, payload));
    }

    pub fn deleted(mut self, id: u64, revision: Revision) {
        self.finish(|p, _| p.delete(id, revision));
    }

    /// Nothing was written.
    pub fn unchanged(mut self) {
        self.finish(|_, _| Change::Ignored);
    }

    fn finish(&mut self, f: impl FnOnce(&mut Page, usize) -> Change) {
        self.done = true;
        self.cache.finish_write(self.channel, f);
    }
}

impl Drop for ChannelWrite<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            self.cache
                .finish_write(self.channel, |_, _| Change::Invalidate);
        }
    }
}

impl RecentCache {
    /// Keep up to `page_len` newest messages per channel within
    /// `capacity_bytes` (payload + per-entry and per-page overhead estimates).
    pub fn new(capacity_bytes: usize, page_len: usize) -> RecentCache {
        RecentCache {
            stripes: (0..CACHE_STRIPES)
                .map(|_| Mutex::new(CacheStripe::default()))
                .collect(),
            epochs: WriteEpochs::default(),
            page_len: page_len.max(1),
            capacity: capacity_bytes,
            stripe_budget: capacity_bytes / CACHE_STRIPES,
            counters: CacheCounters::default(),
        }
    }

    /// Whether pages are retained at all.
    pub fn enabled(&self) -> bool {
        self.stripe_budget > 0
    }

    pub fn page_len(&self) -> usize {
        self.page_len
    }

    fn stripe(&self, channel: u64) -> &Mutex<CacheStripe> {
        &self.stripes[(mix64(channel) >> 60) as usize % CACHE_STRIPES]
    }

    /// Write epoch of a channel (read it before reading the database).
    pub fn epoch(&self, channel: u64) -> u64 {
        self.epochs.current(EpochScope::Partition(channel))
    }

    /// Newest `n` messages if the cached page can answer (`n` within the page,
    /// or the page holds the whole channel).
    pub fn get(&self, channel: u64, n: usize) -> Option<Vec<(u64, Vec<u8>)>> {
        if !self.enabled() {
            return None;
        }
        let snapshot = {
            let mut st = lock(self.stripe(channel));
            let snapshot = st
                .pages
                .get(&channel)
                .filter(|p| p.msgs.len() >= n || p.complete)
                .map(|p| p.msgs.clone());
            if snapshot.is_some() {
                st.touch(channel);
            }
            snapshot
        };
        match snapshot {
            Some(msgs) => {
                self.counters.hits.fetch_add(1, Ordering::Relaxed);
                Some(
                    msgs.iter()
                        .take(n)
                        .map(|m| (m.id, m.payload.clone()))
                        .collect(),
                )
            }
            None => {
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Install a page read from the database after `epoch` was observed.
    /// Refused if a write to the channel is in flight or completed since.
    pub fn install(
        &self,
        channel: u64,
        epoch: u64,
        msgs: &[CachedMessage],
        complete: bool,
    ) -> bool {
        if !self.enabled() {
            return false;
        }
        let page_msgs: Vec<CachedMessage> = msgs.iter().take(self.page_len).cloned().collect();
        let complete = complete && page_msgs.len() == msgs.len();
        let bytes = PAGE_OVERHEAD + page_msgs.iter().map(entry_bytes).sum::<usize>();
        if bytes > self.stripe_budget {
            return false;
        }
        let mut st = lock(self.stripe(channel));
        if st.pending.contains_key(&channel) || self.epoch(channel) != epoch {
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        st.remove(channel);
        st.pages.insert(
            channel,
            Page {
                msgs: Arc::new(page_msgs),
                complete,
                bytes,
                tick: 0,
            },
        );
        st.bytes += bytes;
        st.touch(channel);
        let evicted = st.evict_to(self.stripe_budget);
        drop(st);
        self.counters.installs.fetch_add(1, Ordering::Relaxed);
        self.counters
            .evictions
            .fetch_add(evicted, Ordering::Relaxed);
        true
    }

    /// Announce a write to `channel`; report its outcome on the handle.
    pub fn begin_write(&self, channel: u64) -> ChannelWrite<'_> {
        if self.enabled() {
            *lock(self.stripe(channel))
                .pending
                .entry(channel)
                .or_insert(0) += 1;
        }
        ChannelWrite {
            cache: self,
            channel,
            done: false,
        }
    }

    fn finish_write(&self, channel: u64, f: impl FnOnce(&mut Page, usize) -> Change) {
        if !self.enabled() {
            self.epochs.bump(channel);
            return;
        }
        let mut guard = lock(self.stripe(channel));
        let st = &mut *guard;
        let change = match st.pages.get_mut(&channel) {
            Some(page) => {
                let before = page.bytes;
                let change = f(page, self.page_len);
                st.bytes = st.bytes - before + page.bytes;
                change
            }
            None => Change::Ignored,
        };
        let mut invalidated = false;
        match change {
            Change::Invalidate => invalidated = st.remove(channel),
            Change::Applied => st.touch(channel),
            Change::Ignored => {}
        }
        // Bump while holding the stripe lock: an install checks the epoch
        // under the same lock.
        self.epochs.bump(channel);
        if let Some(n) = st.pending.get_mut(&channel) {
            *n -= 1;
            if *n == 0 {
                st.pending.remove(&channel);
            }
        }
        let evicted = st.evict_to(self.stripe_budget);
        drop(guard);
        if invalidated {
            self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        }
        self.counters
            .evictions
            .fetch_add(evicted, Ordering::Relaxed);
    }

    /// Drop a channel's page (it is refilled by the next read).
    pub fn invalidate(&self, channel: u64) {
        if self.enabled() && lock(self.stripe(channel)).remove(channel) {
            self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn stats(&self) -> RecentCacheStats {
        let (mut used, mut pages) = (0, 0);
        for s in self.stripes.iter() {
            let st = lock(s);
            used += st.bytes;
            pages += st.pages.len();
        }
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        RecentCacheStats {
            capacity_bytes: self.capacity,
            used_bytes: used,
            pages,
            hits: l(&self.counters.hits),
            misses: l(&self.counters.misses),
            installs: l(&self.counters.installs),
            rejected_installs: l(&self.counters.rejected),
            evictions: l(&self.counters.evictions),
            invalidations: l(&self.counters.invalidations),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: u64, rev: Revision, p: &str) -> CachedMessage {
        CachedMessage {
            id,
            revision: rev,
            payload: p.as_bytes().to_vec(),
        }
    }

    fn ids(v: &[(u64, Vec<u8>)]) -> Vec<u64> {
        v.iter().map(|(id, _)| *id).collect()
    }

    #[test]
    fn page_write_through_and_trim() {
        let c = RecentCache::new(1 << 20, 3);
        let e = c.epoch(7);
        assert!(c.install(
            7,
            e,
            &[msg(30, 3, "c"), msg(20, 2, "b"), msg(10, 1, "a")],
            true
        ));
        assert_eq!(ids(&c.get(7, 10).unwrap()), [30, 20, 10]);
        c.begin_write(7).inserted(40, 4, b"d");
        // Trimmed to 3: no longer complete.
        assert_eq!(ids(&c.get(7, 3).unwrap()), [40, 30, 20]);
        assert!(c.get(7, 4).is_none());
        c.begin_write(7).updated(30, 5, b"C");
        assert_eq!(c.get(7, 2).unwrap()[1], (30, b"C".to_vec()));
        // Older revision loses.
        c.begin_write(7).updated(30, 4, b"old");
        assert_eq!(c.get(7, 2).unwrap()[1].1, b"C".to_vec());
        c.begin_write(7).deleted(40, 6);
        assert_eq!(ids(&c.get(7, 2).unwrap()), [30, 20]);
    }

    #[test]
    fn install_rejected_after_racing_write() {
        let c = RecentCache::new(1 << 20, 10);
        let e = c.epoch(1);
        c.begin_write(1).inserted(5, 1, b"x");
        assert!(!c.install(1, e, &[], true));
        let pending = c.begin_write(1);
        let e2 = c.epoch(1);
        assert!(
            !c.install(1, e2, &[], true),
            "a write in flight blocks installs"
        );
        pending.unchanged();
        let e3 = c.epoch(1);
        assert!(c.install(1, e3, &[msg(5, 1, "x")], true));
        assert_eq!(c.stats().rejected_installs, 2);
    }

    #[test]
    fn missing_update_in_window_invalidates() {
        let c = RecentCache::new(1 << 20, 10);
        let e = c.epoch(2);
        assert!(c.install(2, e, &[msg(9, 1, "a")], false));
        c.begin_write(2).updated(12, 3, b"z");
        assert!(c.get(2, 1).is_none());
        assert_eq!(c.stats().invalidations, 1);
        // A failed write (handle dropped) also invalidates.
        let e = c.epoch(2);
        assert!(c.install(2, e, &[msg(9, 1, "a")], false));
        drop(c.begin_write(2));
        assert!(c.get(2, 1).is_none());
    }

    #[test]
    fn byte_budget_evicts_lru() {
        let c = RecentCache::new(16 * 1024, 4);
        let payload = "p".repeat(200);
        let mut installed = 0;
        for ch in 0..2000u64 {
            let e = c.epoch(ch);
            if c.install(ch, e, &[msg(1, 1, &payload)], true) {
                installed += 1;
            }
        }
        let s = c.stats();
        assert_eq!(installed, 2000);
        assert!(s.used_bytes <= 16 * 1024, "{s:?}");
        assert!(s.evictions > 0 && s.pages < 2000);
    }

    #[test]
    fn disabled_cache_still_tracks_epochs() {
        let c = RecentCache::new(0, 10);
        assert!(!c.enabled());
        let e = c.epoch(3);
        c.begin_write(3).inserted(1, 1, b"x");
        assert!(c.epoch(3) > e);
        assert!(!c.install(3, c.epoch(3), &[], true));
        assert!(c.get(3, 1).is_none());
    }

    #[test]
    fn singleflight_reentrant_is_an_error() {
        let sf: SingleFlight<u32, u32> = SingleFlight::new();
        let r = sf.run(1, 0, || match sf.run(1, 0, || Ok(2)) {
            Err(Error::InvalidArgument(_)) => Ok(1),
            other => panic!("unexpected {other:?}"),
        });
        assert_eq!(*r.unwrap().value, 1);
    }
}
