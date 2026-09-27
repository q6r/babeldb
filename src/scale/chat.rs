//! Discord-like chat schema on a [`ShardedDb`].
//!
//! - Key: `channel_id` (u64 BE) ‖ `message_id` (u64 BE), 16 bytes. Keys sort
//!   by channel, then by id; snowflake ids grow with time, so "latest N
//!   messages" is a reverse prefix scan with limit N, and pagination is the
//!   same scan bounded by `key(channel, before_id)`.
//! - Routing: `Router::FirstBytes(8)` (the channel id): a channel lives in one
//!   shard, so its scans never fan out and its writes share one committer.
//! - Value: the message payload, opaque bytes stored and returned exactly.
//! - Ids: [`Snowflake`] (42-bit ms since 2015-01-01 ‖ 5-bit worker ‖ 5-bit
//!   process ‖ 12-bit increment), the layout Discord documents.
//!
//! Reads of the newest page are coalesced ([`SingleFlight`]) and optionally
//! served from a write-through [`RecentCache`]; both keep read-your-writes for
//! writes made through the same `ChatStore` (see `coalesce`).

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::OwnedOp;
use super::coalesce::{CachedMessage, RecentCache, RecentCacheStats, SharedRead, SingleFlight};
use super::group_commit::{GroupCommitConfig, GroupCommitStats};
use super::sharded::{Router, ShardBackend, ShardedDb};
use crate::config::Config;
use crate::engine::{Db, Expect, ScanOptions};
use crate::error::{Error, Result};
use crate::store::redb::RedbStore;

/// Discord epoch: 2015-01-01T00:00:00Z in Unix milliseconds.
pub const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;

const TIMESTAMP_BITS: u32 = 42;
const INCREMENT_BITS: u32 = 12;
const PROCESS_SHIFT: u32 = 12;
const WORKER_SHIFT: u32 = 17;
const TIMESTAMP_SHIFT: u32 = 22;
const INCREMENT_MASK: u64 = (1 << INCREMENT_BITS) - 1;

type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

fn system_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Snowflake id generator, lock-free and strictly monotonic within a process.
///
/// The state is the last issued `(ms since epoch << 12) | increment`; the next
/// value is `max(last + 1, now << 12)`. Hence:
/// - if the clock goes backwards, ids keep increasing: they reuse the last
///   millisecond and move on with the increment (embedded timestamps then
///   stay at the last issued value until the clock catches up);
/// - more than 4096 ids in one millisecond carry into the next millisecond
///   instead of blocking (the embedded timestamp runs ahead of the wall clock
///   by at most the burst length / 4096 ms, and catches up when idle).
///
/// Ids are unique across processes only if each concurrently running
/// generator has its own (worker, process) pair.
pub struct Snowflake {
    worker: u64,
    process: u64,
    last: AtomicU64,
    clock: Clock,
}

impl std::fmt::Debug for Snowflake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snowflake")
            .field("worker", &self.worker)
            .field("process", &self.process)
            .finish()
    }
}

impl Snowflake {
    /// `worker_id` and `process_id` must be < 32.
    pub fn new(worker_id: u8, process_id: u8) -> Result<Snowflake> {
        Self::with_clock(worker_id, process_id, system_clock_ms)
    }

    /// Generator with an injected clock returning Unix milliseconds (tests).
    pub fn with_clock(
        worker_id: u8,
        process_id: u8,
        clock: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Result<Snowflake> {
        if worker_id >= 32 || process_id >= 32 {
            return Err(Error::InvalidArgument(
                "snowflake: worker and process ids must be < 32".into(),
            ));
        }
        Ok(Snowflake {
            worker: u64::from(worker_id),
            process: u64::from(process_id),
            last: AtomicU64::new(0),
            clock: Arc::new(clock),
        })
    }

    pub fn next_id(&self) -> Result<u64> {
        let since_epoch = (self.clock)().saturating_sub(DISCORD_EPOCH_MS);
        if since_epoch >= 1 << TIMESTAMP_BITS {
            return Err(Error::IdExhausted("snowflake timestamp"));
        }
        let floor = since_epoch << INCREMENT_BITS;
        let mut cur = self.last.load(Ordering::Relaxed);
        let next = loop {
            let next = cur.saturating_add(1).max(floor);
            if next >> INCREMENT_BITS >= 1 << TIMESTAMP_BITS {
                return Err(Error::IdExhausted("snowflake timestamp"));
            }
            match self
                .last
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break next,
                Err(actual) => cur = actual,
            }
        };
        let ms = next >> INCREMENT_BITS;
        Ok((ms << TIMESTAMP_SHIFT)
            | (self.worker << WORKER_SHIFT)
            | (self.process << PROCESS_SHIFT)
            | (next & INCREMENT_MASK))
    }
}

/// Fields of a snowflake id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnowflakeParts {
    /// Unix milliseconds.
    pub timestamp_ms: u64,
    pub worker_id: u8,
    pub process_id: u8,
    pub increment: u16,
}

impl SnowflakeParts {
    pub fn decode(id: u64) -> SnowflakeParts {
        SnowflakeParts {
            timestamp_ms: (id >> TIMESTAMP_SHIFT) + DISCORD_EPOCH_MS,
            worker_id: ((id >> WORKER_SHIFT) & 0x1F) as u8,
            process_id: ((id >> PROCESS_SHIFT) & 0x1F) as u8,
            increment: (id & INCREMENT_MASK) as u16,
        }
    }

    pub fn encode(&self) -> Result<u64> {
        let ms = self
            .timestamp_ms
            .checked_sub(DISCORD_EPOCH_MS)
            .filter(|ms| *ms < 1 << TIMESTAMP_BITS)
            .ok_or_else(|| {
                Error::InvalidArgument("snowflake: timestamp outside the 42-bit range".into())
            })?;
        if self.worker_id >= 32
            || self.process_id >= 32
            || u64::from(self.increment) > INCREMENT_MASK
        {
            return Err(Error::InvalidArgument(
                "snowflake: field out of range".into(),
            ));
        }
        Ok((ms << TIMESTAMP_SHIFT)
            | (u64::from(self.worker_id) << WORKER_SHIFT)
            | (u64::from(self.process_id) << PROCESS_SHIFT)
            | u64::from(self.increment))
    }
}

/// `channel_id BE ‖ message_id BE`.
pub fn message_key(channel_id: u64, message_id: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&channel_id.to_be_bytes());
    k[8..].copy_from_slice(&message_id.to_be_bytes());
    k
}

/// Inverse of [`message_key`].
pub fn parse_message_key(key: &[u8]) -> Option<(u64, u64)> {
    let key: &[u8; 16] = key.try_into().ok()?;
    let (c, m) = key.split_at(8);
    Some((
        u64::from_be_bytes(c.try_into().ok()?),
        u64::from_be_bytes(m.try_into().ok()?),
    ))
}

/// Key prefix of every message of a channel.
pub fn channel_prefix(channel_id: u64) -> [u8; 8] {
    channel_id.to_be_bytes()
}

/// Options of a [`ChatStore`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatOptions {
    /// Snowflake worker id (< 32).
    pub worker_id: u8,
    /// Snowflake process id (< 32).
    pub process_id: u8,
    /// Coalesce concurrent identical `latest` reads.
    pub coalesce_reads: bool,
    /// Byte budget of the recent-page cache (0 disables it).
    pub recent_cache_bytes: usize,
    /// Messages kept per cached channel; `latest(n)` with `n` above it
    /// bypasses the cache.
    pub recent_page_len: usize,
}

impl Default for ChatOptions {
    fn default() -> Self {
        ChatOptions {
            worker_id: 0,
            process_id: 0,
            coalesce_reads: true,
            recent_cache_bytes: 32 << 20,
            recent_page_len: 100,
        }
    }
}

/// Counters of a [`ChatStore`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChatStats {
    /// Page scans sent to the database (`latest` misses and `before`).
    pub page_reads: u64,
    /// `latest` calls served by joining another caller's scan.
    pub coalesced_reads: u64,
    pub recent: RecentCacheStats,
    pub commit: Vec<GroupCommitStats>,
}

const EDIT_RETRIES: usize = 16;

type Page = Vec<CachedMessage>;

/// Chat messages on a sharded database (see the module docs). The database
/// should be dedicated to messages: every key under a channel prefix must be
/// a 16-byte message key.
pub struct ChatStore<B = Db<RedbStore>> {
    db: ShardedDb<B>,
    ids: Snowflake,
    opts: ChatOptions,
    flights: SingleFlight<(u64, usize), Page>,
    recent: RecentCache,
    page_reads: AtomicU64,
}

impl ChatStore<Db<RedbStore>> {
    /// Open or create a redb-backed store in `dir` routed by channel.
    pub fn open(
        dir: impl AsRef<Path>,
        shards: usize,
        cfg: Config,
        commit: impl Into<GroupCommitConfig>,
        opts: ChatOptions,
    ) -> Result<Self> {
        let db = ShardedDb::open(dir, shards, cfg, Router::FirstBytes(8), commit)?;
        ChatStore::new(db, opts)
    }
}

impl<B: ShardBackend + 'static> ChatStore<B> {
    pub fn new(db: ShardedDb<B>, opts: ChatOptions) -> Result<Self> {
        let ids = Snowflake::new(opts.worker_id, opts.process_id)?;
        Self::with_ids(db, opts, ids)
    }

    /// Use a specific id generator (e.g. with an injected clock).
    pub fn with_ids(db: ShardedDb<B>, opts: ChatOptions, ids: Snowflake) -> Result<Self> {
        let recent = RecentCache::new(opts.recent_cache_bytes, opts.recent_page_len);
        Ok(ChatStore {
            db,
            ids,
            opts,
            flights: SingleFlight::new(),
            recent,
            page_reads: AtomicU64::new(0),
        })
    }

    pub fn db(&self) -> &ShardedDb<B> {
        &self.db
    }

    pub fn options(&self) -> &ChatOptions {
        &self.opts
    }

    /// Store a new message; returns its snowflake id.
    pub fn send(&self, channel: u64, payload: &[u8]) -> Result<u64> {
        let id = self.ids.next_id()?;
        self.insert(channel, id, payload)?;
        Ok(id)
    }

    /// Store a message with a caller-chosen id (imports). Fails with
    /// `RevisionConflict` if the id already exists in the channel.
    pub fn insert(&self, channel: u64, message_id: u64, payload: &[u8]) -> Result<()> {
        let key = message_key(channel, message_id);
        let write = self.recent.begin_write(channel);
        let revision = self.db.put(&key, payload, Expect::Absent)?;
        write.inserted(message_id, revision, payload);
        Ok(())
    }

    /// Newest `n` messages, newest first.
    pub fn latest(&self, channel: u64, n: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let cached = self.recent.enabled() && n <= self.recent.page_len();
        if cached && let Some(hit) = self.recent.get(channel, n) {
            return Ok(hit);
        }
        let limit = if cached { self.recent.page_len() } else { n };
        // Read the epoch before the database: see `coalesce`.
        let epoch = self.recent.epoch(channel);
        let page = if self.opts.coalesce_reads {
            self.flights.run((channel, limit), epoch, || {
                self.read_page(channel, limit, None)
            })?
        } else {
            SharedRead {
                value: Arc::new(self.read_page(channel, limit, None)?),
                epoch,
                leader: true,
            }
        };
        if cached && page.leader {
            self.recent
                .install(channel, page.epoch, &page.value, page.value.len() < limit);
        }
        Ok(page
            .value
            .iter()
            .take(n)
            .map(|m| (m.id, m.payload.clone()))
            .collect())
    }

    /// Up to `n` messages older than `message_id`, newest first (pagination).
    pub fn before(&self, channel: u64, message_id: u64, n: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        Ok(self
            .read_page(channel, n, Some(message_id))?
            .into_iter()
            .map(|m| (m.id, m.payload))
            .collect())
    }

    /// One message.
    pub fn get(&self, channel: u64, message_id: u64) -> Result<Option<Vec<u8>>> {
        self.db.get(&message_key(channel, message_id))
    }

    /// Replace a message's payload. `Ok(false)` if it does not exist.
    /// Concurrent edits: last writer wins (conflicts are retried).
    pub fn edit(&self, channel: u64, message_id: u64, payload: &[u8]) -> Result<bool> {
        let key = message_key(channel, message_id);
        let mut last_conflict = None;
        for _ in 0..EDIT_RETRIES {
            let Some((current, _)) = self.db.head(&key)? else {
                return Ok(false);
            };
            let write = self.recent.begin_write(channel);
            match self.db.put(&key, payload, Expect::Revision(current)) {
                Ok(revision) => {
                    write.updated(message_id, revision, payload);
                    return Ok(true);
                }
                Err(e @ Error::RevisionConflict { .. }) => {
                    write.unchanged();
                    last_conflict = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_conflict.unwrap_or_else(|| Error::Backend("chat: edit retries exhausted".into())))
    }

    /// Delete a message; returns whether it existed.
    pub fn delete(&self, channel: u64, message_id: u64) -> Result<bool> {
        let key = message_key(channel, message_id);
        let write = self.recent.begin_write(channel);
        match self.db.apply_one(OwnedOp::delete(key, Expect::Any))? {
            Some(revision) => {
                write.deleted(message_id, revision);
                Ok(true)
            }
            None => {
                write.unchanged();
                Ok(false)
            }
        }
    }

    /// Make every acknowledged message durable (buffered mode).
    pub fn flush(&self) -> Result<()> {
        self.db.flush()
    }

    /// Drain and stop the committers.
    pub fn shutdown(&self) -> Result<()> {
        self.db.shutdown()
    }

    pub fn stats(&self) -> ChatStats {
        ChatStats {
            page_reads: self.page_reads.load(Ordering::Relaxed),
            coalesced_reads: self.flights.counts().1,
            recent: self.recent.stats(),
            commit: self.db.commit_stats(),
        }
    }

    /// One reverse scan of the channel (routed to its shard).
    fn read_page(&self, channel: u64, limit: usize, before: Option<u64>) -> Result<Page> {
        self.page_reads.fetch_add(1, Ordering::Relaxed);
        let mut opts = ScanOptions::prefix(&channel_prefix(channel))
            .reverse(true)
            .limit(limit)
            .with_values(true);
        if let Some(id) = before {
            opts.end = Bound::Excluded(message_key(channel, id).to_vec());
        }
        self.db
            .scan(&opts)?
            .into_iter()
            .map(|item| {
                let (ch, id) = parse_message_key(&item.key)
                    .ok_or_else(|| Error::Format("chat: malformed message key".into()))?;
                if ch != channel {
                    return Err(Error::Format(
                        "chat: scan returned a key of another channel".into(),
                    ));
                }
                let payload = item
                    .value
                    .ok_or_else(|| Error::Format("chat: scan returned no value".into()))?;
                Ok(CachedMessage {
                    id,
                    revision: item.revision,
                    payload,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discord_documented_snowflake() {
        // Example from Discord's API reference.
        let p = SnowflakeParts::decode(175_928_847_299_117_063);
        assert_eq!(
            p,
            SnowflakeParts {
                timestamp_ms: 1_462_015_105_796,
                worker_id: 1,
                process_id: 0,
                increment: 7
            }
        );
        assert_eq!(p.encode().unwrap(), 175_928_847_299_117_063);
    }

    #[test]
    fn keys_roundtrip_and_sort_by_channel_then_id() {
        let k = message_key(7, 9);
        assert_eq!(parse_message_key(&k), Some((7, 9)));
        assert_eq!(parse_message_key(&k[..15]), None);
        assert!(message_key(1, u64::MAX) < message_key(2, 0));
        assert!(message_key(2, 5) < message_key(2, 6));
        assert!(k.starts_with(&channel_prefix(7)));
    }

    #[test]
    fn snowflake_rejects_bad_ids_and_far_future() {
        assert!(Snowflake::new(32, 0).is_err());
        assert!(Snowflake::new(0, 32).is_err());
        let far = Snowflake::with_clock(0, 0, || DISCORD_EPOCH_MS + (1 << 42)).unwrap();
        assert!(matches!(far.next_id(), Err(Error::IdExhausted(_))));
    }
}
