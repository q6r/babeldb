//! Bounded cache of verified, decoded blocks keyed by immutable object id.
//! Its byte budget counts toward the RAM budget and is reported in stats.
//!
//! # Design
//!
//! - **Sharded.** The cache is split into `N` shards (a power of two; see
//!   [`BlockCache::new`]) selected by a mixed hash of the object id. Each shard
//!   owns `capacity / N` bytes and is protected by its own `RwLock`.
//! - **CLOCK eviction, lock-shared hits.** Each shard runs CLOCK, implemented
//!   as a second-chance FIFO over a slab-backed doubly linked list. A hit only
//!   sets the entry's atomic reference bit, which needs nothing more than the
//!   shard's *read* lock: concurrent readers of hot blocks never serialize on
//!   each other, and a hit allocates nothing (it returns an `Arc` clone).
//!   Inserts and removals take the write lock for O(1) amortized work: every
//!   step of the eviction loop either evicts an entry or clears a reference bit
//!   that an earlier hit set.
//! - **Byte accounting.** Each entry is charged `len + ENTRY_OVERHEAD_BYTES`.
//!   A shard never holds more than its budget, so `used_bytes <= capacity_bytes`
//!   at every instant, including in `stats()` snapshots taken while other
//!   threads are writing. A block whose charge exceeds a shard's budget is not
//!   cached ([`BlockCache::max_block_len`]). Capacity 0 disables caching.
//! - **Short critical sections.** Evicted, replaced and cleared blocks are
//!   dropped after the lock is released, so freeing a large buffer never
//!   blocks readers. The hit, miss, insertion and eviction counters live in
//!   each shard, on the shard's own cache line, instead of in one global
//!   contended atomic. `stats()` adds them up.
//!
//! The engine only caches blocks whose length and digest it has already
//! verified. Object ids are immutable and never reused, so an entry never needs
//! to be invalidated because its content changed. The only invalidation is
//! `remove` when an object is deleted.
//!
//! # Namespaces
//!
//! Two kinds of entries share the budget and the CLOCK of each shard, under
//! distinct keys (an object id and a revision with the same number never
//! collide):
//!
//! - **blocks** ([`get`](BlockCache::get), [`insert`](BlockCache::insert),
//!   [`remove`](BlockCache::remove)): decoded objects keyed by object id;
//! - **values** ([`get_value`](BlockCache::get_value),
//!   [`insert_value`](BlockCache::insert_value)): decoded and verified values
//!   of inline records, keyed by the revision of their manifest. A revision is
//!   allocated once per manifest and never reused, and a committed manifest is
//!   never rewritten, so the value of a revision never changes: these entries
//!   need no invalidation.

use std::collections::HashMap;
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub capacity_bytes: usize,
    /// Bytes currently held (payload + per-entry overhead estimate).
    pub used_bytes: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
}

/// Estimated bytes of memory held per cached entry beyond its payload. Every
/// entry is charged this amount on top of its length in `used_bytes`.
///
/// Rough breakdown on 64-bit targets: the slab node is 48 bytes, the index
/// entry is about 27 bytes (24-byte bucket plus control byte at a 7/8 load
/// factor), the `Arc` header is 16 bytes; allocator slack takes the rest.
/// Slab slots and index capacity left over after a burst of
/// evictions are not charged. They are bounded by the peak entry count and
/// are released by [`BlockCache::clear`].
pub const ENTRY_OVERHEAD_BYTES: usize = 96;

/// Upper bound on the number of shards chosen by [`BlockCache::new`].
const MAX_DEFAULT_SHARDS: usize = 64;
/// Upper bound on the number of shards accepted by [`BlockCache::with_shards`].
const MAX_SHARDS: usize = 1024;
/// [`BlockCache::new`] keeps at least this many bytes per shard. That way the
/// largest block the engine produces (`config::MAX_BLOCK_SIZE`, 1 MiB) always
/// fits, three times over, in any cache of 4 MiB or more.
const MIN_SHARD_BYTES: usize = 4 << 20;
/// Null link / "no node" marker of the per-shard linked list.
const NIL: u32 = u32::MAX;

pub struct BlockCache {
    capacity: usize,
    /// Byte budget of each shard (`capacity / shards.len()`).
    shard_budget: usize,
    /// `shards.len() - 1` (the shard count is a power of two).
    shard_mask: usize,
    /// False when no block can ever fit, for example when the capacity is 0.
    enabled: bool,
    shards: Box<[Shard]>,
}

// Hand-written so that `Debug` prints the configuration and skips the
// entries (and does not need `ShardInner: Debug`).
impl fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockCache")
            .field("capacity", &self.capacity)
            .field("shards", &self.shards.len())
            .field("shard_budget", &self.shard_budget)
            .finish_non_exhaustive()
    }
}

impl BlockCache {
    /// Cache holding at most `capacity_bytes`, including per-entry overhead.
    ///
    /// The shard count is `capacity / 4 MiB`, clamped to `1..=64` and rounded
    /// down to a power of two. The default 64 MiB cache therefore gets
    /// 16 shards of 4 MiB, and caches smaller than 8 MiB use a single shard,
    /// so every block up to the full capacity fits.
    pub fn new(capacity_bytes: usize) -> BlockCache {
        let by_size = (capacity_bytes / MIN_SHARD_BYTES).clamp(1, MAX_DEFAULT_SHARDS);
        BlockCache::with_shards(capacity_bytes, by_size)
    }

    /// Cache with an explicit shard count. The count is clamped to `1..=1024`
    /// and rounded down to a power of two. Each shard gets
    /// `capacity_bytes / shards` bytes.
    pub fn with_shards(capacity_bytes: usize, shards: usize) -> BlockCache {
        let n = floor_power_of_two(shards.clamp(1, MAX_SHARDS));
        let shard_budget = capacity_bytes / n;
        let enabled = shard_budget >= ENTRY_OVERHEAD_BYTES;
        // Admission window: about as many first offers as small values fit.
        let window = if enabled { (shard_budget / DOORKEEPER_VALUE_BYTES).max(DOORKEEPER_MIN_WINDOW) } else { 0 };
        BlockCache {
            capacity: capacity_bytes,
            shard_budget,
            shard_mask: n - 1,
            enabled,
            shards: (0..n).map(|_| Shard::new(window)).collect(),
        }
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Largest block length `insert` will cache: the shard budget minus
    /// [`ENTRY_OVERHEAD_BYTES`]. `None` when nothing can be cached (capacity 0
    /// or shards smaller than the per-entry overhead).
    pub fn max_block_len(&self) -> Option<usize> {
        if self.enabled {
            Some(self.shard_budget - ENTRY_OVERHEAD_BYTES)
        } else {
            None
        }
    }

    #[inline]
    fn shard(&self, key: Key) -> &Shard {
        // Bits 32.. of the mix pick the shard. The per-shard index uses the
        // low bits (bucket) and the top 7 bits (tag) of the same mix, so ids
        // sharing a shard still spread evenly inside it.
        let i = (mix64(key.mixed()) >> 32) as usize & self.shard_mask;
        &self.shards[i]
    }

    /// Decoded block of an object, if cached.
    pub fn get(&self, object_id: u64) -> Option<Arc<[u8]>> {
        self.lookup(Key::block(object_id))
    }

    /// Insert a verified block, replacing any block cached under the same id.
    /// Least recently useful entries of the same shard are evicted to make room.
    /// A block longer than [`max_block_len`](Self::max_block_len) is not cached.
    /// Any older block for the id is still dropped, so `get` never returns a
    /// superseded block.
    pub fn insert(&self, object_id: u64, bytes: Arc<[u8]>) {
        self.store(Key::block(object_id), bytes);
    }

    /// Drop the block cached under `object_id`, if any (not counted as an eviction).
    pub fn remove(&self, object_id: u64) {
        if !self.enabled {
            return;
        }
        let key = Key::block(object_id);
        let removed = self.shard(key).write().remove(key);
        drop(removed);
    }

    /// Verified value of the inline record whose manifest has `revision`, if cached.
    pub fn get_value(&self, revision: u64) -> Option<Arc<[u8]>> {
        self.lookup(Key::value(revision))
    }

    /// Offer the verified value of the inline record whose manifest has
    /// `revision`. It is stored (same budget, eviction and size limit as
    /// blocks) on its second offer while the shard's admission filter still
    /// remembers the first one: values read once, such as uniform reads over
    /// a dataset larger than the cache, cost no copy and evict nothing.
    /// Returns whether the value was stored.
    pub fn offer_value(&self, revision: u64, value: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        let key = Key::value(revision);
        if !self.shard(key).doorkeeper.admit(mix64(key.mixed())) {
            return false;
        }
        self.store(key, Arc::from(value))
    }

    /// Store the value of `revision` unconditionally (tests).
    #[cfg(test)]
    fn insert_value(&self, revision: u64, bytes: Arc<[u8]>) {
        self.store(Key::value(revision), bytes);
    }

    fn lookup(&self, key: Key) -> Option<Arc<[u8]>> {
        let shard = self.shard(key);
        if self.enabled {
            let inner = shard.read();
            if let Some(&idx) = inner.map.get(&key)
                && let Some(node) = inner.nodes.get(idx as usize)
                && let Some(value) = &node.value
            {
                // Test before setting: a hot entry's bit is almost always set
                // already, and a plain load keeps its cache line shared.
                if !node.referenced.load(Ordering::Relaxed) {
                    node.referenced.store(true, Ordering::Relaxed);
                }
                let value = Arc::clone(value);
                drop(inner);
                shard.hits.fetch_add(1, Ordering::Relaxed);
                return Some(value);
            }
        }
        shard.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Returns whether `bytes` was stored.
    fn store(&self, key: Key, bytes: Arc<[u8]>) -> bool {
        if !self.enabled {
            return false;
        }
        let shard = self.shard(key);
        let charge = bytes
            .len()
            .checked_add(ENTRY_OVERHEAD_BYTES)
            .filter(|&c| c <= self.shard_budget);
        // Declared before the guard: dropped (freed) after the lock is released.
        let mut dropped = Dropped::default();
        let mut inner = shard.write();
        if let Some(old) = inner.remove(key) {
            dropped.push(old);
        }
        let Some(charge) = charge else {
            return false;
        };
        let before = dropped.len();
        if !inner.make_room(charge, self.shard_budget, &mut dropped) {
            return false;
        }
        let evicted = (dropped.len() - before) as u64;
        let stored = match inner.link_new(key, bytes, charge) {
            Ok(()) => true,
            Err(rejected) => {
                dropped.push(rejected);
                false
            }
        };
        debug_assert!(inner.used <= self.shard_budget);
        debug_assert_eq!(inner.map.len() + inner.free.len(), inner.nodes.len());
        drop(inner);
        if evicted > 0 {
            shard.evictions.fetch_add(evicted, Ordering::Relaxed);
        }
        if stored {
            shard.insertions.fetch_add(1, Ordering::Relaxed);
        }
        stored
    }

    /// Drop every cached block and value and release the index memory (the
    /// admission filters forget their offers too). Activity counters (hits,
    /// misses, insertions, evictions) are cumulative and are kept.
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            // The guard is a temporary released at the end of this statement;
            // the taken entries are freed afterwards, outside the lock.
            let old = std::mem::take(&mut *shard.write());
            drop(old);
            shard.doorkeeper.clear();
        }
    }

    /// Aggregated snapshot. Each shard is read separately, so under concurrent
    /// writes the totals may mix slightly different instants. Even then
    /// `used_bytes <= capacity_bytes` holds, because every shard stays within
    /// its own budget.
    pub fn stats(&self) -> CacheStats {
        let mut s = CacheStats {
            capacity_bytes: self.capacity,
            ..CacheStats::default()
        };
        for shard in self.shards.iter() {
            {
                let inner = shard.read();
                s.used_bytes += inner.used;
                s.entries += inner.map.len();
            }
            s.hits += shard.hits.load(Ordering::Relaxed);
            s.misses += shard.misses.load(Ordering::Relaxed);
            s.insertions += shard.insertions.load(Ordering::Relaxed);
            s.evictions += shard.evictions.load(Ordering::Relaxed);
        }
        s
    }
}

/// One shard. Aligned to 128 bytes (two cache lines, which also covers
/// adjacent-line prefetch) so neighbouring shards never share a line. The
/// counters come first, next to the lock word that every access writes
/// anyway.
#[repr(C, align(128))]
struct Shard {
    hits: AtomicU64,
    misses: AtomicU64,
    insertions: AtomicU64,
    evictions: AtomicU64,
    inner: RwLock<ShardInner>,
    /// Admission filter of the value namespace.
    doorkeeper: Doorkeeper,
}

/// Bytes of a typical cached value: sets the admission window of a shard
/// (about as many first offers as such values fit in its budget).
const DOORKEEPER_VALUE_BYTES: usize = 1024;
const DOORKEEPER_MIN_WINDOW: usize = 256;
/// Filter bits per first offer of a window (one hash: ~12 % false positives
/// when the window is full).
const DOORKEEPER_BITS_PER_OFFER: usize = 8;

/// "Seen recently" filter: one bit per hashed key. A key is admitted when its
/// bit was already set; the bits are cleared after `window` first offers, so
/// only a second offer within about one window admits a key. Lock-free; its
/// few KiB per shard (`window * 8` bits) are not charged to the budget.
struct Doorkeeper {
    bits: Box<[AtomicU64]>,
    /// First offers since the bits were last cleared.
    fresh: AtomicU64,
    window: u64,
}

impl Doorkeeper {
    fn new(window: usize) -> Doorkeeper {
        let words = (window * DOORKEEPER_BITS_PER_OFFER).div_ceil(64).max(1).next_power_of_two();
        Doorkeeper {
            bits: (0..words).map(|_| AtomicU64::new(0)).collect(),
            fresh: AtomicU64::new(0),
            window: window as u64,
        }
    }

    /// Whether the key with hash `h` was offered recently; records the offer.
    fn admit(&self, h: u64) -> bool {
        let word = &self.bits[(h >> 6) as usize & (self.bits.len() - 1)];
        let bit = 1u64 << (h & 63);
        // A plain load first: keys offered again are the common repeat case.
        if word.load(Ordering::Relaxed) & bit != 0 || word.fetch_or(bit, Ordering::Relaxed) & bit != 0 {
            return true;
        }
        if self.fresh.fetch_add(1, Ordering::Relaxed) + 1 >= self.window {
            self.clear();
        }
        false
    }

    fn clear(&self) {
        self.fresh.store(0, Ordering::Relaxed);
        for word in self.bits.iter() {
            word.store(0, Ordering::Relaxed);
        }
    }
}

/// Entries to free after a shard lock is released. The first few need no
/// allocation (an insertion usually evicts one or two entries).
#[derive(Default)]
struct Dropped {
    inline: [Option<Arc<[u8]>>; 4],
    len: usize,
    more: Vec<Arc<[u8]>>,
}

impl Dropped {
    fn push(&mut self, entry: Arc<[u8]>) {
        match self.inline.get_mut(self.len) {
            Some(slot) => {
                *slot = Some(entry);
                self.len += 1;
            }
            None => self.more.push(entry),
        }
    }

    fn len(&self) -> usize {
        self.len + self.more.len()
    }
}

impl Shard {
    fn new(doorkeeper_window: usize) -> Shard {
        Shard {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            insertions: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            inner: RwLock::default(),
            doorkeeper: Doorkeeper::new(doorkeeper_window),
        }
    }

    // Critical sections never leave the shard half-updated in a way a later
    // access could misread, so a poisoned lock is simply taken over.
    #[inline]
    fn read(&self) -> RwLockReadGuard<'_, ShardInner> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn write(&self) -> RwLockWriteGuard<'_, ShardInner> {
        self.inner.write().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Namespace of a key (see the module documentation).
const SPACE_BLOCK: u8 = 0;
const SPACE_VALUE: u8 = 1;
/// Mixed into value keys before hashing, so blocks and values with the same
/// number land in unrelated buckets and shards.
const VALUE_SALT: u64 = 0xA076_1D64_78BD_642F;

/// Cache key: an object id (blocks) or a manifest revision (values). The
/// namespace is part of the key, so equal numbers in different namespaces are
/// different entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Key {
    id: u64,
    space: u8,
}

impl Key {
    #[inline]
    fn block(object_id: u64) -> Key {
        Key { id: object_id, space: SPACE_BLOCK }
    }

    #[inline]
    fn value(revision: u64) -> Key {
        Key { id: revision, space: SPACE_VALUE }
    }

    /// Hash input: the id, salted for values.
    #[inline]
    fn mixed(self) -> u64 {
        if self.space == SPACE_BLOCK { self.id } else { self.id ^ VALUE_SALT }
    }
}

impl Hash for Key {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.mixed());
    }
}

struct Node {
    id: Key,
    /// `None` only while the node is on the free list.
    value: Option<Arc<[u8]>>,
    /// Towards the head (newer).
    prev: u32,
    /// Towards the tail (older).
    next: u32,
    /// CLOCK reference bit, set by hits under the shared (read) lock.
    referenced: AtomicBool,
}

/// CLOCK as a second-chance FIFO: new entries enter at the head, the eviction
/// scan works from the tail. A tail entry whose bit is set gets its bit cleared
/// and moves back to the head; an unreferenced tail entry is evicted.
struct ShardInner {
    map: HashMap<Key, u32, IdHashBuilder>,
    nodes: Vec<Node>,
    free: Vec<u32>,
    /// Newest entry.
    head: u32,
    /// Oldest entry: next eviction candidate.
    tail: u32,
    /// Sum of the charges (`len + ENTRY_OVERHEAD_BYTES`) of live entries.
    used: usize,
}

impl Default for ShardInner {
    fn default() -> Self {
        ShardInner {
            map: HashMap::with_hasher(IdHashBuilder),
            nodes: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            used: 0,
        }
    }
}

impl ShardInner {
    fn unlink(&mut self, idx: u32) {
        let (prev, next) = {
            let n = &self.nodes[idx as usize];
            (n.prev, n.next)
        };
        if prev == NIL {
            self.head = next;
        } else {
            self.nodes[prev as usize].next = next;
        }
        if next == NIL {
            self.tail = prev;
        } else {
            self.nodes[next as usize].prev = prev;
        }
    }

    fn push_front(&mut self, idx: u32) {
        let old_head = self.head;
        {
            let n = &mut self.nodes[idx as usize];
            n.prev = NIL;
            n.next = old_head;
        }
        if old_head == NIL {
            self.tail = idx;
        } else {
            self.nodes[old_head as usize].prev = idx;
        }
        self.head = idx;
    }

    /// Unlink a live node, free its slot and return its block.
    fn detach(&mut self, idx: u32) -> Option<Arc<[u8]>> {
        self.unlink(idx);
        let node = &mut self.nodes[idx as usize];
        let value = node.value.take()?;
        self.map.remove(&node.id);
        self.used = self.used.saturating_sub(value.len() + ENTRY_OVERHEAD_BYTES);
        self.free.push(idx);
        Some(value)
    }

    fn remove(&mut self, id: Key) -> Option<Arc<[u8]>> {
        let idx = *self.map.get(&id)?;
        self.detach(idx)
    }

    /// Evict until `charge` more bytes fit in `budget`. Evicted blocks go to
    /// `evicted`, to be dropped outside the lock. Returns false only if the
    /// accounting is inconsistent, which the invariants rule out.
    fn make_room(&mut self, charge: usize, budget: usize, evicted: &mut Dropped) -> bool {
        while charge > budget.saturating_sub(self.used) {
            // `used > 0` here, so the list is not empty.
            let idx = self.tail;
            if idx == NIL {
                return false;
            }
            let referenced = self.nodes[idx as usize].referenced.get_mut();
            if *referenced {
                // Second chance: clear the bit, move to the head.
                *referenced = false;
                self.unlink(idx);
                self.push_front(idx);
            } else if let Some(entry) = self.detach(idx) {
                evicted.push(entry);
            }
        }
        true
    }

    /// Link a new, unreferenced entry at the head. The caller already made
    /// room. Gives the block back if the slab index space (u32) is exhausted.
    fn link_new(&mut self, id: Key, value: Arc<[u8]>, charge: usize) -> Result<(), Arc<[u8]>> {
        let reuse = self.free.pop();
        let idx = match reuse {
            Some(idx) => idx,
            None => match u32::try_from(self.nodes.len()) {
                Ok(idx) if idx != NIL => idx,
                _ => return Err(value),
            },
        };
        let node = Node {
            id,
            value: Some(value),
            prev: NIL,
            next: NIL,
            referenced: AtomicBool::new(false),
        };
        if reuse.is_some() {
            self.nodes[idx as usize] = node;
        } else {
            self.nodes.push(node);
        }
        self.push_front(idx);
        self.map.insert(id, idx);
        self.used += charge;
        Ok(())
    }
}

/// SplitMix64 finalizer: a bijective mix with full avalanche.
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Hasher for the per-shard index. Object ids are assigned by the engine, not
/// chosen by clients, so a fast unkeyed mix is enough and SipHash is not needed.
#[derive(Clone, Copy, Default)]
struct IdHashBuilder;

impl BuildHasher for IdHashBuilder {
    type Hasher = IdHasher;

    #[inline]
    fn build_hasher(&self) -> IdHasher {
        IdHasher(0)
    }
}

struct IdHasher(u64);

impl Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        mix64(self.0)
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.0 = self.0.rotate_left(29) ^ n;
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }
}

fn floor_power_of_two(n: usize) -> usize {
    debug_assert!(n > 0);
    1 << (usize::BITS - 1 - n.leading_zeros())
}

// Compile-time guarantee: the cache is shared across reader threads.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BlockCache>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhead_estimate_covers_structural_minimum() {
        // Slab node + index bucket (key, slot) + control byte + Arc counters.
        let minimum = size_of::<Node>() + size_of::<(Key, u32)>() + 1 + 2 * size_of::<usize>();
        assert!(
            ENTRY_OVERHEAD_BYTES >= minimum,
            "{ENTRY_OVERHEAD_BYTES} < {minimum}"
        );
    }

    #[test]
    fn blocks_and_values_are_separate_namespaces() {
        let cache = BlockCache::with_shards(1 << 20, 4);
        for id in 1..=50u64 {
            cache.insert(id, Arc::from(vec![1u8; 10]));
            cache.insert_value(id, Arc::from(vec![2u8; 20]));
        }
        for id in 1..=50u64 {
            assert_eq!(cache.get(id).as_deref(), Some(&[1u8; 10][..]));
            assert_eq!(cache.get_value(id).as_deref(), Some(&[2u8; 20][..]));
        }
        let s = cache.stats();
        assert_eq!(s.entries, 100);
        assert_eq!(s.used_bytes, 50 * (10 + 20 + 2 * ENTRY_OVERHEAD_BYTES));
        // Removing a block leaves the value with the same number.
        cache.remove(7);
        assert!(cache.get(7).is_none());
        assert!(cache.get_value(7).is_some());
        // A value never answers a block lookup, even for a number never cached as a block.
        cache.insert_value(1000, Arc::from(vec![3u8; 5]));
        assert!(cache.get(1000).is_none());
        cache.clear();
        assert!(cache.get_value(1).is_none() && cache.get(1).is_none());
        assert_eq!(cache.stats().entries, 0);
    }

    #[test]
    fn values_are_admitted_on_their_second_recent_offer() {
        let cache = BlockCache::with_shards(1 << 20, 1);
        assert!(!cache.offer_value(5, b"abc"));
        assert!(cache.get_value(5).is_none());
        assert!(cache.offer_value(5, b"abc"));
        assert_eq!(cache.get_value(5).as_deref(), Some(&b"abc"[..]));
        assert_eq!(cache.stats().insertions, 1);
        cache.clear();
        assert!(!cache.offer_value(5, b"abc"), "clear() forgets the offers");
        assert!(!BlockCache::new(0).offer_value(1, b"x") && !BlockCache::new(0).offer_value(1, b"x"));

        // The filter forgets after `window` first offers. Hashes below the
        // bit count map to distinct bits, so the sequence is deterministic.
        let d = Doorkeeper::new(256);
        assert_eq!(d.bits.len() * 64, 256 * DOORKEEPER_BITS_PER_OFFER);
        assert!(!d.admit(0));
        assert!(d.admit(0));
        for h in 1..255 {
            assert!(!d.admit(h), "{h}");
        }
        assert!(d.admit(7));
        assert!(!d.admit(255), "the 256th first offer clears the filter");
        assert!(!d.admit(0) && !d.admit(7));
    }

    #[test]
    fn shard_layout_is_cache_line_isolated() {
        assert_eq!(align_of::<Shard>(), 128);
        assert_eq!(size_of::<Shard>() % 128, 0);
    }

    #[test]
    fn default_shard_policy() {
        assert_eq!(BlockCache::new(0).shard_count(), 1);
        assert_eq!(BlockCache::new(1 << 20).shard_count(), 1);
        assert_eq!(BlockCache::new(8 << 20).shard_count(), 2);
        assert_eq!(BlockCache::new(12 << 20).shard_count(), 2);
        assert_eq!(BlockCache::new(64 << 20).shard_count(), 16);
        assert_eq!(BlockCache::new(1 << 30).shard_count(), 64);
        assert_eq!(BlockCache::with_shards(1 << 20, 0).shard_count(), 1);
        assert_eq!(BlockCache::with_shards(1 << 20, 6).shard_count(), 4);
        assert_eq!(
            BlockCache::with_shards(1 << 20, usize::MAX).shard_count(),
            1024
        );
    }

    #[test]
    fn shard_selection_is_balanced_for_sequential_ids() {
        let cache = BlockCache::with_shards(1 << 30, 64);
        let mut counts = [0usize; 64];
        for id in 0..64_000u64 {
            let i = (mix64(id) >> 32) as usize & cache.shard_mask;
            counts[i] += 1;
        }
        // Expected 1000 per shard; the tolerance is roughly 6 standard deviations.
        for c in counts {
            assert!((800..=1200).contains(&c), "unbalanced shard: {c}");
        }
    }

    #[test]
    fn list_stays_consistent_through_churn() {
        let cache = BlockCache::with_shards(8 * (100 + ENTRY_OVERHEAD_BYTES), 1);
        for round in 0..50u64 {
            for id in 0..20u64 {
                cache.insert(id, Arc::from(vec![round as u8; 100]));
                if id % 3 == 0 {
                    cache.get(id);
                }
                if id % 7 == 0 {
                    cache.remove(id.wrapping_sub(1));
                }
            }
            let inner = cache.shards[0].read();
            // Walk head -> tail and tail -> head; both must visit every live node.
            let mut forward = 0;
            let mut cur = inner.head;
            while cur != NIL {
                forward += 1;
                cur = inner.nodes[cur as usize].next;
            }
            let mut backward = 0;
            let mut cur = inner.tail;
            while cur != NIL {
                backward += 1;
                cur = inner.nodes[cur as usize].prev;
            }
            assert_eq!(forward, inner.map.len());
            assert_eq!(backward, inner.map.len());
            assert_eq!(inner.used, inner.map.len() * (100 + ENTRY_OVERHEAD_BYTES));
        }
    }
}
