//! Bounded cache of verified, decoded blocks keyed by immutable object id.
//! Its byte budget counts toward the RAM budget and is reported in stats.
//! SKELETON — no-op cache; the util agent implements a sharded CLOCK/LRU.

use std::sync::Arc;

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

pub struct BlockCache {
    capacity: usize,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> BlockCache {
        BlockCache { capacity: capacity_bytes }
    }

    pub fn get(&self, object_id: u64) -> Option<Arc<[u8]>> {
        let _ = object_id;
        None
    }

    /// Insert a verified block. Blocks larger than the capacity are ignored.
    pub fn insert(&self, object_id: u64, bytes: Arc<[u8]>) {
        let _ = (object_id, bytes);
    }

    pub fn remove(&self, object_id: u64) {
        let _ = object_id;
    }

    pub fn clear(&self) {}

    pub fn stats(&self) -> CacheStats {
        CacheStats { capacity_bytes: self.capacity, ..CacheStats::default() }
    }
}
