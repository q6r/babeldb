//! Space and activity accounting. Every byte needed for recovery is counted.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cache::CacheStats;
use crate::config::Mode;
use crate::planner::PlannerSnapshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSize {
    pub path: PathBuf,
    /// Logical file length.
    pub apparent_bytes: u64,
    /// Bytes allocated by the file system (None if unavailable).
    pub allocated_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CodecUsage {
    pub codec: String,
    /// Stored units (objects + inline envelopes) using this codec.
    pub units: u64,
    pub raw_bytes: u64,
    pub body_bytes: u64,
    /// body + 64-byte headers.
    pub envelope_bytes: u64,
}

/// Monotonic activity counters of an open database.
#[derive(Default)]
pub struct EngineCounters {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
    pub commits: AtomicU64,
    /// Bytes returned to callers.
    pub bytes_requested: AtomicU64,
    /// Bytes decoded to serve them (read amplification numerator).
    pub bytes_reconstructed: AtomicU64,
    pub units_decoded: AtomicU64,
    pub dedupe_hits: AtomicU64,
    pub objects_written: AtomicU64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EngineCountersSnapshot {
    pub gets: u64,
    pub puts: u64,
    pub deletes: u64,
    pub commits: u64,
    pub bytes_requested: u64,
    pub bytes_reconstructed: u64,
    pub units_decoded: u64,
    pub dedupe_hits: u64,
    pub objects_written: u64,
}

impl EngineCounters {
    pub fn snapshot(&self) -> EngineCountersSnapshot {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        EngineCountersSnapshot {
            gets: l(&self.gets),
            puts: l(&self.puts),
            deletes: l(&self.deletes),
            commits: l(&self.commits),
            bytes_requested: l(&self.bytes_requested),
            bytes_reconstructed: l(&self.bytes_reconstructed),
            units_decoded: l(&self.units_decoded),
            dedupe_hits: l(&self.dedupe_hits),
            objects_written: l(&self.objects_written),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stats {
    pub backend: &'static str,
    pub mode: Mode,
    pub block_size: u32,
    pub inline_max: u32,

    pub records: u64,
    pub tombstones: u64,
    pub objects: u64,
    pub hash_candidates: u64,
    pub params: u64,
    pub history_entries: u64,
    pub sources: u64,
    pub pending_imports: u64,

    /// Sum of logical lengths of live records.
    pub logical_bytes: u64,
    pub key_bytes: u64,
    /// Encoded manifests in `records` (includes inline envelopes).
    pub manifest_bytes: u64,
    /// Inline envelopes (subset of manifest_bytes).
    pub inline_envelope_bytes: u64,
    /// Envelopes in `objects` (each shared object counted once).
    pub object_bytes: u64,
    pub candidate_bytes: u64,
    pub refcount_bytes: u64,
    pub param_bytes: u64,
    pub history_bytes: u64,
    pub source_bytes: u64,
    pub per_codec: Vec<CodecUsage>,

    /// Files of the backend: the ground truth of space used.
    pub files: Vec<FileSize>,
    pub cache: CacheStats,
    pub planner: PlannerSnapshot,
    pub counters: EngineCountersSnapshot,
}

impl Stats {
    /// Sum of table payloads (keys + values) as seen by the engine, before
    /// backend page overhead.
    pub fn payload_bytes(&self) -> u64 {
        self.key_bytes
            + self.manifest_bytes
            + self.object_bytes
            + self.candidate_bytes
            + self.refcount_bytes
            + self.param_bytes
            + self.history_bytes
            + self.source_bytes
    }

    pub fn file_apparent_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.apparent_bytes).sum()
    }

    pub fn file_allocated_bytes(&self) -> Option<u64> {
        self.files.iter().map(|f| f.allocated_bytes).sum()
    }
}
