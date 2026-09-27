//! Space and activity accounting. Every byte needed for recovery is counted.
//!
//! `Db::stats` scans every table in one read snapshot. Space fields count the
//! payload the engine hands to the backend (keys + values); backend page
//! overhead and free space are only visible in `files`.

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
    /// Stored units using this codec: every object (counted once however many
    /// manifests share it) plus the inline envelopes of the current records.
    /// Inline envelopes kept in `history` are only counted in `history_bytes`.
    pub units: u64,
    /// Σ decoded lengths (`raw_len`) of those units.
    pub raw_bytes: u64,
    /// Σ codec bodies.
    pub body_bytes: u64,
    /// body + 64-byte headers.
    pub envelope_bytes: u64,
}

/// Monotonic activity counters of an open database.
#[derive(Default)]
pub struct EngineCounters {
    /// Value reads (`get`, `get_with_revision`, `get_range`, `get_at`).
    pub gets: AtomicU64,
    /// Records written (puts, generated puts), counted after their commit.
    pub puts: AtomicU64,
    /// Live records deleted, counted after their commit.
    pub deletes: AtomicU64,
    /// Committed write transactions.
    pub commits: AtomicU64,
    /// Bytes returned to callers (reads and scans with values).
    pub bytes_requested: AtomicU64,
    /// Bytes decoded to serve them (read amplification numerator); blocks
    /// served from the cache decode nothing.
    pub bytes_reconstructed: AtomicU64,
    /// Units decoded (blocks, inline envelopes, generated values).
    pub units_decoded: AtomicU64,
    /// Blocks stored as a reference to an existing, byte-identical object.
    pub dedupe_hits: AtomicU64,
    /// New objects written.
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

/// Full accounting of a database (see `Db::stats`).
///
/// Counts are entries of each table. Space fields named after a table other
/// than `records` are Σ (key.len() + value.len()) over that table; `records`
/// is split into `key_bytes` and `manifest_bytes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stats {
    pub backend: &'static str,
    pub mode: Mode,
    pub block_size: u32,
    pub inline_max: u32,

    /// Live records (manifests in `records` that are not tombstones).
    pub records: u64,
    /// Tombstone manifests in `records` (only written when history is kept).
    pub tombstones: u64,
    pub objects: u64,
    /// Rows of `hash_candidates` (one per distinct (digest, raw_len)).
    pub hash_candidates: u64,
    pub params: u64,
    pub history_entries: u64,
    pub sources: u64,
    pub pending_imports: u64,

    /// Σ logical lengths of live records.
    pub logical_bytes: u64,
    /// Σ key lengths in `records` (tombstones included).
    pub key_bytes: u64,
    /// Σ encoded manifests in `records` (includes inline envelopes and tombstones).
    pub manifest_bytes: u64,
    /// Σ inline envelopes of `records` (subset of `manifest_bytes`).
    pub inline_envelope_bytes: u64,
    /// `objects`: 8-byte ids + envelopes (each shared object counted once).
    pub object_bytes: u64,
    /// `hash_candidates`: 36-byte keys + id lists.
    pub candidate_bytes: u64,
    /// `refcounts`: 8-byte ids + 8-byte counts.
    pub refcount_bytes: u64,
    /// `params`: dictionaries and templates.
    pub param_bytes: u64,
    /// `history`: history keys + retained manifests.
    pub history_bytes: u64,
    /// `sources`: source descriptors.
    pub source_bytes: u64,
    /// `meta`: format version, creation parameters, id counters, active params.
    pub meta_bytes: u64,
    /// `pending_imports`: objects prepared by unfinished imports.
    pub pending_import_bytes: u64,
    /// Per codec, ordered by (codec id, version).
    pub per_codec: Vec<CodecUsage>,

    /// Files of the backend: the ground truth of space used.
    pub files: Vec<FileSize>,
    pub cache: CacheStats,
    pub planner: PlannerSnapshot,
    pub counters: EngineCountersSnapshot,
}

impl Stats {
    /// Sum of table payloads (keys + values) as seen by the engine, before
    /// backend page overhead: every table, `meta` and `pending_imports` included.
    pub fn payload_bytes(&self) -> u64 {
        self.key_bytes
            + self.manifest_bytes
            + self.object_bytes
            + self.candidate_bytes
            + self.refcount_bytes
            + self.param_bytes
            + self.history_bytes
            + self.source_bytes
            + self.meta_bytes
            + self.pending_import_bytes
    }

    pub fn file_apparent_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.apparent_bytes).sum()
    }

    pub fn file_allocated_bytes(&self) -> Option<u64> {
        self.files.iter().map(|f| f.allocated_bytes).sum()
    }
}
