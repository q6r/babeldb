//! fjall backend (feature `fjall`): an LSM-tree store for the backend
//! comparison, built on fjall 3.1.10's `SingleWriterTxDatabase`.
//!
//! # Layouts
//!
//! * [`FjallLayout::SingleKeyspace`] (default): one fjall keyspace named
//!   [`SINGLE_KEYSPACE`]; every key is stored as `[tag] ++ key`, where `tag`
//!   is a stable byte per [`Table`] (see `table_tag`). Deferred commits are
//!   buffered and crash-consistent (see *Durability*).
//! * [`FjallLayout::KeyspacePerTable`]: one keyspace per table, named
//!   [`Table::name`]. Deferred commits are executed as Immediate ones, because
//!   fjall flushes each keyspace independently (see *Durability*).
//!
//! A directory keeps the layout it was created with; opening it with the other
//! layout fails with `InvalidArgument` instead of silently showing empty tables.
//!
//! # Transactions
//!
//! * `begin_read` opens a fjall `Snapshot`: one sequence-number instant shared
//!   by every table (MVCC). While a snapshot is alive, compactions keep the
//!   versions it can see, so read transactions should be short.
//! * `begin_write` opens fjall's single-writer transaction. Changes go to a
//!   private memtable (reads, scans and `len` of the transaction see them);
//!   other data is read at the instant the transaction began. `commit` writes
//!   every change as ONE journal batch with one sequence number and publishes
//!   it for all tables at once. fjall serializes write transactions with a
//!   mutex; this store takes its own mutex first, so a panic inside a write
//!   transaction makes later `begin_write` calls return an error instead of
//!   panicking inside fjall (`expect("poisoned tx lock")`).
//! * Dropping a write transaction without `commit` discards it: nothing
//!   reaches the journal before `commit`.
//!
//! # Read snapshot reuse
//!
//! Opening a fjall snapshot registers it in a tracker shared by every reader
//! (a read lock, then a `DashMap` entry keyed by the sequence number, which all
//! readers of the same instant update): ~0.1 us alone, ~4 us per `begin_read`
//! with 16 reading threads on the reference machine. So `begin_read` reuses
//! snapshots: the n-th thread that reads keeps its last snapshot in shard
//! n % [`SNAPSHOT_SHARDS`], and reuses it while its sequence number equals
//! fjall's visible sequence number, loaded at the start of `begin_read`. Such a
//! snapshot sees exactly what a new one would: a commit publishes its new
//! visible sequence number only after its changes are in the memtables, and
//! flushes and compactions do not change what a sequence number sees. The read
//! is linearized at that load. Every commit empties the shards once it is
//! published, so cached snapshots do not keep old versions alive for long
//! (compactions keep what open snapshots can see); `compact` empties them first.
//!
//! # Durability
//!
//! A commit appends one batch (start marker, items, end marker with an xxh3
//! checksum) to the active journal (`<dir>/<n>.jnl`) through an 8 KiB user-space
//! buffer, then applies it to the memtables and publishes it.
//!
//! * `Durability::Immediate` uses `PersistMode::SyncData`: the journal buffer
//!   is written and `File::sync_data` is called (`fdatasync` on Unix,
//!   `FlushFileBuffers` on Windows; the same call redb uses for its Immediate
//!   durability) BEFORE the batch becomes visible, so the commit is durable
//!   when it returns and never visible before it is durable. The sync covers
//!   every earlier batch of the same append-only journal, and older journals
//!   were synced (`SyncAll`) when they were sealed, so it also makes every
//!   earlier Deferred commit durable, in commit order. A commit without
//!   changes (the engine's `sync`) calls `persist(SyncData)` directly, because
//!   fjall skips empty transactions entirely.
//! * `Durability::Deferred` (single-keyspace layout) uses `PersistMode::Buffer`:
//!   the batch is handed to the OS (one `write` call) but not synced. It is
//!   visible at once, survives a crash of this process (the bytes are in the OS
//!   page cache), and can be lost on an OS crash or power loss until a later
//!   Immediate commit (or a clean close: fjall syncs the journal on drop).
//!   With [`DeferredPersist::JournalBuffer`] (see [`FjallOptions`]) the batch is
//!   not persisted: it is written item by item into fjall's 8 KiB journal
//!   buffer, and only what overflows the buffer reaches the OS until a later
//!   commit persists it or the journal is rotated. A crash of this process can
//!   then lose the tail of the journal too (recovery drops the torn last
//!   batch); the crash window below is unchanged (the recovered state is still
//!   a prefix). It saves a system call per commit where another log already
//!   holds the commits: [`WalStore`](super::wal::WalStore), whose inner commits
//!   are all Deferred except its checkpoints.
//!
//! Crash window (OS crash / power loss): the recovered state is a prefix of the
//! commit sequence: every Immediate commit, plus possibly some of the Deferred
//! commits made after the last Immediate one; never part of a transaction and
//! never a later commit without an earlier one. Reasons, from fjall's sources:
//! the journal is replayed in order and recovery truncates it at the first
//! incomplete batch (`journal/batch_reader.rs`); a memtable is sealed while
//! the journal lock is held, i.e. between batches, and a flush writes all
//! sealed memtables of the tree at once (`keyspace/mod.rs`,
//! `lsm-tree abstract_tree.rs`), so with a single tree the flushed tables and
//! the journal are both prefixes. (The journal is rotated, and synced, apart
//! from memtable flushes, `worker_pool.rs` `Flush`: the tables of a flush can be
//! ahead of the synced journal. Recovery keeps the newest sequence number of
//! every key, so the union of two prefixes is the longer one.) fjall's
//! documented recovery mode tolerates a torn LAST batch only; a complete batch
//! with a bad checksum stops the open with an error instead of being dropped.
//!
//! Why Deferred is Immediate in the keyspace-per-table layout: fjall flushes a
//! keyspace's memtable to tables without syncing the journal first
//! (`worker_pool.rs`, `Flush`), and keyspaces flush independently. An unsynced
//! transaction touching two tables could therefore be persisted in one table
//! while its other half is lost with the journal tail: a torn transaction after
//! power loss, which the `Store` contract does not allow ("backends without an
//! equivalent must treat it as Immediate").
//!
//! # Costs and limits
//!
//! * `len` counts the table with a full scan: O(n). fjall only offers an
//!   approximate O(1) count, which also counts overwritten versions and
//!   tombstones.
//! * `remove` does a point lookup and writes a tombstone only when the key
//!   exists (it must return whether the key existed).
//! * Keys: fjall panics on empty keys and on keys longer than 65,535 bytes. An
//!   empty key is rejected with `InvalidArgument`, a longer key than
//!   [`FjallLayout::max_key_len`] (65,534 bytes in the single-keyspace layout,
//!   whose tag byte counts) with `LimitExceeded`. `get`/`remove` of such keys
//!   report "absent"; scan bounds of any length are clamped exactly.
//! * Values: at most `u32::MAX` bytes (`LimitExceeded` above).
//! * `compact` seals and flushes the memtables, runs fjall's major compaction
//!   and then lsm-tree's version GC, so the replaced table files are deleted
//!   before it returns (fjall alone keeps them until a later memtable
//!   rotation). This goes through `#[doc(hidden)]` fjall APIs (`inner`,
//!   `rotate_memtable`, `sealed_memtable_count`, `major_compact`, `tree`,
//!   `AbstractTree::get_version_history_lock`, `visible_seqno`), which are
//!   public but carry no semver promise; `Cargo.lock` pins fjall 3.1.10.
//! * `files` lists every regular file under the directory, which must be
//!   dedicated to the store: journals (`<n>.jnl`, created preallocated to
//!   64 MiB and rotated once a flush finds them past 64 MB), `version`, `lock`,
//!   and `keyspaces/<id>/...` (tables, version files, fjall's internal metadata
//!   keyspace). Background flushes and compactions (fjall runs min(cores, 4)
//!   worker threads) may add or delete files at any time.
//! * [`FjallStore::flush_memtables`] writes the memtables to tables (the data
//!   files) without compacting; until then recent commits live in memory and
//!   in the journal only.
//!
//! # Tuning ([`FjallOptions`])
//!
//! Applied when a keyspace is created: fjall persists every keyspace setting,
//! and a reopened directory keeps the ones it was created with.
//! `FjallOptions::default()` keeps fjall's defaults: 64 MiB memtables, 4 KiB
//! data blocks, data blocks uncompressed on levels 0-1 and LZ4 from level 2
//! on, Bloom filters on every level, no key-value separation; Deferred commits
//! handed to the OS. [`FjallOptions::for_wal`] is the tuning of
//! `Db::open_fjall_wal`: LZ4 on every level, so freshly flushed tables are
//! compressed too (~10% smaller tables on the chat workload of
//! `benches/compare.rs`); index and filter blocks pinned (a point read took
//! ~3.9 us without, ~3.0 us with, on 100k records); and
//! [`DeferredPersist::JournalBuffer`]. fjall 3.1.10 has no other block
//! compression than LZ4. Tables of that workload after a flush, by data block
//! size: 2 KiB 30.5 MB, 4 KiB 29.8 MB, 8 KiB 29.5 MB, 16 KiB 29.0 MB (4 KiB
//! uncompressed: 33.9 MB). Smaller restart intervals and the data block hash
//! index gave no point-read gain beyond run-to-run noise and slower range
//! scans, so fjall's defaults stay there.

use std::borrow::Cow;
use std::fmt;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use ::fjall::config::{BlockSizePolicy, CompressionPolicy, HashRatioPolicy, PinningPolicy, RestartIntervalPolicy};
use ::fjall::{
    AbstractTree, CompressionType, Guard, KeyspaceCreateOptions, PersistMode, Readable,
    SingleWriterTxDatabase, SingleWriterTxKeyspace, SingleWriterWriteTx, Snapshot,
};

use super::{Durability, ReadTxn, ScanFn, Store, Table, WriteTxn};
use crate::error::{Error, Result};

/// Longest key fjall accepts (lsm-tree stores key lengths as `u16`).
const FJALL_MAX_KEY_LEN: usize = u16::MAX as usize;

/// Longest value fjall accepts (lsm-tree stores value lengths as `u32`).
const FJALL_MAX_VALUE_LEN: u64 = u32::MAX as u64;

/// Name of the keyspace of [`FjallLayout::SingleKeyspace`].
pub const SINGLE_KEYSPACE: &str = "tables";

/// Upper bound on the wait for memtable flushes (`compact`, `flush_memtables`).
const FLUSH_TIMEOUT: Duration = Duration::from_secs(300);

/// Data block sizes fjall accepts.
const MIN_DATA_BLOCK: u32 = 1 << 10;
const MAX_DATA_BLOCK: u32 = 1 << 20;

/// Shards of the read snapshot cache (module documentation, *Read snapshot reuse*).
pub const SNAPSHOT_SHARDS: usize = 32;

/// One shard of the snapshot cache, on a cache line of its own.
#[repr(align(128))]
#[derive(Default)]
struct SnapshotSlot(Mutex<Option<Arc<Snapshot>>>);

/// Shard of the calling thread: the n-th thread that asks gets n % SNAPSHOT_SHARDS.
fn snapshot_shard() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SHARD: usize = NEXT.fetch_add(1, Ordering::Relaxed) % SNAPSHOT_SHARDS;
    }
    SHARD.with(|s| *s)
}

/// Physical organisation of the tables (see the module documentation).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum FjallLayout {
    /// One keyspace for every table, keys prefixed with a table tag byte.
    #[default]
    SingleKeyspace,
    /// One keyspace per table; `Durability::Deferred` behaves as `Immediate`.
    KeyspacePerTable,
}

impl FjallLayout {
    fn prefix_len(self) -> usize {
        match self {
            FjallLayout::SingleKeyspace => 1,
            FjallLayout::KeyspacePerTable => 0,
        }
    }

    /// Longest key a table can store with this layout.
    pub fn max_key_len(self) -> usize {
        FJALL_MAX_KEY_LEN - self.prefix_len()
    }
}

/// How a `Durability::Deferred` commit reaches fjall's journal (single-keyspace
/// layout; see the module documentation, *Durability*).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum DeferredPersist {
    /// `PersistMode::Buffer`: one `write` call hands the batch to the OS, so it
    /// survives a crash of this process.
    #[default]
    WriteToOs,
    /// No persist call: the batch goes through the journal buffer (8 KiB), and
    /// its tail stays there until something persists it; a crash of this
    /// process can lose it.
    JournalBuffer,
}

/// Block compression of the data blocks of the tables. fjall 3.1.10 offers
/// LZ4 only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum FjallCompression {
    /// fjall's default: uncompressed on levels 0 and 1 (freshly flushed
    /// tables), LZ4 from level 2 on.
    #[default]
    FjallDefault,
    /// Uncompressed everywhere.
    None,
    /// LZ4 on every level, level 0 included.
    Lz4,
}

/// Tuning of a [`FjallStore`] (module documentation, *Tuning*).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FjallOptions {
    pub layout: FjallLayout,
    pub compression: FjallCompression,
    /// Target uncompressed size of a data block, 1 KiB to 1 MiB (fjall: 4 KiB).
    pub data_block_bytes: u32,
    /// Size at which the active memtable is sealed and flushed to a table
    /// (fjall: 64 MiB).
    pub memtable_bytes: u64,
    /// Skip the Bloom filters of the last level (fjall's
    /// `expect_point_read_hits`): smaller tables, but a lookup of a missing key
    /// then searches the last level's index and block.
    pub expect_point_read_hits: bool,
    /// Keep the index and filter blocks of every table in memory, unpartitioned
    /// (fjall pins them on levels 0-1 only, and partitions both from level 3
    /// on): a point read then takes only its data block from the block cache.
    /// Costs their size in memory (a few bytes per key).
    pub pin_index_and_filters: bool,
    /// Hash index in the data blocks, in hundredths of a slot per entry (fjall:
    /// 0, none): a point read finds its entry in the block without a binary
    /// search, for about that many hundredths of a byte per entry.
    pub data_block_hash_percent: u8,
    /// Entries per restart point in the data blocks (0 keeps fjall's: 10 on
    /// level 0, 16 below). A point read binary-searches the restart points,
    /// then decodes entries one by one from there; a restart point stores its
    /// key whole instead of prefix-compressed.
    pub data_block_restart_interval: u8,
    pub deferred: DeferredPersist,
}

impl Default for FjallOptions {
    fn default() -> Self {
        FjallOptions {
            layout: FjallLayout::default(),
            compression: FjallCompression::default(),
            data_block_bytes: 4 << 10,
            memtable_bytes: 64 << 20,
            expect_point_read_hits: false,
            pin_index_and_filters: false,
            data_block_hash_percent: 0,
            data_block_restart_interval: 0,
            deferred: DeferredPersist::default(),
        }
    }
}

impl FjallOptions {
    /// The tuning of `Db::open_fjall_wal`: single keyspace, LZ4 on every
    /// level, 4 KiB blocks, index and filter blocks pinned, Deferred commits
    /// left in the journal buffer (the WAL in front holds every acknowledged
    /// commit).
    pub fn for_wal() -> FjallOptions {
        FjallOptions {
            compression: FjallCompression::Lz4,
            pin_index_and_filters: true,
            deferred: DeferredPersist::JournalBuffer,
            ..FjallOptions::default()
        }
    }

    pub fn validate(&self) -> Result<()> {
        if !(MIN_DATA_BLOCK..=MAX_DATA_BLOCK).contains(&self.data_block_bytes) {
            return Err(Error::InvalidArgument(format!(
                "fjall data_block_bytes {} outside [{MIN_DATA_BLOCK}, {MAX_DATA_BLOCK}]",
                self.data_block_bytes
            )));
        }
        if self.memtable_bytes < 64 << 10 {
            return Err(Error::InvalidArgument(format!(
                "fjall memtable_bytes {} below 64 KiB",
                self.memtable_bytes
            )));
        }
        Ok(())
    }

    fn keyspace_options(&self) -> KeyspaceCreateOptions {
        let mut opts = KeyspaceCreateOptions::default()
            .max_memtable_size(self.memtable_bytes)
            .data_block_size_policy(BlockSizePolicy::all(self.data_block_bytes))
            .expect_point_read_hits(self.expect_point_read_hits);
        if self.pin_index_and_filters {
            opts = opts
                .index_block_pinning_policy(PinningPolicy::all(true))
                .filter_block_pinning_policy(PinningPolicy::all(true))
                .index_block_partitioning_policy(PinningPolicy::all(false))
                .filter_block_partitioning_policy(PinningPolicy::all(false));
        }
        if self.data_block_restart_interval > 0 {
            opts = opts.data_block_restart_interval_policy(RestartIntervalPolicy::all(self.data_block_restart_interval));
        }
        if self.data_block_hash_percent > 0 {
            let ratio = f32::from(self.data_block_hash_percent) / 100.0;
            opts = opts.data_block_hash_ratio_policy(HashRatioPolicy::all(ratio));
        }
        match self.compression {
            FjallCompression::FjallDefault => opts,
            FjallCompression::None => opts.data_block_compression_policy(CompressionPolicy::disabled()),
            FjallCompression::Lz4 => opts.data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4)),
        }
    }
}

/// Stable on-disk tag of each table in [`FjallLayout::SingleKeyspace`]
/// (independent of the declaration order of `Table`).
fn table_tag(table: Table) -> u8 {
    match table {
        Table::Meta => 0,
        Table::Records => 1,
        Table::Objects => 2,
        Table::HashCandidates => 3,
        Table::Refcounts => 4,
        Table::Params => 5,
        Table::History => 6,
        Table::Sources => 7,
        Table::PendingImports => 8,
    }
}

type PhysicalRange = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// A fjall database in a dedicated directory.
pub struct FjallStore {
    /// Declared before `db`: cached snapshots are released before the database.
    snapshots: Box<[SnapshotSlot]>,
    db: SingleWriterTxDatabase,
    /// Indexed by `Table::index()`. In the single-keyspace layout every entry
    /// is the same handle.
    keyspaces: Vec<SingleWriterTxKeyspace>,
    layout: FjallLayout,
    deferred: DeferredPersist,
    dir: PathBuf,
    /// Taken before fjall's own writer lock (see the module documentation).
    writer: Mutex<()>,
}

impl fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FjallStore")
            .field("dir", &self.dir)
            .field("layout", &self.layout)
            .field("deferred", &self.deferred)
            .finish_non_exhaustive()
    }
}

impl FjallStore {
    /// Open or create a database in `dir` with the default layout
    /// ([`FjallLayout::SingleKeyspace`]). `cache_bytes` is the capacity of
    /// fjall's block cache, shared by all keyspaces.
    pub fn open(dir: impl AsRef<Path>, cache_bytes: usize) -> Result<FjallStore> {
        FjallStore::open_with_layout(dir, cache_bytes, FjallLayout::default())
    }

    /// Open or create a database in `dir` with an explicit layout. Every table
    /// exists once this returns. Fails with `InvalidArgument` if `dir` holds a
    /// database created with the other layout.
    pub fn open_with_layout(dir: impl AsRef<Path>, cache_bytes: usize, layout: FjallLayout) -> Result<FjallStore> {
        FjallStore::open_with(dir, cache_bytes, &FjallOptions { layout, ..FjallOptions::default() })
    }

    /// Open or create a database in `dir` with explicit tuning (applied to the
    /// keyspaces this call creates; see the module documentation, *Tuning*).
    /// Fails with `InvalidArgument` for invalid options or when `dir` holds a
    /// database created with the other layout.
    pub fn open_with(dir: impl AsRef<Path>, cache_bytes: usize, opts: &FjallOptions) -> Result<FjallStore> {
        opts.validate()?;
        let layout = opts.layout;
        let dir = std::path::absolute(dir.as_ref())?;
        let db = SingleWriterTxDatabase::builder(&dir)
            .cache_size(u64::try_from(cache_bytes).unwrap_or(u64::MAX))
            .open()
            .map_err(Error::backend)?;
        let keyspaces = match layout {
            FjallLayout::SingleKeyspace => {
                if let Some(t) = Table::ALL.iter().find(|t| db.keyspace_exists(t.name())) {
                    return Err(layout_mismatch(&dir, layout, t.name()));
                }
                let ks = db.keyspace(SINGLE_KEYSPACE, || opts.keyspace_options()).map_err(Error::backend)?;
                vec![ks; Table::ALL.len()]
            }
            FjallLayout::KeyspacePerTable => {
                if db.keyspace_exists(SINGLE_KEYSPACE) {
                    return Err(layout_mismatch(&dir, layout, SINGLE_KEYSPACE));
                }
                Table::ALL
                    .iter()
                    .map(|t| db.keyspace(t.name(), || opts.keyspace_options()).map_err(Error::backend))
                    .collect::<Result<Vec<_>>>()?
            }
        };
        let snapshots = (0..SNAPSHOT_SHARDS).map(|_| SnapshotSlot::default()).collect();
        Ok(FjallStore { snapshots, db, keyspaces, layout, deferred: opts.deferred, dir, writer: Mutex::new(()) })
    }

    /// Database directory (absolute).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn layout(&self) -> FjallLayout {
        self.layout
    }

    pub fn deferred_persist(&self) -> DeferredPersist {
        self.deferred
    }

    /// Read snapshots currently kept for reuse (diagnostics and tests).
    pub fn cached_snapshots(&self) -> usize {
        self.snapshots
            .iter()
            .filter(|s| s.0.lock().unwrap_or_else(PoisonError::into_inner).is_some())
            .count()
    }

    /// Empty every shard of the snapshot cache (after a commit).
    fn release_snapshots(&self) {
        for slot in &self.snapshots {
            let old = slot.0.lock().unwrap_or_else(PoisonError::into_inner).take();
            drop(old);
        }
    }

    /// Seal the active memtables and wait until every sealed memtable is
    /// written to a table: the data files then hold every commit made before
    /// the call (what a checkpoint does for PostgreSQL or MongoDB). No
    /// compaction; fjall may schedule some in the background afterwards.
    /// Uses fjall's `#[doc(hidden)]` `rotate_memtable` / `sealed_memtable_count`.
    pub fn flush_memtables(&self) -> Result<()> {
        for ks in self.distinct_keyspaces() {
            let ks = ks.inner();
            ks.rotate_memtable().map_err(Error::backend)?;
            let deadline = Instant::now() + FLUSH_TIMEOUT;
            while ks.sealed_memtable_count() > 0 {
                if Instant::now() >= deadline {
                    return Err(Error::backend("fjall: timed out waiting for a memtable flush"));
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        Ok(())
    }

    fn keyspace(&self, table: Table) -> &SingleWriterTxKeyspace {
        &self.keyspaces[table.index()]
    }

    /// Distinct keyspace handles (one per physical LSM-tree).
    fn distinct_keyspaces(&self) -> &[SingleWriterTxKeyspace] {
        match self.layout {
            FjallLayout::SingleKeyspace => &self.keyspaces[..1],
            FjallLayout::KeyspacePerTable => &self.keyspaces,
        }
    }

    fn encode_key<'k>(&self, table: Table, key: &'k [u8]) -> Cow<'k, [u8]> {
        match self.layout {
            FjallLayout::KeyspacePerTable => Cow::Borrowed(key),
            FjallLayout::SingleKeyspace => {
                let mut k = Vec::with_capacity(key.len() + 1);
                k.push(table_tag(table));
                k.extend_from_slice(key);
                Cow::Owned(k)
            }
        }
    }

    /// Physical key of a lookup, or `None` when no stored key can be equal to
    /// `key` (empty or too long): fjall panics on such keys.
    fn lookup_key<'k>(&self, table: Table, key: &'k [u8]) -> Option<Cow<'k, [u8]>> {
        if key.is_empty() || key.len() > self.layout.max_key_len() {
            return None;
        }
        Some(self.encode_key(table, key))
    }

    /// Physical bounds of a scan, or `None` when the range is empty or inverted.
    fn physical_range(&self, table: Table, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Option<PhysicalRange> {
        let (lo, hi) = match self.layout {
            FjallLayout::KeyspacePerTable => (start.map(<[u8]>::to_vec), end.map(<[u8]>::to_vec)),
            FjallLayout::SingleKeyspace => {
                let tag = table_tag(table);
                let enc = |k: &[u8]| self.encode_key(table, k).into_owned();
                let lo = match start {
                    Bound::Unbounded => Bound::Included(vec![tag]),
                    Bound::Included(k) => Bound::Included(enc(k)),
                    Bound::Excluded(k) => Bound::Excluded(enc(k)),
                };
                let hi = match end {
                    Bound::Unbounded => Bound::Excluded(vec![tag + 1]),
                    Bound::Included(k) => Bound::Included(enc(k)),
                    Bound::Excluded(k) => Bound::Excluded(enc(k)),
                };
                (lo, hi)
            }
        };
        let (lo, hi) = (clamp_start(lo), clamp_end(hi));
        if is_empty_range(&lo, &hi) { None } else { Some((lo, hi)) }
    }

    /// `None`: no persist call at all (`DeferredPersist::JournalBuffer`).
    fn persist_mode(&self, durability: Durability) -> Option<PersistMode> {
        match (durability, self.layout, self.deferred) {
            (Durability::Deferred, FjallLayout::SingleKeyspace, DeferredPersist::WriteToOs) => Some(PersistMode::Buffer),
            (Durability::Deferred, FjallLayout::SingleKeyspace, DeferredPersist::JournalBuffer) => None,
            _ => Some(PersistMode::SyncData),
        }
    }
}

fn layout_mismatch(dir: &Path, layout: FjallLayout, found: &str) -> Error {
    Error::InvalidArgument(format!(
        "fjall database {} was not created with the {layout:?} layout (it has keyspace {found:?})",
        dir.display()
    ))
}

/// Stored keys are at most `FJALL_MAX_KEY_LEN` bytes long. For such keys,
/// `k >= s` and `k > s` are both equivalent to `k > s[..MAX]` when `s` is
/// longer than `MAX`, so the bound is rewritten instead of reaching fjall
/// (which panics on long keys).
fn clamp_start(bound: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(mut k) | Bound::Excluded(mut k) if k.len() > FJALL_MAX_KEY_LEN => {
            k.truncate(FJALL_MAX_KEY_LEN);
            Bound::Excluded(k)
        }
        b => b,
    }
}

/// Counterpart of `clamp_start`: `k <= e` and `k < e` are both equivalent to
/// `k <= e[..MAX]` when `e` is longer than `MAX`.
fn clamp_end(bound: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(mut k) | Bound::Excluded(mut k) if k.len() > FJALL_MAX_KEY_LEN => {
            k.truncate(FJALL_MAX_KEY_LEN);
            Bound::Included(k)
        }
        b => b,
    }
}

/// Inverted ranges and equal bounds with an excluded side select nothing.
fn is_empty_range(lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> bool {
    let (l, l_incl) = match lo {
        Bound::Unbounded => return false,
        Bound::Included(k) => (k, true),
        Bound::Excluded(k) => (k, false),
    };
    let (h, h_incl) = match hi {
        Bound::Unbounded => return false,
        Bound::Included(k) => (k, true),
        Bound::Excluded(k) => (k, false),
    };
    match l.cmp(h) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => !(l_incl && h_incl),
        std::cmp::Ordering::Less => false,
    }
}

fn as_slice_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    b.as_ref().map(Vec::as_slice)
}

// ---------------------------------------------------------------------------
// Reads shared by snapshots and write transactions
// ---------------------------------------------------------------------------

fn get_in<R: Readable>(store: &FjallStore, r: &R, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let Some(pk) = store.lookup_key(table, key) else {
        return Ok(None);
    };
    Ok(r.get(store.keyspace(table), &*pk).map_err(Error::backend)?.map(|v| v.to_vec()))
}

fn scan_in<R: Readable>(
    store: &FjallStore,
    r: &R,
    table: Table,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
    f: &mut ScanFn<'_>,
) -> Result<()> {
    let Some((lo, hi)) = store.physical_range(table, start, end) else {
        return Ok(());
    };
    let skip = store.layout.prefix_len();
    let iter = r.range::<&[u8], _>(store.keyspace(table), (as_slice_bound(&lo), as_slice_bound(&hi)));
    let mut visit = |guard: Guard| -> Result<bool> {
        let (k, v) = guard.into_inner().map_err(Error::backend)?;
        let key: &[u8] = &k;
        f(&key[skip..], &v)
    };
    if reverse {
        for guard in iter.rev() {
            if !visit(guard)? {
                break;
            }
        }
    } else {
        for guard in iter {
            if !visit(guard)? {
                break;
            }
        }
    }
    Ok(())
}

fn len_in<R: Readable>(store: &FjallStore, r: &R, table: Table) -> Result<u64> {
    let Some((lo, hi)) = store.physical_range(table, Bound::Unbounded, Bound::Unbounded) else {
        return Ok(0);
    };
    let mut n = 0u64;
    for guard in r.range::<&[u8], _>(store.keyspace(table), (as_slice_bound(&lo), as_slice_bound(&hi))) {
        guard.key().map_err(Error::backend)?;
        n += 1;
    }
    Ok(n)
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

/// Read transaction: a consistent snapshot of every table (possibly shared
/// with other read transactions, see *Read snapshot reuse*).
pub struct FjallRead<'a> {
    snapshot: Arc<Snapshot>,
    store: &'a FjallStore,
}

/// The single write transaction.
pub struct FjallWrite<'a> {
    // Declared before `_writer`: fjall's lock is released before ours.
    tx: SingleWriterWriteTx<'a>,
    store: &'a FjallStore,
    /// Whether the fjall transaction holds a change (fjall skips empty commits).
    dirty: bool,
    _writer: MutexGuard<'a, ()>,
}

impl ReadTxn for FjallRead<'_> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        get_in(self.store, &*self.snapshot, table, key)
    }

    fn scan(&self, table: Table, start: Bound<&[u8]>, end: Bound<&[u8]>, reverse: bool, f: &mut ScanFn<'_>) -> Result<()> {
        scan_in(self.store, &*self.snapshot, table, start, end, reverse, f)
    }

    /// O(n): counts the table with a scan.
    fn len(&self, table: Table) -> Result<u64> {
        len_in(self.store, &*self.snapshot, table)
    }
}

impl ReadTxn for FjallWrite<'_> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        get_in(self.store, &self.tx, table, key)
    }

    fn scan(&self, table: Table, start: Bound<&[u8]>, end: Bound<&[u8]>, reverse: bool, f: &mut ScanFn<'_>) -> Result<()> {
        scan_in(self.store, &self.tx, table, start, end, reverse, f)
    }

    /// O(n): counts the table (with this transaction's changes) with a scan.
    fn len(&self, table: Table) -> Result<u64> {
        len_in(self.store, &self.tx, table)
    }
}

impl WriteTxn for FjallWrite<'_> {
    fn put(&mut self, table: Table, key: &[u8], value: &[u8]) -> Result<()> {
        let store = self.store;
        if key.is_empty() {
            return Err(Error::InvalidArgument("the fjall backend does not store empty keys".into()));
        }
        let max = store.layout.max_key_len();
        if key.len() > max {
            return Err(Error::LimitExceeded(format!(
                "key of {} bytes (the fjall backend stores keys up to {max} bytes)",
                key.len()
            )));
        }
        if value.len() as u64 > FJALL_MAX_VALUE_LEN {
            return Err(Error::LimitExceeded(format!(
                "value of {} bytes (the fjall backend stores values up to {FJALL_MAX_VALUE_LEN} bytes)",
                value.len()
            )));
        }
        let pk = store.encode_key(table, key);
        self.tx.insert(store.keyspace(table), &*pk, value);
        self.dirty = true;
        Ok(())
    }

    fn remove(&mut self, table: Table, key: &[u8]) -> Result<bool> {
        let store = self.store;
        let Some(pk) = store.lookup_key(table, key) else {
            return Ok(false);
        };
        let ks = store.keyspace(table);
        if !self.tx.contains_key(ks, &*pk).map_err(Error::backend)? {
            return Ok(false);
        }
        self.tx.remove(ks, &*pk);
        self.dirty = true;
        Ok(true)
    }

    fn commit(self, durability: Durability) -> Result<()> {
        let FjallWrite { tx, store, dirty, _writer } = self;
        let mode = store.persist_mode(durability);
        if dirty {
            tx.durability(mode).commit().map_err(Error::backend)?;
            // Published: snapshots cached before are stale now.
            store.release_snapshots();
        } else {
            // fjall's commit is a no-op for an empty transaction, but an empty
            // Immediate commit must still make earlier Deferred commits durable.
            drop(tx);
            if let Some(mode @ (PersistMode::SyncData | PersistMode::SyncAll)) = mode {
                store.db.persist(mode).map_err(Error::backend)?;
            }
        }
        Ok(())
    }
}

impl Store for FjallStore {
    type Read<'a> = FjallRead<'a>;
    type Write<'a> = FjallWrite<'a>;

    /// Reuses this thread's cached snapshot when it is still current (module
    /// documentation, *Read snapshot reuse*).
    fn begin_read(&self) -> Result<FjallRead<'_>> {
        let visible = self.db.inner().visible_seqno();
        let mut slot = self.snapshots[snapshot_shard()].0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(cached) = slot.as_ref().filter(|s| s.seqno() == visible) {
            return Ok(FjallRead { snapshot: Arc::clone(cached), store: self });
        }
        let fresh = Arc::new(self.db.read_tx());
        let replaced = slot.replace(Arc::clone(&fresh));
        drop(slot);
        drop(replaced);
        Ok(FjallRead { snapshot: fresh, store: self })
    }

    fn begin_write(&self) -> Result<FjallWrite<'_>> {
        let writer = self
            .writer
            .lock()
            .map_err(|_| Error::backend("fjall writer lock poisoned: a thread panicked inside a write transaction"))?;
        let tx = self.db.write_tx();
        Ok(FjallWrite { tx, store: self, dirty: false, _writer: writer })
    }

    /// Seals and flushes every memtable, runs a major compaction of every
    /// keyspace (drops overwritten versions and tombstones), then releases the
    /// superseded versions so the replaced table files are deleted now.
    /// Always `Ok(true)`.
    fn compact(&mut self) -> Result<bool> {
        // Cached snapshots would hold the versions the compaction drops.
        for slot in self.snapshots.iter_mut() {
            *slot.0.get_mut().unwrap_or_else(PoisonError::into_inner) = None;
        }
        self.flush_memtables()?;
        for ks in self.distinct_keyspaces() {
            ks.inner().major_compact().map_err(Error::backend)?;
        }
        // lsm-tree keeps the newest version older than its GC watermark, i.e.
        // the one that still lists the pre-compaction tables, until a later
        // memtable rotation raises the watermark. `&mut self` guarantees that
        // no snapshot of this store is open, so every version but the latest
        // can go now (the new version was published below `visible_seqno`).
        let watermark = self.db.inner().visible_seqno();
        for ks in self.distinct_keyspaces() {
            let ks = ks.inner();
            ks.tree.get_version_history_lock().maintenance(ks.path(), watermark).map_err(Error::backend)?;
        }
        Ok(true)
    }

    /// Every regular file under the database directory, sorted.
    fn files(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        collect_files(&self.dir, &mut out);
        out.sort();
        out
    }

    fn backend_name(&self) -> &'static str {
        "fjall"
    }
}

/// Recursive listing; entries that vanish while walking (compaction) are skipped.
fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            collect_files(&entry.path(), out);
        } else if kind.is_file() {
            out.push(entry.path());
        }
    }
}
