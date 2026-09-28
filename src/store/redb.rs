//! redb backend: one file of copy-on-write B+trees, MVCC readers, one writer.
//!
//! Every logical table is a `TableDefinition<&[u8], &[u8]>` named `Table::name()`.
//! `open` creates the missing ones, so read transactions never see a missing table.
//!
//! Durability, as implemented by redb 4.3.0 (checked in its sources):
//! - `Immediate` maps to `redb::Durability::Immediate`: a checksummed 1-phase commit that
//!   ends with one fsync (`FlushFileBuffers` on Windows) before `commit` returns.
//! - `Deferred` maps to `redb::Durability::None`: the commit is published in memory (every
//!   later transaction sees it) and its pages are written without fsync, while the header
//!   on disk still names the last durable commit. The next `Immediate` commit is built on
//!   top of that state and makes every earlier deferred commit durable with it; closing the
//!   store cleanly (dropping it) also ends with a durable commit. If the process dies first,
//!   reopening rolls back to the last durable commit (whole transactions only).
//!
//! A redb `Table` borrows its `WriteTransaction`, so write transactions reopen the table on
//! every call instead of caching handles next to it (that would take a self-referential
//! struct); `put_many` opens it once for a whole batch. Scans inside a write transaction copy
//! entries out in bounded batches and close the table before running the callback, so the
//! callback may read the same table again (redb allows one open handle per table and
//! transaction). Read transactions open each table at most once, on first use, and scan
//! without copying.
//!
//! # Read snapshot reuse
//!
//! `Database::begin_read` and the drop of every redb `ReadTransaction` lock redb's global
//! `TransactionTracker` mutex (redb 4.3.0 `transaction_tracker.rs`,
//! `register_read_transaction` / `deallocate_read_transaction`; `begin_read` also locks the
//! `TransactionalMemory` state), and opening a table walks the table tree. With one read
//! transaction per point read, concurrent readers queue on those locks. So `begin_read` hands
//! out again a snapshot the calling thread opened earlier, as long as nothing was committed
//! since:
//!
//! - `generation` counts commit boundaries: `commit` increments it right before redb's
//!   `commit` and again right after it returned (with or without error), so it is odd exactly
//!   while a commit runs. Commits never overlap: a store-level writer mutex is held from
//!   `begin_write` until after the second increment (redb alone would let the next writer in
//!   as soon as its `commit` returns, before our second increment).
//! - The snapshot cache is a small array of mutex-protected slots (shards); a thread uses the
//!   shard of its number, threads being numbered in the order they first read, so concurrent
//!   readers normally use different shards. All loads and increments are `SeqCst`.
//! - `begin_read` locks its shard, loads `generation` and reuses the cached snapshot if its tag
//!   equals the value loaded. Otherwise it loads `generation` (`g`), calls `db.begin_read()`,
//!   and caches the new snapshot with tag `g` if `g` is even and `generation` still equals
//!   `g` when the shard is locked again.
//! - The first increment of every commit empties every shard, so a cached snapshot never
//!   keeps redb from reusing the pages the commit frees; `compact` empties them too (redb
//!   refuses to compact under a live read transaction). A `RedbRead` keeps its own reference:
//!   an open read transaction is unaffected by commits, as before.
//!
//! Why a reader sees every acknowledged commit and never goes back in time:
//!
//! 1. A snapshot cached with tag `g` holds exactly `S(g)`, the state after the commits whose
//!    second increment produced a value `<= g`. Such a commit was published by redb before its
//!    second increment; the opener's load of `g` read that increment or a later one of the
//!    same chain of read-modify-writes, so the publication happens-before the opener's
//!    `begin_read`, which takes the redb lock the publication released: the snapshot has it.
//!    It has no later commit `C`: redb publishes `C` after `C`'s first increment, so a
//!    `begin_read` that saw `C` would make the later load under the shard lock read at least
//!    that increment, not `g`, and the snapshot would not be cached. All snapshots tagged `g`
//!    hold the same state.
//! 2. No stale read: once `commit` returned (the engine acknowledges after that), its second
//!    increment happened-before any read that starts afterwards, so that read loads a value at
//!    least that large. It reuses only a tag equal to that value, i.e. a snapshot holding the
//!    commit (1), or calls `db.begin_read()` after the load, which sees the commit.
//! 3. No going back: if read `R1` finished before read `R2` started, every commit `R1`'s
//!    snapshot holds was published after its first increment, before `R1` (1 for a reused
//!    snapshot, redb's lock for a fresh one), so `R2` loads a value past that first increment.
//!    `R2` reuses only an even tag equal to its load, hence past that commit's second increment
//!    too, holding it (1); a fresh `begin_read` of `R2` comes after `R1`'s snapshot was taken,
//!    and redb's snapshots only move forward.
//! 4. A put into shard `s` happens only if `generation` still equals the tag with `s` locked. A
//!    commit that started after the tag was loaded empties `s` after its first increment:
//!    either before the put (the put then loads the new value and skips) or after it (and
//!    removes the snapshot). So no stale snapshot survives in a shard past a commit start.
//!
//! Reads that start while a commit runs (odd generation) open fresh snapshots and do not cache
//! them; they cost what every read cost before. Tests: `tests/store_conformance.rs`
//! (`redb_snapshot_*`, including readers checking every acknowledged and every observed value
//! against concurrent commits) and `tests/concurrency.rs`.
//!
//! # Parallel file reads (Windows)
//!
//! A page missing from redb's cache is read from the file. std opens files for synchronous
//! I/O, and Windows serializes all I/O on one such handle (its file object lock), so with one
//! handle every cache miss of every thread queued behind the others. `RedbStore::open`
//! therefore gives redb a `StorageBackend` that reads through a pool of read-only handles (one
//! per logical CPU, picked by thread) and writes, resizes and syncs through redb's own
//! `FileBackend`. All handles see the same cached file contents. redb's byte-range locks
//! cover the whole data range and are mandatory on Windows (a second handle of the same
//! process could not read), so the backend does not lock; instead its write handle shares
//! reading only, so no other handle can open the file for writing while the store is open
//! (a second `RedbStore::open` fails, as before).
//!
//! Measured with the store benchmark (200 000 S3 records of 512 B, file 269 MB, 64 MiB cache,
//! uniform point reads, one read transaction per get, machine shared with other work): one
//! handle 40-70 k gets/s whatever the thread count (p50 95 us with 16 threads); the pool
//! 107 k/s with 1 thread, 255 k/s with 4 and 287 k/s with 16 (p50 20 us).
//!
//! # File size
//!
//! redb 4.3 grows the file by doubling its usable size while it is smaller than one 4 GiB
//! region (`TransactionalMemory::grow`; the region size is only settable in redb's own tests,
//! and the cache size does not affect growth), and shrinks it at commit only when the free
//! tail is at least half of the last region. So after a load the file is 1x to 2x the pages
//! in use. Those pages are themselves far more than the data when keys arrive out of order:
//! redb splits a full leaf evenly unless the new key is the greatest of the whole tree, so
//! appends at the end of many key ranges (chat messages per channel) leave most leaves about
//! half full for good. redb's `Database::compact` only moves pages down and truncates the
//! file, so `compact` first rewrites every table that meets the `REBUILD_*` thresholds (at
//! least 25 % of its page bytes free): one transaction copies it in ascending key order into
//! an empty table, which fills each leaf before starting the next, and replaces the original
//! with it (or is aborted when the copy is not at least 10 % smaller). The price: a full copy
//! of those tables, during which the file holds both (and may double once more), then redb's
//! compaction moves the packed copy down.
//!
//! Measured with `cargo bench --bench store -- --phases compact` (200 000 S3 records of 512 B,
//! 110 MB of keys and values, loaded in random channel order, 64 MiB cache): file 269.5 MB
//! after the load; redb's compaction alone 207.9 MB; rewrite + compaction 120.1 MB, in about
//! 7 s, with a peak of 539 MB allocated while both copies existed. (On NTFS the allocation of
//! a shrunk file is only trimmed at close: measure after dropping the store.)

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use ::redb::{
    AccessGuard, Builder, Database, Durability as RedbDurability, ReadOnlyTable, ReadTransaction,
    ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageError, TableDefinition,
    TableHandle, WriteTransaction,
};

use super::{Durability, KeyValue, ReadTxn, ScanFn, Store, Table, WriteTxn};
use crate::error::{Error, Result};

type Bytes = &'static [u8];
type ReadTable = ReadOnlyTable<Bytes, Bytes>;
type Entry<'a> =
    std::result::Result<(AccessGuard<'a, Bytes>, AccessGuard<'a, Bytes>), StorageError>;

/// Entries copied per table open by a write-transaction scan: starts small so short or
/// early-stopped scans stay cheap, then doubles.
const WRITE_SCAN_FIRST_BATCH: usize = 32;
const WRITE_SCAN_MAX_BATCH: usize = 4096;
/// A batch also ends once it holds this many key+value bytes (it always holds one entry).
const WRITE_SCAN_BATCH_BYTES: usize = 1 << 20;

/// Shards of the snapshot cache: the n-th thread that reads uses shard n % SNAPSHOT_SHARDS.
const SNAPSHOT_SHARDS: usize = 64;

fn definition(table: Table) -> TableDefinition<'static, Bytes, Bytes> {
    TableDefinition::new(table.name())
}

pub struct RedbStore {
    /// Declared before `db`: cached snapshots are released before the database closes.
    snapshots: SnapshotCache,
    /// Serializes write transactions from `begin_write` to the end of their commit window.
    writer: Mutex<()>,
    db: Database,
    path: PathBuf,
}

impl RedbStore {
    /// Open or create the database file. Creates every table of `Table::ALL`
    /// so that read transactions never observe a missing table.
    pub fn open(path: impl AsRef<Path>, cache_bytes: usize) -> Result<RedbStore> {
        let path = path.as_ref().to_path_buf();
        let mut builder = Builder::new();
        builder.set_cache_size(cache_bytes);
        #[cfg(windows)]
        let db = builder.create_with_backend(file::PooledFile::open(&path)?);
        #[cfg(not(windows))]
        let db = builder.create(&path);
        let db = db.map_err(Error::backend)?;
        create_tables(&db)?;
        Ok(RedbStore {
            snapshots: SnapshotCache::new(),
            writer: Mutex::new(()),
            db,
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read snapshots currently kept for reuse (diagnostics and tests): at most one per shard,
    /// none right after a commit started.
    pub fn cached_snapshots(&self) -> usize {
        self.snapshots
            .shards
            .iter()
            .filter(|shard| shard.lock().is_some())
            .count()
    }
}

/// Opens every table in one write transaction (which also checks the types of existing
/// ones) and commits only when some table was missing, so reopening costs no fsync.
fn create_tables(db: &Database) -> Result<()> {
    let txn = db.begin_write().map_err(Error::backend)?;
    let existing: Vec<String> = txn
        .list_tables()
        .map_err(Error::backend)?
        .map(|h| h.name().to_string())
        .collect();
    let mut missing = false;
    for table in Table::ALL {
        missing |= !existing.iter().any(|name| name == table.name());
        txn.open_table(definition(table)).map_err(Error::backend)?;
    }
    if missing {
        txn.commit().map_err(Error::backend)
    } else {
        txn.abort().map_err(Error::backend)
    }
}

/// `compact` rewrites a table whose pages are at least this share free space (percent)...
const REBUILD_MIN_FREE_PERCENT: u64 = 25;
/// ... that spans at least this many bytes of pages ...
const REBUILD_MIN_BYTES: u64 = 64 << 10;
/// ... and whose leaves hold at least this many entries on average (a table of values that
/// each fill a leaf of their own gains nothing from a rewrite) ...
const REBUILD_MIN_ENTRIES_PER_LEAF: u64 = 2;
/// ... and keeps the rewrite only if it is at least this much smaller (percent); otherwise
/// the rewriting transaction is aborted and the table stays as it was.
const REBUILD_MIN_GAIN_PERCENT: u64 = 10;
/// The copy a rewrite builds; it only exists inside the rewriting transaction.
const REBUILD_TABLE: &str = "babeldb-rebuild";

/// Bytes of the pages of a table (entries, their overhead and the free space around them).
fn page_bytes(t: &impl ReadableTableMetadata) -> Result<u64> {
    let s = t.stats().map_err(Error::backend)?;
    Ok(s.stored_bytes() + s.metadata_bytes() + s.fragmented_bytes())
}

/// Rewrites, each in one transaction, the tables that meet the `REBUILD_*` thresholds: copied
/// in ascending key order into an empty table (redb then fills every leaf before starting the
/// next one), which then replaces the original. Content and table names are unchanged.
fn rebuild_fragmented_tables(db: &Database) -> Result<()> {
    let copy = TableDefinition::<Bytes, Bytes>::new(REBUILD_TABLE);
    for table in Table::ALL {
        let txn = db.begin_write().map_err(Error::backend)?;
        let packed_smaller = {
            let original = txn.open_table(definition(table)).map_err(Error::backend)?;
            let s = original.stats().map_err(Error::backend)?;
            let bytes = s.stored_bytes() + s.metadata_bytes() + s.fragmented_bytes();
            let entries = original.len().map_err(Error::backend)?;
            let fragmented = bytes >= REBUILD_MIN_BYTES
                && s.fragmented_bytes() * 100 >= bytes * REBUILD_MIN_FREE_PERCENT
                && entries >= s.leaf_pages() * REBUILD_MIN_ENTRIES_PER_LEAF;
            fragmented && {
                // never exists outside this transaction; start from nothing all the same
                txn.delete_table(copy).map_err(Error::backend)?;
                let mut packed = txn.open_table(copy).map_err(Error::backend)?;
                for entry in original.range::<&[u8]>(..).map_err(Error::backend)? {
                    let (k, v) = entry.map_err(Error::backend)?;
                    packed
                        .insert(k.value(), v.value())
                        .map_err(Error::backend)?;
                }
                page_bytes(&packed)? * 100 <= bytes * (100 - REBUILD_MIN_GAIN_PERCENT)
            }
        };
        if !packed_smaller {
            txn.abort().map_err(Error::backend)?;
            continue;
        }
        txn.delete_table(definition(table))
            .map_err(Error::backend)?;
        txn.rename_table(copy, definition(table))
            .map_err(Error::backend)?;
        txn.commit().map_err(Error::backend)?;
    }
    Ok(())
}

/// Windows file access for redb (see "Parallel file reads" in the module documentation).
#[cfg(windows)]
mod file {
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::windows::fs::{FileExt, OpenOptionsExt};
    use std::path::Path;

    use ::redb::StorageBackend;
    use ::redb::backends::FileBackend;

    use crate::error::{Error, Result};

    const FILE_SHARE_READ: u32 = 1;
    const FILE_SHARE_WRITE: u32 = 2;
    const FILE_SHARE_DELETE: u32 = 4;
    /// Read handles: one per logical CPU, within these bounds.
    const MIN_READERS: usize = 4;
    const MAX_READERS: usize = 32;

    /// redb's `FileBackend` for writes, length and sync, plus read-only handles for reads.
    #[derive(Debug)]
    pub(super) struct PooledFile {
        main: FileBackend,
        readers: Vec<File>,
    }

    impl PooledFile {
        pub(super) fn open(path: &Path) -> Result<PooledFile> {
            // Shares reading only: no other handle may write while the store is open (this
            // replaces redb's byte-range locks, see the module documentation).
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_DELETE)
                .open(path)
                .map_err(Error::backend)?;
            let count = std::thread::available_parallelism()
                .map_or(MIN_READERS, |n| n.get())
                .clamp(MIN_READERS, MAX_READERS);
            let readers = (0..count)
                .map(|_| {
                    OpenOptions::new()
                        .read(true)
                        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
                        .open(path)
                })
                .collect::<io::Result<Vec<File>>>()
                .map_err(Error::backend)?;
            let main = FileBackend::new(file).map_err(Error::backend)?;
            Ok(PooledFile { main, readers })
        }
    }

    /// The lock methods keep their default (`Unsupported`): redb then runs without file locks,
    /// which it supports in its default `ExclusiveWriter` mode.
    impl StorageBackend for PooledFile {
        fn len(&self) -> io::Result<u64> {
            self.main.len()
        }

        fn read(&self, mut offset: u64, out: &mut [u8]) -> io::Result<()> {
            let file = &self.readers[super::thread_index() % self.readers.len()];
            let mut done = 0;
            while done < out.len() {
                let n = file.seek_read(&mut out[done..], offset)?;
                if n == 0 {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                }
                done += n;
                offset += n as u64;
            }
            Ok(())
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            self.main.set_len(len)
        }

        fn sync_data(&self) -> io::Result<()> {
            self.main.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            self.main.write(offset, data)
        }

        fn close(&self) -> io::Result<()> {
            self.main.close()
        }
    }
}

fn bound_slice(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    b.as_ref().map(Vec::as_slice)
}

/// Zero-copy visit of a redb range.
fn visit<'a>(entries: impl Iterator<Item = Entry<'a>>, f: &mut ScanFn<'_>) -> Result<()> {
    for entry in entries {
        let (k, v) = entry.map_err(Error::backend)?;
        if !f(k.value(), v.value())? {
            break;
        }
    }
    Ok(())
}

/// Keys and values copied out of a table, stored back to back in one buffer.
#[derive(Default)]
struct Batch {
    data: Vec<u8>,
    /// (key end, value end) offsets into `data`; each entry starts where the previous ended.
    ends: Vec<(usize, usize)>,
}

impl Batch {
    fn clear(&mut self) {
        self.data.clear();
        self.ends.clear();
    }

    /// Copies up to `limit` entries; returns true when `entries` ran out.
    fn fill<'a>(&mut self, entries: impl Iterator<Item = Entry<'a>>, limit: usize) -> Result<bool> {
        for entry in entries {
            let (k, v) = entry.map_err(Error::backend)?;
            self.data.extend_from_slice(k.value());
            let key_end = self.data.len();
            self.data.extend_from_slice(v.value());
            self.ends.push((key_end, self.data.len()));
            if self.ends.len() >= limit || self.data.len() >= WRITE_SCAN_BATCH_BYTES {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let mut start = 0;
        self.ends.iter().map(move |&(key_end, value_end)| {
            let entry = (&self.data[start..key_end], &self.data[key_end..value_end]);
            start = value_end;
            entry
        })
    }

    fn last_key(&self) -> Option<&[u8]> {
        let (key_end, _) = *self.ends.last()?;
        let start = match self.ends.len() {
            1 => 0,
            n => self.ends[n - 2].1,
        };
        Some(&self.data[start..key_end])
    }
}

// ---------------------------------------------------------------------------
// Snapshot cache (see "Read snapshot reuse" in the module documentation)
// ---------------------------------------------------------------------------

/// A redb read transaction and the tables opened in it, each at most once, on first use.
struct Snapshot {
    txn: ReadTransaction,
    tables: [OnceLock<ReadTable>; Table::ALL.len()],
}

impl Snapshot {
    fn table(&self, table: Table) -> Result<&ReadTable> {
        let slot = self
            .tables
            .get(table.index())
            .ok_or_else(|| Error::backend(format!("table {table:?} missing from Table::ALL")))?;
        if let Some(t) = slot.get() {
            return Ok(t);
        }
        let opened = self
            .txn
            .open_table(definition(table))
            .map_err(Error::backend)?;
        Ok(slot.get_or_init(|| opened))
    }
}

/// A snapshot kept for reuse, with the generation it was opened in (always even).
struct Cached {
    generation: u64,
    snapshot: Arc<Snapshot>,
}

/// Own line pair per shard, so threads using different shards never share a cache line.
#[repr(align(128))]
#[derive(Default)]
struct Shard(Mutex<Option<Cached>>);

impl Shard {
    fn lock(&self) -> MutexGuard<'_, Option<Cached>> {
        // The slot is replaced as a whole, so a panic elsewhere cannot leave it half-written.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct SnapshotCache {
    /// Commit boundaries seen so far; odd while a commit runs.
    generation: AtomicU64,
    shards: Box<[Shard]>,
}

impl SnapshotCache {
    fn new() -> SnapshotCache {
        SnapshotCache {
            generation: AtomicU64::new(0),
            shards: (0..SNAPSHOT_SHARDS).map(|_| Shard::default()).collect(),
        }
    }

    /// The calling thread's shard.
    fn shard(&self) -> &Shard {
        &self.shards[thread_index() % self.shards.len()]
    }

    fn begin_read(&self, db: &Database) -> Result<Arc<Snapshot>> {
        let shard = self.shard();
        {
            let slot = shard.lock();
            // Loaded with the shard held (argument 4 of the module documentation).
            let generation = self.generation.load(Ordering::SeqCst);
            if let Some(cached) = slot.as_ref()
                && cached.generation == generation
            {
                return Ok(Arc::clone(&cached.snapshot));
            }
        }
        let before = self.generation.load(Ordering::SeqCst);
        let txn = db.begin_read().map_err(Error::backend)?;
        let snapshot = Arc::new(Snapshot {
            txn,
            tables: Default::default(),
        });
        if before.is_multiple_of(2) {
            let replaced = {
                let mut slot = shard.lock();
                // Unchanged since before `begin_read`: no commit started in between (the
                // snapshot holds exactly S(before)), nor emptied this shard since.
                if self.generation.load(Ordering::SeqCst) == before {
                    slot.replace(Cached {
                        generation: before,
                        snapshot: Arc::clone(&snapshot),
                    })
                } else {
                    None
                }
            };
            // may end a redb read transaction (its tracker lock): not under the shard lock
            drop(replaced);
        }
        Ok(snapshot)
    }

    /// Opens a commit window: the generation becomes odd, which stops every reuse, and the
    /// cached snapshots are released so that they do not pin the pages the commit frees. The
    /// window closes (generation even again) when the returned guard drops, also on error or
    /// panic.
    fn commit_window(&self) -> CommitWindow<'_> {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.clear();
        CommitWindow(self)
    }

    /// Releases every cached snapshot.
    fn clear(&self) {
        for shard in self.shards.iter() {
            let released = shard.lock().take();
            drop(released);
        }
    }
}

struct CommitWindow<'a>(&'a SnapshotCache);

impl Drop for CommitWindow<'_> {
    fn drop(&mut self) {
        self.0.generation.fetch_add(1, Ordering::SeqCst);
    }
}

/// Position of the calling thread among the threads that have read from any `RedbStore`
/// (threads that start reading at about the same time get consecutive shards).
fn thread_index() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static INDEX: usize = NEXT.fetch_add(1, Ordering::Relaxed);
    }
    // During thread teardown the index may be gone: any shard is correct, only slower.
    INDEX.try_with(|index| *index).unwrap_or(0)
}

/// Read transaction over a consistent snapshot. The snapshot may be shared with earlier read
/// transactions of the same thread when nothing was committed in between (see the module
/// documentation); it stays valid for as long as this transaction lives.
pub struct RedbRead {
    snapshot: Arc<Snapshot>,
}

impl ReadTxn for RedbRead {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = self
            .snapshot
            .table(table)?
            .get(key)
            .map_err(Error::backend)?;
        Ok(guard.map(|v| v.value().to_vec()))
    }

    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()> {
        let range = self
            .snapshot
            .table(table)?
            .range::<&[u8]>((start, end))
            .map_err(Error::backend)?;
        if reverse {
            visit(range.rev(), f)
        } else {
            visit(range, f)
        }
    }

    fn len(&self, table: Table) -> Result<u64> {
        self.snapshot.table(table)?.len().map_err(Error::backend)
    }
}

// ---------------------------------------------------------------------------
// Write transactions
// ---------------------------------------------------------------------------

/// Write transaction. Dropping it without `commit` aborts it: redb 4.3 rolls back an
/// uncompleted `WriteTransaction` in its `Drop`.
pub struct RedbWrite<'a> {
    /// Dropped first, so an aborted transaction is rolled back before `_writer` is released.
    txn: WriteTransaction,
    snapshots: &'a SnapshotCache,
    _writer: MutexGuard<'a, ()>,
}

impl RedbWrite<'_> {
    fn open(&self, table: Table) -> Result<::redb::Table<'_, Bytes, Bytes>> {
        self.txn
            .open_table(definition(table))
            .map_err(Error::backend)
    }
}

impl ReadTxn for RedbWrite<'_> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let t = self.open(table)?;
        let value = t
            .get(key)
            .map_err(Error::backend)?
            .map(|v| v.value().to_vec());
        Ok(value)
    }

    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()> {
        let mut lower = start.map(<[u8]>::to_vec);
        let mut upper = end.map(<[u8]>::to_vec);
        let mut batch = Batch::default();
        let mut limit = WRITE_SCAN_FIRST_BATCH;
        loop {
            batch.clear();
            let exhausted = {
                let t = self.open(table)?;
                let range = t
                    .range::<&[u8]>((bound_slice(&lower), bound_slice(&upper)))
                    .map_err(Error::backend)?;
                if reverse {
                    batch.fill(range.rev(), limit)?
                } else {
                    batch.fill(range, limit)?
                }
            };
            for (k, v) in batch.iter() {
                if !f(k, v)? {
                    return Ok(());
                }
            }
            let resume = match batch.last_key() {
                Some(last) if !exhausted => Bound::Excluded(last.to_vec()),
                _ => return Ok(()),
            };
            if reverse {
                upper = resume;
            } else {
                lower = resume;
            }
            limit = (limit * 2).min(WRITE_SCAN_MAX_BATCH);
        }
    }

    fn len(&self, table: Table) -> Result<u64> {
        self.open(table)?.len().map_err(Error::backend)
    }

    /// One table open for the whole batch (`get` pays one per key).
    fn get_many(&self, table: Table, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        let t = self.open(table)?;
        keys.iter()
            .map(|&key| {
                let value = t.get(key).map_err(Error::backend)?;
                Ok(value.map(|v| v.value().to_vec()))
            })
            .collect()
    }
}

impl WriteTxn for RedbWrite<'_> {
    fn put(&mut self, table: Table, key: &[u8], value: &[u8]) -> Result<()> {
        let mut t = self.open(table)?;
        t.insert(key, value).map_err(Error::backend)?;
        Ok(())
    }

    /// One table open for the whole batch (`put` pays one per entry).
    fn put_many(
        &mut self,
        table: Table,
        entries: &mut dyn Iterator<Item = KeyValue<'_>>,
    ) -> Result<()> {
        let mut t = self.open(table)?;
        for (key, value) in entries {
            t.insert(key, value).map_err(Error::backend)?;
        }
        Ok(())
    }

    fn remove(&mut self, table: Table, key: &[u8]) -> Result<bool> {
        let mut t = self.open(table)?;
        let existed = t.remove(key).map_err(Error::backend)?.is_some();
        Ok(existed)
    }

    fn commit(self, durability: Durability) -> Result<()> {
        let RedbWrite {
            mut txn,
            snapshots,
            _writer: writer,
        } = self;
        txn.set_durability(match durability {
            Durability::Immediate => RedbDurability::Immediate,
            Durability::Deferred => RedbDurability::None,
        })
        .map_err(Error::backend)?;
        let window = snapshots.commit_window();
        let committed = txn.commit().map_err(Error::backend);
        // Close the window, then let the next writer in (see the module documentation).
        drop(window);
        drop(writer);
        committed
    }
}

impl Store for RedbStore {
    type Read<'a> = RedbRead;
    type Write<'a> = RedbWrite<'a>;

    fn begin_read(&self) -> Result<RedbRead> {
        Ok(RedbRead {
            snapshot: self.snapshots.begin_read(&self.db)?,
        })
    }

    fn begin_write(&self) -> Result<RedbWrite<'_>> {
        let writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let txn = self.db.begin_write().map_err(Error::backend)?;
        Ok(RedbWrite {
            txn,
            snapshots: &self.snapshots,
            _writer: writer,
        })
    }

    /// Rewrites the fragmented tables in key order (see "File size" in the module
    /// documentation), then runs redb compaction; `Ok(true)` means it ran (whether redb moved
    /// any page is not part of the contract). Fails while any `RedbRead` is still alive (the
    /// snapshots cached for reuse are released first); the tables may have been rewritten by
    /// then (same contents).
    fn compact(&mut self) -> Result<bool> {
        self.snapshots.clear();
        let result = rebuild_fragmented_tables(&self.db)
            .and_then(|()| self.db.compact().map_err(Error::backend));
        // Both commit: nothing opened before them may be reused (none is cached, and a live
        // reader makes this fail; this keeps the rule independent of that).
        self.snapshots.generation.fetch_add(2, Ordering::SeqCst);
        result?;
        Ok(true)
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn backend_name(&self) -> &'static str {
        "redb"
    }
}
