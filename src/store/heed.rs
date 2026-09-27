//! LMDB backend through heed 0.22 (crate feature `lmdb`), for the backend
//! comparison of stage 8. No winner is presumed: this module exposes LMDB's
//! real behaviour, including where it differs from redb.
//!
//! # Layout
//!
//! [`HeedStore::open`] creates the directory if needed and opens an LMDB
//! environment in it: `data.mdb` (the B+tree pages) and `lock.mdb` (reader
//! table and lock state). Every [`Table`] is a named LMDB database
//! (`Table::name()`), created in one write transaction at open time so that
//! read transactions never observe a missing table.
//!
//! # One environment handle per process
//!
//! LMDB forbids opening the same environment twice in one process (its locks
//! are per process). heed 0.22 enforces this: `EnvOpenOptions::open` records
//! the canonicalized directory in a process-wide registry and fails with
//! `EnvAlreadyOpened` while any handle to it is alive; the entry is removed
//! when the last `Env` clone is dropped. Canonicalization resolves `.`/`..`,
//! symlinks and junctions, and on Windows letter case and 8.3 short names, so
//! no extra guard is needed here; the failure is reported as
//! [`Error::Backend`]. A `HeedStore` holds the only `Env` clone, so dropping
//! it closes the environment and the directory can be reopened immediately.
//!
//! # Transactions
//!
//! - Read transactions are opened with `MDB_NOTLS`
//!   (`read_txn_without_tls`): a thread may hold several at once (with TLS a
//!   second one on the same thread fails with `MDB_BAD_RSLOT`) and
//!   [`HeedRead`] is `Send`. Each live read transaction takes one slot of the
//!   reader table (LMDB default: 126 slots; more concurrent readers fail with
//!   [`Error::LimitExceeded`]); with `MDB_NOTLS` claiming a slot takes LMDB's
//!   reader mutex on every `begin_read`.
//! - Writers: LMDB serializes them with a mutex held from `mdb_txn_begin` to
//!   commit/abort. On Windows that mutex is a recursive Win32 mutex, so a
//!   second `Env::write_txn` on the same thread *succeeds* and re-initializes
//!   the one internal write transaction under the first (observed with heed
//!   0.22.1 on Windows 11). `HeedStore` therefore adds an in-process writer
//!   lock: `begin_write` blocks while another thread writes and fails with
//!   [`Error::Backend`] if the calling thread already holds a write
//!   transaction (the alternative is a deadlock or silent aliasing).
//!   [`HeedWrite`] is `!Send` because the LMDB writer mutex is owned by the
//!   thread that began the transaction.
//!
//! # Durability
//!
//! [`Durability::Immediate`] is LMDB's synchronous commit. In this LMDB
//! (mdb.master via lmdb-master-sys 0.2.6) on Windows, dirty pages are written
//! through a `FILE_FLAG_WRITE_THROUGH | FILE_FLAG_OVERLAPPED` handle and the
//! meta page through a second write-through handle; `FlushFileBuffers` is not
//! called. On Unix it is `fdatasync` plus an `O_DSYNC` meta-page write.
//! (redb calls `FlushFileBuffers`/`fsync` instead; keep this in mind when
//! comparing commit latencies.)
//!
//! [`Durability::Deferred`] is committed exactly like `Immediate`, as the
//! contract requires of backends without an equivalent. LMDB does have
//! per-transaction `MDB_NOSYNC`/`MDB_NOMETASYNC`, but only as flags of
//! `mdb_txn_begin`, which heed 0.22 does not expose, and the durability is only
//! known at commit time. The env-wide alternative, `Env::set_flags`, is
//! `unsafe` and writes `me_flags` while reader threads read it, and
//! `MDB_NOSYNC` can *corrupt* the database on an OS crash (the meta page may
//! reach the disk before the pages it points to), which is weaker than the
//! contract's "may be lost".
//!
//! # Map size
//!
//! The map size is a hard cap on `data.mdb`: when it is reached, `put` or
//! `commit` fails with [`Error::LimitExceeded`] and the transaction is aborted
//! (committed data is intact). Reopen with a larger `map_size` to grow it: LMDB
//! uses the larger of the configured and the already used size (see
//! [`HeedStore::map_size`]). The requested size is rounded up to a multiple of
//! 64 KiB (LMDB needs a multiple of the OS page size).
//!
//! Measured on Windows 11 (Ryzen 7 5700, debug build): with map sizes of 1 GiB
//! and 64 GiB, `data.mdb` is not preallocated (8 KiB after open, 24 KiB after
//! 4 KB of values, ~1.4 MB after 1 MB; apparent = allocated size), because
//! this LMDB maps the file with `SEC_RESERVE`/`MEM_RESERVE` and grows it
//! incrementally unless built with `MDB_FIXEDSIZE`. The map is not free,
//! though: an open environment adds about `map_size / 512 + 3 MiB` to the
//! process commit charge (private bytes; +5 MiB at 1 GiB, +35 MiB at 16 GiB,
//! +131 MiB at 64 GiB, +2 GiB at 1 TiB; consistent with 8 bytes of
//! page-table entry per 4 KiB page of the view), released on close, while
//! the working set does not change. Size the map to the data set when process
//! memory is compared. See [`DEFAULT_MAP_SIZE`].
//!
//! # Keys and values
//!
//! LMDB rejects empty keys: every point operation with an empty key fails with
//! [`Error::InvalidArgument`] (empty values are fine). Keys are limited to
//! [`HeedStore::max_key_size`] bytes (511 with heed's default features): a
//! longer key fails `put` with [`Error::LimitExceeded`], while `get`/`remove`
//! report it as absent. This is below the engine's default `max_key_len`
//! (4096), and `format::history_key` needs up to `2 * len + 10` bytes. Values
//! are limited to `u32::MAX` bytes. Scan bounds may be empty or longer than
//! the key limit: an empty start bound means "from the first key", an empty
//! end bound selects nothing, and inverted or empty ranges visit nothing.
//!
//! # Space
//!
//! [`Store::files`] reports `data.mdb` and `lock.mdb`. `lock.mdb` holds no
//! data and is recreated on demand, but it is disk space the backend uses
//! (8128 bytes, 8 KiB allocated, with 126 reader slots), so it is counted.
//! LMDB reuses freed pages but never shrinks `data.mdb`, and a long-lived read
//! transaction prevents the reuse of pages freed after it began.
//! [`Store::compact`] returns `Ok(false)`: a compacting copy
//! (`mdb_env_copy2` with `MDB_CP_COMPACT`) would have to replace `data.mdb`
//! with the environment closed, which is not crash-atomic on Windows and
//! breaks any other process that has the environment open.

use std::marker::PhantomData;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex, PoisonError};
use std::thread::{self, ThreadId};

use ::heed::types::Bytes;
use ::heed::{
    Database, Env, EnvOpenOptions, Error as HeedError, MdbError, RoTxn, RwTxn, WithoutTls,
};

use super::{Durability, ReadTxn, ScanFn, Store, Table, WriteTxn};
use crate::error::{Error, Result};

/// Default map size for callers without a better estimate (64-bit targets).
///
/// It caps `data.mdb` at 16 GiB, enough for the benchmark data sets, while an
/// open store costs about 35 MiB of process commit on Windows (see the module
/// docs). Larger-than-RAM runs must pass an explicit, larger `map_size`.
#[cfg(target_pointer_width = "64")]
pub const DEFAULT_MAP_SIZE: usize = 16 << 30;

/// Default map size for callers without a better estimate (32-bit targets).
#[cfg(not(target_pointer_width = "64"))]
pub const DEFAULT_MAP_SIZE: usize = 1 << 30;

/// Named LMDB databases the environment can hold (`Table::ALL` has 9).
const MAX_DBS: u32 = 16;

/// Map sizes are rounded up to this multiple (a multiple of every common OS
/// page size: 4, 16 and 64 KiB).
const MAP_SIZE_GRANULE: usize = 64 * 1024;

/// Largest value LMDB stores (`MAXDATASIZE`).
const MAX_VALUE_SIZE: usize = u32::MAX as usize;

type Db = Database<Bytes, Bytes>;

/// LMDB environment with one named database per [`Table`].
///
/// LMDB memory-maps `data.mdb`: other programs must not modify the files of
/// an open environment, and the directory must be on a local file system.
pub struct HeedStore {
    dir: PathBuf,
    env: Env<WithoutTls>,
    /// Indexed by `Table::index()`.
    dbs: Vec<Db>,
    map_size: usize,
    max_key_size: usize,
    writer: WriterLock,
}

impl HeedStore {
    /// Open or create an LMDB environment in `dir` with the given map size
    /// (bytes, rounded up to a multiple of 64 KiB; see [`DEFAULT_MAP_SIZE`]).
    pub fn open(dir: impl AsRef<Path>, map_size: usize) -> Result<HeedStore> {
        let dir = dir.as_ref();
        let map_size = round_map_size(map_size)?;
        // heed converts the path with `to_str().unwrap()` on Windows.
        #[cfg(windows)]
        if dir.to_str().is_none() {
            return Err(Error::InvalidArgument(format!(
                "LMDB needs a valid Unicode path on Windows: {}",
                dir.display()
            )));
        }
        std::fs::create_dir_all(dir)?;

        let mut options = EnvOpenOptions::new().read_txn_without_tls();
        options.map_size(map_size).max_dbs(MAX_DBS);
        // SAFETY: `open` is unsafe because LMDB memory-maps `data.mdb`, and
        // modifying or truncating the file outside LMDB's locking protocol
        // while it is mapped is undefined behaviour. Invariants upheld here:
        // - the environment is opened at most once per process: heed keeps a
        //   process-wide registry of canonicalized environment paths and fails
        //   with `EnvAlreadyOpened` on a second open (mapped to an error below);
        // - no unsafe flag is set (`NO_LOCK`, `NO_SYNC`, `NO_META_SYNC`,
        //   `WRITE_MAP`), so `lock.mdb` coordinates every process that opens
        //   the directory;
        // - this module never touches `data.mdb`/`lock.mdb` except through LMDB.
        // What remains (no foreign writer, local file system) is inherent to any
        // mmap-based store and documented on `HeedStore`.
        let env = unsafe { options.open(dir) }.map_err(|e| match e {
            HeedError::EnvAlreadyOpened => Error::Backend(format!(
                "LMDB environment {} is already open in this process (LMDB allows one handle per \
                 environment and process)",
                dir.display()
            )),
            e => map_heed_error(e, map_size),
        })?;

        let mut wtxn = env.write_txn().map_err(|e| map_heed_error(e, map_size))?;
        let mut dbs = Vec::with_capacity(Table::ALL.len());
        for table in Table::ALL {
            let db = env
                .create_database::<Bytes, Bytes>(&mut wtxn, Some(table.name()))
                .map_err(|e| map_heed_error(e, map_size))?;
            dbs.push(db);
        }
        wtxn.commit().map_err(|e| map_heed_error(e, map_size))?;

        // LMDB raises the configured size to the size already in use.
        let map_size = env.info().map_size;
        let max_key_size = env.max_key_size();
        Ok(HeedStore {
            dir: dir.to_path_buf(),
            env,
            dbs,
            map_size,
            max_key_size,
            writer: WriterLock::default(),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Effective map size in bytes: the requested size rounded up to 64 KiB,
    /// or the size already used by `data.mdb` if that is larger.
    pub fn map_size(&self) -> usize {
        self.map_size
    }

    /// Longest key `put` accepts (LMDB `MDB_MAXKEYSIZE`, 511 by default).
    pub fn max_key_size(&self) -> usize {
        self.max_key_size
    }

    fn db(&self, table: Table) -> Db {
        self.dbs[table.index()]
    }

    fn err(&self, e: HeedError) -> Error {
        map_heed_error(e, self.map_size)
    }
}

fn round_map_size(map_size: usize) -> Result<usize> {
    if map_size == 0 {
        return Err(Error::InvalidArgument("LMDB map_size must be > 0".into()));
    }
    map_size
        .checked_next_multiple_of(MAP_SIZE_GRANULE)
        .ok_or_else(|| Error::InvalidArgument(format!("LMDB map_size {map_size} is too large")))
}

/// heed errors become [`Error::Backend`], except LMDB's resource limits, which
/// become [`Error::LimitExceeded`].
fn map_heed_error(e: HeedError, map_size: usize) -> Error {
    match e {
        HeedError::Mdb(MdbError::MapFull) => Error::LimitExceeded(format!(
            "LMDB map is full: data.mdb reached map_size ({map_size} bytes); reopen with a \
             larger map_size"
        )),
        HeedError::Mdb(MdbError::TxnFull) => Error::LimitExceeded(
            "LMDB transaction has too many dirty pages; split it into smaller transactions".into(),
        ),
        HeedError::Mdb(MdbError::ReadersFull) => Error::LimitExceeded(
            "LMDB reader table is full: too many concurrent read transactions".into(),
        ),
        HeedError::Mdb(MdbError::BadValSize) => {
            Error::LimitExceeded("key or value size not supported by LMDB".into())
        }
        e => Error::backend(e),
    }
}

fn check_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidArgument(
            "LMDB does not support empty keys".into(),
        ));
    }
    Ok(())
}

/// `(start, end)` key bounds of a scan.
type KeyRange<'k> = (Bound<&'k [u8]>, Bound<&'k [u8]>);

/// Rewrites bounds LMDB cannot position on and detects ranges that contain
/// no key; `None` means "visit nothing".
fn lmdb_range<'k>(start: Bound<&'k [u8]>, end: Bound<&'k [u8]>) -> Option<KeyRange<'k>> {
    // `put` rejects empty keys and every non-empty key sorts after `[]`, so an
    // empty start bound excludes nothing and an empty end bound admits nothing
    // (LMDB fails with MDB_BAD_VALSIZE when asked to position on `[]`).
    let start = match start {
        Bound::Included([]) | Bound::Excluded([]) => Bound::Unbounded,
        b => b,
    };
    if matches!(end, Bound::Included([]) | Bound::Excluded([])) {
        return None;
    }
    let empty = match (start, end) {
        (Bound::Included(s), Bound::Included(e)) => s > e,
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => {
            s >= e
        }
        _ => false,
    };
    (!empty).then_some((start, end))
}

fn txn_get(
    store: &HeedStore,
    txn: &RoTxn<'_>,
    table: Table,
    key: &[u8],
) -> Result<Option<Vec<u8>>> {
    check_key(key)?;
    let value = store.db(table).get(txn, key).map_err(|e| store.err(e))?;
    Ok(value.map(<[u8]>::to_vec))
}

fn txn_scan(
    store: &HeedStore,
    txn: &RoTxn<'_>,
    table: Table,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
    f: &mut ScanFn<'_>,
) -> Result<()> {
    let Some(range) = lmdb_range(start, end) else {
        return Ok(());
    };
    let db = store.db(table);
    if reverse {
        visit(
            store,
            db.rev_range(txn, &range).map_err(|e| store.err(e))?,
            f,
        )
    } else {
        visit(store, db.range(txn, &range).map_err(|e| store.err(e))?, f)
    }
}

fn visit<'t>(
    store: &HeedStore,
    entries: impl Iterator<Item = ::heed::Result<(&'t [u8], &'t [u8])>>,
    f: &mut ScanFn<'_>,
) -> Result<()> {
    for entry in entries {
        let (key, value) = entry.map_err(|e| store.err(e))?;
        if !f(key, value)? {
            break;
        }
    }
    Ok(())
}

fn txn_len(store: &HeedStore, txn: &RoTxn<'_>, table: Table) -> Result<u64> {
    store.db(table).len(txn).map_err(|e| store.err(e))
}

/// Read-only snapshot (an LMDB `MDB_NOTLS` read transaction).
pub struct HeedRead<'a> {
    store: &'a HeedStore,
    txn: RoTxn<'a, WithoutTls>,
}

/// The single write transaction; dropping it without `commit` aborts.
pub struct HeedWrite<'a> {
    // Drop order matters: the LMDB transaction aborts (releasing LMDB's writer
    // mutex) before `_writer` releases the in-process writer lock.
    txn: RwTxn<'a>,
    store: &'a HeedStore,
    _writer: WriterGuard<'a>,
    /// LMDB's writer mutex belongs to the thread that began the transaction.
    _not_send: PhantomData<*const ()>,
}

impl ReadTxn for HeedRead<'_> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        txn_get(self.store, &self.txn, table, key)
    }

    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()> {
        txn_scan(self.store, &self.txn, table, start, end, reverse, f)
    }

    fn len(&self, table: Table) -> Result<u64> {
        txn_len(self.store, &self.txn, table)
    }
}

impl ReadTxn for HeedWrite<'_> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        txn_get(self.store, &self.txn, table, key)
    }

    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()> {
        txn_scan(self.store, &self.txn, table, start, end, reverse, f)
    }

    fn len(&self, table: Table) -> Result<u64> {
        txn_len(self.store, &self.txn, table)
    }
}

impl WriteTxn for HeedWrite<'_> {
    fn put(&mut self, table: Table, key: &[u8], value: &[u8]) -> Result<()> {
        check_key(key)?;
        if key.len() > self.store.max_key_size {
            return Err(Error::LimitExceeded(format!(
                "key of {} bytes exceeds the LMDB maximum key size ({} bytes)",
                key.len(),
                self.store.max_key_size
            )));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::LimitExceeded(format!(
                "value of {} bytes exceeds the LMDB maximum value size ({MAX_VALUE_SIZE} bytes)",
                value.len()
            )));
        }
        let store = self.store;
        store
            .db(table)
            .put(&mut self.txn, key, value)
            .map_err(|e| store.err(e))
    }

    fn remove(&mut self, table: Table, key: &[u8]) -> Result<bool> {
        check_key(key)?;
        let store = self.store;
        store
            .db(table)
            .delete(&mut self.txn, key)
            .map_err(|e| store.err(e))
    }

    fn commit(self, durability: Durability) -> Result<()> {
        match durability {
            // No per-commit durability control is reachable through heed: a
            // deferred commit is a normal synchronous commit (module docs).
            Durability::Immediate | Durability::Deferred => {}
        }
        let HeedWrite {
            txn,
            store,
            _writer,
            ..
        } = self;
        txn.commit().map_err(|e| store.err(e))
    }
}

impl Store for HeedStore {
    type Read<'a> = HeedRead<'a>;
    type Write<'a> = HeedWrite<'a>;

    fn begin_read(&self) -> Result<HeedRead<'_>> {
        let txn = self.env.read_txn().map_err(|e| self.err(e))?;
        Ok(HeedRead { store: self, txn })
    }

    fn begin_write(&self) -> Result<HeedWrite<'_>> {
        let writer = self.writer.acquire()?;
        let txn = self.env.write_txn().map_err(|e| self.err(e))?;
        Ok(HeedWrite {
            txn,
            store: self,
            _writer: writer,
            _not_send: PhantomData,
        })
    }

    fn compact(&mut self) -> Result<bool> {
        Ok(false)
    }

    fn files(&self) -> Vec<PathBuf> {
        let dir = self.env.path();
        vec![dir.join("data.mdb"), dir.join("lock.mdb")]
    }

    fn backend_name(&self) -> &'static str {
        "lmdb"
    }
}

/// In-process writer lock: one write transaction at a time, and a clear
/// error instead of a deadlock (or, on Windows, LMDB's recursive writer mutex
/// letting it through) when a thread begins a second one.
#[derive(Default)]
struct WriterLock {
    owner: Mutex<Option<ThreadId>>,
    released: Condvar,
}

impl WriterLock {
    fn acquire(&self) -> Result<WriterGuard<'_>> {
        let me = thread::current().id();
        // The mutex only guards `owner` and nothing panics while holding it.
        let mut owner = self.owner.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            match *owner {
                None => break,
                Some(thread) if thread == me => {
                    return Err(Error::backend(
                        "this thread already holds the LMDB write transaction; commit or drop it \
                         before beginning another",
                    ));
                }
                Some(_) => {
                    owner = self
                        .released
                        .wait(owner)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
        *owner = Some(me);
        Ok(WriterGuard { lock: self })
    }
}

struct WriterGuard<'a> {
    lock: &'a WriterLock,
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        *self
            .lock
            .owner
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        self.lock.released.notify_one();
    }
}
