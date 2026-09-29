//! Python binding of babeldb: the native module `babeldb._native`.
//!
//! A `Db` is one database directory opened the way the README recommends for
//! embedding: `Db::open_fjall_wal` (fjall + write-through WAL). Single writes
//! (`put`, `delete`) go through one `GroupCommitter` per database, so writers
//! of many threads share commits; reads go straight to the database; `batch`
//! is one atomic `Db::write_batch`. Every database call runs with the GIL
//! released.
//!
//! Lifetime: the open database sits behind an `RwLock`. A call holds the read
//! lock while it runs; `close` takes the write lock, so it waits for the calls
//! in flight, then shuts the committer down (drains its queue and, in buffered
//! mode, makes it durable) and drops the database. A process-wide registry of
//! open directories refuses a second open of the same directory with a clear
//! error instead of reaching fjall's lock file.

use std::collections::HashMap;
use std::ops::Bound as KeyBound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::Duration;

use babeldb::config::WalConfig;
use babeldb::engine::FjallWalDb;
use babeldb::scale::chat;
use babeldb::scale::group_commit::{GroupCommitConfig, GroupCommitter, WriteDurability};
use babeldb::{BatchOp, Config, Db, Error, Expect, Revision, ScanOptions};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyList, PyMemoryView, PyString, PyTuple};

create_exception!(
    babeldb,
    BabelError,
    PyException,
    "Base class of every error raised by babeldb."
);
create_exception!(
    babeldb,
    ConflictError,
    BabelError,
    "A write expectation failed (if_absent=True on an existing key, or an if_revision that is not the current revision); nothing was written."
);
create_exception!(
    babeldb,
    ClosedError,
    BabelError,
    "The database is closed: every method except close() raises it."
);
create_exception!(
    babeldb,
    InvalidArgumentError,
    BabelError,
    "An argument was rejected: empty key or key over 4096 bytes, value too large, unknown durability, conflicting options, malformed batch operation."
);

/// Buffered mode: acknowledged writes become durable within this interval.
const BUFFERED_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// Buffered mode: at most this many key + value bytes wait to become durable.
const BUFFERED_MAX_PENDING_BYTES: usize = 64 << 20;

const CLOSED_MESSAGE: &str = "the database is closed";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Failure of a call made without the GIL (converted to a Python exception
/// once the GIL is held again).
enum Failure {
    Db(Error),
    Closed,
    Invalid(String),
    AlreadyOpen(PathBuf),
}

impl From<std::io::Error> for Failure {
    fn from(e: std::io::Error) -> Self {
        Failure::Db(Error::Io(e))
    }
}

impl From<Failure> for PyErr {
    fn from(f: Failure) -> PyErr {
        match f {
            Failure::Db(e) => db_error(e),
            Failure::Closed => ClosedError::new_err(CLOSED_MESSAGE),
            Failure::Invalid(msg) => InvalidArgumentError::new_err(msg),
            Failure::AlreadyOpen(dir) => BabelError::new_err(format!(
                "the database directory {} is already open in this process (one Db per directory \
                 per process: share the open Db between threads, or close it first)",
                dir.display()
            )),
        }
    }
}

/// `RevisionConflict` -> `ConflictError`; `InvalidArgument` and
/// `LimitExceeded` -> `InvalidArgumentError`; everything else (I/O, backend,
/// format, integrity, ...) -> `BabelError`.
fn db_error(e: Error) -> PyErr {
    let msg = e.to_string();
    match e {
        Error::RevisionConflict { .. } => ConflictError::new_err(msg),
        Error::InvalidArgument(_) | Error::LimitExceeded(_) => InvalidArgumentError::new_err(msg),
        _ => BabelError::new_err(msg),
    }
}

// ---------------------------------------------------------------------------
// Open databases
// ---------------------------------------------------------------------------

/// An open database. Field order is drop order: the committer (already shut
/// down by `Core::close`) goes before the database.
struct Open {
    writer: GroupCommitter,
    db: Arc<FjallWalDb>,
}

struct Core {
    /// Canonical directory: the registry key, shown as `Db.path`.
    dir: PathBuf,
    durability: &'static str,
    /// Set as soon as `close` starts (read without any lock): new calls fail
    /// at once while `close` waits for the calls in flight.
    closed: AtomicBool,
    /// Held for the whole of `close`, so a concurrent `close` returns only
    /// once the directory is released.
    close_lock: Mutex<()>,
    /// `None` once closed.
    state: RwLock<Option<Open>>,
}

impl Core {
    /// Run `f` on the open database, holding the read lock for the whole
    /// call (so `close` waits for it).
    fn with<T>(&self, f: impl FnOnce(&Open) -> babeldb::Result<T>) -> Result<T, Failure> {
        if self.is_closed() {
            return Err(Failure::Closed);
        }
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        let open = state.as_ref().ok_or(Failure::Closed)?;
        f(open).map_err(Failure::Db)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Wait for the calls in flight, drain and stop the committer (buffered
    /// mode: make everything durable), drop the database, then release the
    /// directory. Idempotent.
    fn close(&self) -> Result<(), Failure> {
        let _closing = self.close_lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.closed.store(true, Ordering::Release);
        let taken = self
            .state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(open) = taken else {
            return Ok(());
        };
        let shutdown = open.writer.shutdown();
        drop(open);
        unregister(&self.dir);
        shutdown.map_err(Failure::Db)
    }
}

impl Drop for Core {
    /// A `Db` garbage-collected without `close()` is closed here.
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Directories open in this process. An entry is added before the database
/// is opened and removed after it is fully closed, so a directory is never
/// opened twice at once. Never drop a `Core` while holding this lock (its
/// `Drop` takes it).
static OPEN_DIRS: LazyLock<Mutex<HashMap<PathBuf, Weak<Core>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn registry() -> MutexGuard<'static, HashMap<PathBuf, Weak<Core>>> {
    OPEN_DIRS.lock().unwrap_or_else(PoisonError::into_inner)
}

fn unregister(dir: &Path) {
    registry().remove(dir);
}

fn write_durability(name: &str) -> Result<(&'static str, WriteDurability), Failure> {
    match name {
        "immediate" => Ok(("immediate", WriteDurability::Immediate)),
        "buffered" => Ok((
            "buffered",
            WriteDurability::Buffered {
                flush_interval: BUFFERED_FLUSH_INTERVAL,
                max_pending_bytes: BUFFERED_MAX_PENDING_BYTES,
            },
        )),
        other => Err(Failure::Invalid(format!(
            "durability must be \"immediate\" or \"buffered\", not {other:?}"
        ))),
    }
}

fn open_core(path: &Path, durability: &str) -> Result<Arc<Core>, Failure> {
    let (name, mode) = write_durability(durability)?;
    std::fs::create_dir_all(path)?;
    let dir = strip_verbatim(std::fs::canonicalize(path)?);
    {
        let mut open_dirs = registry();
        if open_dirs.contains_key(&dir) {
            return Err(Failure::AlreadyOpen(dir));
        }
        // Reserved while opening: a concurrent open of the same directory fails.
        open_dirs.insert(dir.clone(), Weak::new());
    }
    let opened = open_database(&dir, mode);
    let mut open_dirs = registry();
    match opened {
        Ok(open) => {
            let core = Arc::new(Core {
                dir: dir.clone(),
                durability: name,
                closed: AtomicBool::new(false),
                close_lock: Mutex::new(()),
                state: RwLock::new(Some(open)),
            });
            open_dirs.insert(dir, Arc::downgrade(&core));
            Ok(core)
        }
        Err(e) => {
            open_dirs.remove(&dir);
            Err(Failure::Db(e))
        }
    }
}

/// The embedding recipe of the README: fjall + WAL, writes group-committed.
fn open_database(dir: &Path, durability: WriteDurability) -> babeldb::Result<Open> {
    let db = Arc::new(Db::open_fjall_wal(dir, Config::adaptive(), WalConfig::default())?);
    let writer = GroupCommitter::new(Arc::clone(&db), GroupCommitConfig::from(durability))?;
    Ok(Open { writer, db })
}

/// `canonicalize` returns verbatim paths on Windows (`\\?\C:\x`); keep the
/// plain form (`C:\x`) of drive paths, which every API accepts.
fn strip_verbatim(path: PathBuf) -> PathBuf {
    if cfg!(windows)
        && let Some(s) = path.to_str()
        && let Some(rest) = s.strip_prefix(r"\\?\")
        && is_drive_path(rest)
    {
        return PathBuf::from(rest);
    }
    path
}

fn is_drive_path(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 3 && b[0].is_ascii_alphabetic() && b[1] == b':' && b[2] == b'\\'
}

// ---------------------------------------------------------------------------
// Argument conversion
// ---------------------------------------------------------------------------

/// A key, value or bound: `bytes`, `bytearray`, `memoryview` or `str`
/// (encoded as UTF-8). Copied: the call then runs without the GIL.
fn to_bytes(obj: &Bound<'_, PyAny>, what: &str) -> PyResult<Vec<u8>> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(s.to_cow()?.into_owned().into_bytes());
    }
    if let Ok(b) = obj.cast::<PyByteArray>() {
        return Ok(b.to_vec());
    }
    if obj.is_instance_of::<PyMemoryView>() {
        let copy = obj.call_method0("tobytes")?;
        return Ok(copy.cast::<PyBytes>()?.as_bytes().to_vec());
    }
    Err(PyTypeError::new_err(format!(
        "{what} must be bytes, bytearray, memoryview or str, not {}",
        type_name(obj)
    )))
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .name()
        .map_or_else(|_| "?".to_string(), |n| n.to_string())
}

fn put_expectation(if_absent: bool, if_revision: Option<Revision>) -> PyResult<Expect> {
    match (if_absent, if_revision) {
        (false, None) => Ok(Expect::Any),
        (true, None) => Ok(Expect::Absent),
        (false, Some(r)) => Ok(Expect::Revision(r)),
        (true, Some(_)) => Err(InvalidArgumentError::new_err(
            "put: if_absent and if_revision cannot be combined",
        )),
    }
}

fn scan_options(
    prefix: Option<&Bound<'_, PyAny>>,
    start: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
    reverse: bool,
    limit: i64,
    with_values: bool,
) -> PyResult<ScanOptions> {
    let limit = usize::try_from(limit)
        .map_err(|_| InvalidArgumentError::new_err(format!("limit must be >= 0, not {limit}")))?;
    let opts = match prefix {
        Some(prefix) => {
            if start.is_some() || end.is_some() {
                return Err(InvalidArgumentError::new_err(
                    "scan: prefix cannot be combined with start/end",
                ));
            }
            ScanOptions::prefix(&to_bytes(prefix, "prefix")?)
        }
        None => {
            let mut opts = ScanOptions::all();
            if let Some(start) = start {
                opts.start = KeyBound::Included(to_bytes(start, "start")?);
            }
            if let Some(end) = end {
                opts.end = KeyBound::Excluded(to_bytes(end, "end")?);
            }
            opts
        }
    };
    Ok(opts.reverse(reverse).limit(limit).with_values(with_values))
}

/// `(key, Some(value))` for a put, `(key, None)` for a delete.
type OwnedBatchOp = (Vec<u8>, Option<Vec<u8>>);

fn parse_batch(ops: &Bound<'_, PyAny>) -> PyResult<Vec<OwnedBatchOp>> {
    let mut parsed = Vec::new();
    for (i, item) in ops.try_iter()?.enumerate() {
        let item = item?;
        let fields: Vec<Bound<'_, PyAny>> = if let Ok(t) = item.cast::<PyTuple>() {
            t.iter().collect()
        } else if let Ok(l) = item.cast::<PyList>() {
            l.iter().collect()
        } else {
            return Err(InvalidArgumentError::new_err(format!(
                "batch op {i}: expected a tuple ('put', key, value) or ('delete', key), not {}",
                type_name(&item)
            )));
        };
        let kind = fields.first().and_then(|f| f.extract::<String>().ok());
        match (kind.as_deref(), fields.len()) {
            (Some("put"), 3) => parsed.push((
                to_bytes(&fields[1], "key")?,
                Some(to_bytes(&fields[2], "value")?),
            )),
            (Some("delete"), 2) => parsed.push((to_bytes(&fields[1], "key")?, None)),
            _ => {
                return Err(InvalidArgumentError::new_err(format!(
                    "batch op {i}: expected ('put', key, value) or ('delete', key)"
                )));
            }
        }
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// Python API
// ---------------------------------------------------------------------------

/// An open babeldb database: one directory, opened with fjall + WAL.
///
/// Db(path, durability="immediate") opens or creates the database in the
/// directory `path` (its WAL is the file `<path>.wal` next to it). Share one
/// Db between threads: every method is thread-safe and releases the GIL.
/// A directory can be open only once per process.
///
/// durability="immediate": every write is durable when the call returns
/// (concurrent writers share commits). durability="buffered": writes are
/// visible when the call returns and durable within about 100 ms (sync()
/// forces it; close() makes everything durable).
///
/// Keys and values are bytes, bytearray, memoryview or str (UTF-8); values
/// come back as bytes. Use it as a context manager to close it.
#[pyclass(frozen, module = "babeldb", name = "Db")]
struct PyDb {
    core: Arc<Core>,
}

#[pymethods]
impl PyDb {
    #[new]
    #[pyo3(signature = (path, durability = "immediate"))]
    fn new(py: Python<'_>, path: PathBuf, durability: &str) -> PyResult<Self> {
        let core = py.detach(|| open_core(&path, durability))?;
        Ok(PyDb { core })
    }

    /// Store `value` under `key`; returns the new revision (an int that grows
    /// with every write of the database).
    ///
    /// if_absent=True writes only if the key does not exist; if_revision=r
    /// writes only if the key's current revision is r (compare-and-set). A
    /// failed expectation raises ConflictError.
    #[pyo3(signature = (key, value, *, if_absent = false, if_revision = None))]
    fn put(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        if_absent: bool,
        if_revision: Option<Revision>,
    ) -> PyResult<Revision> {
        let expect = put_expectation(if_absent, if_revision)?;
        let key = to_bytes(key, "key")?;
        let value = to_bytes(value, "value")?;
        Ok(py.detach(|| self.core.with(|o| o.writer.put(key, value, expect)))?)
    }

    /// The current value of `key` as bytes, or None if it does not exist.
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let key = to_bytes(key, "key")?;
        let value = py.detach(|| self.core.with(|o| o.db.get(&key)))?;
        Ok(value.map(|v| PyBytes::new(py, &v)))
    }

    /// Delete `key`; returns whether a record was deleted (False if it did
    /// not exist). if_revision=r deletes only if the current revision is r,
    /// else raises ConflictError (also when the key does not exist).
    #[pyo3(signature = (key, *, if_revision = None))]
    fn delete(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        if_revision: Option<Revision>,
    ) -> PyResult<bool> {
        let key = to_bytes(key, "key")?;
        let expect = if_revision.map_or(Expect::Any, Expect::Revision);
        Ok(py.detach(|| self.core.with(|o| o.writer.delete(key, expect)))?)
    }

    /// Records in key order (byte-wise) as a list of (key, value) tuples.
    ///
    /// prefix: only keys starting with it. start (inclusive) / end
    /// (exclusive): a key range; cannot be combined with prefix.
    /// reverse=True: descending order. limit: at most this many records
    /// (0 = no limit); with reverse=True, the last ones.
    #[pyo3(signature = (prefix = None, *, start = None, end = None, reverse = false, limit = 0))]
    fn scan<'py>(
        &self,
        py: Python<'py>,
        prefix: Option<&Bound<'py, PyAny>>,
        start: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        reverse: bool,
        limit: i64,
    ) -> PyResult<Bound<'py, PyList>> {
        let opts = scan_options(prefix, start, end, reverse, limit, true)?;
        let items = py.detach(|| self.core.with(|o| o.db.scan(&opts)))?;
        PyList::new(
            py,
            items.into_iter().map(|item| {
                let value = item.value.unwrap_or_default();
                (PyBytes::new(py, &item.key), PyBytes::new(py, &value))
            }),
        )
    }

    /// The keys scan() would return (same arguments), without the values.
    #[pyo3(signature = (prefix = None, *, start = None, end = None, reverse = false, limit = 0))]
    fn keys<'py>(
        &self,
        py: Python<'py>,
        prefix: Option<&Bound<'py, PyAny>>,
        start: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        reverse: bool,
        limit: i64,
    ) -> PyResult<Bound<'py, PyList>> {
        let opts = scan_options(prefix, start, end, reverse, limit, false)?;
        let items = py.detach(|| self.core.with(|o| o.db.scan(&opts)))?;
        PyList::new(py, items.into_iter().map(|item| PyBytes::new(py, &item.key)))
    }

    /// Apply ("put", key, value) and ("delete", key) operations atomically,
    /// in order, in one durable commit.
    ///
    /// All or nothing: if any operation is malformed or invalid (e.g. an
    /// empty key) nothing is written and the error is raised. Durable when it
    /// returns, in both durability modes. Returns one entry per operation:
    /// the new revision for a put; for a delete, the revision of the deleted
    /// record, or None if the key did not exist.
    fn batch(&self, py: Python<'_>, ops: &Bound<'_, PyAny>) -> PyResult<Vec<Option<Revision>>> {
        let parsed = parse_batch(ops)?;
        Ok(py.detach(|| {
            let ops: Vec<BatchOp<'_>> = parsed
                .iter()
                .map(|(key, value)| match value {
                    Some(value) => BatchOp::Put {
                        key,
                        value,
                        expect: Expect::Any,
                    },
                    None => BatchOp::Delete {
                        key,
                        expect: Expect::Any,
                    },
                })
                .collect();
            self.core.with(|o| o.db.write_batch(&ops))
        })?)
    }

    /// Wait until every write acknowledged before this call is durable
    /// (buffered mode: forces it now; immediate mode: already the case).
    fn sync(&self, py: Python<'_>) -> PyResult<()> {
        Ok(py.detach(|| self.core.with(|o| o.writer.flush()))?)
    }

    /// Close the database: waits for the calls in flight, makes every
    /// acknowledged write durable and releases the directory. Idempotent;
    /// every other method then raises ClosedError.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        Ok(py.detach(|| self.core.close())?)
    }

    /// True once close() was called.
    #[getter]
    fn closed(&self) -> bool {
        self.core.is_closed()
    }

    /// The database directory (absolute).
    #[getter]
    fn path(&self) -> String {
        self.core.dir.to_string_lossy().into_owned()
    }

    /// "immediate" or "buffered".
    #[getter]
    fn durability(&self) -> &'static str {
        self.core.durability
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.core.is_closed() {
            return Err(ClosedError::new_err(CLOSED_MESSAGE));
        }
        Ok(slf)
    }

    #[pyo3(signature = (_exc_type = None, _exc_value = None, _traceback = None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!(
            "<babeldb.Db path={:?} durability={:?}{}>",
            self.core.dir.to_string_lossy(),
            self.core.durability,
            if self.core.is_closed() { " closed" } else { "" }
        )
    }
}

/// Open (or create) the database in the directory `path`; same as
/// Db(path, durability).
#[pyfunction]
#[pyo3(signature = (path, durability = "immediate"))]
fn open(py: Python<'_>, path: PathBuf, durability: &str) -> PyResult<PyDb> {
    PyDb::new(py, path, durability)
}

/// Chat message key: channel (u64 big-endian) followed by message_id (u64
/// big-endian), 16 bytes. Keys sort by channel, then by message id.
#[pyfunction]
fn message_key(py: Python<'_>, channel: u64, message_id: u64) -> Bound<'_, PyBytes> {
    PyBytes::new(py, &chat::message_key(channel, message_id))
}

/// Inverse of message_key: (channel, message_id), or None if `key` is not 16
/// bytes long.
#[pyfunction]
fn parse_message_key(key: &Bound<'_, PyAny>) -> PyResult<Option<(u64, u64)>> {
    Ok(chat::parse_message_key(&to_bytes(key, "key")?))
}

/// Prefix (8 bytes) shared by every message key of `channel`: scan it with
/// reverse=True, limit=n for the n newest messages.
#[pyfunction]
fn channel_prefix(py: Python<'_>, channel: u64) -> Bound<'_, PyBytes> {
    PyBytes::new(py, &chat::channel_prefix(channel))
}

/// Close every database still open in this process (registered with atexit
/// by the babeldb package, so buffered writes are made durable at exit).
#[pyfunction]
#[pyo3(name = "_close_all")]
fn close_all(py: Python<'_>) {
    let cores: Vec<Arc<Core>> = registry().values().filter_map(Weak::upgrade).collect();
    py.detach(|| {
        for core in &cores {
            let _ = core.close();
        }
    });
}

#[pymodule]
#[pyo3(name = "_native")]
fn native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("BabelError", py.get_type::<BabelError>())?;
    m.add("ConflictError", py.get_type::<ConflictError>())?;
    m.add("ClosedError", py.get_type::<ClosedError>())?;
    m.add("InvalidArgumentError", py.get_type::<InvalidArgumentError>())?;
    m.add_class::<PyDb>()?;
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(message_key, m)?)?;
    m.add_function(wrap_pyfunction!(parse_message_key, m)?)?;
    m.add_function(wrap_pyfunction!(channel_prefix, m)?)?;
    m.add_function(wrap_pyfunction!(close_all, m)?)?;
    Ok(())
}
