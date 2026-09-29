//! The `Database` class: one babeldb store (fjall + WAL) per directory, with
//! writes group-committed, and every async method a libuv threadpool task
//! (`AsyncTask`), so the event loop never waits on the store.
//!
//! # Life cycle
//!
//! A call takes a *lease* on the open handle when it is made (JS thread) and
//! gives it back once its store work is done (threadpool). A `put`/`delete`
//! gives it back as soon as its request is queued in the committer, which
//! completes queued requests even when the handle closes meanwhile.
//! `close()` detaches the handle (later calls fail with `CLOSED`), waits for
//! the leases out, shuts the committer down (draining its queue; buffered
//! writes become durable) and closes the store, which releases the directory
//! (and its claim in this process) before `close()` returns.
//!
//! # Completions of group-committed writes
//!
//! A threadpool task only queues a `put`/`delete` and returns, so libuv's
//! threads never sleep on a commit and every write issued concurrently from
//! JS can ride the same commit (one WAL write for all of them). If the commit
//! has not completed when the task resolves on the JS thread, the task
//! resolves to a second promise, which this handle's waiter thread settles
//! (through a `JsDeferred`) once the commit is done.

use std::ffi::c_void;
use std::ops::Bound;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use babeldb::config::WalConfig;
use babeldb::engine::FjallWalDb;
use babeldb::scale::group_commit::{GroupCommitConfig, GroupCommitter, Ticket, WriteDurability};
use babeldb::scale::{OpResult, OwnedOp};
use babeldb::{BatchOp, Config, Db, Expect, ScanItem, ScanOptions};
use napi::bindgen_prelude::{AsyncTask, Unknown};
use napi::{Env, JsDeferred, JsValue, Task, sys};
use napi_derive::napi;

use crate::js::{BResult, BindError, Code, Js, Raw};

/// `durability: 'buffered'`: acknowledged writes become durable at most this
/// long after their commit (plus the commit in progress and the sync)...
const BUFFERED_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// ... or before this many bytes of keys + values wait to become durable.
const BUFFERED_MAX_PENDING_BYTES: usize = 64 << 20;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// One handle per directory per process
// ---------------------------------------------------------------------------

/// Canonical paths of the directories open in this process. The addon is
/// loaded once per process, so this covers worker threads too.
static OPEN_DIRS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// A directory claimed in [`OPEN_DIRS`] until dropped (after its store closed).
struct Claim(PathBuf);

impl Claim {
    fn take(canonical: PathBuf) -> BResult<Claim> {
        let mut open = lock(&OPEN_DIRS);
        if open.contains(&canonical) {
            return Err(BindError::new(
                Code::AlreadyOpen,
                format!(
                    "the database directory {} is already open in this process (one handle per \
                     directory per process: share the open handle, or close it first)",
                    canonical.display()
                ),
            ));
        }
        open.push(canonical.clone());
        Ok(Claim(canonical))
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        lock(&OPEN_DIRS).retain(|p| *p != self.0);
    }
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
// Process exit: close what is still open
// ---------------------------------------------------------------------------

/// Handles by environment (`napi_env` address: the main thread or a worker).
static HANDLES: Mutex<Vec<(usize, Weak<Shared>)>> = Mutex::new(Vec::new());
/// Environments whose exit hooks are installed.
static HOOKED_ENVS: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// Remember `shared` for the exit of its environment, installing the exit
/// hooks of that environment on its first open.
fn track(js: Js, shared: &Arc<Shared>) {
    let env = js.addr();
    {
        let mut handles = lock(&HANDLES);
        handles.retain(|(_, weak)| weak.strong_count() > 0);
        handles.push((env, Arc::downgrade(shared)));
    }
    let mut hooked = lock(&HOOKED_ENVS);
    if !hooked.contains(&env) && js.install_exit_hooks(exit_listener, env_teardown).is_ok() {
        hooked.push(env);
    }
}

/// Close every handle still open in environment `env`, without waiting for
/// calls in flight: the committer drains its queue, so every write already
/// accepted (buffered ones included) is durable before the process exits.
fn close_env(env: usize) {
    let open: Vec<Arc<Shared>> = lock(&HANDLES)
        .iter()
        .filter(|(e, _)| *e == env)
        .filter_map(|(_, weak)| weak.upgrade())
        .collect();
    for shared in open {
        shared.close_for_exit();
    }
}

/// `process.on('exit')` listener: runs on a natural exit and on
/// `process.exit()` (whose environment is not torn down).
unsafe extern "C" fn exit_listener(env: sys::napi_env, _info: sys::napi_callback_info) -> sys::napi_value {
    let env = env as usize;
    let _ = panic::catch_unwind(|| close_env(env));
    std::ptr::null_mut()
}

/// Cleanup hook of an environment being torn down (a worker that ends
/// without an `exit` event, for one).
unsafe extern "C" fn env_teardown(arg: *mut c_void) {
    let env = arg as usize;
    let _ = panic::catch_unwind(|| {
        close_env(env);
        lock(&HANDLES).retain(|(e, weak)| *e != env && weak.strong_count() > 0);
        lock(&HOOKED_ENVS).retain(|e| *e != env);
    });
}

// ---------------------------------------------------------------------------
// The open store
// ---------------------------------------------------------------------------

type Resolver = Box<dyn FnOnce(Env) -> napi::Result<Raw> + Send>;

/// A queued write whose promise the waiter thread settles.
struct Waiter {
    ticket: Ticket,
    put: bool,
    deferred: JsDeferred<Raw, Resolver>,
}

/// Fields drop in this order after `Drop::drop`: the store closes (its last
/// `Arc`), then the directory claim is released.
struct Inner {
    writer: GroupCommitter,
    waiters: Mutex<Option<Sender<Waiter>>>,
    waiter_thread: Mutex<Option<JoinHandle<()>>>,
    db: Arc<FjallWalDb>,
    _claim: Claim,
}

impl Inner {
    fn open(dir: &Path, claim: Claim, durability: WriteDurability) -> BResult<Inner> {
        let db = Db::open_fjall_wal(dir, Config::adaptive(), WalConfig::default()).map_err(|e| open_error(e, dir))?;
        let db = Arc::new(db);
        let cfg = GroupCommitConfig {
            durability,
            thread_name: "babeldb-node-commit".into(),
            ..GroupCommitConfig::default()
        };
        let writer = GroupCommitter::new(Arc::clone(&db), cfg)?;
        let (tx, rx) = mpsc::channel();
        let waiter_thread = thread::Builder::new()
            .name("babeldb-node-waiter".into())
            .spawn(move || wait_loop(rx))?;
        Ok(Inner {
            writer,
            waiters: Mutex::new(Some(tx)),
            waiter_thread: Mutex::new(Some(waiter_thread)),
            db,
            _claim: claim,
        })
    }

    /// Drain and stop the committer (buffered writes become durable), then
    /// release everything; returns the error of the final durability step.
    fn close(self) -> BResult<()> {
        let result = self.writer.shutdown();
        drop(self);
        result.map_err(BindError::from)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.writer.shutdown(); // idempotent
        // Every ticket is complete now: the waiter thread settles what it
        // holds and exits.
        drop(lock(&self.waiters).take());
        if let Some(t) = lock(&self.waiter_thread).take() {
            let _ = t.join();
        }
    }
}

/// A directory another process holds (fjall's lock file, or the WAL opened
/// exclusively) is reported as `ALREADY_OPEN` too.
fn open_error(e: babeldb::Error, dir: &Path) -> BindError {
    if let babeldb::Error::Backend(msg) = &e
        && (msg.contains("FjallError: Locked") || msg.contains("in use by another open store"))
    {
        return BindError::new(
            Code::AlreadyOpen,
            format!("the database directory {} is open in another process ({msg})", dir.display()),
        );
    }
    e.into()
}

/// Settles the promises of queued writes, in queue order (the committer
/// completes requests in that order).
fn wait_loop(rx: Receiver<Waiter>) {
    for Waiter { ticket, put, deferred } in rx {
        let result = single(ticket.wait());
        deferred.resolve(Box::new(move |env| write_value(Js::of(&env), result, put)));
    }
}

// ---------------------------------------------------------------------------
// Handle state and leases
// ---------------------------------------------------------------------------

struct Shared {
    state: Mutex<State>,
    /// Signalled when the last lease is given back.
    idle: Condvar,
    /// The database directory (canonical, absolute).
    path: String,
    /// `"immediate"` or `"buffered"`.
    durability: &'static str,
}

struct State {
    /// `None` once closed (or garbage collected).
    inner: Option<Arc<Inner>>,
    leases: usize,
}

impl Shared {
    fn lease(self: &Arc<Self>) -> BResult<Lease> {
        let mut st = lock(&self.state);
        let inner = st.inner.clone().ok_or_else(BindError::closed)?;
        st.leases += 1;
        Ok(Lease {
            shared: Arc::clone(self),
            inner: Some(inner),
        })
    }

    /// See [`Database::close`].
    fn close(&self) -> BResult<()> {
        let inner = {
            let mut st = lock(&self.state);
            let inner = st.inner.take();
            while st.leases > 0 {
                st = self.idle.wait(st).unwrap_or_else(PoisonError::into_inner);
            }
            inner
        };
        match inner.map(Arc::try_unwrap) {
            None => Ok(()), // already closed
            Some(Ok(inner)) => inner.close(),
            // Unreachable: only leases share the handle, and none is left.
            Some(Err(_)) => Err(BindError::internal("close: the handle is still referenced")),
        }
    }

    /// Close at process exit: detach the handle and drain the committer (the
    /// accepted writes become durable), without waiting for the calls in
    /// flight; the store itself closes now, or when the last of them ends.
    fn close_for_exit(&self) {
        let inner = lock(&self.state).inner.take();
        if let Some(inner) = inner {
            let _ = inner.writer.shutdown();
            drop(inner);
        }
    }

    fn is_closed(&self) -> bool {
        lock(&self.state).inner.is_none()
    }

    /// The waiter thread's queue, while the handle is open.
    fn waiters(&self) -> Option<Sender<Waiter>> {
        let st = lock(&self.state);
        st.inner.as_ref().and_then(|inner| lock(&inner.waiters).clone())
    }
}

/// The right to use the open handle for one call.
struct Lease {
    shared: Arc<Shared>,
    /// `Some` until dropped.
    inner: Option<Arc<Inner>>,
}

impl Lease {
    fn inner(&self) -> &Inner {
        self.inner.as_deref().expect("a lease holds the handle until it is dropped")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // Let go of the handle before counting the lease out, so `close()`
        // finds itself the only owner. (If the Database was garbage collected
        // this is the last reference, and the store closes here.)
        drop(self.inner.take());
        let mut st = lock(&self.shared.state);
        st.leases -= 1;
        if st.leases == 0 {
            self.shared.idle.notify_all();
        }
    }
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// A call, with its arguments converted on the JS thread.
enum Job {
    /// Invalid arguments, or a closed handle: fail without touching the store.
    Failed(BindError),
    /// Nothing to read or write (an empty batch).
    Ready(Done),
    Write { lease: Lease, op: OwnedOp },
    Get { lease: Lease, key: Vec<u8> },
    Scan { lease: Lease, opts: ScanOptions },
    Batch { lease: Lease, ops: Vec<OwnedOp> },
    Sync { lease: Lease },
}

/// What a call produced, before its conversion to JS values.
pub enum Done {
    /// A put (`true`) or delete queued in the committer.
    Queued(Ticket, bool),
    /// A put (`true`) or delete, committed.
    Written(OpResult, bool),
    Value(Option<Vec<u8>>),
    Entries(Vec<ScanItem>),
    Keys(Vec<ScanItem>),
    Revisions(Vec<Option<u64>>),
    Nothing,
}

/// Run `job` against the store. `wait`: wait for a queued write's commit
/// (sync methods) instead of returning its ticket.
fn run(job: Job, wait: bool) -> BResult<Done> {
    match job {
        Job::Failed(e) => Err(e),
        Job::Ready(done) => Ok(done),
        Job::Write { lease, op } => {
            let put = op.is_put();
            let ticket = lease.inner().writer.submit(vec![op])?;
            drop(lease); // queued: the committer completes it even if the handle closes now
            Ok(if wait {
                Done::Written(single(ticket.wait()), put)
            } else {
                Done::Queued(ticket, put)
            })
        }
        Job::Get { lease, key } => Ok(Done::Value(lease.inner().db.get(&key)?)),
        Job::Scan { lease, opts } => {
            let items = lease.inner().db.scan(&opts)?;
            Ok(if opts.with_values {
                Done::Entries(items)
            } else {
                Done::Keys(items)
            })
        }
        Job::Batch { lease, ops } => {
            let batch: Vec<BatchOp<'_>> = ops.iter().map(OwnedOp::as_batch_op).collect();
            Ok(Done::Revisions(lease.inner().db.write_batch(&batch)?))
        }
        Job::Sync { lease } => {
            lease.inner().writer.flush()?;
            Ok(Done::Nothing)
        }
    }
}

/// [`run`], with a panic turned into an `INTERNAL` error.
fn run_guarded(job: Job, wait: bool) -> BResult<Done> {
    panic::catch_unwind(AssertUnwindSafe(|| run(job, wait))).unwrap_or_else(|payload| {
        let msg = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".into());
        Err(BindError::internal(format!("babeldb panicked: {msg}")))
    })
}

/// The single result of a one-operation request.
fn single(results: babeldb::Result<Vec<OpResult>>) -> OpResult {
    let mut results = results?;
    match (results.pop(), results.is_empty()) {
        (Some(result), true) => result,
        _ => Err(babeldb::Error::Backend("group commit: expected exactly one result".into())),
    }
}

/// The JS value of a committed put (its revision) or delete (whether a live
/// record was deleted).
fn write_value(js: Js, result: OpResult, put: bool) -> napi::Result<Raw> {
    let value = match result {
        Ok(Some(revision)) if put => js.bigint(revision)?,
        Ok(None) if put => return Err(js.error(BindError::internal("the store returned no revision for a put"))),
        Ok(deleted) => js.boolean(deleted.is_some())?,
        Err(e) => return Err(js.error(e.into())),
    };
    Ok(Raw(value))
}

/// Convert what a call produced (JS thread).
fn settle(js: Js, done: Done, shared: &Shared) -> napi::Result<Raw> {
    let value = match done {
        Done::Queued(mut ticket, put) => {
            return match ticket.try_wait() {
                Some(result) => write_value(js, single(result), put),
                None => defer(js, shared, ticket, put),
            };
        }
        Done::Written(result, put) => return write_value(js, result, put),
        Done::Value(Some(value)) => js.buffer(value)?,
        Done::Value(None) => js.null()?,
        Done::Entries(items) => js.array(
            items.len(),
            items.into_iter().map(|item| {
                let key = js.buffer(item.key)?;
                let value = js.buffer(item.value.unwrap_or_default())?;
                js.array(2, [Ok(key), Ok(value)].into_iter())
            }),
        )?,
        Done::Keys(items) => js.array(items.len(), items.into_iter().map(|item| js.buffer(item.key)))?,
        Done::Revisions(revisions) => js.array(
            revisions.len(),
            revisions.into_iter().map(|r| match r {
                Some(revision) => js.bigint(revision),
                None => js.null(),
            }),
        )?,
        Done::Nothing => js.undefined()?,
    };
    Ok(Raw(value))
}

/// The commit of a queued write is still running: resolve to a promise the
/// waiter thread settles when it completes.
fn defer(js: Js, shared: &Shared, ticket: Ticket, put: bool) -> napi::Result<Raw> {
    let Some(waiters) = shared.waiters() else {
        // Closed: `close()` drained the committer, so the ticket is complete.
        return write_value(js, single(ticket.wait()), put);
    };
    let env = js.env();
    let (deferred, promise) = env.create_deferred::<Raw, Resolver>()?;
    let promise = promise.raw();
    let waiter = Waiter { ticket, put, deferred };
    if let Err(mpsc::SendError(waiter)) = waiters.send(waiter) {
        // The waiter thread is gone (it runs as long as the handle is open):
        // settle from here.
        let result = single(waiter.ticket.wait());
        waiter.deferred.resolve(Box::new(move |env| write_value(Js::of(&env), result, put)));
    }
    Ok(Raw(promise))
}

/// A method call running on the libuv threadpool.
pub struct OpTask {
    job: Option<Job>,
    shared: Arc<Shared>,
}

impl Task for OpTask {
    type Output = BResult<Done>;
    type JsValue = Raw;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let job = self
            .job
            .take()
            .unwrap_or_else(|| Job::Failed(BindError::internal("task computed twice")));
        Ok(run_guarded(job, false))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Raw> {
        let js = Js::of(&env);
        match output {
            Ok(done) => settle(js, done, &self.shared),
            Err(e) => Err(js.error(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

fn options_object(js: Js, options: Option<sys::napi_value>, what: &str) -> BResult<Option<sys::napi_value>> {
    match options {
        None => Ok(None),
        Some(o) => js.expect_object(o, what).map(|()| Some(o)),
    }
}

/// `ifAbsent` / `ifRevision` of a put.
fn put_expect(js: Js, obj: sys::napi_value) -> BResult<Expect> {
    let absent = js.opt_bool(obj, "ifAbsent")?.unwrap_or(false);
    match (absent, js.opt_u64(obj, "ifRevision")?) {
        (true, Some(_)) => Err(BindError::invalid("`ifAbsent` and `ifRevision` cannot be combined")),
        (true, None) => Ok(Expect::Absent),
        (false, Some(revision)) => Ok(Expect::Revision(revision)),
        (false, None) => Ok(Expect::Any),
    }
}

/// `ifRevision` of a delete.
fn delete_expect(js: Js, obj: sys::napi_value) -> BResult<Expect> {
    Ok(js.opt_u64(obj, "ifRevision")?.map_or(Expect::Any, Expect::Revision))
}

fn scan_options(js: Js, options: Option<sys::napi_value>, with_values: bool) -> BResult<ScanOptions> {
    let mut opts = ScanOptions::all().with_values(with_values);
    let Some(o) = options_object(js, options, "scan options")? else {
        return Ok(opts);
    };
    let prefix = js.opt_bytes(o, "prefix")?;
    let start = js.opt_bytes(o, "start")?;
    let end = js.opt_bytes(o, "end")?;
    match prefix {
        Some(_) if start.is_some() || end.is_some() => {
            return Err(BindError::invalid("`prefix` cannot be combined with `start` or `end`"));
        }
        Some(prefix) => opts = ScanOptions::prefix(&prefix).with_values(with_values),
        None => {
            if let Some(start) = start {
                opts.start = Bound::Included(start);
            }
            if let Some(end) = end {
                opts.end = Bound::Excluded(end);
            }
        }
    }
    opts.reverse = js.opt_bool(o, "reverse")?.unwrap_or(false);
    if let Some(limit) = js.opt_u64(o, "limit")? {
        // 0 = no limit (as `ScanOptions` and the Python binding).
        opts.limit = usize::try_from(limit).unwrap_or(usize::MAX);
    }
    Ok(opts)
}

/// `{ type: 'put', key, value }` / `{ type: 'del', key }` operations (every
/// expectation `Any`, as in the Python binding).
fn batch_ops(js: Js, ops: sys::napi_value) -> BResult<Vec<OwnedOp>> {
    let len = js.array_len(ops, "the batch")?;
    let mut out = Vec::with_capacity(len as usize);
    for i in 0..len {
        let what = format!("batch operation {i}");
        let op = js.element(ops, i, &what)?;
        js.expect_object(op, &what)?;
        let kind = match js.prop(op, "type")? {
            Some(v) => js.string(v, &format!("{what}: `type`"))?,
            None => String::new(),
        };
        let key = js
            .opt_bytes(op, "key")?
            .ok_or_else(|| BindError::invalid(format!("{what}: `key` is required")))?;
        out.push(match kind.as_str() {
            "put" => {
                let value = js
                    .opt_bytes(op, "value")?
                    .ok_or_else(|| BindError::invalid(format!("{what}: `value` is required for a put")))?;
                OwnedOp::put(key, value, Expect::Any)
            }
            "del" => OwnedOp::delete(key, Expect::Any),
            _ => return Err(BindError::invalid(format!("{what}: `type` must be 'put' or 'del'"))),
        });
    }
    Ok(out)
}

fn raw(v: Option<Unknown<'_>>) -> Option<sys::napi_value> {
    v.map(|v| v.raw())
}

// ---------------------------------------------------------------------------
// JavaScript API
// ---------------------------------------------------------------------------

/// Open (or create) the database stored in directory `path`: an fjall
/// LSM-tree in `path` plus a write-ahead log next to it (`<path>.wal`).
///
/// Only one handle per directory may be open in a process: opening an open
/// directory again throws at once with `code === 'ALREADY_OPEN'` (so does a
/// directory another process has open). Throws `INVALID_ARGUMENT` for bad
/// options and `IO` when the directory cannot be created.
///
/// Handles still open when the process exits (a natural exit or
/// `process.exit()`) are closed then, so the writes they accepted (buffered
/// ones included) are durable. A crash or a kill may lose the buffered
/// writes of the last ~100 ms.
#[napi(ts_args_type = "path: string, options?: OpenOptions")]
pub fn open(env: &Env, path: Unknown<'_>, options: Option<Unknown<'_>>) -> napi::Result<Database> {
    let js = Js::of(env);
    open_database(js, path.raw(), raw(options)).map_err(|e| js.error(e))
}

fn open_database(js: Js, path: sys::napi_value, options: Option<sys::napi_value>) -> BResult<Database> {
    let path = js.string(path, "path")?;
    if path.is_empty() {
        return Err(BindError::invalid("path must not be empty"));
    }
    let (name, durability) = match options_object(js, options, "open options")? {
        Some(o) => match js.prop(o, "durability")? {
            Some(v) => write_durability(&js.string(v, "`durability`")?)?,
            None => write_durability("immediate")?,
        },
        None => write_durability("immediate")?,
    };
    std::fs::create_dir_all(&path)?;
    let dir = strip_verbatim(std::fs::canonicalize(&path)?);
    let claim = Claim::take(dir.clone())?;
    let inner = Inner::open(&dir, claim, durability)?;
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            inner: Some(Arc::new(inner)),
            leases: 0,
        }),
        idle: Condvar::new(),
        path: dir.to_string_lossy().into_owned(),
        durability: name,
    });
    track(js, &shared);
    Ok(Database { shared })
}

fn write_durability(name: &str) -> BResult<(&'static str, WriteDurability)> {
    match name {
        "immediate" => Ok(("immediate", WriteDurability::Immediate)),
        "buffered" => Ok((
            "buffered",
            WriteDurability::Buffered {
                flush_interval: BUFFERED_FLUSH_INTERVAL,
                max_pending_bytes: BUFFERED_MAX_PENDING_BYTES,
            },
        )),
        other => Err(BindError::invalid(format!(
            "`durability` must be 'immediate' or 'buffered', not '{other}'"
        ))),
    }
}

/// An open babeldb database: one directory, opened with `open()`.
///
/// Every async method runs on the libuv threadpool and never blocks the
/// event loop; its `*Sync` twin does the same work on the calling thread
/// (for scripts). Concurrent calls are safe. Calls that are not awaited run
/// in no particular order: await a write before an operation that must see
/// it.
#[napi]
pub struct Database {
    shared: Arc<Shared>,
}

impl Drop for Database {
    /// Garbage collected without `close()`: detach the handle; the store
    /// closes now, or when the last call in flight ends.
    fn drop(&mut self) {
        let inner = lock(&self.shared.state).inner.take();
        drop(inner);
    }
}

#[napi]
impl Database {
    /// Write `value` under `key` and resolve with the new revision once the
    /// write is committed: durable in `'immediate'` mode, visible (durable
    /// within ~100 ms) in `'buffered'` mode. Concurrent writes share commits.
    ///
    /// `ifAbsent: true` writes only if the key does not exist; `ifRevision`
    /// only if its current revision is exactly that one. A failed expectation
    /// rejects with `code === 'CONFLICT'` and writes nothing.
    #[napi(
        ts_args_type = "key: Key, value: Value, options?: PutOptions",
        ts_return_type = "Promise<bigint>"
    )]
    pub fn put(
        &self,
        env: &Env,
        key: Unknown<'_>,
        value: Unknown<'_>,
        options: Option<Unknown<'_>>,
    ) -> AsyncTask<OpTask> {
        self.spawn(self.put_job(Js::of(env), key.raw(), value.raw(), raw(options)))
    }

    /// Synchronous {@link Database.put}: blocks the calling thread until the
    /// write is committed; returns the new revision.
    #[napi(ts_args_type = "key: Key, value: Value, options?: PutOptions", ts_return_type = "bigint")]
    pub fn put_sync(
        &self,
        env: &Env,
        key: Unknown<'_>,
        value: Unknown<'_>,
        options: Option<Unknown<'_>>,
    ) -> napi::Result<Raw> {
        let js = Js::of(env);
        self.now(js, self.put_job(js, key.raw(), value.raw(), raw(options)))
    }

    /// The current value of `key`, or `null` when it does not exist.
    #[napi(ts_args_type = "key: Key", ts_return_type = "Promise<Buffer | null>")]
    pub fn get(&self, env: &Env, key: Unknown<'_>) -> AsyncTask<OpTask> {
        self.spawn(self.get_job(Js::of(env), key.raw()))
    }

    /// Synchronous {@link Database.get}.
    #[napi(ts_args_type = "key: Key", ts_return_type = "Buffer | null")]
    pub fn get_sync(&self, env: &Env, key: Unknown<'_>) -> napi::Result<Raw> {
        let js = Js::of(env);
        self.now(js, self.get_job(js, key.raw()))
    }

    /// Delete `key`; resolves `true` if a live record was deleted, `false` if
    /// there was none. With `ifRevision`, deletes only if the current revision
    /// is exactly that one (a missing key then conflicts too): otherwise
    /// rejects with `code === 'CONFLICT'`. Durability as for `put`.
    #[napi(ts_args_type = "key: Key, options?: DeleteOptions", ts_return_type = "Promise<boolean>")]
    pub fn delete(&self, env: &Env, key: Unknown<'_>, options: Option<Unknown<'_>>) -> AsyncTask<OpTask> {
        self.spawn(self.delete_job(Js::of(env), key.raw(), raw(options)))
    }

    /// Synchronous {@link Database.delete}.
    #[napi(ts_args_type = "key: Key, options?: DeleteOptions", ts_return_type = "boolean")]
    pub fn delete_sync(&self, env: &Env, key: Unknown<'_>, options: Option<Unknown<'_>>) -> napi::Result<Raw> {
        let js = Js::of(env);
        self.now(js, self.delete_job(js, key.raw(), raw(options)))
    }

    /// `[key, value]` pairs in key order (bytewise), or descending with
    /// `reverse`. Bounds: `prefix`, or `start` (inclusive) and/or `end`
    /// (exclusive); `prefix` cannot be combined with `start`/`end`
    /// (`INVALID_ARGUMENT`). `limit` caps the number of pairs (with
    /// `reverse`: the last ones); `0` or absent = no limit. Reads one
    /// consistent snapshot.
    #[napi(ts_args_type = "options?: ScanOptions", ts_return_type = "Promise<Array<[Buffer, Buffer]>>")]
    pub fn scan(&self, env: &Env, options: Option<Unknown<'_>>) -> AsyncTask<OpTask> {
        self.spawn(self.scan_job(Js::of(env), raw(options), true))
    }

    /// Synchronous {@link Database.scan}.
    #[napi(ts_args_type = "options?: ScanOptions", ts_return_type = "Array<[Buffer, Buffer]>")]
    pub fn scan_sync(&self, env: &Env, options: Option<Unknown<'_>>) -> napi::Result<Raw> {
        let js = Js::of(env);
        self.now(js, self.scan_job(js, raw(options), true))
    }

    /// The keys a {@link Database.scan} with the same options would return
    /// (values are not read).
    #[napi(ts_args_type = "options?: ScanOptions", ts_return_type = "Promise<Buffer[]>")]
    pub fn keys(&self, env: &Env, options: Option<Unknown<'_>>) -> AsyncTask<OpTask> {
        self.spawn(self.scan_job(Js::of(env), raw(options), false))
    }

    /// Synchronous {@link Database.keys}.
    #[napi(ts_args_type = "options?: ScanOptions", ts_return_type = "Buffer[]")]
    pub fn keys_sync(&self, env: &Env, options: Option<Unknown<'_>>) -> napi::Result<Raw> {
        let js = Js::of(env);
        self.now(js, self.scan_job(js, raw(options), false))
    }

    /// Apply `ops` atomically, in order, in ONE commit that is durable when
    /// the promise resolves, in both durability modes (`Db::write_batch`).
    /// All or nothing: if any operation is malformed or invalid (an empty
    /// key, a key or value over the limits...) nothing is written and the
    /// promise rejects (`INVALID_ARGUMENT`, or `IO`... if the commit fails).
    /// A later operation sees the effects of earlier ones on the same key.
    ///
    /// Resolves with one entry per operation: for a put, its new revision;
    /// for a `del`, the revision of the deleted record, or `null` if the key
    /// did not exist. The batch is its own commit: it does not ride the group
    /// commits of `put`/`delete` (and it makes earlier buffered writes
    /// durable too).
    #[napi(ts_args_type = "ops: BatchOp[]", ts_return_type = "Promise<Array<bigint | null>>")]
    pub fn batch(&self, env: &Env, ops: Unknown<'_>) -> AsyncTask<OpTask> {
        self.spawn(self.batch_job(Js::of(env), ops.raw()))
    }

    /// Synchronous {@link Database.batch}.
    #[napi(ts_args_type = "ops: BatchOp[]", ts_return_type = "Array<bigint | null>")]
    pub fn batch_sync(&self, env: &Env, ops: Unknown<'_>) -> napi::Result<Raw> {
        let js = Js::of(env);
        self.now(js, self.batch_job(js, ops.raw()))
    }

    /// Resolves once every write acknowledged before this call is durable
    /// (in `'buffered'` mode it forces the flush; in `'immediate'` mode they
    /// already are).
    #[napi(ts_return_type = "Promise<void>")]
    pub fn sync(&self) -> AsyncTask<OpTask> {
        self.spawn(self.job(|lease| Ok(Job::Sync { lease })))
    }

    /// Close the database. Operations already called complete first
    /// (waiting for them blocks this thread), buffered writes become durable,
    /// and the directory is released: it can be opened again as soon as this
    /// returns. Afterwards every method throws (sync) or rejects (async) with
    /// `code === 'CLOSED'`. Closing a closed database does nothing.
    #[napi]
    pub fn close(&self, env: &Env) -> napi::Result<()> {
        self.shared.close().map_err(|e| Js::of(env).error(e))
    }

    /// `true` once `close()` was called (or the process is exiting).
    #[napi(getter)]
    pub fn closed(&self) -> bool {
        self.shared.is_closed()
    }

    /// The database directory (absolute, canonical). The write-ahead log is
    /// the file `<path>.wal` next to it.
    #[napi(getter)]
    pub fn path(&self) -> String {
        self.shared.path.clone()
    }

    /// The durability mode the database was opened with.
    #[napi(getter, ts_return_type = "Durability")]
    pub fn durability(&self) -> String {
        self.shared.durability.to_string()
    }
}

impl Database {
    fn spawn(&self, job: Job) -> AsyncTask<OpTask> {
        AsyncTask::new(OpTask {
            job: Some(job),
            shared: Arc::clone(&self.shared),
        })
    }

    /// Run `job` on this (the JS) thread.
    fn now(&self, js: Js, job: Job) -> napi::Result<Raw> {
        match run_guarded(job, true) {
            Ok(done) => settle(js, done, &self.shared),
            Err(e) => Err(js.error(e)),
        }
    }

    /// A job built with a lease, or the error (closed handle first).
    fn job(&self, build: impl FnOnce(Lease) -> BResult<Job>) -> Job {
        match self.shared.lease().and_then(build) {
            Ok(job) => job,
            Err(e) => Job::Failed(e),
        }
    }

    fn put_job(
        &self,
        js: Js,
        key: sys::napi_value,
        value: sys::napi_value,
        options: Option<sys::napi_value>,
    ) -> Job {
        self.job(|lease| {
            let key = js.bytes(key, "key")?;
            let value = js.bytes(value, "value")?;
            let expect = match options_object(js, options, "put options")? {
                Some(o) => put_expect(js, o)?,
                None => Expect::Any,
            };
            Ok(Job::Write {
                lease,
                op: OwnedOp::put(key, value, expect),
            })
        })
    }

    fn get_job(&self, js: Js, key: sys::napi_value) -> Job {
        self.job(|lease| {
            Ok(Job::Get {
                lease,
                key: js.bytes(key, "key")?,
            })
        })
    }

    fn delete_job(&self, js: Js, key: sys::napi_value, options: Option<sys::napi_value>) -> Job {
        self.job(|lease| {
            let key = js.bytes(key, "key")?;
            let expect = match options_object(js, options, "delete options")? {
                Some(o) => delete_expect(js, o)?,
                None => Expect::Any,
            };
            Ok(Job::Write {
                lease,
                op: OwnedOp::delete(key, expect),
            })
        })
    }

    fn scan_job(&self, js: Js, options: Option<sys::napi_value>, with_values: bool) -> Job {
        self.job(|lease| {
            Ok(Job::Scan {
                lease,
                opts: scan_options(js, options, with_values)?,
            })
        })
    }

    fn batch_job(&self, js: Js, ops: sys::napi_value) -> Job {
        self.job(|lease| {
            let ops = batch_ops(js, ops)?;
            Ok(if ops.is_empty() {
                Job::Ready(Done::Revisions(Vec::new()))
            } else {
                Job::Batch { lease, ops }
            })
        })
    }
}
