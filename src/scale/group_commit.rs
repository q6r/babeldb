//! Group commit: a dedicated writer thread turns concurrent writes into one
//! commit (one fsync) per batch.
//!
//! Callers enqueue requests (one or more [`OwnedOp`]s) in a bounded FIFO queue
//! and wait on a [`Ticket`]. The writer thread owns the [`BatchSink`]: it
//! drains the queue into one batch until `max_batch_ops` / `max_batch_bytes`
//! or until the queue is empty ("natural batching": while one commit and its
//! fsync run, new requests accumulate and all of them ride the next commit),
//! optionally waits up to `max_delay` for more, calls [`BatchSink::apply`] ONCE
//! and completes every waiter with its own per-operation results.
//!
//! Preparation: before queueing a request, the submitting thread reserves its
//! room in the queue and hands it to [`BatchSink::prepare`] (validation and
//! encoding, for a [`crate::Db`]), so that work runs in parallel in the
//! callers' threads and the writer thread only runs transactions
//! ([`BatchSink::apply_prepared`]). A batch never mixes prepared and
//! unprepared requests (the kind of request ends a batch when it changes;
//! order is kept). A panic in `prepare` fails that request only.
//!
//! Guarantees:
//! - Order: requests are applied in queue (submission) order, so operations
//!   from one thread, and all operations on one key, keep their order. A
//!   request is never split across commits: its operations become visible and
//!   durable together (each still checks its own expectation).
//! - Durability: see [`WriteDurability`].
//! - Failure: an error that aborts a commit is delivered to every request of
//!   that batch; a failing expectation only fails its own operation.
//! - Panics: if the writer thread panics (for example inside the sink), every
//!   in-flight and queued request completes with an error, and later calls
//!   fail fast; nobody hangs.
//! - Shutdown: [`GroupCommitter::shutdown`] rejects new work, drains the queue
//!   (and in buffered mode makes it durable), then joins the thread.

use std::collections::VecDeque;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::{
    BatchSink, OpResult, OwnedOp, SinkOps, lock, panic_message, replicate_error, wait, wait_timeout,
};
use crate::engine::{Expect, PreparedOp, Revision};
use crate::error::{Error, Result};
use crate::store::Durability;

/// When a write is acknowledged to its caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteDurability {
    /// Every batch is committed with `Durability::Immediate`: a caller returns
    /// only after its batch is durable. One fsync per batch.
    Immediate,
    /// Batches are committed with `Durability::Deferred`: a caller returns once
    /// its writes are committed and visible, NOT durable. They become durable
    /// with the next `Immediate` commit, which the writer issues (piggybacked
    /// on the next batch when there is one, otherwise as a `sync`):
    /// - at most `flush_interval` after the oldest non-durable commit (plus
    ///   the commit already running at that moment and the sync itself);
    /// - before the non-durable key + value bytes would reach
    ///   `max_pending_bytes`;
    /// - on [`GroupCommitter::flush`] and on shutdown.
    ///
    /// Loss window on a crash: the acknowledged writes of roughly the last
    /// `flush_interval` (+ two commit durations), never more than
    /// `max_pending_bytes`; with redb the survivors are a prefix of the commit
    /// order. If a sync fails, the next batch is committed `Immediate`, so the
    /// window never grows silently: either that commit succeeds (and makes
    /// everything before it durable) or its callers get the error.
    Buffered {
        flush_interval: Duration,
        max_pending_bytes: usize,
    },
}

impl WriteDurability {
    /// `Buffered` with the given interval and a 16 MiB bound.
    pub fn buffered(flush_interval: Duration) -> WriteDurability {
        WriteDurability::Buffered {
            flush_interval,
            max_pending_bytes: 16 << 20,
        }
    }

    pub fn is_buffered(&self) -> bool {
        matches!(self, WriteDurability::Buffered { .. })
    }
}

/// Tuning of a [`GroupCommitter`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupCommitConfig {
    pub durability: WriteDurability,
    /// A batch stops growing at this many operations (a single larger request
    /// still forms its own batch).
    pub max_batch_ops: usize,
    /// A batch stops growing at this many key + value bytes.
    pub max_batch_bytes: usize,
    /// After draining the queue, wait up to this long for more requests while
    /// the batch is not full. Zero = natural batching only (no added latency).
    pub max_delay: Duration,
    /// Queue bound in operations (a flush marker counts as one).
    pub queue_max_ops: usize,
    /// Queue bound in key + value bytes.
    pub queue_max_bytes: usize,
    /// Name of the writer thread.
    pub thread_name: String,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        GroupCommitConfig {
            durability: WriteDurability::Immediate,
            max_batch_ops: 4096,
            max_batch_bytes: 16 << 20,
            max_delay: Duration::ZERO,
            queue_max_ops: 16 * 1024,
            queue_max_bytes: 64 << 20,
            thread_name: "babeldb-commit".to_string(),
        }
    }
}

impl From<WriteDurability> for GroupCommitConfig {
    fn from(durability: WriteDurability) -> Self {
        GroupCommitConfig {
            durability,
            ..GroupCommitConfig::default()
        }
    }
}

impl GroupCommitConfig {
    pub fn with_durability(mut self, durability: WriteDurability) -> Self {
        self.durability = durability;
        self
    }

    pub fn validate(&self) -> Result<()> {
        let zero = [
            ("max_batch_ops", self.max_batch_ops),
            ("max_batch_bytes", self.max_batch_bytes),
            ("queue_max_ops", self.queue_max_ops),
            ("queue_max_bytes", self.queue_max_bytes),
        ];
        for (name, v) in zero {
            if v == 0 {
                return Err(Error::InvalidArgument(format!(
                    "group commit: {name} must be > 0"
                )));
            }
        }
        if let WriteDurability::Buffered {
            flush_interval,
            max_pending_bytes,
        } = self.durability
            && (flush_interval.is_zero() || max_pending_bytes == 0)
        {
            return Err(Error::InvalidArgument(
                "group commit: buffered flush_interval and max_pending_bytes must be > 0".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Completions: a one-shot slot shared by the writer (Completer) and the caller
// (Pending). Dropping a Completer without completing it (writer panic or bug)
// completes it with an error, so a waiter can never hang.
// ---------------------------------------------------------------------------

enum Slot<T> {
    Waiting { parked: bool },
    Ready(Result<T>),
    Taken,
}

struct Completion<T> {
    slot: Mutex<Slot<T>>,
    cv: Condvar,
}

impl<T> Completion<T> {
    fn set(&self, r: Result<T>) {
        let mut slot = lock(&self.slot);
        if let Slot::Waiting { parked } = *slot {
            *slot = Slot::Ready(r);
            drop(slot);
            if parked {
                self.cv.notify_all();
            }
        }
    }
}

/// Handle on the result of a submitted request (or flush).
pub struct Pending<T> {
    completion: Arc<Completion<T>>,
}

/// Result of a submitted batch of operations: one [`OpResult`] per operation,
/// in submission order. The outer `Err` means the commit carrying the request
/// failed (nothing of it was applied) or the committer stopped.
pub type Ticket = Pending<Vec<OpResult>>;

impl<T> Pending<T> {
    fn ready(r: Result<T>) -> Pending<T> {
        Pending {
            completion: Arc::new(Completion {
                slot: Mutex::new(Slot::Ready(r)),
                cv: Condvar::new(),
            }),
        }
    }

    /// Block until the result is available.
    pub fn wait(self) -> Result<T> {
        let mut slot = lock(&self.completion.slot);
        loop {
            match std::mem::replace(&mut *slot, Slot::Taken) {
                Slot::Ready(r) => return r,
                Slot::Taken => return Err(taken_error()),
                Slot::Waiting { .. } => {
                    *slot = Slot::Waiting { parked: true };
                    slot = wait(&self.completion.cv, slot);
                }
            }
        }
    }

    /// Wait at most `timeout`. `None` = not ready yet (the handle stays
    /// usable); `Some` takes the result out of the handle.
    pub fn wait_timeout(&mut self, timeout: Duration) -> Option<Result<T>> {
        let deadline = Instant::now().checked_add(timeout);
        let mut slot = lock(&self.completion.slot);
        loop {
            match std::mem::replace(&mut *slot, Slot::Taken) {
                Slot::Ready(r) => return Some(r),
                Slot::Taken => return Some(Err(taken_error())),
                Slot::Waiting { .. } => {
                    let now = Instant::now();
                    match deadline {
                        Some(d) if now >= d => {
                            *slot = Slot::Waiting { parked: false };
                            return None;
                        }
                        Some(d) => {
                            *slot = Slot::Waiting { parked: true };
                            slot = wait_timeout(&self.completion.cv, slot, d - now);
                        }
                        None => {
                            *slot = Slot::Waiting { parked: true };
                            slot = wait(&self.completion.cv, slot);
                        }
                    }
                }
            }
        }
    }

    /// Non-blocking poll; `Some` takes the result out of the handle.
    pub fn try_wait(&mut self) -> Option<Result<T>> {
        self.wait_timeout(Duration::ZERO)
    }

    /// Whether the result is available (and not yet taken).
    pub fn is_ready(&self) -> bool {
        matches!(*lock(&self.completion.slot), Slot::Ready(_))
    }
}

impl<T> fmt::Debug for Pending<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pending")
            .field("ready", &self.is_ready())
            .finish()
    }
}

struct Completer<T> {
    completion: Option<Arc<Completion<T>>>,
}

impl<T> Completer<T> {
    fn complete(mut self, r: Result<T>) {
        if let Some(c) = self.completion.take() {
            c.set(r);
        }
    }
}

impl<T> Drop for Completer<T> {
    fn drop(&mut self) {
        if let Some(c) = self.completion.take() {
            c.set(Err(dropped_error()));
        }
    }
}

fn completion_pair<T>() -> (Completer<T>, Pending<T>) {
    let c = Arc::new(Completion {
        slot: Mutex::new(Slot::Waiting { parked: false }),
        cv: Condvar::new(),
    });
    (
        Completer {
            completion: Some(c.clone()),
        },
        Pending { completion: c },
    )
}

fn closed_error() -> Error {
    Error::Backend("group commit: the committer is shut down".into())
}

fn dead_error(reason: &str) -> Error {
    Error::Backend(format!("group commit: {reason}"))
}

fn dropped_error() -> Error {
    Error::Backend(
        "group commit: the request was dropped before completion (the writer thread panicked or stopped)".into(),
    )
}

fn taken_error() -> Error {
    Error::InvalidArgument("group commit: the result was already taken".into())
}

fn prepare_panicked(payload: &(dyn std::any::Any + Send)) -> Error {
    Error::Backend(format!(
        "group commit: preparing the request panicked ({}); nothing of it was applied",
        panic_message(payload)
    ))
}

/// Error of [`GroupCommitter::try_submit`].
#[derive(Debug)]
pub enum TrySubmitError {
    /// The queue is full; the operations are handed back untouched.
    Full(Vec<OwnedOp>),
    /// The committer is shut down or its writer thread died (or preparing
    /// the request panicked: nothing of it was applied).
    Closed(Error),
}

impl TrySubmitError {
    /// The operations, when they were not consumed.
    pub fn into_ops(self) -> Option<Vec<OwnedOp>> {
        match self {
            TrySubmitError::Full(ops) => Some(ops),
            TrySubmitError::Closed(_) => None,
        }
    }
}

impl fmt::Display for TrySubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySubmitError::Full(ops) => write!(
                f,
                "group commit queue is full ({} operations handed back)",
                ops.len()
            ),
            TrySubmitError::Closed(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TrySubmitError {}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Activity counters of a [`GroupCommitter`] (monotonic, except the gauges).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GroupCommitStats {
    /// Requests accepted (a `put` is one request with one operation).
    pub requests: u64,
    /// Operations handed to the sink.
    pub ops: u64,
    /// `BatchSink::apply` calls (= commits).
    pub batches: u64,
    pub max_batch_ops: u64,
    /// Batches committed with `Durability::Immediate`.
    pub immediate_batches: u64,
    /// Batches committed with `Durability::Deferred` (buffered mode).
    pub deferred_batches: u64,
    /// Batches whose commit failed (every request of it got the error).
    pub failed_batches: u64,
    /// Standalone `BatchSink::sync` calls that succeeded / failed.
    pub syncs: u64,
    pub failed_syncs: u64,
    /// Flush requests completed.
    pub flushes: u64,
    /// Submissions that had to wait for queue space (backpressure).
    pub blocked_submits: u64,
    /// Gauges: queued (not yet batched) operations and bytes.
    pub queued_ops: u64,
    pub queued_bytes: u64,
    /// Gauge: committed but not yet durable key + value bytes (buffered mode).
    pub unsynced_bytes: u64,
}

impl GroupCommitStats {
    /// Average operations per commit.
    pub fn avg_batch_ops(&self) -> f64 {
        if self.batches == 0 {
            0.0
        } else {
            self.ops as f64 / self.batches as f64
        }
    }
}

#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    ops: AtomicU64,
    batches: AtomicU64,
    max_batch_ops: AtomicU64,
    immediate_batches: AtomicU64,
    deferred_batches: AtomicU64,
    failed_batches: AtomicU64,
    syncs: AtomicU64,
    failed_syncs: AtomicU64,
    flushes: AtomicU64,
    blocked_submits: AtomicU64,
    unsynced_bytes: AtomicU64,
}

fn bump(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Queue state (guarded by `Shared::state`)
// ---------------------------------------------------------------------------

enum Item {
    Write {
        ops: SinkOps,
        /// Operations submitted (`ops` holds one per operation).
        n: usize,
        bytes: usize,
        done: Completer<Vec<OpResult>>,
    },
    Flush {
        done: Completer<()>,
    },
}

struct State {
    queue: VecDeque<Item>,
    /// Queued write operations + one per flush marker.
    queued_ops: usize,
    queued_bytes: usize,
    /// Room held by requests being prepared (not queued yet).
    reserved_ops: usize,
    reserved_bytes: usize,
    /// Shutdown requested: no new submissions.
    closed: bool,
    /// The writer loop ended (drained or died).
    exited: bool,
    /// Why the writer died (panic), if it did.
    dead: Option<String>,
    /// Error of the final durability step (buffered mode), reported by shutdown.
    final_error: Option<Error>,
    /// The writer is blocked on `Shared::work` (skip needless wakeups).
    writer_parked: bool,
    /// Submitters blocked on `Shared::space`.
    blocked_submitters: usize,
}

impl State {
    fn admission_error(&self) -> Option<Error> {
        if let Some(reason) = &self.dead {
            Some(dead_error(reason))
        } else if self.closed || self.exited {
            Some(closed_error())
        } else {
            None
        }
    }

    /// An oversized request is admitted when no write is queued or being
    /// prepared, so it can never wait forever.
    fn has_room(&self, ops: usize, bytes: usize, cfg: &GroupCommitConfig) -> bool {
        let held_ops = self.queued_ops.saturating_add(self.reserved_ops);
        let held_bytes = self.queued_bytes.saturating_add(self.reserved_bytes);
        held_ops == 0
            || (held_ops.saturating_add(ops) <= cfg.queue_max_ops
                && held_bytes.saturating_add(bytes) <= cfg.queue_max_bytes)
    }

    fn reserve(&mut self, ops: usize, bytes: usize) {
        self.reserved_ops += ops;
        self.reserved_bytes += bytes;
    }

    fn unreserve(&mut self, ops: usize, bytes: usize) {
        self.reserved_ops -= ops;
        self.reserved_bytes -= bytes;
    }
}

struct Shared {
    state: Mutex<State>,
    /// The writer waits here for work.
    work: Condvar,
    /// Submitters wait here for queue space.
    space: Condvar,
    counters: Counters,
    cfg: GroupCommitConfig,
}

impl Shared {
    /// Push an item (the caller checked admission and room) and wake the
    /// writer if it sleeps. Consumes the guard.
    fn push(&self, mut st: MutexGuard<'_, State>, item: Item, ops: usize, bytes: usize) {
        st.queue.push_back(item);
        st.queued_ops += ops;
        st.queued_bytes += bytes;
        let wake = st.writer_parked;
        drop(st);
        if wake {
            self.work.notify_one();
        }
    }

    /// Release the guard after room was freed without queueing anything:
    /// blocked submitters may fit now.
    fn release(&self, st: MutexGuard<'_, State>) {
        let wake = st.blocked_submitters > 0;
        drop(st);
        if wake {
            self.space.notify_all();
        }
    }

    /// Wait until `ops` / `bytes` fit (or the committer stops).
    fn wait_for_room(&self, ops: usize, bytes: usize) -> Result<MutexGuard<'_, State>> {
        let mut st = lock(&self.state);
        let mut counted = false;
        loop {
            if let Some(e) = st.admission_error() {
                return Err(e);
            }
            if st.has_room(ops, bytes, &self.cfg) {
                return Ok(st);
            }
            if !counted {
                counted = true;
                bump(&self.counters.blocked_submits, 1);
            }
            st.blocked_submitters += 1;
            st = wait(&self.space, st);
            st.blocked_submitters -= 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

/// Group-commit front end of one database. Share it (`&` / `Arc`) between
/// threads; every method takes `&self`.
pub struct GroupCommitter {
    shared: Arc<Shared>,
    handle: Mutex<Option<JoinHandle<()>>>,
    /// The sink, for [`BatchSink::prepare`] in the submitting threads (read
    /// lock held while preparing); released by `shutdown`.
    sink: RwLock<Option<Arc<dyn BatchSink>>>,
}

impl GroupCommitter {
    /// Spawn the writer thread; it owns `sink` until shutdown (submitting
    /// threads use it only for [`BatchSink::prepare`]).
    pub fn new<K: BatchSink + 'static>(sink: K, cfg: GroupCommitConfig) -> Result<GroupCommitter> {
        cfg.validate()?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                queued_ops: 0,
                queued_bytes: 0,
                reserved_ops: 0,
                reserved_bytes: 0,
                closed: false,
                exited: false,
                dead: None,
                final_error: None,
                writer_parked: false,
                blocked_submitters: 0,
            }),
            work: Condvar::new(),
            space: Condvar::new(),
            counters: Counters::default(),
            cfg,
        });
        let sink = Arc::new(sink);
        let for_writer = (shared.clone(), sink.clone());
        let handle = thread::Builder::new()
            .name(shared.cfg.thread_name.clone())
            .spawn(move || writer_main(for_writer.1, for_writer.0))?;
        Ok(GroupCommitter {
            shared,
            handle: Mutex::new(Some(handle)),
            sink: RwLock::new(Some(sink)),
        })
    }

    pub fn config(&self) -> &GroupCommitConfig {
        &self.shared.cfg
    }

    /// Enqueue a request without waiting for its commit. Blocks only while
    /// the queue is full (backpressure); the request is prepared
    /// ([`BatchSink::prepare`]) in this thread once its room is reserved. An
    /// empty request completes at once.
    pub fn submit(&self, ops: Vec<OwnedOp>) -> Result<Ticket> {
        if ops.is_empty() {
            return Ok(Pending::ready(Ok(Vec::new())));
        }
        let n = ops.len();
        let bytes = request_bytes(&ops);
        let st = self.shared.wait_for_room(n, bytes)?;
        self.enqueue(st, ops, n, bytes)
    }

    /// Enqueue a request without ever blocking on the queue: a full queue
    /// hands the operations back, unprepared, in [`TrySubmitError::Full`].
    /// (Preparing an admitted request still takes the time it takes.)
    pub fn try_submit(&self, ops: Vec<OwnedOp>) -> std::result::Result<Ticket, TrySubmitError> {
        if ops.is_empty() {
            return Ok(Pending::ready(Ok(Vec::new())));
        }
        let n = ops.len();
        let bytes = request_bytes(&ops);
        let st = lock(&self.shared.state);
        if let Some(e) = st.admission_error() {
            return Err(TrySubmitError::Closed(e));
        }
        if !st.has_room(n, bytes, &self.shared.cfg) {
            return Err(TrySubmitError::Full(ops));
        }
        self.enqueue(st, ops, n, bytes)
            .map_err(TrySubmitError::Closed)
    }

    /// Reserve the room `st` shows is free, prepare `ops` without any lock
    /// of the queue, then queue them (unless the committer stopped
    /// meanwhile).
    fn enqueue(
        &self,
        mut st: MutexGuard<'_, State>,
        ops: Vec<OwnedOp>,
        n: usize,
        bytes: usize,
    ) -> Result<Ticket> {
        st.reserve(n, bytes);
        drop(st);
        let prepared = self.prepare(ops);
        let mut st = lock(&self.shared.state);
        st.unreserve(n, bytes);
        let ops = match prepared {
            Ok(ops) => ops,
            Err(e) => {
                self.shared.release(st);
                return Err(e);
            }
        };
        if let Some(e) = st.admission_error() {
            self.shared.release(st);
            return Err(e);
        }
        let (done, ticket) = completion_pair();
        self.shared.push(
            st,
            Item::Write {
                ops,
                n,
                bytes,
                done,
            },
            n,
            bytes,
        );
        bump(&self.shared.counters.requests, 1);
        Ok(ticket)
    }

    /// [`BatchSink::prepare`] in the calling thread. A panic fails this
    /// request only.
    fn prepare(&self, ops: Vec<OwnedOp>) -> Result<SinkOps> {
        let sink = self.sink.read().unwrap_or_else(PoisonError::into_inner);
        let Some(sink) = sink.as_ref() else {
            return Err(closed_error());
        };
        match panic::catch_unwind(AssertUnwindSafe(|| sink.prepare(ops))) {
            Ok(prepared) => Ok(prepared),
            Err(payload) => Err(prepare_panicked(payload.as_ref())),
        }
    }

    /// Submit and wait: one result per operation, in order.
    pub fn apply(&self, ops: Vec<OwnedOp>) -> Result<Vec<OpResult>> {
        self.submit(ops)?.wait()
    }

    /// Submit one operation and wait for its own result.
    pub fn apply_one(&self, op: OwnedOp) -> OpResult {
        single_result(self.apply(vec![op])?)
    }

    /// Group-committed put; returns the new revision.
    pub fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        expect: Expect,
    ) -> Result<Revision> {
        self.apply_one(OwnedOp::put(key, value, expect))?
            .ok_or_else(|| {
                Error::Backend("group commit: the sink returned no revision for a put".into())
            })
    }

    /// Group-committed delete; returns whether a live record was deleted.
    pub fn delete(&self, key: impl Into<Vec<u8>>, expect: Expect) -> Result<bool> {
        Ok(self.apply_one(OwnedOp::delete(key, expect))?.is_some())
    }

    /// Wait until everything submitted before this call is durable (in
    /// immediate mode: committed). Returns the error of the sync that was
    /// supposed to make it durable, if it failed.
    pub fn flush(&self) -> Result<()> {
        self.flush_async()?.wait()
    }

    /// Queue a flush marker behind everything submitted so far. The marker
    /// counts as one operation against the queue bound.
    pub fn flush_async(&self) -> Result<Pending<()>> {
        let st = self.shared.wait_for_room(1, 0)?;
        let (done, pending) = completion_pair();
        self.shared.push(st, Item::Flush { done }, 1, 0);
        Ok(pending)
    }

    /// Stop accepting work and let the writer drain the queue, without
    /// waiting. [`GroupCommitter::shutdown`] also waits.
    pub fn close(&self) {
        lock(&self.shared.state).closed = true;
        self.shared.work.notify_all();
        self.shared.space.notify_all();
    }

    /// Reject new work, drain the queue (buffered mode: make it durable) and
    /// join the writer. Idempotent. Returns the error of the final durability
    /// step, or an error if the writer thread died. Must not be called from
    /// the sink. Once it returns, this committer holds no reference to the
    /// sink (a request still being prepared is waited for, then refused).
    pub fn shutdown(&self) -> Result<()> {
        self.close();
        // Wait for the requests being prepared (they are refused: the queue
        // is closed) and drop this handle's reference to the sink.
        self.sink
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        {
            let mut handle = lock(&self.handle);
            if let Some(h) = handle.take()
                && h.join().is_err()
            {
                // `writer_main` catches panics; this is only a safety net.
                let mut st = lock(&self.shared.state);
                st.exited = true;
                st.dead
                    .get_or_insert_with(|| "writer thread panicked".to_string());
            }
        }
        let st = lock(&self.shared.state);
        if let Some(reason) = &st.dead {
            return Err(dead_error(reason));
        }
        match &st.final_error {
            Some(e) => Err(replicate_error(e)),
            None => Ok(()),
        }
    }

    /// Whether the writer thread is still running.
    pub fn is_running(&self) -> bool {
        !lock(&self.shared.state).exited
    }

    pub fn stats(&self) -> GroupCommitStats {
        let (queued_ops, queued_bytes) = {
            let st = lock(&self.shared.state);
            (st.queued_ops as u64, st.queued_bytes as u64)
        };
        let c = &self.shared.counters;
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        GroupCommitStats {
            requests: l(&c.requests),
            ops: l(&c.ops),
            batches: l(&c.batches),
            max_batch_ops: l(&c.max_batch_ops),
            immediate_batches: l(&c.immediate_batches),
            deferred_batches: l(&c.deferred_batches),
            failed_batches: l(&c.failed_batches),
            syncs: l(&c.syncs),
            failed_syncs: l(&c.failed_syncs),
            flushes: l(&c.flushes),
            blocked_submits: l(&c.blocked_submits),
            queued_ops,
            queued_bytes,
            unsynced_bytes: l(&c.unsynced_bytes),
        }
    }
}

impl Drop for GroupCommitter {
    /// Drains the queue and joins the writer (errors are ignored here; call
    /// [`GroupCommitter::shutdown`] to observe them).
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl fmt::Debug for GroupCommitter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupCommitter")
            .field("config", &self.shared.cfg)
            .field("stats", &self.stats())
            .finish()
    }
}

fn request_bytes(ops: &[OwnedOp]) -> usize {
    ops.iter()
        .fold(0usize, |acc, op| acc.saturating_add(op.payload_bytes()))
}

/// The single result of a one-operation request.
pub(crate) fn single_result(results: Vec<OpResult>) -> OpResult {
    let mut it = results.into_iter();
    match (it.next(), it.next()) {
        (Some(r), None) => r,
        _ => Err(Error::Backend(
            "group commit: expected exactly one result".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Writer thread
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Batch {
    /// The operations of the requests taken, in order: all unprepared
    /// (`ops`) or all prepared (`prepared`).
    ops: Vec<OwnedOp>,
    prepared: Vec<PreparedOp>,
    /// Operations taken (submitted count).
    n: usize,
    bytes: usize,
    /// (completion, number of operations) per request, in batch order.
    waiters: Vec<(Completer<Vec<OpResult>>, usize)>,
    flushes: Vec<Completer<()>>,
}

impl Batch {
    /// Whether a request of this kind can join the batch.
    fn same_kind(&self, ops: &SinkOps) -> bool {
        self.n == 0 || matches!(ops, SinkOps::Prepared(_)) == !self.prepared.is_empty()
    }

    fn append(&mut self, ops: SinkOps) {
        match ops {
            SinkOps::Owned(mut ops) => self.ops.append(&mut ops),
            SinkOps::Prepared(mut ops) => self.prepared.append(&mut ops),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Take {
    /// The queue is empty.
    Drained,
    /// The batch reached a limit, or the next request is of the other kind
    /// (prepared / unprepared).
    Full,
    /// A flush marker ends the batch.
    Flush,
}

/// Committed-but-not-durable work (buffered mode).
#[derive(Default)]
struct Unsynced {
    commits: u64,
    bytes: usize,
    /// When the oldest non-durable commit happened (or the last failed sync).
    since: Option<Instant>,
}

impl Unsynced {
    fn pending(&self) -> bool {
        self.commits > 0
    }

    fn add(&mut self, bytes: usize) {
        self.commits += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        self.since.get_or_insert_with(Instant::now);
    }

    fn clear(&mut self) {
        *self = Unsynced::default();
    }

    fn deadline(&self, interval: Duration) -> Option<Instant> {
        if !self.pending() {
            return None;
        }
        self.since.and_then(|s| s.checked_add(interval))
    }
}

fn writer_main<K: BatchSink>(sink: K, shared: Arc<Shared>) {
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| writer_loop(&sink, &shared)));
    let (final_error, dead) = match outcome {
        Ok(Ok(())) => (None, None),
        Ok(Err(e)) => (Some(e), None),
        Err(payload) => (
            None,
            Some(format!(
                "writer thread panicked: {}",
                panic_message(payload.as_ref())
            )),
        ),
    };
    let leftovers = {
        let mut st = lock(&shared.state);
        st.exited = true;
        st.final_error = final_error;
        if dead.is_some() {
            st.dead.clone_from(&dead);
        }
        st.queued_ops = 0;
        st.queued_bytes = 0;
        std::mem::take(&mut st.queue)
    };
    shared.space.notify_all();
    // Only a dead writer leaves work behind (a clean exit requires an empty,
    // closed queue). Fail it outside the lock.
    let reason = dead.unwrap_or_else(|| "the committer stopped".to_string());
    for item in leftovers {
        match item {
            Item::Write { done, .. } => done.complete(Err(dead_error(&reason))),
            Item::Flush { done } => done.complete(Err(dead_error(&reason))),
        }
    }
    drop(sink);
}

fn writer_loop<K: BatchSink + ?Sized>(sink: &K, sh: &Shared) -> Result<()> {
    let cfg = &sh.cfg;
    let (buffered, interval, max_pending) = match cfg.durability {
        WriteDurability::Immediate => (false, Duration::MAX, usize::MAX),
        WriteDurability::Buffered {
            flush_interval,
            max_pending_bytes,
        } => (true, flush_interval, max_pending_bytes),
    };
    let mut batch = Batch::default();
    let mut unsynced = Unsynced::default();
    // Set after a failed sync: the next commit must be durable.
    let mut force_durable = false;
    loop {
        // 1. Wait for work, a durability deadline or shutdown.
        let mut st = lock(&sh.state);
        loop {
            if !st.queue.is_empty() || st.closed {
                break;
            }
            let deadline = if buffered {
                unsynced.deadline(interval)
            } else {
                None
            };
            st.writer_parked = true;
            match deadline {
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        st.writer_parked = false;
                        break;
                    }
                    st = wait_timeout(&sh.work, st, d - now);
                }
                None => st = wait(&sh.work, st),
            }
            st.writer_parked = false;
        }

        // 2. Take a batch; optionally linger for more.
        let mut take = take_batch(&mut st, cfg, &mut batch);
        if take == Take::Drained
            && !cfg.max_delay.is_zero()
            && batch.n > 0
            && !st.closed
            && let Some(until) = Instant::now().checked_add(cfg.max_delay)
        {
            loop {
                let now = Instant::now();
                if now >= until {
                    break;
                }
                st.writer_parked = true;
                st = wait_timeout(&sh.work, st, until - now);
                st.writer_parked = false;
                take = take_batch(&mut st, cfg, &mut batch);
                if take != Take::Drained || st.closed {
                    break;
                }
            }
        }
        let closing = st.closed && st.queue.is_empty();
        let wake_submitters = st.blocked_submitters > 0;
        drop(st);
        if wake_submitters {
            sh.space.notify_all();
        }

        // 3. Commit. In buffered mode the batch itself carries the
        // durability that is owed, instead of a separate sync.
        let now = Instant::now();
        let owe_durable = buffered
            && (force_durable
                || !batch.flushes.is_empty()
                || closing
                || unsynced.deadline(interval).is_some_and(|d| now >= d)
                || unsynced.bytes.saturating_add(batch.bytes) >= max_pending);
        if batch.n > 0 {
            let durability = if !buffered || owe_durable {
                Durability::Immediate
            } else {
                Durability::Deferred
            };
            let bytes = batch.bytes;
            let outcome = apply_batch(sink, sh, &mut batch, durability);
            // A result (even a malformed one) means the sink committed.
            if buffered && outcome.is_ok() {
                if durability == Durability::Immediate {
                    unsynced.clear();
                    force_durable = false;
                } else {
                    unsynced.add(bytes);
                }
            }
            // Account before releasing any caller, so stats never lag an
            // acknowledged write.
            sh.counters
                .unsynced_bytes
                .store(unsynced.bytes as u64, Ordering::Relaxed);
            complete_batch(sh, &mut batch, outcome);
        }

        // 4. Standalone sync when durability is owed and no batch carried it.
        let mut sync_result = None;
        if owe_durable && unsynced.pending() {
            let r = sink.sync();
            match &r {
                Ok(()) => {
                    bump(&sh.counters.syncs, 1);
                    unsynced.clear();
                    force_durable = false;
                }
                Err(_) => {
                    bump(&sh.counters.failed_syncs, 1);
                    force_durable = true;
                    // Retry after another interval (no busy loop).
                    unsynced.since = Some(Instant::now());
                }
            }
            sync_result = Some(r);
        }
        sh.counters
            .unsynced_bytes
            .store(unsynced.bytes as u64, Ordering::Relaxed);

        // 5. Complete flush markers.
        for done in batch.flushes.drain(..) {
            bump(&sh.counters.flushes, 1);
            done.complete(match &sync_result {
                Some(Err(e)) => Err(replicate_error(e)),
                _ => Ok(()),
            });
        }
        if closing {
            return match sync_result {
                Some(Err(e)) => Err(e),
                _ => Ok(()),
            };
        }
    }
}

/// Move requests from the queue into `batch` without splitting any request.
fn take_batch(st: &mut State, cfg: &GroupCommitConfig, batch: &mut Batch) -> Take {
    loop {
        if batch.n >= cfg.max_batch_ops || batch.bytes >= cfg.max_batch_bytes {
            return Take::Full;
        }
        let Some(item) = st.queue.pop_front() else {
            return Take::Drained;
        };
        match item {
            Item::Write {
                ops,
                n,
                bytes,
                done,
            } => {
                let fits = batch.n == 0
                    || (batch.same_kind(&ops)
                        && batch.n + n <= cfg.max_batch_ops
                        && batch.bytes.saturating_add(bytes) <= cfg.max_batch_bytes);
                if !fits {
                    st.queue.push_front(Item::Write {
                        ops,
                        n,
                        bytes,
                        done,
                    });
                    return Take::Full;
                }
                st.queued_ops -= n;
                st.queued_bytes -= bytes;
                batch.waiters.push((done, n));
                batch.n += n;
                batch.bytes = batch.bytes.saturating_add(bytes);
                batch.append(ops);
            }
            Item::Flush { done } => {
                st.queued_ops -= 1;
                batch.flushes.push(done);
                // Consecutive markers share the same durability step.
                while matches!(st.queue.front(), Some(Item::Flush { .. })) {
                    if let Some(Item::Flush { done }) = st.queue.pop_front() {
                        st.queued_ops -= 1;
                        batch.flushes.push(done);
                    }
                }
                return Take::Flush;
            }
        }
    }
}

/// One `apply` (or `apply_prepared`) call for the whole batch (the
/// operations are released).
fn apply_batch<K: BatchSink + ?Sized>(
    sink: &K,
    sh: &Shared,
    batch: &mut Batch,
    durability: Durability,
) -> Result<Vec<OpResult>> {
    let n = batch.n;
    let result = if batch.prepared.is_empty() {
        sink.apply(&batch.ops, durability)
    } else {
        sink.apply_prepared(std::mem::take(&mut batch.prepared), durability)
    };
    batch.ops.clear();
    batch.n = 0;
    batch.bytes = 0;
    let c = &sh.counters;
    bump(&c.batches, 1);
    bump(&c.ops, n as u64);
    c.max_batch_ops.fetch_max(n as u64, Ordering::Relaxed);
    match durability {
        Durability::Immediate => bump(&c.immediate_batches, 1),
        Durability::Deferred => bump(&c.deferred_batches, 1),
    }
    result
}

/// Hand every request of the batch its own slice of the results, or the
/// batch error.
fn complete_batch(sh: &Shared, batch: &mut Batch, outcome: Result<Vec<OpResult>>) {
    let expected: usize = batch.waiters.iter().map(|(_, count)| count).sum();
    match outcome {
        Ok(results) if results.len() == expected => {
            let mut results = results.into_iter();
            for (done, count) in batch.waiters.drain(..) {
                done.complete(Ok(results.by_ref().take(count).collect()));
            }
        }
        Ok(results) => {
            bump(&sh.counters.failed_batches, 1);
            let msg = format!(
                "group commit: the sink returned {} results for {expected} operations (the batch may have been applied)",
                results.len()
            );
            for (done, _) in batch.waiters.drain(..) {
                done.complete(Err(Error::Backend(msg.clone())));
            }
        }
        Err(e) => {
            bump(&sh.counters.failed_batches, 1);
            for (done, _) in batch.waiters.drain(..) {
                done.complete(Err(replicate_error(&e)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validation() {
        assert!(GroupCommitConfig::default().validate().is_ok());
        let bad = GroupCommitConfig {
            max_batch_ops: 0,
            ..GroupCommitConfig::default()
        };
        assert!(bad.validate().is_err());
        let bad = GroupCommitConfig::from(WriteDurability::Buffered {
            flush_interval: Duration::ZERO,
            max_pending_bytes: 1,
        });
        assert!(bad.validate().is_err());
        assert!(
            GroupCommitConfig::from(WriteDurability::buffered(Duration::from_millis(5)))
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn pending_ready_and_taken() {
        let mut p: Pending<u32> = Pending::ready(Ok(7));
        assert!(p.is_ready());
        assert_eq!(p.try_wait().map(|r| r.ok()), Some(Some(7)));
        assert!(matches!(p.try_wait(), Some(Err(Error::InvalidArgument(_)))));
    }

    #[test]
    fn dropped_completer_fails_the_waiter() {
        let (completer, mut pending) = completion_pair::<u32>();
        assert!(pending.try_wait().is_none());
        drop(completer);
        assert!(matches!(
            pending.wait_timeout(Duration::from_secs(5)),
            Some(Err(Error::Backend(_)))
        ));
    }

    #[test]
    fn single_result_requires_exactly_one() {
        assert_eq!(single_result(vec![Ok(Some(3))]).ok(), Some(Some(3)));
        assert!(single_result(vec![]).is_err());
        assert!(single_result(vec![Ok(None), Ok(None)]).is_err());
    }
}
