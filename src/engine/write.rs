//! Write path. Values are validated and encoded outside the write transaction
//! (spread over threads when the estimated encode work is large); inside it
//! each op only checks its expectation, stores or reuses objects, allocates a
//! revision and publishes the manifest. `put`, `delete`, `write_batch`,
//! `write_batch_each` and `write_prepared_each` share the same per-op steps.
//! A batch reads the records its ops replace ahead, with one `get_many` (for
//! the keys that occur once in it), and puts its Inline manifests with one
//! sorted `put_many` before the commit.
//!
//! Small values are prepared as their complete Inline manifest (revision
//! left at 0): inside the transaction the op only writes the revision into
//! it and stores it, and the record it replaces is inspected without copying
//! its envelope. Before preparing, each write feeds the automatic dictionary
//! (`autodict`) and installs a dictionary that finished training.
//!
//! # Prepared ops
//!
//! [`PreparedOp`]s (`Db::prepare`, `Db::prepare_batch`, and the group
//! committer through `BatchSink::prepare`, in the thread that submits them)
//! are encoded before, possibly long before, the transaction that applies
//! them (`Db::write_prepared_each`). What they carry stays exact:
//! - The planner may have switched to a new dictionary or template in
//!   between: the value keeps the representation it was prepared with, which
//!   stays readable because params are immutable and never replaced.
//! - `Db::gc` (it needs `&mut Db`, so it never runs during a write, but it
//!   can run between a prepare and its write) removes params that no stored
//!   value references. The transaction checks that every param a prepared
//!   value needs still exists and otherwise fails that op alone with
//!   `MissingDependency` (prepare it again).
//! - An op prepared by another `Db` handle (other params, block size and
//!   inline threshold) is refused with `InvalidArgument`.
//! - Validation happens when preparing (an invalid op is reported in its
//!   slot when applied); the expectation is checked and dedupe candidates are
//!   compared byte for byte with the raw value inside the transaction,
//!   exactly as in `write_batch_each`.

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::ops::Bound;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use super::ops::{self, PreparedUnit};
use super::read::inline_revision;
use super::{BatchOp, Db, Expect, Revision, prefix_successor};
use crate::chunk;
use crate::error::{Error, Result};
use crate::format::{self, ChunkRef, Manifest, ManifestBody, SourceDescriptor, id_key, meta_key};
use crate::generator::Generator;
use crate::hash::{Digest, StreamHasher};
use crate::planner::Planner;
use crate::store::{Durability, ReadTxn, Store, Table, WriteTxn};

/// Source of `Db` handle ids (`PreparedOp` remembers the handle that made it).
static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);

/// A new, process-unique id for a `Db` handle.
pub(crate) fn new_db_id() -> u64 {
    NEXT_DB_ID.fetch_add(1, Ordering::Relaxed)
}

/// A value encoded outside the write transaction.
pub(crate) enum PreparedValue {
    /// Complete Inline manifest with revision 0 (`ops::prepare_inline`), and
    /// the param its envelope needs, if any.
    Inline { manifest: Vec<u8>, param: Option<u64> },
    /// One unit per `block_size` block, in order.
    Chunks(Vec<PreparedUnit>),
}

/// A write operation validated and encoded ahead of the transaction that
/// applies it: made by [`Db::prepare`] / [`Db::prepare_batch`] (and by the
/// group committer, in the submitting thread), applied by
/// [`Db::write_prepared_each`] of the same handle. It owns its key and its
/// encoded value (plus the raw value for chunked values, which dedupe
/// compares byte for byte); the expectation is checked when it is applied.
pub struct PreparedOp {
    db: u64,
    key: Vec<u8>,
    expect: Expect,
    body: Body,
}

enum Body {
    /// `raw`: the value of a chunked put (dedupe compares candidates with
    /// it); empty for Inline values.
    Put { value: PreparedValue, raw: Vec<u8> },
    Delete,
    /// Refused by validation when prepared; reported when applied.
    Rejected { put: bool, error: Error },
}

impl PreparedOp {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn expect(&self) -> Expect {
        self.expect
    }

    pub fn is_put(&self) -> bool {
        matches!(self.body, Body::Put { .. } | Body::Rejected { put: true, .. })
    }

    /// Why validation refused the op (it will be reported in its slot).
    pub fn error(&self) -> Option<&Error> {
        match &self.body {
            Body::Rejected { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl fmt::Debug for PreparedOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match &self.body {
            Body::Put { value: PreparedValue::Inline { manifest, .. }, .. } => format!("put inline ({} B manifest)", manifest.len()),
            Body::Put { value: PreparedValue::Chunks(units), raw } => format!("put {} B in {} units", raw.len(), units.len()),
            Body::Delete => "delete".to_string(),
            Body::Rejected { error, .. } => format!("rejected ({error})"),
        };
        f.debug_struct("PreparedOp")
            .field("key_len", &self.key.len())
            .field("expect", &self.expect)
            .field("op", &kind)
            .finish()
    }
}

/// One op to prepare, with a borrowed (`Db::prepare_batch`) or owned (group
/// commit) key and value. `value` is None for a delete.
pub(crate) struct Source<'a> {
    pub(crate) key: Cow<'a, [u8]>,
    pub(crate) value: Option<Cow<'a, [u8]>>,
    pub(crate) expect: Expect,
}

/// Failure of one op inside a write transaction.
enum OpError {
    /// Detected before the op changed anything: the op alone can be skipped.
    Skip(Error),
    /// The transaction may hold partial changes of the op: it must be aborted.
    Abort(Error),
}

impl From<Error> for OpError {
    fn from(e: Error) -> Self {
        OpError::Abort(e)
    }
}

impl OpError {
    fn into_error(self) -> Error {
        match self {
            OpError::Skip(e) | OpError::Abort(e) => e,
        }
    }
}

type OpResult<T> = std::result::Result<T, OpError>;

/// Bookkeeping of one write transaction. Counters and the block cache are
/// only touched after a successful commit.
#[derive(Default)]
struct TxnCtx {
    /// Next revision to persist in `meta`: loaded on first use, written once.
    next_revision: Option<u64>,
    /// Objects removed by the transaction (evicted from the cache after commit).
    removed: Vec<u64>,
    puts: u64,
    deletes: u64,
    dedupe_hits: u64,
    objects_written: u64,
    /// Whether the transaction changed anything.
    dirty: bool,
    /// Inline manifests (key, manifest) not in `records` yet: put together,
    /// in key order, with one `put_many` (`flush_records`), before the
    /// commit or before one of their keys is read again. Keys are distinct.
    pending: Vec<(Vec<u8>, Vec<u8>)>,
    /// `key_hash` of every pending key.
    pending_hashes: HashSet<u64>,
}

/// Cheap hash of a record key (a collision only flushes early).
fn key_hash(key: &[u8]) -> u64 {
    let mut h = key.len() as u64;
    for word in key.chunks(8) {
        let mut buf = [0u8; 8];
        buf[..word.len()].copy_from_slice(word);
        h = (h ^ u64::from_le_bytes(buf)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= h >> 29;
    }
    h
}

impl TxnCtx {
    fn defer_record(&mut self, key: &[u8], manifest: Vec<u8>) {
        self.pending_hashes.insert(key_hash(key));
        self.pending.push((key.to_vec(), manifest));
    }

    /// Before `key` is read (and then possibly written) directly: put the
    /// pending records if it may be one of them.
    fn flush_if_pending<W: WriteTxn + ?Sized>(&mut self, w: &mut W, key: &[u8]) -> Result<()> {
        if !self.pending.is_empty() && self.pending_hashes.contains(&key_hash(key)) {
            self.flush_records(w)?;
        }
        Ok(())
    }

    fn flush_records<W: WriteTxn + ?Sized>(&mut self, w: &mut W) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut pending = std::mem::take(&mut self.pending);
        self.pending_hashes.clear();
        pending.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut entries = pending.iter().map(|(k, m)| (k.as_slice(), m.as_slice()));
        w.put_many(Table::Records, &mut entries)
    }

    fn alloc_revision<T: ReadTxn + ?Sized>(&mut self, t: &T) -> Result<Revision> {
        let next = match self.next_revision {
            Some(n) => n,
            None => ops::get_meta_u64(t, meta_key::NEXT_REVISION)?
                .ok_or_else(|| Error::format("missing meta counter next_revision"))?,
        };
        if next == u64::MAX {
            return Err(Error::IdExhausted("revision"));
        }
        self.next_revision = Some(next + 1);
        Ok(next)
    }
}

/// Record currently stored under a key, as seen inside the write transaction.
struct Current {
    raw: Vec<u8>,
    revision: u64,
    tombstone: bool,
    /// Decoded manifest, except for Inline records (which reference no
    /// object, so only their revision matters here).
    manifest: Option<Manifest>,
}

impl Current {
    /// Revision of the live record (a tombstone counts as absent).
    fn live_revision(&self) -> Option<Revision> {
        (!self.tombstone).then_some(self.revision)
    }
}

fn current_of(raw: Vec<u8>) -> OpResult<Current> {
    if let Some(revision) = inline_revision(&raw) {
        return Ok(Current { raw, revision, tombstone: false, manifest: None });
    }
    let manifest = Manifest::decode(&raw).map_err(OpError::Skip)?;
    Ok(Current { revision: manifest.revision, tombstone: manifest.is_tombstone(), manifest: Some(manifest), raw })
}

/// The record an op replaces, when it was read ahead of the op
/// (`Txn::read_ahead`); None: read it when the op runs.
type Ahead = Option<Option<Vec<u8>>>;

/// The record stored under `key` when the op runs: the one read ahead, or
/// read now (after putting the pending records it may be one of).
fn load_current<W: WriteTxn + ?Sized>(w: &mut W, ctx: &mut TxnCtx, key: &[u8], ahead: Ahead) -> OpResult<Option<Current>> {
    let raw = match ahead {
        Some(raw) => raw,
        None => {
            ctx.flush_if_pending(w, key)?;
            w.get(Table::Records, key)?
        }
    };
    raw.map(current_of).transpose()
}

/// Check an optimistic-concurrency condition against the revision of the
/// live record (None if absent; a tombstone counts as absent).
fn check_expect_revision(key: &[u8], expect: Expect, actual: Option<Revision>) -> Result<()> {
    let holds = match expect {
        Expect::Any => true,
        Expect::Absent => actual.is_none(),
        Expect::Revision(r) => actual == Some(r),
    };
    if holds {
        return Ok(());
    }
    let expected = match expect {
        Expect::Any => "any".to_string(),
        Expect::Absent => "absent".to_string(),
        Expect::Revision(r) => format!("revision {r}"),
    };
    Err(Error::RevisionConflict { key: key.to_vec(), expected, actual })
}

/// Deletes only reject empty keys, so records written under a larger
/// `max_key_len` stay deletable.
fn check_delete_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidArgument("empty key".into()));
    }
    Ok(())
}

fn foreign_op() -> Error {
    Error::InvalidArgument("the operation was prepared by another database handle".into())
}

// ---------------------------------------------------------------------------
// Preparation (outside the write transaction)
// ---------------------------------------------------------------------------

/// Inputs below this many units are always encoded on the calling thread.
const PARALLEL_MIN_UNITS: usize = 16;
/// Units encoded first on the calling thread to estimate the per-unit cost.
const PROBE_UNITS: usize = 4;
/// Estimated encode work each thread must get for the rest to be spread over
/// threads (starting a thread costs tens of microseconds): at least two
/// threads' worth, then one thread per this much work.
const PER_THREAD_MIN_WORK: Duration = Duration::from_micros(250);
const MAX_PREPARE_THREADS: usize = 8;

fn prepare_threads() -> usize {
    static THREADS: OnceLock<usize> = OnceLock::new();
    *THREADS.get_or_init(|| {
        std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(MAX_PREPARE_THREADS)
    })
}

/// Threads worth starting for `estimate` of work (at most `max`).
fn threads_for(estimate: Duration, max: usize) -> usize {
    let n = estimate.as_nanos() / PER_THREAD_MIN_WORK.as_nanos().max(1);
    usize::try_from(n).unwrap_or(usize::MAX).min(max)
}

/// `f` over `inputs`, preserving order. When the first inputs show that the
/// rest is expensive, the rest is split over scoped threads; the output is
/// the same.
fn map_units<I: Sync, T: Send>(inputs: &[I], f: &(impl Fn(&I) -> T + Sync)) -> Vec<T> {
    let max_threads = prepare_threads();
    if inputs.len() < PARALLEL_MIN_UNITS || max_threads < 2 {
        return inputs.iter().map(f).collect();
    }
    let started = Instant::now();
    let (probe, rest) = inputs.split_at(PROBE_UNITS);
    let mut out = Vec::with_capacity(inputs.len());
    out.extend(probe.iter().map(f));
    let per_unit = started.elapsed() / PROBE_UNITS as u32;
    let estimate = per_unit.saturating_mul(u32::try_from(rest.len()).unwrap_or(u32::MAX));
    let threads = threads_for(estimate, max_threads);
    if threads < 2 {
        out.extend(rest.iter().map(f));
    } else {
        out.extend(map_parallel(rest, threads, f));
    }
    out
}

/// `f` over `inputs` with up to `threads` threads (the caller's included).
fn map_parallel<I: Sync, T: Send>(inputs: &[I], threads: usize, f: &(impl Fn(&I) -> T + Sync)) -> Vec<T> {
    let per_thread = inputs.len().div_ceil(threads.max(1)).max(1);
    std::thread::scope(|scope| {
        let mut parts = inputs.chunks(per_thread);
        let first = parts.next().unwrap_or(&[]);
        let spawned: Vec<_> = parts
            .map(|part| {
                let handle = std::thread::Builder::new()
                    .name("babeldb-prepare".into())
                    .spawn_scoped(scope, move || part.iter().map(f).collect::<Vec<T>>());
                (part, handle)
            })
            .collect();
        let mut out = Vec::with_capacity(inputs.len());
        out.extend(first.iter().map(f));
        for (part, handle) in spawned {
            match handle {
                Ok(h) => match h.join() {
                    Ok(units) => out.extend(units),
                    Err(panic) => std::panic::resume_unwind(panic),
                },
                // No thread available: encode this part here.
                Err(_) => out.extend(part.iter().map(f)),
            }
        }
        out
    })
}

/// Encode units, preserving order (spread over threads when expensive).
pub(crate) fn prepare_units(planner: &Planner, inputs: &[&[u8]]) -> Vec<PreparedUnit> {
    map_units(inputs, &|d: &&[u8]| ops::prepare_unit(planner, d))
}

/// One unit of `prepare_values`: a whole small value or one block.
enum Job<'a> {
    Inline(&'a [u8]),
    Block(&'a [u8]),
}

enum Built {
    Inline(Vec<u8>, Option<u64>),
    Block(PreparedUnit),
}

fn build(planner: &Planner, job: &Job<'_>) -> Built {
    match *job {
        Job::Inline(v) => {
            let (manifest, param) = ops::prepare_inline(planner, v);
            Built::Inline(manifest, param)
        }
        Job::Block(b) => Built::Block(ops::prepare_unit(planner, b)),
    }
}

/// The jobs of `values`, in order: one per inline value, one per block.
fn jobs_of<'v>(values: &[&'v [u8]], inline_max: usize, block_size: usize) -> Vec<Job<'v>> {
    let mut jobs = Vec::with_capacity(values.len());
    for &value in values {
        if value.len() <= inline_max {
            jobs.push(Job::Inline(value));
        } else {
            jobs.extend(chunk::split(value.len(), block_size).map(|r| Job::Block(&value[r])));
        }
    }
    jobs
}

/// Prepared values, in order, from the built jobs of `values` (`jobs_of`).
struct Assemble<'v, I> {
    values: std::slice::Iter<'v, &'v [u8]>,
    built: I,
    inline_max: usize,
    block_size: usize,
}

impl<I: Iterator<Item = Built>> Iterator for Assemble<'_, I> {
    type Item = PreparedValue;

    fn next(&mut self) -> Option<PreparedValue> {
        let value = self.values.next()?;
        if value.len() <= self.inline_max {
            return match self.built.next() {
                Some(Built::Inline(manifest, param)) => Some(PreparedValue::Inline { manifest, param }),
                _ => unreachable!("one prepared manifest per inline value"),
            };
        }
        let units = (&mut self.built)
            .take(value.len().div_ceil(self.block_size))
            .map(|b| match b {
                Built::Block(u) => u,
                Built::Inline(..) => unreachable!("blocks of a chunked value"),
            })
            .collect();
        Some(PreparedValue::Chunks(units))
    }
}

/// Jobs per chunk of a pipelined preparation.
const PIPELINE_CHUNK: usize = 32;

type ChunkResult = std::thread::Result<Vec<Built>>;

/// Chunks of jobs encoded by helper threads ahead of the consumer
/// (`Db::with_prepared`). Chunks are claimed in order.
struct Pipeline<'j, 'v> {
    jobs: &'j [Job<'v>],
    planner: &'j Planner,
    chunks: usize,
    claimed: AtomicUsize,
    done: Mutex<Vec<Option<ChunkResult>>>,
    ready: Condvar,
    /// The consumer is gone: claim nothing more.
    stop: AtomicBool,
}

impl Pipeline<'_, '_> {
    fn claim(&self) -> Option<usize> {
        if self.stop.load(Ordering::Acquire) {
            return None;
        }
        let c = self.claimed.fetch_add(1, Ordering::AcqRel);
        (c < self.chunks).then_some(c)
    }

    /// Encode chunk `c`; a panic is handed to the consumer.
    fn encode(&self, c: usize) {
        let jobs = &self.jobs[c * PIPELINE_CHUNK..((c + 1) * PIPELINE_CHUNK).min(self.jobs.len())];
        let built = std::panic::catch_unwind(AssertUnwindSafe(|| {
            jobs.iter().map(|job| build(self.planner, job)).collect::<Vec<Built>>()
        }));
        lock_ignoring_poison(&self.done)[c] = Some(built);
        self.ready.notify_all();
    }

    /// Chunk `c`: waited for when a helper has it, encoded here otherwise.
    fn take(&self, c: usize) -> Vec<Built> {
        loop {
            {
                let mut done = lock_ignoring_poison(&self.done);
                loop {
                    if let Some(r) = done[c].take() {
                        return r.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
                    }
                    if self.claimed.load(Ordering::Acquire) <= c {
                        break;
                    }
                    done = self.ready.wait(done).unwrap_or_else(PoisonError::into_inner);
                }
            }
            // Nobody has claimed `c`: encode the next unclaimed chunk here
            // (`c`, unless a helper just took it).
            if let Some(k) = self.claim() {
                self.encode(k);
            }
        }
    }
}

fn lock_ignoring_poison<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The built jobs of a pipeline, in order (consumer side).
struct Chunks<'p, 'j, 'v> {
    pipeline: &'p Pipeline<'j, 'v>,
    next: usize,
    current: std::vec::IntoIter<Built>,
}

impl Iterator for Chunks<'_, '_, '_> {
    type Item = Built;

    fn next(&mut self) -> Option<Built> {
        loop {
            if let Some(b) = self.current.next() {
                return Some(b);
            }
            if self.next >= self.pipeline.chunks {
                return None;
            }
            self.current = self.pipeline.take(self.next).into_iter();
            self.next += 1;
        }
    }
}

impl Drop for Chunks<'_, '_, '_> {
    fn drop(&mut self) {
        self.pipeline.stop.store(true, Ordering::Release);
    }
}

/// Run `consume` over the built `jobs`, in order, while `helpers` threads
/// encode chunks ahead of it (the caller encodes the chunks nobody took). A
/// helper's panic is resumed in the caller; once `consume` returns, the
/// helpers stop after their current chunk.
fn pipelined<R>(
    planner: &Planner,
    jobs: &[Job<'_>],
    helpers: usize,
    consume: impl FnOnce(&mut dyn Iterator<Item = Built>) -> R,
) -> R {
    let chunks = jobs.len().div_ceil(PIPELINE_CHUNK);
    let pipeline = Pipeline {
        jobs,
        planner,
        chunks,
        claimed: AtomicUsize::new(0),
        done: Mutex::new((0..chunks).map(|_| None).collect()),
        ready: Condvar::new(),
        stop: AtomicBool::new(false),
    };
    std::thread::scope(|scope| {
        for _ in 0..helpers {
            // Without a thread, the caller encodes the chunks itself.
            let _ = std::thread::Builder::new().name("babeldb-prepare".into()).spawn_scoped(scope, || {
                while let Some(c) = pipeline.claim() {
                    pipeline.encode(c);
                }
            });
        }
        consume(&mut Chunks { pipeline: &pipeline, next: 0, current: Vec::new().into_iter() })
    })
}

impl<S: Store> Db<S> {
    /// Keys of new records: non-empty and at most `max_key_len` bytes.
    pub(crate) fn check_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidArgument("empty key".into()));
        }
        if key.len() > self.cfg.max_key_len {
            return Err(Error::LimitExceeded(format!(
                "key of {} bytes exceeds max_key_len {}",
                key.len(),
                self.cfg.max_key_len
            )));
        }
        Ok(())
    }

    pub(crate) fn check_value_len(&self, len: u64) -> Result<()> {
        if len > self.cfg.max_value_len {
            return Err(Error::LimitExceeded(format!(
                "value of {len} bytes exceeds max_value_len {}",
                self.cfg.max_value_len
            )));
        }
        Ok(())
    }

    fn check_op(&self, op: &BatchOp<'_>) -> Result<()> {
        match *op {
            BatchOp::Put { key, value, .. } => self.check_put(key, value),
            BatchOp::Delete { key, .. } => check_delete_key(key),
        }
    }

    fn check_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_key(key)?;
        self.check_value_len(value.len() as u64)
    }

    /// Encode one value: a complete Inline manifest when `len <= inline_max`,
    /// otherwise one unit per block.
    pub(crate) fn prepare_value(&self, planner: &Planner, value: &[u8]) -> PreparedValue {
        if value.len() <= self.inline_max as usize {
            let (manifest, param) = ops::prepare_inline(planner, value);
            return PreparedValue::Inline { manifest, param };
        }
        let blocks: Vec<&[u8]> = chunk::split(value.len(), self.block_size as usize)
            .map(|r| &value[r])
            .collect();
        PreparedValue::Chunks(prepare_units(planner, &blocks))
    }

    /// `prepare_value` for many values; every unit of every value is encoded
    /// in one parallel map so small values are spread over threads too.
    pub(crate) fn prepare_values(&self, planner: &Planner, values: &[&[u8]]) -> Vec<PreparedValue> {
        if let [value] = values {
            return vec![self.prepare_value(planner, value)];
        }
        let jobs = jobs_of(values, self.inline_max as usize, self.block_size as usize);
        let built = map_units(&jobs, &|job: &Job<'_>| build(planner, job));
        self.assemble(values, built.into_iter()).collect()
    }

    fn assemble<'v, I: Iterator<Item = Built>>(&self, values: &'v [&'v [u8]], built: I) -> Assemble<'v, I> {
        Assemble { values: values.iter(), built, inline_max: self.inline_max as usize, block_size: self.block_size as usize }
    }

    /// Run `apply` with the prepared `values`, in order. When encoding them
    /// is expensive, helper threads encode chunks ahead while `apply` (the
    /// write transaction) consumes the ones already done, instead of
    /// everything being encoded first; the values are the same.
    fn with_prepared<R>(
        &self,
        planner: &Planner,
        values: &[&[u8]],
        apply: impl FnOnce(&mut dyn Iterator<Item = PreparedValue>) -> R,
    ) -> R {
        let jobs = jobs_of(values, self.inline_max as usize, self.block_size as usize);
        let max_threads = prepare_threads();
        if jobs.len() < 2 * PIPELINE_CHUNK || max_threads < 2 {
            let built = map_units(&jobs, &|job: &Job<'_>| build(planner, job));
            return apply(&mut self.assemble(values, built.into_iter()));
        }
        let started = Instant::now();
        let (probe, rest) = jobs.split_at(PROBE_UNITS);
        let probed: Vec<Built> = probe.iter().map(|job| build(planner, job)).collect();
        let per_job = started.elapsed() / PROBE_UNITS as u32;
        let estimate = per_job.saturating_mul(u32::try_from(rest.len()).unwrap_or(u32::MAX));
        // The caller applies meanwhile, and encodes a chunk itself only when
        // the helpers fall behind.
        let helpers = threads_for(estimate, max_threads - 1);
        if helpers == 0 {
            let built = probed.into_iter().chain(rest.iter().map(|job| build(planner, job)));
            return apply(&mut self.assemble(values, built));
        }
        pipelined(planner, rest, helpers, |built| apply(&mut self.assemble(values, probed.into_iter().chain(built))))
    }

    // -----------------------------------------------------------------------
    // Per-op steps (inside the write transaction)
    // -----------------------------------------------------------------------

    /// Check the expectation, then publish the new manifest that `build`
    /// produces. New references are taken before the replaced manifest is
    /// retired, so shared objects never drop to zero in between.
    fn apply_record<W: WriteTxn + ?Sized>(
        &self,
        w: &mut W,
        key: &[u8],
        expect: Expect,
        ahead: Ahead,
        ctx: &mut TxnCtx,
        build: impl FnOnce(&mut W, &mut TxnCtx) -> Result<NewRecord>,
    ) -> OpResult<Revision> {
        let old = load_current(w, ctx, key, ahead)?;
        check_expect_revision(key, expect, old.as_ref().and_then(Current::live_revision)).map_err(OpError::Skip)?;
        let record = build(w, ctx)?;
        let revision = ctx.alloc_revision(&*w)?;
        self.retire(w, key, old, ctx)?;
        match record {
            NewRecord::Encoded(mut manifest) => {
                ops::set_manifest_revision(&mut manifest, revision);
                ctx.defer_record(key, manifest);
            }
            NewRecord::Body { logical_len, body } => {
                ops::put_manifest(w, key, &Manifest { revision, logical_len, source_id: None, body })?;
            }
        }
        ctx.puts += 1;
        ctx.dirty = true;
        Ok(revision)
    }

    /// `value`: the raw value; only chunked values read it (dedupe).
    #[allow(clippy::too_many_arguments)]
    fn apply_put<W: WriteTxn + ?Sized>(
        &self,
        w: &mut W,
        key: &[u8],
        value: &[u8],
        prepared: PreparedValue,
        expect: Expect,
        ahead: Ahead,
        ctx: &mut TxnCtx,
    ) -> OpResult<Revision> {
        let block_size = self.block_size as usize;
        let dedupe = self.dedupe();
        self.apply_record(w, key, expect, ahead, ctx, |w, ctx| match prepared {
            PreparedValue::Inline { manifest, .. } => Ok(NewRecord::Encoded(manifest)),
            PreparedValue::Chunks(units) => {
                if units.len() != value.len().div_ceil(block_size) {
                    return Err(Error::InvalidArgument("prepared units do not match the value".into()));
                }
                let mut refs = Vec::with_capacity(units.len());
                for (unit, range) in units.iter().zip(chunk::split(value.len(), block_size)) {
                    let (object_id, reused) = ops::store_unit(w, unit, &value[range.clone()], dedupe, &self.params)?;
                    if reused {
                        ctx.dedupe_hits += 1;
                    } else {
                        ctx.objects_written += 1;
                    }
                    refs.push(ChunkRef { logical_end: range.end as u64, object_id });
                }
                Ok(NewRecord::Body { logical_len: value.len() as u64, body: ManifestBody::Chunks(refs) })
            }
        })
    }

    /// Returns the revision of the deleted record, or None if there was none.
    fn apply_delete<W: WriteTxn + ?Sized>(
        &self,
        w: &mut W,
        key: &[u8],
        expect: Expect,
        ahead: Ahead,
        ctx: &mut TxnCtx,
    ) -> OpResult<Option<Revision>> {
        let old = load_current(w, ctx, key, ahead)?;
        check_expect_revision(key, expect, old.as_ref().and_then(Current::live_revision)).map_err(OpError::Skip)?;
        let Some(old) = old.filter(|c| !c.tombstone) else {
            return Ok(None);
        };
        let deleted = old.revision;
        if self.cfg.keep_history {
            let revision = ctx.alloc_revision(&*w)?;
            w.put(Table::History, &format::history_key(key, deleted), &old.raw)?;
            let tombstone = Manifest { revision, logical_len: 0, source_id: None, body: ManifestBody::Tombstone };
            ops::put_manifest(w, key, &tombstone)?;
        } else {
            w.remove(Table::Records, key)?;
            if let Some(m) = &old.manifest {
                ctx.removed.extend(ops::release_manifest(w, m)?);
            }
        }
        ctx.deletes += 1;
        ctx.dirty = true;
        Ok(Some(deleted))
    }

    /// Retire a replaced manifest: kept in `history` (its references stay
    /// counted) or released.
    fn retire<W: WriteTxn + ?Sized>(
        &self,
        w: &mut W,
        key: &[u8],
        old: Option<Current>,
        ctx: &mut TxnCtx,
    ) -> Result<()> {
        let Some(old) = old else {
            return Ok(());
        };
        if self.cfg.keep_history {
            w.put(Table::History, &format::history_key(key, old.revision), &old.raw)
        } else {
            if let Some(m) = &old.manifest {
                ctx.removed.extend(ops::release_manifest(w, m)?);
            }
            Ok(())
        }
    }

    fn commit_txn<W: WriteTxn>(&self, mut w: W, mut ctx: TxnCtx, durability: Durability) -> Result<()> {
        ctx.flush_records(&mut w)?;
        if let Some(next) = ctx.next_revision {
            ops::put_meta_u64(&mut w, meta_key::NEXT_REVISION, next)?;
        }
        w.commit(durability)?;
        for &id in &ctx.removed {
            self.cache.remove(id);
        }
        let c = &self.counters;
        c.commits.fetch_add(1, Ordering::Relaxed);
        for (counter, n) in [
            (&c.puts, ctx.puts),
            (&c.deletes, ctx.deletes),
            (&c.dedupe_hits, ctx.dedupe_hits),
            (&c.objects_written, ctx.objects_written),
        ] {
            if n > 0 {
                counter.fetch_add(n, Ordering::Relaxed);
            }
        }
        Ok(())
    }
}

/// The manifest an op publishes, before its revision is allocated.
enum NewRecord {
    /// Encoded manifest with revision 0 (Inline values, prepared outside the
    /// transaction).
    Encoded(Vec<u8>),
    /// Body to encode once the revision is known.
    Body { logical_len: u64, body: ManifestBody },
}

/// A write transaction that applies ops one by one.
struct Txn<'d, S: Store> {
    db: &'d Db<S>,
    w: S::Write<'d>,
    ctx: TxnCtx,
    /// Params found to exist in this transaction (`require_params`).
    params_seen: Vec<u64>,
    /// Records read ahead, by op index (`read_ahead`).
    ahead: Vec<Ahead>,
}

impl<'d, S: Store> Txn<'d, S> {
    fn begin(db: &'d Db<S>) -> Result<Self> {
        Ok(Txn { db, w: db.store.begin_write()?, ctx: TxnCtx::default(), params_seen: Vec::new(), ahead: Vec::new() })
    }

    /// Read ahead, with one `get_many`, the records replaced by the ops
    /// whose key no other op of the batch has (`keys[i]`: the key of op `i`,
    /// None for an op that is not applied). Nothing else in the transaction
    /// writes such a key before its op runs, so the op finds the same record
    /// it would read then. Ops sharing a key (or a key hash) read theirs
    /// when they run.
    fn read_ahead(&mut self, keys: &[Option<&[u8]>]) -> Result<()> {
        let hashes: Vec<Option<u64>> = keys.iter().map(|k| k.map(key_hash)).collect();
        let mut sorted: Vec<u64> = hashes.iter().flatten().copied().collect();
        if sorted.len() < 2 {
            return Ok(());
        }
        sorted.sort_unstable();
        let unique = |h: &u64| sorted.get(sorted.partition_point(|x| x < h) + 1) != Some(h);
        let (index, wanted): (Vec<usize>, Vec<&[u8]>) = keys
            .iter()
            .zip(&hashes)
            .enumerate()
            .filter_map(|(i, pair)| match pair {
                (Some(key), Some(h)) if unique(h) => Some((i, *key)),
                _ => None,
            })
            .unzip();
        if wanted.is_empty() {
            return Ok(());
        }
        let records = self.w.get_many(Table::Records, &wanted)?;
        self.ahead = (0..keys.len()).map(|_| None).collect();
        for (i, record) in index.into_iter().zip(records) {
            self.ahead[i] = Some(record);
        }
        Ok(())
    }

    /// Op `i`'s record, if it was read ahead.
    fn take_ahead(&mut self, i: usize) -> Ahead {
        self.ahead.get_mut(i).and_then(Option::take)
    }

    /// Op `i` of the transaction: a put.
    fn put(&mut self, i: usize, key: &[u8], raw: &[u8], value: PreparedValue, expect: Expect) -> OpResult<Revision> {
        let ahead = self.take_ahead(i);
        self.db.apply_put(&mut self.w, key, raw, value, expect, ahead, &mut self.ctx)
    }

    /// Op `i` of the transaction: a delete.
    fn delete(&mut self, i: usize, key: &[u8], expect: Expect) -> OpResult<Option<Revision>> {
        let ahead = self.take_ahead(i);
        self.db.apply_delete(&mut self.w, key, expect, ahead, &mut self.ctx)
    }

    /// Every param `value` needs must exist (see "Prepared ops"); a missing
    /// one fails the op before it changed anything.
    fn require_params(&mut self, value: &PreparedValue) -> OpResult<()> {
        match value {
            PreparedValue::Inline { param: Some(id), .. } => self.require_param(*id),
            PreparedValue::Inline { param: None, .. } => Ok(()),
            PreparedValue::Chunks(units) => {
                for unit in units {
                    if let Some(id) = ops::envelope_param(&unit.envelope).map_err(OpError::Skip)? {
                        self.require_param(id)?;
                    }
                }
                Ok(())
            }
        }
    }

    fn require_param(&mut self, id: u64) -> OpResult<()> {
        if self.params_seen.contains(&id) {
            return Ok(());
        }
        // Params only disappear through `gc`, which also drops them from the
        // cache: a cached param exists.
        if self.db.params.get(id).is_none() && self.w.get(Table::Params, &id_key(id))?.is_none() {
            return Err(OpError::Skip(Error::MissingDependency { param_id: id }));
        }
        self.params_seen.push(id);
        Ok(())
    }

    /// Commit when anything changed (otherwise the transaction is dropped).
    fn commit(self, durability: Durability) -> Result<()> {
        if self.ctx.dirty {
            self.db.commit_txn(self.w, self.ctx, durability)?;
        }
        Ok(())
    }
}

/// Record the outcome of one op of a `*_each` batch: a skipped op is reported
/// in its slot, an abort fails the whole batch.
fn push_each(results: &mut Vec<Result<Option<Revision>>>, outcome: OpResult<Option<Revision>>) -> Result<()> {
    match outcome {
        Ok(r) => results.push(Ok(r)),
        Err(OpError::Skip(e)) => results.push(Err(e)),
        Err(OpError::Abort(e)) => return Err(e),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry points (called by the public methods in `engine/mod.rs`)
// ---------------------------------------------------------------------------

pub(super) fn put<S: Store>(db: &Db<S>, key: &[u8], value: &[u8], expect: Expect) -> Result<Revision> {
    db.check_put(key, value)?;
    db.auto_dictionary_step(&[value]);
    let prepared = db.prepare_value(&db.planner(), value);
    let mut txn = Txn::begin(db)?;
    let revision = txn.put(0, key, value, prepared, expect).map_err(OpError::into_error)?;
    txn.commit(Durability::Immediate)?;
    Ok(revision)
}

pub(super) fn put_generated<S: Store>(
    db: &Db<S>,
    key: &[u8],
    generator_id: u16,
    generator_version: u16,
    params: &[u8],
    expect: Expect,
) -> Result<Revision> {
    db.check_key(key)?;
    if params.len() > format::MAX_PARAMS_LEN {
        return Err(Error::LimitExceeded(format!(
            "generator params of {} bytes exceed {}",
            params.len(),
            format::MAX_PARAMS_LEN
        )));
    }
    let generator = db.generators.get(generator_id, generator_version)?;
    let len = generator.output_len(params)?;
    db.check_value_len(len)?;
    let digest = generated_digest(generator, params, len)?;
    let mut txn = Txn::begin(db)?;
    let revision = db
        .apply_record(&mut txn.w, key, expect, None, &mut txn.ctx, |_, _| {
            let body = ManifestBody::Generated { generator_id, generator_version, params: params.to_vec(), digest };
            Ok(NewRecord::Body { logical_len: len, body })
        })
        .map_err(OpError::into_error)?;
    txn.commit(Durability::Immediate)?;
    Ok(revision)
}

/// BLAKE3 of a generator's output, streamed in 64 KiB pieces.
fn generated_digest(generator: &dyn Generator, params: &[u8], len: u64) -> Result<Digest> {
    const PIECE: u64 = 64 * 1024;
    let mut buf = vec![0u8; len.min(PIECE) as usize];
    let mut hasher = StreamHasher::new();
    let mut offset = 0u64;
    while offset < len {
        let n = (len - offset).min(PIECE) as usize;
        generator.generate(params, offset, &mut buf[..n])?;
        hasher.update(&buf[..n]);
        offset += n as u64;
    }
    Ok(hasher.finalize())
}

pub(super) fn delete<S: Store>(db: &Db<S>, key: &[u8], expect: Expect) -> Result<bool> {
    check_delete_key(key)?;
    let mut txn = Txn::begin(db)?;
    let deleted = txn.delete(0, key, expect).map_err(OpError::into_error)?;
    txn.commit(Durability::Immediate)?;
    Ok(deleted.is_some())
}

/// Values of the valid puts of a batch, in order.
fn valid_put_values<'a>(batch: &[BatchOp<'a>], valid: impl Fn(usize) -> bool) -> Vec<&'a [u8]> {
    batch
        .iter()
        .enumerate()
        .filter_map(|(i, op)| match *op {
            BatchOp::Put { value, .. } if valid(i) => Some(value),
            _ => None,
        })
        .collect()
}

fn missing_prepared() -> OpError {
    OpError::Abort(Error::InvalidArgument("batch put without a prepared value".into()))
}

/// Apply op `i` of a borrowed batch (its value, if a put, is the next of
/// `prepared`).
fn apply_borrowed<S: Store>(
    txn: &mut Txn<'_, S>,
    i: usize,
    op: &BatchOp<'_>,
    prepared: &mut dyn Iterator<Item = PreparedValue>,
) -> OpResult<Option<Revision>> {
    match *op {
        BatchOp::Put { key, value, expect } => match prepared.next() {
            Some(p) => txn.put(i, key, value, p, expect).map(Some),
            None => Err(missing_prepared()),
        },
        BatchOp::Delete { key, expect } => txn.delete(i, key, expect),
    }
}

fn op_key<'a>(op: &BatchOp<'a>) -> &'a [u8] {
    match *op {
        BatchOp::Put { key, .. } | BatchOp::Delete { key, .. } => key,
    }
}

pub(super) fn write_batch<S: Store>(db: &Db<S>, batch: &[BatchOp<'_>]) -> Result<Vec<Option<Revision>>> {
    for op in batch {
        db.check_op(op)?;
    }
    if batch.is_empty() {
        return Ok(Vec::new());
    }
    let values = valid_put_values(batch, |_| true);
    db.auto_dictionary_step(&values);
    let planner = db.planner();
    db.with_prepared(&planner, &values, |prepared| {
        let mut txn = Txn::begin(db)?;
        let keys: Vec<Option<&[u8]>> = batch.iter().map(|op| Some(op_key(op))).collect();
        txn.read_ahead(&keys)?;
        let mut results = Vec::with_capacity(batch.len());
        for (i, op) in batch.iter().enumerate() {
            results.push(apply_borrowed(&mut txn, i, op, prepared).map_err(OpError::into_error)?);
        }
        txn.commit(Durability::Immediate)?;
        Ok(results)
    })
}

pub(super) fn write_batch_each<S: Store>(
    db: &Db<S>,
    batch: &[BatchOp<'_>],
    durability: Durability,
) -> Result<Vec<Result<Option<Revision>>>> {
    let checks: Vec<Result<()>> = batch.iter().map(|op| db.check_op(op)).collect();
    if checks.iter().all(|c| c.is_err()) {
        return Ok(checks.into_iter().map(|c| c.map(|()| None)).collect());
    }
    let values = valid_put_values(batch, |i| checks[i].is_ok());
    db.auto_dictionary_step(&values);
    let planner = db.planner();
    db.with_prepared(&planner, &values, |prepared| {
        let mut txn = Txn::begin(db)?;
        let keys: Vec<Option<&[u8]>> = batch.iter().zip(&checks).map(|(op, c)| c.is_ok().then(|| op_key(op))).collect();
        txn.read_ahead(&keys)?;
        let mut results = Vec::with_capacity(batch.len());
        for (i, (op, check)) in batch.iter().zip(checks).enumerate() {
            match check {
                Err(e) => results.push(Err(e)),
                Ok(()) => push_each(&mut results, apply_borrowed(&mut txn, i, op, prepared))?,
            }
        }
        txn.commit(durability)?;
        Ok(results)
    })
}

/// Validate and encode `sources` (one auto-dictionary step and one parallel
/// map for all of them). Invalid ops become `Rejected`.
pub(crate) fn prepare_ops<S: Store>(db: &Db<S>, sources: Vec<Source<'_>>) -> Vec<PreparedOp> {
    let checks: Vec<Result<()>> = sources
        .iter()
        .map(|s| match &s.value {
            Some(v) => db.check_put(&s.key, v),
            None => check_delete_key(&s.key),
        })
        .collect();
    let values: Vec<&[u8]> = sources
        .iter()
        .zip(&checks)
        .filter_map(|(s, check)| match (&s.value, check) {
            (Some(v), Ok(())) => Some(&**v),
            _ => None,
        })
        .collect();
    let prepared = if values.is_empty() {
        Vec::new()
    } else {
        db.auto_dictionary_step(&values);
        db.prepare_values(&db.planner(), &values)
    };
    drop(values);
    let mut prepared = prepared.into_iter();
    sources
        .into_iter()
        .zip(checks)
        .map(|(Source { key, value, expect }, check)| {
            let body = match (check, value) {
                (Err(error), value) => Body::Rejected { put: value.is_some(), error },
                (Ok(()), None) => Body::Delete,
                (Ok(()), Some(v)) => match prepared.next() {
                    Some(p @ PreparedValue::Inline { .. }) => Body::Put { value: p, raw: Vec::new() },
                    Some(p @ PreparedValue::Chunks(_)) => Body::Put { value: p, raw: v.into_owned() },
                    None => unreachable!("one prepared value per valid put"),
                },
            };
            PreparedOp { db: db.id, key: key.into_owned(), expect, body }
        })
        .collect()
}

pub(super) fn write_prepared_each<S: Store>(
    db: &Db<S>,
    ops: Vec<PreparedOp>,
    durability: Durability,
) -> Result<Vec<Result<Option<Revision>>>> {
    let applicable = |op: &PreparedOp| op.db == db.id && !matches!(op.body, Body::Rejected { .. });
    if !ops.iter().any(applicable) {
        return Ok(ops
            .into_iter()
            .map(|op| match op.body {
                Body::Rejected { error, .. } if op.db == db.id => Err(error),
                _ => Err(foreign_op()),
            })
            .collect());
    }
    let mut txn = Txn::begin(db)?;
    let keys: Vec<Option<&[u8]>> = ops.iter().map(|op| applicable(op).then_some(op.key.as_slice())).collect();
    txn.read_ahead(&keys)?;
    drop(keys);
    let mut results = Vec::with_capacity(ops.len());
    for (i, PreparedOp { db: owner, key, expect, body }) in ops.into_iter().enumerate() {
        if owner != db.id {
            results.push(Err(foreign_op()));
            continue;
        }
        let outcome = match body {
            Body::Rejected { error, .. } => {
                results.push(Err(error));
                continue;
            }
            Body::Delete => txn.delete(i, &key, expect),
            Body::Put { value, raw } => match txn.require_params(&value) {
                Ok(()) => txn.put(i, &key, &raw, value, expect).map(Some),
                Err(e) => Err(e),
            },
        };
        push_each(&mut results, outcome)?;
    }
    txn.commit(durability)?;
    Ok(results)
}

pub(super) fn sync<S: Store>(db: &Db<S>) -> Result<()> {
    let w = db.store.begin_write()?;
    db.commit_txn(w, TxnCtx::default(), Durability::Immediate)
}

pub(super) fn register_source<S: Store>(db: &Db<S>, desc: &SourceDescriptor) -> Result<u64> {
    let mut w = db.store.begin_write()?;
    let id = ops::alloc_id(&mut w, meta_key::NEXT_SOURCE_ID, "source id")?;
    w.put(Table::Sources, &format::id_key(id), &desc.encode())?;
    db.commit_txn(w, TxnCtx::default(), Durability::Immediate)?;
    Ok(id)
}

pub(super) fn prune_history<S: Store>(db: &Db<S>, key: Option<&[u8]>, keep_last: usize) -> Result<u64> {
    let mut w = db.store.begin_write()?;
    let prefix = key.map(format::history_prefix);
    let end = prefix.as_deref().and_then(prefix_successor);
    let start = prefix.as_deref().map_or(Bound::Unbounded, Bound::Included);
    let end = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
    // Newest first: within each key the first `keep_last` entries are kept.
    let mut doomed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut group: Vec<u8> = Vec::new();
    let mut seen = 0usize;
    w.scan(Table::History, start, end, true, &mut |k: &[u8], v: &[u8]| {
        let (user_key, _) = format::parse_history_key(k)?;
        if seen == 0 || user_key != group {
            group = user_key;
            seen = 0;
        }
        seen += 1;
        if seen > keep_last {
            doomed.push((k.to_vec(), v.to_vec()));
        }
        Ok(true)
    })?;
    if doomed.is_empty() {
        return Ok(0);
    }
    let mut ctx = TxnCtx::default();
    for (k, v) in &doomed {
        let manifest = Manifest::decode(v)?;
        w.remove(Table::History, k)?;
        ctx.removed.extend(ops::release_manifest(&mut w, &manifest)?);
    }
    ctx.dirty = true;
    db.commit_txn(w, ctx, Durability::Immediate)?;
    Ok(doomed.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CodecPolicy, Mode};
    use crate::store::mem::MemStore;

    fn same(a: &[PreparedUnit], b: &[PreparedUnit]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| (x.digest, x.raw_len, &x.envelope) == (y.digest, y.raw_len, &y.envelope))
    }

    fn encode_all(planner: &Planner, inputs: &[&[u8]]) -> Vec<PreparedUnit> {
        inputs.iter().map(|d| ops::prepare_unit(planner, d)).collect()
    }

    fn prepare_parallel(planner: &Planner, inputs: &[&[u8]], threads: usize) -> Vec<PreparedUnit> {
        map_parallel(inputs, threads, &|d: &&[u8]| ops::prepare_unit(planner, d))
    }

    /// The expectation rule on a manifest (a tombstone counts as absent).
    fn check_expect(key: &[u8], expect: Expect, current: Option<&Manifest>) -> Result<()> {
        check_expect_revision(key, expect, current.filter(|m| !m.is_tombstone()).map(|m| m.revision))
    }

    #[test]
    fn parallel_preparation_matches_sequential() {
        let planner = Planner::new(Mode::Adaptive, CodecPolicy::raw_only());
        let data: Vec<Vec<u8>> = (0..37usize)
            .map(|i| (0..(i * 53) % 700).map(|j| (i * 31 + j) as u8).collect())
            .collect();
        let inputs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
        let sequential = encode_all(&planner, &inputs);
        for threads in [0, 1, 2, 3, 8, 64] {
            assert!(same(&prepare_parallel(&planner, &inputs, threads), &sequential), "{threads} threads");
        }
        assert!(prepare_parallel(&planner, &[], 4).is_empty());
        assert!(same(&prepare_units(&planner, &inputs), &sequential));
        assert!(same(&prepare_units(&planner, &inputs[..3]), &sequential[..3]));
    }

    fn same_built(a: &[Built], b: &[Built]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| match (x, y) {
                (Built::Inline(m, p), Built::Inline(n, q)) => (m, p) == (n, q),
                (Built::Block(u), Built::Block(v)) => same(std::slice::from_ref(u), std::slice::from_ref(v)),
                _ => false,
            })
    }

    fn same_values(a: &[PreparedValue], b: &[PreparedValue]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| match (x, y) {
                (PreparedValue::Inline { manifest: m, param: p }, PreparedValue::Inline { manifest: n, param: q }) => (m, p) == (n, q),
                (PreparedValue::Chunks(u), PreparedValue::Chunks(v)) => same(u, v),
                _ => false,
            })
    }

    #[test]
    fn pipelined_preparation_matches_sequential() {
        let small = crate::config::Config { block_size: 512, inline_max: 64, ..crate::config::Config::adaptive() };
        for cfg in [small.clone(), crate::config::Config { codecs: CodecPolicy::raw_only(), ..small }] {
            let db = Db::with_store(MemStore::new(), cfg).unwrap();
            let planner = db.planner();
            let data: Vec<Vec<u8>> = (0..300usize)
                .map(|i| (0..(i * 37) % 1500).map(|j| ((i * 7 + j / 5) % 251) as u8).collect())
                .collect();
            let values: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
            let jobs = jobs_of(&values, 64, 512);
            let sequential: Vec<Built> = jobs.iter().map(|job| build(&planner, job)).collect();
            for helpers in [0, 1, 3, 7] {
                let got = pipelined(&planner, &jobs, helpers, |built| built.collect::<Vec<Built>>());
                assert!(same_built(&got, &sequential), "{helpers} helpers");
                // The consumer may stop early; the helpers stop too.
                assert_eq!(pipelined(&planner, &jobs, helpers, |built| built.take(5).count()), 5);
            }
            let prepared = db.with_prepared(&planner, &values, |p| p.collect::<Vec<PreparedValue>>());
            assert!(same_values(&prepared, &db.prepare_values(&planner, &values)));
            assert!(db.with_prepared(&planner, &[], |p| p.next().is_none()));
        }
    }

    #[test]
    fn a_panic_while_pipelining_reaches_the_caller() {
        let planner = Planner::new(Mode::Adaptive, CodecPolicy::raw_only());
        let huge = vec![0u8; format::MAX_UNIT_LEN as usize + 1];
        let small = [1u8; 10];
        let mut jobs: Vec<Job<'_>> = (0..100).map(|_| Job::Inline(&small)).collect();
        jobs.push(Job::Inline(&huge));
        jobs.extend((0..100).map(|_| Job::Inline(&small)));
        for helpers in [0, 2] {
            let r = std::panic::catch_unwind(AssertUnwindSafe(|| pipelined(&planner, &jobs, helpers, |built| built.count())));
            assert!(r.is_err(), "{helpers} helpers");
        }
    }

    #[test]
    fn thread_count_follows_the_estimated_work() {
        assert_eq!(threads_for(Duration::ZERO, 8), 0);
        assert_eq!(threads_for(PER_THREAD_MIN_WORK * 2 - Duration::from_nanos(1), 8), 1);
        assert_eq!(threads_for(PER_THREAD_MIN_WORK * 2, 8), 2);
        assert_eq!(threads_for(PER_THREAD_MIN_WORK * 5, 8), 5);
        assert_eq!(threads_for(Duration::from_secs(10), 8), 8);
        assert_eq!(threads_for(Duration::MAX, 3), 3);
    }

    #[test]
    fn prepared_values_match_one_by_one_preparation() {
        let db = Db::with_store(MemStore::new(), crate::config::Config { block_size: 512, inline_max: 64, ..crate::config::Config::adaptive() }).unwrap();
        let planner = db.planner();
        let data: Vec<Vec<u8>> = (0..40usize)
            .map(|i| (0..(i * 97) % 1500).map(|j| ((i * 7 + j / 3) % 251) as u8).collect())
            .collect();
        let values: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
        let batch = db.prepare_values(&planner, &values);
        assert_eq!(batch.len(), values.len());
        for (p, v) in batch.iter().zip(&values) {
            match (p, db.prepare_value(&planner, v)) {
                (PreparedValue::Inline { manifest: a, param: pa }, PreparedValue::Inline { manifest: b, param: pb }) => {
                    assert_eq!((a, pa), (&b, &pb));
                }
                (PreparedValue::Chunks(a), PreparedValue::Chunks(b)) => assert!(same(a, &b)),
                _ => panic!("value of {} bytes prepared differently", v.len()),
            }
        }
    }

    #[test]
    fn prepared_ops_keep_raw_values_only_when_chunked() {
        let db = Db::with_store(MemStore::new(), crate::config::Config { block_size: 512, inline_max: 64, ..crate::config::Config::raw_only() }).unwrap();
        let big = vec![3u8; 700];
        let ops = db.prepare_batch(&[
            BatchOp::Put { key: b"small", value: b"tiny", expect: Expect::Any },
            BatchOp::Put { key: b"big", value: &big, expect: Expect::Absent },
            BatchOp::Delete { key: b"gone", expect: Expect::Revision(4) },
            BatchOp::Put { key: b"", value: b"x", expect: Expect::Any },
            BatchOp::Delete { key: b"", expect: Expect::Any },
        ]);
        let raw_len = |op: &PreparedOp| match &op.body {
            Body::Put { raw, .. } => Some(raw.len()),
            _ => None,
        };
        assert_eq!(ops.iter().map(raw_len).collect::<Vec<_>>(), vec![Some(0), Some(700), None, None, None]);
        assert_eq!(ops.iter().map(PreparedOp::is_put).collect::<Vec<_>>(), vec![true, true, false, true, false]);
        assert_eq!(ops[2].expect(), Expect::Revision(4));
        assert_eq!(ops[1].key(), b"big");
        assert!(ops[..3].iter().all(|op| op.error().is_none()));
        assert!(matches!(ops[3].error(), Some(Error::InvalidArgument(_))));
        assert!(format!("{:?}", ops[1]).contains("700 B in 2 units"));
    }

    #[test]
    fn expectations() {
        let m = |revision, body| Manifest { revision, logical_len: 0, source_id: None, body };
        let live = m(5, ManifestBody::Chunks(Vec::new()));
        let tomb = m(7, ManifestBody::Tombstone);
        let conflict = |r: Result<()>| match r {
            Err(Error::RevisionConflict { actual, .. }) => actual,
            other => panic!("{other:?}"),
        };
        for current in [None, Some(&tomb)] {
            assert!(check_expect(b"k", Expect::Any, current).is_ok());
            assert!(check_expect(b"k", Expect::Absent, current).is_ok());
            assert_eq!(conflict(check_expect(b"k", Expect::Revision(7), current)), None);
        }
        assert!(check_expect(b"k", Expect::Any, Some(&live)).is_ok());
        assert!(check_expect(b"k", Expect::Revision(5), Some(&live)).is_ok());
        assert_eq!(conflict(check_expect(b"k", Expect::Absent, Some(&live))), Some(5));
        assert_eq!(conflict(check_expect(b"k", Expect::Revision(4), Some(&live))), Some(5));
    }

    #[test]
    fn revisions_never_wrap() {
        let store = MemStore::new();
        let mut w = store.begin_write().unwrap();
        ops::put_meta_u64(&mut w, meta_key::NEXT_REVISION, u64::MAX - 1).unwrap();
        let mut ctx = TxnCtx::default();
        assert_eq!(ctx.alloc_revision(&w).unwrap(), u64::MAX - 1);
        assert!(matches!(ctx.alloc_revision(&w), Err(Error::IdExhausted("revision"))));
        assert_eq!(ctx.next_revision, Some(u64::MAX));
    }
}
