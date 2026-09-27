//! Write path. Values are validated and encoded outside the write transaction
//! (spread over threads when the estimated encode work is large); inside it
//! each op only checks its expectation, stores or reuses objects, allocates a
//! revision and publishes the manifest. `put`, `delete`, `write_batch` and
//! `write_batch_each` share the same per-op steps.

use std::ops::Bound;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::ops::{self, PreparedUnit};
use super::{BatchOp, Db, Expect, Revision, prefix_successor};
use crate::chunk;
use crate::error::{Error, Result};
use crate::format::{self, ChunkRef, Manifest, ManifestBody, SourceDescriptor, meta_key};
use crate::generator::Generator;
use crate::hash::{Digest, StreamHasher};
use crate::planner::Planner;
use crate::store::{Durability, ReadTxn, Store, Table, WriteTxn};

/// A value encoded outside the write transaction.
pub(crate) enum PreparedValue {
    /// Complete envelope, stored inside the manifest.
    Inline(Vec<u8>),
    /// One unit per `block_size` block, in order.
    Chunks(Vec<PreparedUnit>),
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
}

impl TxnCtx {
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
    manifest: Manifest,
}

fn load_current<T: ReadTxn + ?Sized>(t: &T, key: &[u8]) -> OpResult<Option<Current>> {
    let Some(raw) = t.get(Table::Records, key)? else {
        return Ok(None);
    };
    let manifest = Manifest::decode(&raw).map_err(OpError::Skip)?;
    Ok(Some(Current { raw, manifest }))
}

/// Check an optimistic-concurrency condition against the stored manifest.
/// A tombstone counts as absent.
pub(crate) fn check_expect(key: &[u8], expect: Expect, current: Option<&Manifest>) -> Result<()> {
    let actual = current.filter(|m| !m.is_tombstone()).map(|m| m.revision);
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

// ---------------------------------------------------------------------------
// Preparation (outside the write transaction)
// ---------------------------------------------------------------------------

/// Inputs below this many units are always encoded on the calling thread.
const PARALLEL_MIN_UNITS: usize = 16;
/// Units encoded first on the calling thread to estimate the per-unit cost.
const PROBE_UNITS: usize = 4;
/// Estimated remaining encode time above which the rest is spread over threads.
const PARALLEL_MIN_WORK: Duration = Duration::from_micros(500);
const MAX_PREPARE_THREADS: usize = 8;

fn prepare_threads() -> usize {
    static THREADS: OnceLock<usize> = OnceLock::new();
    *THREADS.get_or_init(|| {
        std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(MAX_PREPARE_THREADS)
    })
}

/// Encode units, preserving order. When the first units show that the rest
/// is expensive, the rest is split over scoped threads; the output is the same.
pub(crate) fn prepare_units(planner: &Planner, inputs: &[&[u8]]) -> Vec<PreparedUnit> {
    let threads = prepare_threads();
    if inputs.len() < PARALLEL_MIN_UNITS || threads < 2 {
        return encode_all(planner, inputs);
    }
    let started = Instant::now();
    let (probe, rest) = inputs.split_at(PROBE_UNITS);
    let mut out = Vec::with_capacity(inputs.len());
    out.extend(encode_all(planner, probe));
    let per_unit = started.elapsed() / PROBE_UNITS as u32;
    let estimate = per_unit.saturating_mul(u32::try_from(rest.len()).unwrap_or(u32::MAX));
    if estimate < PARALLEL_MIN_WORK {
        out.extend(encode_all(planner, rest));
    } else {
        out.extend(prepare_parallel(planner, rest, threads));
    }
    out
}

fn encode_all(planner: &Planner, inputs: &[&[u8]]) -> Vec<PreparedUnit> {
    inputs.iter().map(|d| ops::prepare_unit(planner, d)).collect()
}

/// Encode `inputs` with up to `threads` threads (the caller's included).
pub(crate) fn prepare_parallel(planner: &Planner, inputs: &[&[u8]], threads: usize) -> Vec<PreparedUnit> {
    let per_thread = inputs.len().div_ceil(threads.max(1)).max(1);
    std::thread::scope(|scope| {
        let mut parts = inputs.chunks(per_thread);
        let first = parts.next().unwrap_or(&[]);
        let spawned: Vec<_> = parts
            .map(|part| {
                let handle = std::thread::Builder::new()
                    .name("babeldb-prepare".into())
                    .spawn_scoped(scope, move || encode_all(planner, part));
                (part, handle)
            })
            .collect();
        let mut out = Vec::with_capacity(inputs.len());
        out.extend(encode_all(planner, first));
        for (part, handle) in spawned {
            match handle {
                Ok(h) => match h.join() {
                    Ok(units) => out.extend(units),
                    Err(panic) => std::panic::resume_unwind(panic),
                },
                // No thread available: encode this part here.
                Err(_) => out.extend(encode_all(planner, part)),
            }
        }
        out
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
            BatchOp::Put { key, value, .. } => {
                self.check_key(key)?;
                self.check_value_len(value.len() as u64)
            }
            BatchOp::Delete { key, .. } => check_delete_key(key),
        }
    }

    /// Encode one value: a single inline unit when `len <= inline_max`,
    /// otherwise one unit per block.
    pub(crate) fn prepare_value(&self, planner: &Planner, value: &[u8]) -> PreparedValue {
        if value.len() <= self.inline_max as usize {
            return PreparedValue::Inline(ops::prepare_unit(planner, value).envelope);
        }
        let blocks: Vec<&[u8]> = chunk::split(value.len(), self.block_size as usize)
            .map(|r| &value[r])
            .collect();
        PreparedValue::Chunks(prepare_units(planner, &blocks))
    }

    /// `prepare_value` for many values; every unit of every value is encoded
    /// in one `prepare_units` call so small values are spread over threads too.
    pub(crate) fn prepare_values(&self, planner: &Planner, values: &[&[u8]]) -> Vec<PreparedValue> {
        if let [value] = values {
            return vec![self.prepare_value(planner, value)];
        }
        let inline_max = self.inline_max as usize;
        let block_size = self.block_size as usize;
        let mut inputs: Vec<&[u8]> = Vec::with_capacity(values.len());
        for &value in values {
            if value.len() <= inline_max {
                inputs.push(value);
            } else {
                inputs.extend(chunk::split(value.len(), block_size).map(|r| &value[r]));
            }
        }
        let mut units = prepare_units(planner, &inputs).into_iter();
        values
            .iter()
            .map(|value| {
                if value.len() <= inline_max {
                    let unit = units.next().expect("one prepared unit per inline value");
                    PreparedValue::Inline(unit.envelope)
                } else {
                    PreparedValue::Chunks(units.by_ref().take(value.len().div_ceil(block_size)).collect())
                }
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Per-op steps (inside the write transaction)
    // -----------------------------------------------------------------------

    /// Check the expectation, then publish a new manifest whose body `build`
    /// produces. New references are taken before the replaced manifest is
    /// retired, so shared objects never drop to zero in between.
    fn apply_record<W: WriteTxn + ?Sized>(
        &self,
        w: &mut W,
        key: &[u8],
        expect: Expect,
        ctx: &mut TxnCtx,
        build: impl FnOnce(&mut W, &mut TxnCtx) -> Result<(u64, ManifestBody)>,
    ) -> OpResult<Revision> {
        let old = load_current(&*w, key)?;
        check_expect(key, expect, old.as_ref().map(|c| &c.manifest)).map_err(OpError::Skip)?;
        let (logical_len, body) = build(w, ctx)?;
        let revision = ctx.alloc_revision(&*w)?;
        self.retire(w, key, old, ctx)?;
        ops::put_manifest(w, key, &Manifest { revision, logical_len, source_id: None, body })?;
        ctx.puts += 1;
        ctx.dirty = true;
        Ok(revision)
    }

    fn apply_put<W: WriteTxn + ?Sized>(
        &self,
        w: &mut W,
        key: &[u8],
        value: &[u8],
        prepared: PreparedValue,
        expect: Expect,
        ctx: &mut TxnCtx,
    ) -> OpResult<Revision> {
        let block_size = self.block_size as usize;
        let dedupe = self.dedupe();
        self.apply_record(w, key, expect, ctx, |w, ctx| {
            let body = match prepared {
                PreparedValue::Inline(envelope) => ManifestBody::Inline(envelope),
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
                    ManifestBody::Chunks(refs)
                }
            };
            Ok((value.len() as u64, body))
        })
    }

    /// Returns the revision of the deleted record, or None if there was none.
    fn apply_delete<W: WriteTxn + ?Sized>(
        &self,
        w: &mut W,
        key: &[u8],
        expect: Expect,
        ctx: &mut TxnCtx,
    ) -> OpResult<Option<Revision>> {
        let old = load_current(&*w, key)?;
        check_expect(key, expect, old.as_ref().map(|c| &c.manifest)).map_err(OpError::Skip)?;
        let Some(old) = old.filter(|c| !c.manifest.is_tombstone()) else {
            return Ok(None);
        };
        let deleted = old.manifest.revision;
        if self.cfg.keep_history {
            let revision = ctx.alloc_revision(&*w)?;
            w.put(Table::History, &format::history_key(key, deleted), &old.raw)?;
            let tombstone = Manifest { revision, logical_len: 0, source_id: None, body: ManifestBody::Tombstone };
            ops::put_manifest(w, key, &tombstone)?;
        } else {
            w.remove(Table::Records, key)?;
            ctx.removed.extend(ops::release_manifest(w, &old.manifest)?);
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
            w.put(Table::History, &format::history_key(key, old.manifest.revision), &old.raw)
        } else {
            ctx.removed.extend(ops::release_manifest(w, &old.manifest)?);
            Ok(())
        }
    }

    fn commit_txn<W: WriteTxn>(&self, mut w: W, ctx: TxnCtx, durability: Durability) -> Result<()> {
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

// ---------------------------------------------------------------------------
// Entry points (called by the public methods in `engine/mod.rs`)
// ---------------------------------------------------------------------------

pub(super) fn put<S: Store>(db: &Db<S>, key: &[u8], value: &[u8], expect: Expect) -> Result<Revision> {
    db.check_key(key)?;
    db.check_value_len(value.len() as u64)?;
    let prepared = db.prepare_value(&db.planner(), value);
    let mut w = db.store.begin_write()?;
    let mut ctx = TxnCtx::default();
    let revision = db
        .apply_put(&mut w, key, value, prepared, expect, &mut ctx)
        .map_err(OpError::into_error)?;
    db.commit_txn(w, ctx, Durability::Immediate)?;
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
    let mut w = db.store.begin_write()?;
    let mut ctx = TxnCtx::default();
    let revision = db
        .apply_record(&mut w, key, expect, &mut ctx, |_, _| {
            let body = ManifestBody::Generated { generator_id, generator_version, params: params.to_vec(), digest };
            Ok((len, body))
        })
        .map_err(OpError::into_error)?;
    db.commit_txn(w, ctx, Durability::Immediate)?;
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
    let mut w = db.store.begin_write()?;
    let mut ctx = TxnCtx::default();
    let deleted = db
        .apply_delete(&mut w, key, expect, &mut ctx)
        .map_err(OpError::into_error)?;
    if ctx.dirty {
        db.commit_txn(w, ctx, Durability::Immediate)?;
    }
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

pub(super) fn write_batch<S: Store>(db: &Db<S>, batch: &[BatchOp<'_>]) -> Result<Vec<Option<Revision>>> {
    for op in batch {
        db.check_op(op)?;
    }
    if batch.is_empty() {
        return Ok(Vec::new());
    }
    let mut prepared = db
        .prepare_values(&db.planner(), &valid_put_values(batch, |_| true))
        .into_iter();
    let mut w = db.store.begin_write()?;
    let mut ctx = TxnCtx::default();
    let mut results = Vec::with_capacity(batch.len());
    for op in batch {
        let outcome = match *op {
            BatchOp::Put { key, value, expect } => match prepared.next() {
                Some(p) => db.apply_put(&mut w, key, value, p, expect, &mut ctx).map(Some),
                None => Err(missing_prepared()),
            },
            BatchOp::Delete { key, expect } => db.apply_delete(&mut w, key, expect, &mut ctx),
        };
        results.push(outcome.map_err(OpError::into_error)?);
    }
    if ctx.dirty {
        db.commit_txn(w, ctx, Durability::Immediate)?;
    }
    Ok(results)
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
    let mut prepared = db.prepare_values(&db.planner(), &values).into_iter();
    let mut w = db.store.begin_write()?;
    let mut ctx = TxnCtx::default();
    let mut results = Vec::with_capacity(batch.len());
    for (op, check) in batch.iter().zip(checks) {
        if let Err(e) = check {
            results.push(Err(e));
            continue;
        }
        let outcome = match *op {
            BatchOp::Put { key, value, expect } => match prepared.next() {
                Some(p) => db.apply_put(&mut w, key, value, p, expect, &mut ctx).map(Some),
                None => Err(missing_prepared()),
            },
            BatchOp::Delete { key, expect } => db.apply_delete(&mut w, key, expect, &mut ctx),
        };
        match outcome {
            Ok(r) => results.push(Ok(r)),
            Err(OpError::Skip(e)) => results.push(Err(e)),
            Err(OpError::Abort(e)) => return Err(e),
        }
    }
    if ctx.dirty {
        db.commit_txn(w, ctx, durability)?;
    }
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
