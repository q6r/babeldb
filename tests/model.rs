//! Model-based property tests: random operation sequences applied both to a
//! real database and to a reference model (`BTreeMap` of live values plus
//! revision tracking), compared after every operation.
//!
//! | test                          | backend  | mode      | extras                          |
//! |-------------------------------|----------|-----------|---------------------------------|
//! | `model_adaptive_mem`          | MemStore | Adaptive  | default codecs + dedupe         |
//! | `model_babel_pure_mem`        | MemStore | BabelPure | 2 KiB block cache (evictions)   |
//! | `model_adaptive_history_mem`  | MemStore | Adaptive  | `keep_history`, cache disabled  |
//! | `model_adaptive_redb`         | redb     | Adaptive  | reopen mid-sequence and at end  |
//! | `model_babel_pure_redb`       | redb     | BabelPure | idem                            |
//! | `model_adaptive_history_redb` | redb     | Adaptive  | `keep_history`                  |
//!
//! Every database uses `block_size = 512` and `inline_max = 64`, so generated
//! sizes (0, 1, 63..65, 511..513, ..., ~5 blocks) cross every inline/block
//! boundary, and the value generators (zeros, short motifs, u64 arithmetic
//! sequences, random bytes, UTF-8 text, mixtures, copies and edited copies of
//! earlier values) drive every Adaptive codec path and the byte-verified dedupe.
//!
//! Operations: put / put_generated (Expect::Any, Absent, correct and wrong
//! revisions), delete, get / get_with_revision / head, get_range (offset 0,
//! == len, > len -> InvalidArgument, block edges, u64::MAX lengths),
//! write_batch (sometimes with a failing expectation or an invalid op: nothing
//! may apply), write_batch_each (only the failing ops are skipped; Immediate or
//! Deferred + sync), scan (prefix / arbitrary bounds / reverse / limit /
//! with_values), clear_cache, sync, reopen (redb) and full checks.
//!
//! At the end of each sequence: full comparison, structural checks (inspect
//! units and codecs per mode, refcounts, exact object counts with and without
//! dedupe, history), `verify(true)`; for redb the database is closed and
//! reopened with different creation parameters (the persisted ones must win)
//! and compared again; `gc()` must then find nothing to do, `verify` must stay
//! ok, and deleting everything must release every object.
//!
//! Scale with `PROPTEST_CASES=<n>` (defaults: 64 cases per MemStore test, 24
//! per redb test). Failing sequences are persisted next to this file in
//! `model.proptest-regressions` and replayed first on the next run.
//!
//! Run: `cargo test --test model` (add `-- --nocapture` to see progress).

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::path::PathBuf;

use babeldb::engine::HistoryEntry;
use babeldb::generator::{self, ids};
use babeldb::maintenance::GcReport;
use babeldb::store::{Durability, Store};
use babeldb::{
    BatchOp, Config, Db, Error, Expect, MemStore, Mode, RedbStore, Revision, ScanOptions,
};
use common::{
    BLOCK, INLINE, Pattern, Snapshot, assert_bytes_eq, assert_snapshot_eq, candidate_ids,
    is_conflict, is_invalid_argument, is_rejected_input, key_str, mem_db, open_redb,
    pattern_bytes, small_config, snapshot, temp_dir, verify_ok,
};
use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;

const BS: usize = BLOCK as usize;
const IM: usize = INLINE as usize;
/// `Config::max_value_len` of the model databases (larger values are rejected).
const MAX_VALUE: u64 = 4096;
/// Default `Config::max_key_len`.
const MAX_KEY: usize = 4096;
/// Header bytes of every stored envelope.
const ENVELOPE_HEADER: u64 = 64;

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/// Keys the sequences operate on: nested prefixes, binary bytes (0x00, 0xFF)
/// and, at index `KEY_LITERALS.len()`, a key of exactly `max_key_len` bytes.
const KEY_LITERALS: &[&[u8]] = &[
    b"a",
    b"a/0",
    b"a/1",
    b"a/10",
    b"a/2",
    b"ab",
    b"b/0",
    b"b/1",
    b"\x00",
    b"\x00\x01",
    b"\xff",
    b"\xff\xff",
    b"k\x00k",
    b"zz/deep/key/0001",
];
const POOL_LEN: usize = KEY_LITERALS.len() + 1;

fn pool_key(i: usize) -> Vec<u8> {
    match KEY_LITERALS.get(i) {
        Some(k) => k.to_vec(),
        // Exactly max_key_len: must be accepted.
        None => vec![b'm'; MAX_KEY],
    }
}

/// One byte over `max_key_len`: must be rejected.
fn oversized_key() -> Vec<u8> {
    vec![b'o'; MAX_KEY + 1]
}

const PREFIXES: &[&[u8]] = &[
    b"",
    b"a",
    b"a/",
    b"a/1",
    b"b",
    b"b/",
    b"\x00",
    b"\xff",
    b"\xff\xff",
    b"zz/",
    b"m",
    b"nope",
];

/// Extra scan bounds that are not keys of the pool.
const EXTRA_BOUNDS: &[&[u8]] = &[
    b"",
    b"a/05",
    b"aa",
    b"b",
    b"\x00\x00",
    b"\xff\xff\xff",
    b"mm",
];
const BOUND_KEYS: usize = POOL_LEN + EXTRA_BOUNDS.len();

fn bound_key(i: usize) -> Vec<u8> {
    if i < POOL_LEN {
        pool_key(i)
    } else {
        EXTRA_BOUNDS[(i - POOL_LEN) % EXTRA_BOUNDS.len()].to_vec()
    }
}

// ---------------------------------------------------------------------------
// Operation specs (resolved against the model when applied, so shrinking
// always produces valid sequences)
// ---------------------------------------------------------------------------

/// Expectation to use, resolved against the model's current state.
#[derive(Clone, Copy, Debug)]
enum ExpectSpec {
    /// `Expect::Any`: always holds.
    Any,
    /// `Revision(current)` for a live key, `Absent` otherwise: always holds.
    Correct,
    /// `Expect::Absent`: holds iff the key is not live.
    Absent,
    /// A revision that is not the current one (or any revision for an absent
    /// key): always fails.
    Stale,
    /// A revision never allocated: always fails.
    Future,
}

#[derive(Clone, Copy, Debug)]
enum KeySel {
    Pool(usize),
    Oversized,
}

#[derive(Clone, Debug)]
enum ValueSpec {
    Pattern {
        pattern: Pattern,
        len: usize,
        seed: u64,
    },
    /// Copy of a value written earlier (dedupe), optionally resized
    /// (shared prefix, new tail) and with one byte flipped.
    Copy {
        pick: usize,
        resize: Option<(usize, u64)>,
        flip: Option<(usize, u8)>,
    },
    /// One byte over `max_value_len`: must be rejected.
    TooLarge,
}

#[derive(Clone, Debug)]
enum GenSpec {
    Repeat { total: u64, motif: Vec<u8> },
    Arith { start: u64, step: u64, count: u64 },
}

#[derive(Clone, Copy, Debug)]
enum OffsetSpec {
    Zero,
    AtEnd,
    PastEnd(u8),
    Fraction(u16),
    BlockEdge(u8, bool),
    Huge,
}

#[derive(Clone, Copy, Debug)]
enum LenSpec {
    Zero,
    One,
    Small(u16),
    CrossBlock,
    ToEnd,
    Max,
}

#[derive(Clone, Copy, Debug)]
enum BoundSpec {
    Unbounded,
    Included(usize),
    Excluded(usize),
}

impl BoundSpec {
    fn resolve(self) -> Bound<Vec<u8>> {
        match self {
            BoundSpec::Unbounded => Bound::Unbounded,
            BoundSpec::Included(i) => Bound::Included(bound_key(i)),
            BoundSpec::Excluded(i) => Bound::Excluded(bound_key(i)),
        }
    }
}

/// One operation of a batch. Deletes never use the oversized key (whether a
/// delete of an impossible key errors or reports "nothing deleted" is not
/// specified).
#[derive(Clone, Debug)]
enum ItemSpec {
    Put {
        key: KeySel,
        value: ValueSpec,
        expect: ExpectSpec,
    },
    Delete {
        key: usize,
        expect: ExpectSpec,
    },
}

#[derive(Clone, Debug)]
enum Op {
    Put {
        key: KeySel,
        value: ValueSpec,
        expect: ExpectSpec,
    },
    PutGenerated {
        key: usize,
        generator: GenSpec,
        expect: ExpectSpec,
    },
    Delete {
        key: usize,
        expect: ExpectSpec,
    },
    Get {
        key: usize,
    },
    GetRange {
        key: usize,
        offset: OffsetSpec,
        len: LenSpec,
    },
    /// `write_batch`; `fail_at` forces a failing expectation on one op.
    Batch {
        items: Vec<ItemSpec>,
        fail_at: Option<usize>,
    },
    /// `write_batch_each`, optionally `Deferred` and followed by `sync`.
    BatchEach {
        items: Vec<ItemSpec>,
        deferred: bool,
        sync_after: bool,
    },
    /// `scan` with a prefix (when `prefix` is set) or arbitrary bounds.
    Scan {
        prefix: Option<usize>,
        lower: BoundSpec,
        upper: BoundSpec,
        reverse: bool,
        limit: u8,
        with_values: bool,
    },
    ClearCache,
    Sync,
    Reopen,
    CheckAll,
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

fn pattern_strategy() -> impl Strategy<Value = Pattern> {
    prop::sample::select(Pattern::ALL.to_vec())
}

fn size_strategy() -> impl Strategy<Value = usize> {
    let edges = vec![
        0,
        1,
        2,
        7,
        8,
        IM - 1,
        IM,
        IM + 1,
        BS - 1,
        BS,
        BS + 1,
        2 * BS - 1,
        2 * BS,
        2 * BS + 1,
        3 * BS + 17,
        5 * BS - 1,
        5 * BS,
        5 * BS + 1,
    ];
    prop_oneof![
        4 => prop::sample::select(edges),
        3 => 0..=5 * BS + 1,
        1 => 0..=IM + 8,
    ]
}

fn value_strategy() -> impl Strategy<Value = ValueSpec> {
    prop_oneof![
        12 => (pattern_strategy(), size_strategy(), any::<u64>())
            .prop_map(|(pattern, len, seed)| ValueSpec::Pattern { pattern, len, seed }),
        6 => (
            any::<usize>(),
            prop::option::weighted(0.3, (any::<usize>(), any::<u64>())),
            prop::option::weighted(0.5, (any::<usize>(), any::<u8>())),
        )
            .prop_map(|(pick, resize, flip)| ValueSpec::Copy { pick, resize, flip }),
        1 => Just(ValueSpec::TooLarge),
    ]
}

fn expect_strategy() -> BoxedStrategy<ExpectSpec> {
    prop_oneof![
        6 => Just(ExpectSpec::Any),
        5 => Just(ExpectSpec::Correct),
        3 => Just(ExpectSpec::Absent),
        3 => Just(ExpectSpec::Stale),
        2 => Just(ExpectSpec::Future),
    ]
    .boxed()
}

/// Mostly-holding expectations for `write_batch`, so that a good share of the
/// batches commit (failures are injected explicitly through `fail_at`).
fn holding_expect_strategy() -> BoxedStrategy<ExpectSpec> {
    prop_oneof![
        5 => Just(ExpectSpec::Any),
        4 => Just(ExpectSpec::Correct),
        1 => Just(ExpectSpec::Absent),
    ]
    .boxed()
}

fn put_key_strategy() -> impl Strategy<Value = KeySel> {
    prop_oneof![
        40 => (0..POOL_LEN).prop_map(KeySel::Pool),
        1 => Just(KeySel::Oversized),
    ]
}

fn gen_strategy() -> impl Strategy<Value = GenSpec> {
    prop_oneof![
        (
            0u64..=(5 * BS as u64 + 40),
            prop::collection::vec(any::<u8>(), 1..=12)
        )
            .prop_map(|(total, motif)| GenSpec::Repeat { total, motif }),
        (any::<u32>(), 0u64..1000, 0u64..=330).prop_map(|(start, step, count)| GenSpec::Arith {
            start: u64::from(start),
            step,
            count
        }),
    ]
}

fn offset_strategy() -> impl Strategy<Value = OffsetSpec> {
    prop_oneof![
        2 => Just(OffsetSpec::Zero),
        2 => Just(OffsetSpec::AtEnd),
        2 => (0u8..3).prop_map(OffsetSpec::PastEnd),
        4 => any::<u16>().prop_map(OffsetSpec::Fraction),
        3 => (any::<u8>(), any::<bool>()).prop_map(|(i, before)| OffsetSpec::BlockEdge(i, before)),
        1 => Just(OffsetSpec::Huge),
    ]
}

fn len_strategy() -> impl Strategy<Value = LenSpec> {
    prop_oneof![
        1 => Just(LenSpec::Zero),
        2 => Just(LenSpec::One),
        4 => (0u16..700).prop_map(LenSpec::Small),
        2 => Just(LenSpec::CrossBlock),
        2 => Just(LenSpec::ToEnd),
        1 => Just(LenSpec::Max),
    ]
}

fn bound_strategy() -> impl Strategy<Value = BoundSpec> {
    prop_oneof![
        1 => Just(BoundSpec::Unbounded),
        2 => (0..BOUND_KEYS).prop_map(BoundSpec::Included),
        2 => (0..BOUND_KEYS).prop_map(BoundSpec::Excluded),
    ]
}

fn item_strategy(expect: BoxedStrategy<ExpectSpec>) -> impl Strategy<Value = ItemSpec> {
    prop_oneof![
        3 => (put_key_strategy(), value_strategy(), expect.clone())
            .prop_map(|(key, value, expect)| ItemSpec::Put { key, value, expect }),
        1 => (0..POOL_LEN, expect).prop_map(|(key, expect)| ItemSpec::Delete { key, expect }),
    ]
}

fn scan_strategy() -> impl Strategy<Value = Op> {
    (
        prop::option::weighted(0.5, 0..PREFIXES.len()),
        bound_strategy(),
        bound_strategy(),
        any::<bool>(),
        prop_oneof![3 => Just(0u8), 2 => 1u8..=4],
        any::<bool>(),
    )
        .prop_map(
            |(prefix, lower, upper, reverse, limit, with_values)| Op::Scan {
                prefix,
                lower,
                upper,
                reverse,
                limit,
                with_values,
            },
        )
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        30 => (put_key_strategy(), value_strategy(), expect_strategy())
            .prop_map(|(key, value, expect)| Op::Put { key, value, expect }),
        4 => (0..POOL_LEN, gen_strategy(), expect_strategy())
            .prop_map(|(key, generator, expect)| Op::PutGenerated { key, generator, expect }),
        8 => (0..POOL_LEN, expect_strategy()).prop_map(|(key, expect)| Op::Delete { key, expect }),
        5 => (0..POOL_LEN).prop_map(|key| Op::Get { key }),
        10 => (0..POOL_LEN, offset_strategy(), len_strategy())
            .prop_map(|(key, offset, len)| Op::GetRange { key, offset, len }),
        7 => (
            prop::collection::vec(item_strategy(holding_expect_strategy()), 0..=6),
            prop::option::weighted(0.35, any::<usize>()),
        )
            .prop_map(|(items, fail_at)| Op::Batch { items, fail_at }),
        7 => (
            prop::collection::vec(item_strategy(expect_strategy()), 0..=6),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(|(items, deferred, sync_after)| Op::BatchEach { items, deferred, sync_after }),
        8 => scan_strategy(),
        2 => Just(Op::ClearCache),
        1 => Just(Op::Sync),
        2 => Just(Op::Reopen),
        1 => Just(Op::CheckAll),
    ]
}

fn ops_strategy() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op_strategy(), 1..=40)
}

/// Proptest configuration: `PROPTEST_CASES` overrides the default case count.
fn pt_config(default_cases: u32, max_shrink_iters: u32) -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default_cases);
    ProptestConfig {
        cases,
        max_shrink_iters,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Backends under test
// ---------------------------------------------------------------------------

trait Harness {
    type S: Store;
    fn db(&self) -> &Db<Self::S>;
    fn db_mut(&mut self) -> &mut Db<Self::S>;
    /// Close and reopen with `cfg`; false when the backend cannot reopen.
    fn reopen(&mut self, cfg: &Config) -> bool;
    fn name(&self) -> &'static str;
}

struct MemHarness {
    db: Db<MemStore>,
}

impl MemHarness {
    fn new(cfg: &Config) -> MemHarness {
        MemHarness {
            db: mem_db(cfg.clone()),
        }
    }
}

impl Harness for MemHarness {
    type S = MemStore;

    fn db(&self) -> &Db<MemStore> {
        &self.db
    }

    fn db_mut(&mut self) -> &mut Db<MemStore> {
        &mut self.db
    }

    fn reopen(&mut self, _cfg: &Config) -> bool {
        false
    }

    fn name(&self) -> &'static str {
        "mem"
    }
}

/// Field order matters: the database is closed before the directory is removed.
struct RedbHarness {
    db: Option<Db<RedbStore>>,
    path: PathBuf,
    _dir: tempfile::TempDir,
}

impl RedbHarness {
    fn new(cfg: &Config) -> RedbHarness {
        let dir = temp_dir("babeldb-model-");
        let path = dir.path().join("model.redb");
        let db = open_redb(&path, cfg.clone());
        RedbHarness {
            db: Some(db),
            path,
            _dir: dir,
        }
    }
}

impl Harness for RedbHarness {
    type S = RedbStore;

    fn db(&self) -> &Db<RedbStore> {
        self.db.as_ref().expect("database is open")
    }

    fn db_mut(&mut self) -> &mut Db<RedbStore> {
        self.db.as_mut().expect("database is open")
    }

    fn reopen(&mut self, cfg: &Config) -> bool {
        self.db = None;
        self.db = Some(open_redb(&self.path, cfg.clone()));
        true
    }

    fn name(&self) -> &'static str {
        "redb"
    }
}

// ---------------------------------------------------------------------------
// Reference model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Entry {
    rev: Revision,
    value: Vec<u8>,
    /// Stored through `put_generated` (no objects).
    generated: bool,
}

#[derive(Default)]
struct Model {
    live: BTreeMap<Vec<u8>, Entry>,
    /// Every successful put per key, oldest first (history checks, object accounting).
    puts: BTreeMap<Vec<u8>, Vec<Entry>>,
    /// Keys whose current manifest is a tombstone (only with `keep_history`).
    tombstoned: BTreeSet<Vec<u8>>,
    /// Recently written values: sources of dedupe copies.
    pool: Vec<Vec<u8>>,
    /// Highest revision returned so far.
    max_rev: Revision,
}

impl Model {
    fn record_put(&mut self, key: Vec<u8>, rev: Revision, value: Vec<u8>, generated: bool) {
        self.max_rev = self.max_rev.max(rev);
        self.tombstoned.remove(&key);
        if !generated {
            self.pool.push(value.clone());
            if self.pool.len() > 48 {
                self.pool.remove(0);
            }
        }
        let entry = Entry {
            rev,
            value,
            generated,
        };
        self.puts
            .entry(key.clone())
            .or_default()
            .push(entry.clone());
        self.live.insert(key, entry);
    }

    fn record_delete(&mut self, key: &[u8], history: bool) {
        if self.live.remove(key).is_some() && history {
            self.tombstoned.insert(key.to_vec());
        }
    }

    fn last_put_rev(&self, key: &[u8]) -> Option<Revision> {
        self.puts.get(key).and_then(|v| v.last()).map(|e| e.rev)
    }
}

/// A batch op resolved against the model.
struct Prepared {
    key: Vec<u8>,
    /// `Some` for puts, `None` for deletes.
    value: Option<Vec<u8>>,
    expect: Expect,
    /// The expectation holds against the pre-batch state.
    holds: bool,
    /// Key and value respect the configured limits.
    valid: bool,
    was_live: bool,
}

fn resolve_key(sel: KeySel) -> (Vec<u8>, bool) {
    match sel {
        KeySel::Pool(i) => (pool_key(i), true),
        KeySel::Oversized => (oversized_key(), false),
    }
}

fn in_bounds(key: &[u8], start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    let lower = match start {
        Bound::Unbounded => true,
        Bound::Included(s) => key >= s.as_slice(),
        Bound::Excluded(s) => key > s.as_slice(),
    };
    let upper = match end {
        Bound::Unbounded => true,
        Bound::Included(e) => key <= e.as_slice(),
        Bound::Excluded(e) => key < e.as_slice(),
    };
    lower && upper
}

/// Empty or inverted ranges: the engine may return an empty result or
/// `InvalidArgument`, but must not panic.
fn is_degenerate(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(s), Bound::Included(e)) => s > e,
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => {
            s >= e
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Runner: applies ops to the database and the model, compares both
// ---------------------------------------------------------------------------

struct Runner<H: Harness> {
    h: H,
    cfg: Config,
    mode: Mode,
    history: bool,
    dedupe: bool,
    m: Model,
    /// Deferred commits since the last `sync`.
    unsynced: bool,
    /// Context prefix of every failure message.
    ctx: String,
}

impl<H: Harness> Runner<H> {
    fn new(h: H, cfg: Config) -> Self {
        let mode = cfg.mode;
        Runner {
            history: cfg.keep_history,
            dedupe: cfg.effective_dedupe(mode),
            mode,
            h,
            cfg,
            m: Model::default(),
            unsynced: false,
            ctx: String::new(),
        }
    }

    fn db(&self) -> &Db<H::S> {
        self.h.db()
    }

    fn label(&self) -> String {
        format!(
            "{} {:?}{}",
            self.h.name(),
            self.mode,
            if self.history { "+history" } else { "" }
        )
    }

    fn apply(&mut self, index: usize, op: &Op) {
        let mut shown = format!("{op:?}");
        if shown.len() > 300 {
            let mut cut = 300;
            while !shown.is_char_boundary(cut) {
                cut -= 1;
            }
            shown.truncate(cut);
            shown.push_str("...");
        }
        self.ctx = format!("[{} op #{index} {shown}]", self.label());
        match op {
            Op::Put { key, value, expect } => self.put(*key, value, *expect),
            Op::PutGenerated {
                key,
                generator,
                expect,
            } => self.put_generated(*key, generator, *expect),
            Op::Delete { key, expect } => self.delete(*key, *expect),
            Op::Get { key } => self.check_key(&pool_key(*key)),
            Op::GetRange { key, offset, len } => self.get_range(*key, *offset, *len),
            Op::Batch { items, fail_at } => self.batch(items, *fail_at),
            Op::BatchEach {
                items,
                deferred,
                sync_after,
            } => self.batch_each(items, *deferred, *sync_after),
            Op::Scan {
                prefix,
                lower,
                upper,
                reverse,
                limit,
                with_values,
            } => {
                let (start, end) = match prefix {
                    Some(p) => {
                        let o = ScanOptions::prefix(PREFIXES[*p]);
                        (o.start, o.end)
                    }
                    None => (lower.resolve(), upper.resolve()),
                };
                let opts = ScanOptions {
                    start,
                    end,
                    reverse: *reverse,
                    limit: usize::from(*limit),
                    with_values: *with_values,
                };
                self.scan(&opts);
            }
            Op::ClearCache => self.db().clear_cache(),
            Op::Sync => self.sync(),
            Op::Reopen => {
                let cfg = self.cfg.clone();
                self.reopen(&cfg, "reopen op");
            }
            Op::CheckAll => self.check_all("CheckAll op"),
        }
    }

    fn materialize(&self, spec: &ValueSpec) -> (Vec<u8>, bool) {
        match spec {
            ValueSpec::Pattern { pattern, len, seed } => {
                (pattern_bytes(*pattern, *len, *seed), true)
            }
            ValueSpec::TooLarge => (vec![0x5A; MAX_VALUE as usize + 1], false),
            ValueSpec::Copy { pick, resize, flip } => {
                let mut v = if self.m.pool.is_empty() {
                    pattern_bytes(Pattern::Random, 3 * BS + 5, *pick as u64)
                } else {
                    self.m.pool[pick % self.m.pool.len()].clone()
                };
                if let Some((n, seed)) = resize {
                    let n = n % (5 * BS + 2);
                    if n <= v.len() {
                        v.truncate(n);
                    } else {
                        let tail = pattern_bytes(Pattern::Random, n - v.len(), *seed);
                        v.extend_from_slice(&tail);
                    }
                }
                if let Some((pos, x)) = flip
                    && !v.is_empty()
                {
                    let i = pos % v.len();
                    v[i] ^= (*x).max(1);
                }
                (v, true)
            }
        }
    }

    /// Concrete expectation for `spec` and whether it holds right now.
    fn resolve(&self, key: &[u8], spec: ExpectSpec) -> (Expect, bool) {
        let cur = self.m.live.get(key).map(|e| e.rev);
        match (spec, cur) {
            (ExpectSpec::Any, _) => (Expect::Any, true),
            (ExpectSpec::Correct, Some(r)) => (Expect::Revision(r), true),
            (ExpectSpec::Correct, None) => (Expect::Absent, true),
            (ExpectSpec::Absent, cur) => (Expect::Absent, cur.is_none()),
            (ExpectSpec::Stale, Some(r)) => (Expect::Revision(r.wrapping_sub(1)), false),
            (ExpectSpec::Stale, None) => (
                Expect::Revision(self.m.last_put_rev(key).unwrap_or(1)),
                false,
            ),
            (ExpectSpec::Future, _) => (Expect::Revision(self.m.max_rev + 1_000_003), false),
        }
    }

    #[track_caller]
    fn assert_conflict(&self, err: &Error, key: &[u8], what: &str) {
        match err {
            Error::RevisionConflict { key: k, actual, .. } => {
                assert_eq!(
                    k.as_slice(),
                    key,
                    "{}: {what}: RevisionConflict names the wrong key",
                    self.ctx
                );
                match self.m.live.get(key) {
                    Some(e) => assert_eq!(
                        *actual,
                        Some(e.rev),
                        "{}: {what}: RevisionConflict.actual must be the current revision",
                        self.ctx
                    ),
                    None if !self.history => assert_eq!(
                        *actual, None,
                        "{}: {what}: RevisionConflict.actual must be None for an absent key",
                        self.ctx
                    ),
                    // With history a tombstone may report its own revision.
                    None => {}
                }
            }
            other => panic!(
                "{}: {what}: expected RevisionConflict, got {other:?}",
                self.ctx
            ),
        }
    }

    fn put(&mut self, sel: KeySel, spec: &ValueSpec, espec: ExpectSpec) {
        let (key, key_ok) = resolve_key(sel);
        let (value, value_ok) = self.materialize(spec);
        if !(key_ok && value_ok) {
            match self.db().put(&key, &value, Expect::Any) {
                Ok(rev) => panic!(
                    "{}: put of a {}-byte key / {}-byte value exceeds the configured limits \
                     and must be rejected, got revision {rev}",
                    self.ctx,
                    key.len(),
                    value.len()
                ),
                Err(e) => assert!(
                    is_rejected_input(&e),
                    "{}: over-limit put must fail with InvalidArgument/LimitExceeded, got {e:?}",
                    self.ctx
                ),
            }
            if key_ok {
                self.check_key(&key);
            }
            return;
        }
        let (expect, holds) = self.resolve(&key, espec);
        let res = self.db().put(&key, &value, expect);
        self.finish_put(key, value, expect, holds, res, false);
    }

    fn put_generated(&mut self, key_idx: usize, spec: &GenSpec, espec: ExpectSpec) {
        let key = pool_key(key_idx);
        let (id, params, value) = match spec {
            GenSpec::Repeat { total, motif } => (
                ids::REPEAT,
                generator::repeat_params(*total, motif),
                (0..*total as usize)
                    .map(|i| motif[i % motif.len()])
                    .collect::<Vec<u8>>(),
            ),
            GenSpec::Arith { start, step, count } => (
                ids::ARITH_U64,
                generator::arith_params(*start, *step, *count),
                (0..*count)
                    .flat_map(|i| (start + i * step).to_le_bytes())
                    .collect(),
            ),
        };
        let (expect, holds) = self.resolve(&key, espec);
        let res = self.db().put_generated(&key, id, 1, &params, expect);
        self.finish_put(key, value, expect, holds, res, true);
    }

    fn finish_put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        expect: Expect,
        holds: bool,
        res: babeldb::Result<Revision>,
        generated: bool,
    ) {
        if holds {
            let rev = res.unwrap_or_else(|e| {
                panic!(
                    "{}: put {} ({} bytes, {expect:?}) failed: {e}",
                    self.ctx,
                    key_str(&key),
                    value.len()
                )
            });
            assert!(
                rev > self.m.max_rev,
                "{}: put returned revision {rev}, not above the previous maximum {}",
                self.ctx,
                self.m.max_rev
            );
            self.m.record_put(key.clone(), rev, value, generated);
        } else {
            match res {
                Ok(rev) => panic!(
                    "{}: put {} with {expect:?} must conflict (model revision {:?}), got {rev}",
                    self.ctx,
                    key_str(&key),
                    self.m.live.get(&key).map(|e| e.rev)
                ),
                Err(e) => self.assert_conflict(&e, &key, "put"),
            }
        }
        self.check_key(&key);
    }

    fn delete(&mut self, key_idx: usize, espec: ExpectSpec) {
        let key = pool_key(key_idx);
        let (expect, holds) = self.resolve(&key, espec);
        let was_live = self.m.live.contains_key(&key);
        let res = self.db().delete(&key, expect);
        if holds {
            let deleted = res.unwrap_or_else(|e| {
                panic!(
                    "{}: delete {} ({expect:?}) failed: {e}",
                    self.ctx,
                    key_str(&key)
                )
            });
            assert_eq!(
                deleted, was_live,
                "{}: delete must report whether a live record was deleted",
                self.ctx
            );
            if was_live {
                self.m.record_delete(&key, self.history);
            }
        } else {
            match res {
                Ok(d) => panic!(
                    "{}: delete {} with {expect:?} must conflict, got Ok({d})",
                    self.ctx,
                    key_str(&key)
                ),
                Err(e) => self.assert_conflict(&e, &key, "delete"),
            }
        }
        self.check_key(&key);
    }

    fn get_range(&self, key_idx: usize, off: OffsetSpec, len: LenSpec) {
        let key = pool_key(key_idx);
        let entry = self.m.live.get(&key);
        let total = entry.map_or(0, |e| e.value.len() as u64);
        let bs = BS as u64;
        let offset = match off {
            OffsetSpec::Zero => 0,
            OffsetSpec::AtEnd => total,
            OffsetSpec::PastEnd(k) => total + 1 + u64::from(k),
            OffsetSpec::Fraction(f) => total * u64::from(f) / u64::from(u16::MAX),
            OffsetSpec::BlockEdge(i, before) => {
                let edge = (u64::from(i) % (total / bs + 1)) * bs;
                let edge = if before { edge.saturating_sub(1) } else { edge };
                edge.min(total)
            }
            OffsetSpec::Huge => u64::MAX,
        };
        let length = match len {
            LenSpec::Zero => 0,
            LenSpec::One => 1,
            LenSpec::Small(n) => u64::from(n),
            LenSpec::CrossBlock => bs + 1,
            LenSpec::ToEnd => total.saturating_sub(offset),
            LenSpec::Max => u64::MAX,
        };
        let res = self.db().get_range(&key, offset, length);
        let ctx = format!(
            "{}: get_range({}, {offset}, {length}) on a {total}-byte value",
            self.ctx,
            key_str(&key)
        );
        match entry {
            None => {
                let got =
                    res.unwrap_or_else(|e| panic!("{ctx}: absent key must give Ok(None): {e}"));
                assert!(got.is_none(), "{ctx}: absent key must give None");
            }
            Some(_) if offset > total => match res {
                Err(e) => assert!(
                    is_invalid_argument(&e),
                    "{ctx}: offset past the end must be InvalidArgument, got {e:?}"
                ),
                Ok(v) => panic!(
                    "{ctx}: offset past the end must be InvalidArgument, got Ok({:?})",
                    v.map(|b| b.len())
                ),
            },
            Some(e) => {
                let end = offset.saturating_add(length).min(total);
                let expected = &e.value[offset as usize..end as usize];
                let got = res
                    .unwrap_or_else(|err| panic!("{ctx}: failed: {err}"))
                    .unwrap_or_else(|| panic!("{ctx}: live key returned None"));
                assert_bytes_eq(&got, expected, &ctx);
            }
        }
    }

    /// Resolve batch items against the pre-batch state; repeated keys keep
    /// only their first occurrence (in-batch visibility is not modelled here,
    /// see `write_batch_applies_ops_in_order`).
    fn prepare(&self, items: &[ItemSpec]) -> Vec<Prepared> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for item in items {
            let (sel, value_spec, espec) = match item {
                ItemSpec::Put { key, value, expect } => (*key, Some(value), *expect),
                ItemSpec::Delete { key, expect } => (KeySel::Pool(*key), None, *expect),
            };
            let (key, key_ok) = resolve_key(sel);
            if !seen.insert(key.clone()) {
                continue;
            }
            let (value, value_ok) = match value_spec {
                Some(spec) => {
                    let (v, ok) = self.materialize(spec);
                    (Some(v), ok)
                }
                None => (None, true),
            };
            let valid = key_ok && value_ok;
            let (expect, holds) = if valid {
                self.resolve(&key, espec)
            } else {
                (Expect::Any, true)
            };
            let was_live = self.m.live.contains_key(&key);
            out.push(Prepared {
                key,
                value,
                expect,
                holds,
                valid,
                was_live,
            });
        }
        out
    }

    /// Record the outcome of one successful batch op in the model.
    fn record_applied(
        &mut self,
        p: &Prepared,
        result: Option<Revision>,
        before: Revision,
        what: &str,
    ) {
        match &p.value {
            Some(v) => {
                let rev = result.unwrap_or_else(|| {
                    panic!(
                        "{}: {what} returned None for a put of {}",
                        self.ctx,
                        key_str(&p.key)
                    )
                });
                assert!(
                    rev > before,
                    "{}: {what} put revision {rev} is not above the pre-batch maximum {before}",
                    self.ctx
                );
                self.m.record_put(p.key.clone(), rev, v.clone(), false);
            }
            None => {
                assert_eq!(
                    result.is_some(),
                    p.was_live,
                    "{}: {what} delete of {} must return Some iff a live record was deleted",
                    self.ctx,
                    key_str(&p.key)
                );
                if p.was_live {
                    self.m.record_delete(&p.key, self.history);
                }
            }
        }
    }

    fn batch(&mut self, items: &[ItemSpec], fail_at: Option<usize>) {
        let mut prepared = self.prepare(items);
        if let (Some(i), false) = (fail_at, prepared.is_empty()) {
            let j = i % prepared.len();
            if prepared[j].valid {
                let (expect, _) = self.resolve(&prepared[j].key, ExpectSpec::Stale);
                prepared[j].expect = expect;
                prepared[j].holds = false;
            }
        }
        let ops: Vec<BatchOp<'_>> = prepared
            .iter()
            .map(|p| match &p.value {
                Some(v) => BatchOp::Put {
                    key: &p.key,
                    value: v,
                    expect: p.expect,
                },
                None => BatchOp::Delete {
                    key: &p.key,
                    expect: p.expect,
                },
            })
            .collect();
        let res = self.db().write_batch(&ops);
        drop(ops);
        let any_invalid = prepared.iter().any(|p| !p.valid);
        let any_conflict = prepared.iter().any(|p| !p.holds);
        if !any_invalid && !any_conflict {
            let results = res.unwrap_or_else(|e| panic!("{}: write_batch failed: {e}", self.ctx));
            assert_eq!(
                results.len(),
                prepared.len(),
                "{}: write_batch must return one result per op",
                self.ctx
            );
            let before = self.m.max_rev;
            for (p, r) in prepared.iter().zip(results) {
                self.record_applied(p, r, before, "write_batch");
            }
        } else {
            let err = match res {
                Ok(r) => panic!(
                    "{}: write_batch with a failing op must fail as a whole, got {r:?}",
                    self.ctx
                ),
                Err(e) => e,
            };
            let acceptable =
                (any_conflict && is_conflict(&err)) || (any_invalid && is_rejected_input(&err));
            assert!(
                acceptable,
                "{}: unexpected write_batch error {err:?} (conflict expected: {any_conflict}, \
                 invalid op: {any_invalid})",
                self.ctx
            );
        }
        for p in prepared.iter().filter(|p| p.valid) {
            self.check_key(&p.key);
        }
    }

    fn batch_each(&mut self, items: &[ItemSpec], deferred: bool, sync_after: bool) {
        let prepared = self.prepare(items);
        let ops: Vec<BatchOp<'_>> = prepared
            .iter()
            .map(|p| match &p.value {
                Some(v) => BatchOp::Put {
                    key: &p.key,
                    value: v,
                    expect: p.expect,
                },
                None => BatchOp::Delete {
                    key: &p.key,
                    expect: p.expect,
                },
            })
            .collect();
        let durability = if deferred {
            Durability::Deferred
        } else {
            Durability::Immediate
        };
        let res = self.db().write_batch_each(&ops, durability);
        drop(ops);
        let results = res.unwrap_or_else(|e| {
            panic!(
                "{}: write_batch_each ({durability:?}) failed as a whole: {e}",
                self.ctx
            )
        });
        assert_eq!(
            results.len(),
            prepared.len(),
            "{}: write_batch_each must return one result per op",
            self.ctx
        );
        let before = self.m.max_rev;
        for (p, r) in prepared.iter().zip(results) {
            let what = format!("write_batch_each op {}", key_str(&p.key));
            if !p.valid {
                match r {
                    Ok(v) => panic!(
                        "{}: {what}: over-limit op must be skipped, got Ok({v:?})",
                        self.ctx
                    ),
                    Err(e) => assert!(
                        is_rejected_input(&e),
                        "{}: {what}: over-limit op must report InvalidArgument/LimitExceeded, got {e:?}",
                        self.ctx
                    ),
                }
            } else if !p.holds {
                match r {
                    Ok(v) => panic!(
                        "{}: {what} with {:?} must conflict, got Ok({v:?})",
                        self.ctx, p.expect
                    ),
                    Err(e) => self.assert_conflict(&e, &p.key, &what),
                }
            } else {
                let r = r.unwrap_or_else(|e| {
                    panic!("{}: {what} ({:?}) failed: {e}", self.ctx, p.expect)
                });
                self.record_applied(p, r, before, &what);
            }
        }
        if deferred {
            self.unsynced = true;
        }
        if sync_after {
            self.sync();
        }
        for p in prepared.iter().filter(|p| p.valid) {
            self.check_key(&p.key);
        }
    }

    fn sync(&mut self) {
        self.db()
            .sync()
            .unwrap_or_else(|e| panic!("{}: sync failed: {e}", self.ctx));
        self.unsynced = false;
    }

    fn scan(&self, opts: &ScanOptions) {
        let ctx = format!("{}: scan {opts:?}", self.ctx);
        let res = self.db().scan(opts);
        if is_degenerate(&opts.start, &opts.end) {
            match res {
                Ok(items) => assert!(
                    items.is_empty(),
                    "{ctx}: empty range returned {} items",
                    items.len()
                ),
                Err(e) => assert!(
                    is_invalid_argument(&e),
                    "{ctx}: an empty/inverted range may only fail with InvalidArgument, got {e:?}"
                ),
            }
            return;
        }
        let items = res.unwrap_or_else(|e| panic!("{ctx}: failed: {e}"));
        let mut expected: Vec<(&Vec<u8>, &Entry)> = self
            .m
            .live
            .iter()
            .filter(|(k, _)| in_bounds(k, &opts.start, &opts.end))
            .collect();
        if opts.reverse {
            expected.reverse();
        }
        if opts.limit > 0 {
            expected.truncate(opts.limit);
        }
        let got_keys: Vec<String> = items.iter().map(|i| key_str(&i.key)).collect();
        let want_keys: Vec<String> = expected.iter().map(|(k, _)| key_str(k)).collect();
        assert_eq!(got_keys, want_keys, "{ctx}: keys (in order)");
        for (item, (key, e)) in items.iter().zip(&expected) {
            assert_eq!(item.revision, e.rev, "{ctx}: revision of {}", key_str(key));
            assert_eq!(
                item.logical_len,
                e.value.len() as u64,
                "{ctx}: logical_len of {}",
                key_str(key)
            );
            match (&item.value, opts.with_values) {
                (Some(v), true) => {
                    assert_bytes_eq(v, &e.value, &format!("{ctx}: value of {}", key_str(key)))
                }
                (None, false) => {}
                (Some(_), false) => panic!("{ctx}: value returned although with_values is false"),
                (None, true) => panic!(
                    "{ctx}: no value for {} although with_values is true",
                    key_str(key)
                ),
            }
        }
    }

    /// Close and reopen (redb only) with `cfg`; the persisted creation
    /// parameters must win over `cfg`.
    fn reopen(&mut self, cfg: &Config, what: &str) {
        if self.unsynced {
            self.sync();
        }
        if !self.h.reopen(cfg) {
            return;
        }
        let db = self.h.db();
        assert_eq!(
            db.mode(),
            self.mode,
            "{}: {what}: persisted mode must win",
            self.ctx
        );
        assert_eq!(
            db.block_size(),
            BLOCK,
            "{}: {what}: persisted block_size must win",
            self.ctx
        );
        assert_eq!(
            db.inline_max(),
            INLINE,
            "{}: {what}: persisted inline_max must win",
            self.ctx
        );
        self.check_all(what);
    }

    /// Point reads of one key against the model (`get_with_revision`, `get`, `head`).
    #[track_caller]
    fn check_key(&self, key: &[u8]) {
        let db = self.db();
        let ctx = format!("{} key {}", self.ctx, key_str(key));
        let expected = self.m.live.get(key);
        let got = db
            .get_with_revision(key)
            .unwrap_or_else(|e| panic!("{ctx}: get_with_revision failed: {e}"));
        match (&got, expected) {
            (None, None) => {}
            (Some((rev, bytes)), Some(e)) => {
                assert_eq!(*rev, e.rev, "{ctx}: revision");
                assert_bytes_eq(bytes, &e.value, &ctx);
            }
            (Some((rev, bytes)), None) => panic!(
                "{ctx}: must be absent, but get_with_revision returned revision {rev} ({} bytes)",
                bytes.len()
            ),
            (None, Some(e)) => panic!(
                "{ctx}: must hold revision {} ({} bytes) but is absent",
                e.rev,
                e.value.len()
            ),
        }
        let plain = db
            .get(key)
            .unwrap_or_else(|e| panic!("{ctx}: get failed: {e}"));
        match (plain, expected) {
            (None, None) => {}
            (Some(bytes), Some(e)) => assert_bytes_eq(&bytes, &e.value, &format!("{ctx} (get)")),
            (plain, _) => panic!(
                "{ctx}: get returned {:?} bytes, model expects {:?}",
                plain.map(|b| b.len()),
                expected.map(|e| e.value.len())
            ),
        }
        let head = db
            .head(key)
            .unwrap_or_else(|e| panic!("{ctx}: head failed: {e}"));
        assert_eq!(
            head,
            expected.map(|e| (e.rev, e.value.len() as u64)),
            "{ctx}: head"
        );
    }

    /// Whole content through one scan (with point-read cross-check) plus
    /// point reads of every key of the pool.
    fn check_all(&self, what: &str) {
        let ctx = format!("{} [{what}]", self.ctx);
        let snap = snapshot(self.db(), &ctx);
        let expected: Snapshot = self
            .m
            .live
            .iter()
            .map(|(k, e)| (k.clone(), (e.rev, e.value.clone())))
            .collect();
        assert_snapshot_eq(&snap, &expected, &ctx);
        for i in 0..POOL_LEN {
            self.check_key(&pool_key(i));
        }
    }

    /// `history` / `get_at` against the model: without `keep_history` only the
    /// current manifest exists; with it every successful put is retained.
    fn check_history(&self, what: &str) {
        let db = self.db();
        let bogus = self.m.max_rev + 1_000_003;
        for i in 0..POOL_LEN {
            let key = pool_key(i);
            let ctx = format!("{} [{what}] history of {}", self.ctx, key_str(&key));
            let hist = db
                .history(&key)
                .unwrap_or_else(|e| panic!("{ctx}: history failed: {e}"));
            let live = self.m.live.get(&key);
            let puts: &[Entry] = self.m.puts.get(&key).map_or(&[], |v| v.as_slice());
            if !self.history {
                let want: Vec<HistoryEntry> = live
                    .map(|e| HistoryEntry {
                        revision: e.rev,
                        logical_len: e.value.len() as u64,
                        tombstone: false,
                        current: true,
                    })
                    .into_iter()
                    .collect();
                assert_eq!(
                    hist, want,
                    "{ctx}: without keep_history only the current manifest exists"
                );
                for p in puts.iter().filter(|p| Some(p.rev) != live.map(|e| e.rev)) {
                    let old = db
                        .get_at(&key, p.rev)
                        .unwrap_or_else(|e| panic!("{ctx}: get_at({}) failed: {e}", p.rev));
                    assert!(
                        old.is_none(),
                        "{ctx}: revision {} was replaced and history is off, get_at must be None",
                        p.rev
                    );
                }
            } else {
                assert!(
                    hist.windows(2).all(|w| w[0].revision < w[1].revision),
                    "{ctx}: entries must be oldest first with increasing revisions: {hist:?}"
                );
                for (n, h) in hist.iter().enumerate() {
                    assert_eq!(
                        h.current,
                        n + 1 == hist.len(),
                        "{ctx}: only the last entry is the current manifest: {hist:?}"
                    );
                }
                let got: Vec<(Revision, u64)> = hist
                    .iter()
                    .filter(|h| !h.tombstone)
                    .map(|h| (h.revision, h.logical_len))
                    .collect();
                let want: Vec<(Revision, u64)> =
                    puts.iter().map(|p| (p.rev, p.value.len() as u64)).collect();
                assert_eq!(
                    got, want,
                    "{ctx}: every successful put must be retained, in order"
                );
                match (live, hist.last()) {
                    (Some(e), Some(last)) => assert!(
                        !last.tombstone && last.revision == e.rev,
                        "{ctx}: current entry must be the live revision {}: {last:?}",
                        e.rev
                    ),
                    (None, Some(last)) => assert!(
                        last.tombstone,
                        "{ctx}: a deleted key's current manifest must be a tombstone: {last:?}"
                    ),
                    (Some(e), None) => {
                        panic!("{ctx}: live revision {} missing from history", e.rev)
                    }
                    (None, None) => assert!(puts.is_empty(), "{ctx}: written key has no history"),
                }
                for p in puts {
                    let old = db
                        .get_at(&key, p.rev)
                        .unwrap_or_else(|e| panic!("{ctx}: get_at({}) failed: {e}", p.rev));
                    match old {
                        Some(bytes) => {
                            assert_bytes_eq(&bytes, &p.value, &format!("{ctx}: get_at({})", p.rev))
                        }
                        None => panic!(
                            "{ctx}: retained revision {} not readable with get_at",
                            p.rev
                        ),
                    }
                }
            }
            if let Some(e) = live {
                let cur = db
                    .get_at(&key, e.rev)
                    .unwrap_or_else(|err| panic!("{ctx}: get_at(current) failed: {err}"));
                assert_eq!(
                    cur.as_deref(),
                    Some(e.value.as_slice()),
                    "{ctx}: get_at(current)"
                );
            }
            let none = db
                .get_at(&key, bogus)
                .unwrap_or_else(|e| panic!("{ctx}: get_at({bogus}) failed: {e}"));
            assert!(
                none.is_none(),
                "{ctx}: get_at of a never-allocated revision must be None"
            );
        }
    }

    /// `inspect` and `stats` against the model: inline/chunk layout, codecs per
    /// mode, refcounts and the exact number of stored objects (distinct block
    /// contents with dedupe, one object per chunk reference without).
    fn check_structure(&self, what: &str) {
        let ctx = format!("{} [{what}]", self.ctx);
        let db = self.db();
        let retained: Vec<&Entry> = if self.history {
            self.m.puts.values().flatten().collect()
        } else {
            self.m.live.values().collect()
        };
        let mut occurrences: HashMap<&[u8], u64> = HashMap::new();
        let mut chunk_refs = 0u64;
        for e in retained
            .iter()
            .filter(|e| !e.generated && e.value.len() > IM)
        {
            for block in e.value.chunks(BS) {
                *occurrences.entry(block).or_default() += 1;
                chunk_refs += 1;
            }
        }
        let expected_objects = if self.dedupe {
            occurrences.len() as u64
        } else {
            chunk_refs
        };
        let mut id_of_content: HashMap<&[u8], u64> = HashMap::new();
        let mut content_of_id: HashMap<u64, &[u8]> = HashMap::new();
        for (key, e) in &self.m.live {
            let kctx = format!("{ctx} inspect {}", key_str(key));
            let insp = db
                .inspect(key)
                .unwrap_or_else(|err| panic!("{kctx}: failed: {err}"))
                .unwrap_or_else(|| panic!("{kctx}: live key has no inspection"));
            assert_eq!(insp.key, *key, "{kctx}: key");
            assert_eq!(insp.revision, e.rev, "{kctx}: revision");
            assert_eq!(
                insp.logical_len,
                e.value.len() as u64,
                "{kctx}: logical_len"
            );
            if e.generated {
                assert_eq!(insp.kind, "generated", "{kctx}: kind");
                assert!(
                    insp.units.is_empty(),
                    "{kctx}: generated records store no units"
                );
                let (gid, gver, _, _) = insp
                    .generator
                    .clone()
                    .unwrap_or_else(|| panic!("{kctx}: generated record without generator info"));
                assert!(
                    gid == ids::REPEAT || gid == ids::ARITH_U64,
                    "{kctx}: generator id {gid}"
                );
                assert_eq!(gver, 1, "{kctx}: generator version");
                continue;
            }
            // `encoded_bytes` counts each distinct object once: a block repeated
            // inside the value is one shared (deduplicated) object.
            let mut encoded = 0u64;
            let mut seen_objects = std::collections::HashSet::new();
            for u in &insp.units {
                if u.object_id.is_none_or(|id| seen_objects.insert(id)) {
                    encoded += ENVELOPE_HEADER + u64::from(u.body_len);
                }
                match self.mode {
                    Mode::BabelPure => {
                        assert_eq!(
                            u.codec, "BabelAffineV1",
                            "{kctx}: BabelPure stores affine seeds only"
                        );
                        assert_eq!(
                            u.body_len, u.raw_len,
                            "{kctx}: an affine seed has exactly raw_len bytes"
                        );
                    }
                    Mode::Adaptive => {
                        assert_ne!(
                            u.codec, "BabelAffineV1",
                            "{kctx}: Adaptive must not use BabelAffineV1"
                        )
                    }
                }
            }
            assert_eq!(
                insp.encoded_bytes, encoded,
                "{kctx}: encoded_bytes must be the sum of 64-byte headers + bodies"
            );
            if e.value.len() <= IM {
                assert_eq!(
                    insp.kind, "inline",
                    "{kctx}: values <= inline_max are inline"
                );
                assert_eq!(insp.units.len(), 1, "{kctx}: one inline envelope");
                let u = &insp.units[0];
                assert_eq!(u.object_id, None, "{kctx}: inline units have no object");
                assert_eq!(u.refcount, None, "{kctx}: inline units have no refcount");
                assert_eq!(u.raw_len as usize, e.value.len(), "{kctx}: inline raw_len");
                continue;
            }
            assert_eq!(
                insp.kind, "chunks",
                "{kctx}: values > inline_max are chunked"
            );
            let blocks: Vec<&[u8]> = e.value.chunks(BS).collect();
            assert_eq!(
                insp.units.len(),
                blocks.len(),
                "{kctx}: one unit per {BS}-byte block"
            );
            for (n, (u, block)) in insp.units.iter().zip(&blocks).enumerate() {
                assert_eq!(u.raw_len as usize, block.len(), "{kctx}: unit {n} raw_len");
                let id = u
                    .object_id
                    .unwrap_or_else(|| panic!("{kctx}: chunk unit {n} has no object id"));
                let rc = u
                    .refcount
                    .unwrap_or_else(|| panic!("{kctx}: chunk unit {n} has no refcount"));
                if let Some(other) = content_of_id.insert(id, block) {
                    assert!(
                        other == *block,
                        "{kctx}: object {id} is shared by different contents"
                    );
                }
                if self.dedupe {
                    let want = occurrences[block];
                    assert_eq!(
                        rc, want,
                        "{kctx}: unit {n}: refcount must equal the {want} references to this \
                         content (byte-verified dedupe)"
                    );
                    if let Some(prev) = id_of_content.insert(block, id) {
                        assert_eq!(
                            prev, id,
                            "{kctx}: identical blocks stored as objects {prev} and {id}"
                        );
                    }
                } else {
                    assert_eq!(
                        rc, 1,
                        "{kctx}: without dedupe every object has one reference"
                    );
                }
            }
        }
        self.check_stats(&ctx, expected_objects, chunk_refs, occurrences.len());
    }

    fn check_stats(&self, ctx: &str, expected_objects: u64, chunk_refs: u64, distinct: usize) {
        let db = self.db();
        for i in 0..POOL_LEN {
            let key = pool_key(i);
            if self.m.live.contains_key(&key) {
                continue;
            }
            let insp = db
                .inspect(&key)
                .unwrap_or_else(|e| panic!("{ctx}: inspect({}) failed: {e}", key_str(&key)));
            if let Some(insp) = insp {
                assert_eq!(
                    insp.kind,
                    "tombstone",
                    "{ctx}: inspect of absent key {} must be None or a tombstone",
                    key_str(&key)
                );
            }
        }
        let st = db
            .stats()
            .unwrap_or_else(|e| panic!("{ctx}: stats failed: {e}"));
        assert_eq!(st.mode, self.mode, "{ctx}: stats.mode");
        assert_eq!(st.block_size, BLOCK, "{ctx}: stats.block_size");
        assert_eq!(st.inline_max, INLINE, "{ctx}: stats.inline_max");
        let logical: u64 = self.m.live.values().map(|e| e.value.len() as u64).sum();
        assert_eq!(st.logical_bytes, logical, "{ctx}: stats.logical_bytes");
        assert_eq!(
            st.objects, expected_objects,
            "{ctx}: stats.objects must be {expected_objects} ({chunk_refs} chunk references, \
             {distinct} distinct contents, dedupe {})",
            self.dedupe
        );
        // With dedupe every stored object is listed once as a candidate; any
        // other listed id is a stale id of a released object.
        let rep = db
            .verify(false)
            .unwrap_or_else(|e| panic!("{ctx}: verify failed: {e}"));
        assert_eq!(rep.dangling_candidates, 0, "{ctx}: verify.dangling_candidates");
        let listed = candidate_ids(&st);
        if self.dedupe {
            assert_eq!(
                listed - rep.stale_candidates,
                st.objects,
                "{ctx}: candidate ids of stored objects ({listed} listed, {} stale)",
                rep.stale_candidates
            );
            assert!(
                st.hash_candidates >= st.objects,
                "{ctx}: stats.hash_candidates {} < objects {}",
                st.hash_candidates,
                st.objects
            );
        } else {
            assert_eq!(
                (st.hash_candidates, listed),
                (0, 0),
                "{ctx}: no candidates without dedupe"
            );
        }
        assert_eq!(st.pending_imports, 0, "{ctx}: stats.pending_imports");
        assert_eq!(st.sources, 0, "{ctx}: stats.sources");
        assert_eq!(st.params, 0, "{ctx}: stats.params (nothing was trained)");
        let live = self.m.live.len() as u64;
        let tombstones = if self.history {
            self.m.tombstoned.len() as u64
        } else {
            0
        };
        assert_eq!(st.tombstones, tombstones, "{ctx}: stats.tombstones");
        assert!(
            st.records == live || st.records == live + tombstones,
            "{ctx}: stats.records = {} (live {live}, tombstones {tombstones})",
            st.records
        );
    }

    /// End-of-sequence checks, reopen (redb), gc, then delete everything.
    fn finish(mut self) {
        self.ctx = format!("[{} end of sequence]", self.label());
        if self.unsynced {
            self.sync();
        }
        self.check_all("final");
        self.check_history("final");
        self.check_structure("final");
        let rep = verify_ok(self.db(), true, &self.ctx);
        let stale = rep.stale_candidates;
        assert_eq!(
            rep.pending_imports, 0,
            "{}: verify.pending_imports",
            self.ctx
        );
        assert_eq!(
            rep.orphan_objects, 0,
            "{}: objects without references (refcount leak) found by verify",
            self.ctx
        );
        // Different creation parameters on reopen: the persisted ones must win.
        let reopen_cfg = Config {
            block_size: 4096,
            inline_max: 1024,
            ..self.cfg.clone()
        };
        if self.h.reopen(&reopen_cfg) {
            let db = self.h.db();
            assert_eq!(
                db.mode(),
                self.mode,
                "{}: persisted mode after reopen",
                self.ctx
            );
            assert_eq!(
                db.block_size(),
                BLOCK,
                "{}: persisted block_size must win",
                self.ctx
            );
            assert_eq!(
                db.inline_max(),
                INLINE,
                "{}: persisted inline_max must win",
                self.ctx
            );
            self.check_all("after reopen");
            self.check_history("after reopen");
            self.check_structure("after reopen");
            verify_ok(self.db(), true, &self.ctx);
        }
        let gc = self
            .h
            .db_mut()
            .gc()
            .unwrap_or_else(|e| panic!("{}: gc failed: {e}", self.ctx));
        // Only the stale candidate ids of released objects are collectable.
        assert_eq!(
            gc,
            GcReport {
                stale_candidates_removed: stale,
                ..GcReport::default()
            },
            "{}: gc found work on a database that never crashed or imported \
             (leaked objects/candidates or refcount drift)",
            self.ctx
        );
        verify_ok(self.db(), true, &self.ctx);
        self.check_all("after gc");
        self.drain();
    }

    /// Delete every live key (and prune history): no object may survive.
    fn drain(&mut self) {
        let keys: Vec<Vec<u8>> = self.m.live.keys().cloned().collect();
        for key in keys {
            let deleted = self
                .db()
                .delete(&key, Expect::Any)
                .unwrap_or_else(|e| panic!("{}: drain delete failed: {e}", self.ctx));
            assert!(
                deleted,
                "{}: drain delete of live {} returned false",
                self.ctx,
                key_str(&key)
            );
            self.m.record_delete(&key, self.history);
        }
        if self.history {
            let before = self
                .db()
                .stats()
                .unwrap_or_else(|e| panic!("{}: stats failed: {e}", self.ctx))
                .history_entries;
            let removed = self
                .db()
                .prune_history(None, 0)
                .unwrap_or_else(|e| panic!("{}: prune_history failed: {e}", self.ctx));
            assert_eq!(
                removed, before,
                "{}: prune_history(None, 0) must drop every retained entry",
                self.ctx
            );
        }
        let st = self
            .db()
            .stats()
            .unwrap_or_else(|e| panic!("{}: stats failed: {e}", self.ctx));
        assert_eq!(
            st.objects, 0,
            "{}: objects left after deleting every record and pruning history (leak)",
            self.ctx
        );
        assert_eq!(st.logical_bytes, 0, "{}: logical bytes left", self.ctx);
        let rest = self
            .db()
            .scan(&ScanOptions::all())
            .unwrap_or_else(|e| panic!("{}: scan failed: {e}", self.ctx));
        assert!(
            rest.is_empty(),
            "{}: {} records left after drain",
            self.ctx,
            rest.len()
        );
        let rep = verify_ok(self.db(), true, &self.ctx);
        assert_eq!(
            rep.stale_candidates,
            candidate_ids(&st),
            "{}: only stale candidate ids are left",
            self.ctx
        );
        let gc = self
            .h
            .db_mut()
            .gc()
            .unwrap_or_else(|e| panic!("{}: gc failed: {e}", self.ctx));
        assert_eq!(
            gc,
            GcReport {
                stale_candidates_removed: rep.stale_candidates,
                ..GcReport::default()
            },
            "{}: gc after drain",
            self.ctx
        );
        let st = self
            .db()
            .stats()
            .unwrap_or_else(|e| panic!("{}: stats failed: {e}", self.ctx));
        assert_eq!(st.hash_candidates, 0, "{}: hash candidates left after gc", self.ctx);
    }
}

fn model_config(mode: Mode, history: bool) -> Config {
    let mut cfg = small_config(mode);
    cfg.keep_history = history;
    cfg.max_value_len = MAX_VALUE;
    match (mode, history) {
        // Tiny block cache: constant evictions on reads.
        (Mode::BabelPure, _) => cfg.cache_bytes = 2 * 1024,
        // No block cache at all.
        (Mode::Adaptive, true) => cfg.cache_bytes = 0,
        (Mode::Adaptive, false) => {}
    }
    cfg
}

fn run_model<H: Harness>(harness: H, cfg: Config, ops: &[Op]) {
    let mut runner = Runner::new(harness, cfg);
    for (i, op) in ops.iter().enumerate() {
        runner.apply(i, op);
        if i % 10 == 9 {
            runner.check_all("periodic check");
        }
    }
    runner.finish();
}

fn run_mem(mode: Mode, history: bool, ops: &[Op]) {
    let cfg = model_config(mode, history);
    run_model(MemHarness::new(&cfg), cfg, ops);
}

fn run_redb(mode: Mode, history: bool, ops: &[Op]) {
    let cfg = model_config(mode, history);
    run_model(RedbHarness::new(&cfg), cfg, ops);
}

proptest! {
    #![proptest_config(pt_config(64, 2048))]

    #[test]
    fn model_adaptive_mem(ops in ops_strategy()) {
        run_mem(Mode::Adaptive, false, &ops);
    }

    #[test]
    fn model_babel_pure_mem(ops in ops_strategy()) {
        run_mem(Mode::BabelPure, false, &ops);
    }

    #[test]
    fn model_adaptive_history_mem(ops in ops_strategy()) {
        run_mem(Mode::Adaptive, true, &ops);
    }
}

proptest! {
    #![proptest_config(pt_config(24, 256))]

    #[test]
    fn model_adaptive_redb(ops in ops_strategy()) {
        run_redb(Mode::Adaptive, false, &ops);
    }

    #[test]
    fn model_babel_pure_redb(ops in ops_strategy()) {
        run_redb(Mode::BabelPure, false, &ops);
    }

    #[test]
    fn model_adaptive_history_redb(ops in ops_strategy()) {
        run_redb(Mode::Adaptive, true, &ops);
    }
}

// ---------------------------------------------------------------------------
// Deterministic contract checks (clearer failures than a shrunk sequence)
// ---------------------------------------------------------------------------

#[track_caller]
fn expect_conflict<T: std::fmt::Debug>(
    res: babeldb::Result<T>,
    actual: Option<Option<Revision>>,
    ctx: &str,
) {
    match res {
        Ok(v) => panic!("{ctx}: expected RevisionConflict, got Ok({v:?})"),
        Err(Error::RevisionConflict { actual: got, .. }) => {
            if let Some(want) = actual {
                assert_eq!(got, want, "{ctx}: RevisionConflict.actual");
            }
        }
        Err(e) => panic!("{ctx}: expected RevisionConflict, got {e:?}"),
    }
}

#[test]
fn expectations_follow_the_contract() {
    for mode in [Mode::Adaptive, Mode::BabelPure] {
        for history in [false, true] {
            let mut cfg = small_config(mode);
            cfg.keep_history = history;
            let db = mem_db(cfg);
            let ctx = format!("{mode:?} keep_history={history}");
            let v1 = pattern_bytes(Pattern::Text, 3 * BS + 9, 1);
            let v2 = pattern_bytes(Pattern::Motif, IM, 2);
            let r1 = db
                .put(b"k", &v1, Expect::Absent)
                .expect("first put with Expect::Absent");
            expect_conflict(
                db.put(b"k", &v2, Expect::Absent),
                Some(Some(r1)),
                &format!("{ctx}: put Absent on live key"),
            );
            expect_conflict(
                db.put(b"k", &v2, Expect::Revision(r1 + 1)),
                Some(Some(r1)),
                &format!("{ctx}: put wrong revision"),
            );
            let r2 = db
                .put(b"k", &v2, Expect::Revision(r1))
                .expect("put with the current revision");
            assert!(r2 > r1, "{ctx}: revisions must grow");
            expect_conflict(
                db.delete(b"k", Expect::Absent),
                Some(Some(r2)),
                &format!("{ctx}: delete Absent on live key"),
            );
            expect_conflict(
                db.delete(b"k", Expect::Revision(r1)),
                Some(Some(r2)),
                &format!("{ctx}: delete stale revision"),
            );
            assert_eq!(
                db.get_with_revision(b"k").unwrap(),
                Some((r2, v2.clone())),
                "{ctx}: failed ops changed the record"
            );
            assert!(
                db.delete(b"k", Expect::Revision(r2)).unwrap(),
                "{ctx}: delete with the current revision"
            );
            assert!(
                !db.delete(b"k", Expect::Any).unwrap(),
                "{ctx}: second delete deletes nothing"
            );
            assert!(
                !db.delete(b"k", Expect::Absent).unwrap(),
                "{ctx}: delete Absent of an absent key"
            );
            let absent_actual = if history { None } else { Some(None) };
            expect_conflict(
                db.delete(b"k", Expect::Revision(r2)),
                absent_actual,
                &format!("{ctx}: delete revision of deleted key"),
            );
            expect_conflict(
                db.put(b"k", &v1, Expect::Revision(r2)),
                absent_actual,
                &format!("{ctx}: put revision of deleted key"),
            );
            let r3 = db
                .put(b"k", &v1, Expect::Absent)
                .expect("a deleted key (tombstone) counts as absent");
            assert!(r3 > r2, "{ctx}: revisions must grow after a delete");
            assert_eq!(
                db.get_with_revision(b"k").unwrap(),
                Some((r3, v1.clone())),
                "{ctx}"
            );
            if history {
                assert_eq!(
                    db.get_at(b"k", r1).unwrap(),
                    Some(v1.clone()),
                    "{ctx}: get_at(r1)"
                );
                assert_eq!(
                    db.get_at(b"k", r2).unwrap(),
                    Some(v2.clone()),
                    "{ctx}: get_at(r2)"
                );
                let revs: Vec<(Revision, bool, bool)> = db
                    .history(b"k")
                    .unwrap()
                    .iter()
                    .map(|h| (h.revision, h.tombstone, h.current))
                    .collect();
                let puts: Vec<(Revision, bool, bool)> =
                    revs.iter().copied().filter(|r| !r.1).collect();
                assert_eq!(
                    puts,
                    vec![(r1, false, false), (r2, false, false), (r3, false, true)],
                    "{ctx}: retained puts and current manifest: {revs:?}"
                );
                assert!(
                    revs.len() <= 4,
                    "{ctx}: at most one retained tombstone: {revs:?}"
                );
            } else {
                assert_eq!(db.get_at(b"k", r1).unwrap(), None, "{ctx}: no history kept");
            }
            verify_ok(&db, true, &ctx);
        }
    }
}

#[test]
fn get_range_edges() {
    for mode in [Mode::Adaptive, Mode::BabelPure] {
        let db = mem_db(small_config(mode));
        let ctx = format!("{mode:?}");
        let v = pattern_bytes(Pattern::Random, 3 * BS + 100, 7);
        let n = v.len() as u64;
        let bs = BS as u64;
        db.put(b"r", &v, Expect::Any).unwrap();
        let cases = [
            (0, 0),
            (0, 1),
            (0, n),
            (0, u64::MAX),
            (bs - 1, 2),
            (bs, bs),
            (7, 3 * bs),
            (n - 1, 1),
            (n - 1, 100),
            (n, 0),
            (n, 5),
            (n, u64::MAX),
        ];
        for (off, len) in cases {
            let end = off.saturating_add(len).min(n);
            let got = db.get_range(b"r", off, len).unwrap().expect("live key");
            assert_bytes_eq(
                &got,
                &v[off as usize..end as usize],
                &format!("{ctx}: get_range({off}, {len})"),
            );
        }
        for off in [n + 1, n + bs, u64::MAX] {
            let err = db.get_range(b"r", off, 1).expect_err("offset past the end");
            assert!(
                is_invalid_argument(&err),
                "{ctx}: get_range({off}, 1): {err:?}"
            );
        }
        assert_eq!(
            db.get_range(b"missing", 0, 10).unwrap(),
            None,
            "{ctx}: absent key"
        );
        db.put(b"i", b"inline", Expect::Any).unwrap();
        assert_eq!(
            db.get_range(b"i", 2, 2).unwrap().as_deref(),
            Some(&b"li"[..]),
            "{ctx}"
        );
        assert_eq!(
            db.get_range(b"i", 6, 1).unwrap().as_deref(),
            Some(&b""[..]),
            "{ctx}"
        );
        assert!(
            is_invalid_argument(&db.get_range(b"i", 7, 0).unwrap_err()),
            "{ctx}"
        );
        let re = db.put(b"e", b"", Expect::Any).unwrap();
        assert_eq!(
            db.get(b"e").unwrap(),
            Some(Vec::new()),
            "{ctx}: empty value"
        );
        assert_eq!(
            db.head(b"e").unwrap(),
            Some((re, 0)),
            "{ctx}: empty value head"
        );
        assert_eq!(
            db.get_range(b"e", 0, 10).unwrap(),
            Some(Vec::new()),
            "{ctx}"
        );
        assert!(
            is_invalid_argument(&db.get_range(b"e", 1, 0).unwrap_err()),
            "{ctx}"
        );
        verify_ok(&db, true, &ctx);
    }
}

#[test]
fn write_batch_applies_ops_in_order() {
    let mut db = mem_db(small_config(Mode::Adaptive));
    let a = pattern_bytes(Pattern::Random, 2 * BS, 1);
    let b = pattern_bytes(Pattern::Text, 40, 2);
    let c = pattern_bytes(Pattern::Mixed, 3 * BS, 3);
    let revs = db
        .write_batch(&[
            BatchOp::Put {
                key: b"k",
                value: &a,
                expect: Expect::Any,
            },
            BatchOp::Put {
                key: b"k",
                value: &b,
                expect: Expect::Any,
            },
        ])
        .unwrap();
    assert_eq!(revs.len(), 2);
    assert!(
        revs.iter().all(Option::is_some),
        "puts return revisions: {revs:?}"
    );
    assert_eq!(
        db.get(b"k").unwrap(),
        Some(b.clone()),
        "last put of the batch wins"
    );
    assert_eq!(
        db.head(b"k").unwrap().map(|h| h.0),
        revs[1],
        "revision of the last put"
    );
    db.write_batch(&[
        BatchOp::Put {
            key: b"k",
            value: &c,
            expect: Expect::Any,
        },
        BatchOp::Delete {
            key: b"k",
            expect: Expect::Any,
        },
    ])
    .unwrap();
    assert_eq!(db.get(b"k").unwrap(), None, "put then delete in one batch");
    db.write_batch(&[
        BatchOp::Delete {
            key: b"k",
            expect: Expect::Any,
        },
        BatchOp::Put {
            key: b"k",
            value: &a,
            expect: Expect::Any,
        },
    ])
    .unwrap();
    assert_eq!(
        db.get(b"k").unwrap(),
        Some(a.clone()),
        "delete then put in one batch"
    );
    assert!(db.write_batch(&[]).unwrap().is_empty(), "empty batch");
    assert!(
        db.write_batch_each(&[], Durability::Immediate)
            .unwrap()
            .is_empty(),
        "empty group"
    );
    let stale = verify_ok(&db, true, "in-order batches").stale_candidates;
    assert_eq!(
        db.gc().unwrap(),
        GcReport {
            stale_candidates_removed: stale,
            ..GcReport::default()
        },
        "gc after in-order batches"
    );
}

#[test]
fn write_batch_each_skips_only_failing_ops() {
    let db = mem_db(small_config(Mode::Adaptive));
    let rc = db.put(b"c", b"live-c", Expect::Any).unwrap();
    let re = db.put(b"e", b"live-e", Expect::Any).unwrap();
    let big = pattern_bytes(Pattern::Mixed, 3 * BS, 5);
    let ops = [
        BatchOp::Put {
            key: b"a",
            value: &big,
            expect: Expect::Any,
        },
        BatchOp::Put {
            key: b"b",
            value: b"never",
            expect: Expect::Revision(rc + 999),
        },
        BatchOp::Delete {
            key: b"c",
            expect: Expect::Absent,
        },
        BatchOp::Put {
            key: b"d",
            value: b"fresh",
            expect: Expect::Absent,
        },
        BatchOp::Delete {
            key: b"e",
            expect: Expect::Revision(re),
        },
    ];
    let before = snapshot(&db, "before");
    let err = db
        .write_batch(&ops)
        .expect_err("write_batch with failing expectations");
    assert!(is_conflict(&err), "write_batch error: {err:?}");
    assert_eq!(
        snapshot(&db, "after failed batch"),
        before,
        "a failed write_batch must change nothing"
    );
    let res = db.write_batch_each(&ops, Durability::Immediate).unwrap();
    assert_eq!(res.len(), ops.len());
    assert!(matches!(res[0], Ok(Some(_))), "op 0: {:?}", res[0]);
    assert!(
        matches!(&res[1], Err(e) if is_conflict(e)),
        "op 1: {:?}",
        res[1]
    );
    assert!(
        matches!(&res[2], Err(e) if is_conflict(e)),
        "op 2: {:?}",
        res[2]
    );
    assert!(matches!(res[3], Ok(Some(_))), "op 3: {:?}", res[3]);
    assert!(matches!(res[4], Ok(Some(_))), "op 4: {:?}", res[4]);
    assert_eq!(db.get(b"a").unwrap(), Some(big), "applied put");
    assert_eq!(db.get(b"b").unwrap(), None, "skipped put");
    assert_eq!(
        db.get_with_revision(b"c").unwrap(),
        Some((rc, b"live-c".to_vec())),
        "skipped delete"
    );
    assert_eq!(
        db.get(b"d").unwrap(),
        Some(b"fresh".to_vec()),
        "applied put"
    );
    assert_eq!(db.get(b"e").unwrap(), None, "applied delete");
    verify_ok(&db, true, "write_batch_each");
}

fn unit_ids<S: Store>(db: &Db<S>, key: &[u8]) -> Vec<(u64, u64)> {
    db.inspect(key)
        .unwrap()
        .unwrap_or_else(|| panic!("inspect({}) of a live key", key_str(key)))
        .units
        .iter()
        .map(|u| {
            (
                u.object_id.expect("chunk unit"),
                u.refcount.expect("chunk refcount"),
            )
        })
        .collect()
}

#[test]
fn adaptive_dedupes_identical_blocks_byte_for_byte() {
    let db = mem_db(small_config(Mode::Adaptive));
    let block = pattern_bytes(Pattern::Random, BS, 3);
    let mut v = block.repeat(3);
    v.extend_from_slice(&pattern_bytes(Pattern::Random, 100, 4));
    db.put(b"x", &v, Expect::Any).unwrap();
    let x = unit_ids(&db, b"x");
    assert_eq!(x.len(), 4, "3 identical blocks + tail");
    assert!(
        x[0].0 == x[1].0 && x[1].0 == x[2].0,
        "identical blocks share one object: {x:?}"
    );
    assert_ne!(x[0].0, x[3].0, "different content, different object");
    assert_eq!((x[0].1, x[3].1), (3, 1), "refcounts: {x:?}");
    assert_eq!(db.stats().unwrap().objects, 2);
    db.put(b"y", &v, Expect::Any).unwrap();
    let y = unit_ids(&db, b"y");
    assert_eq!(y[0].0, x[0].0, "second value reuses the objects");
    assert_eq!((y[0].1, y[3].1), (6, 2), "refcounts after the copy: {y:?}");
    assert_eq!(
        db.stats().unwrap().objects,
        2,
        "dedupe: no new object for a copy"
    );
    assert!(db.delete(b"x", Expect::Any).unwrap());
    let y = unit_ids(&db, b"y");
    assert_eq!(
        (y[0].1, y[3].1),
        (3, 1),
        "refcounts after deleting x: {y:?}"
    );
    assert!(db.delete(b"y", Expect::Any).unwrap());
    let st = db.stats().unwrap();
    assert_eq!(st.objects, 0, "every object released");
    // Releasing leaves the two ids in their candidate lists, as stale ids.
    let rep = verify_ok(&db, true, "dedupe");
    assert_eq!(
        (st.hash_candidates, candidate_ids(&st), rep.stale_candidates),
        (2, 2, 2),
        "stale candidates"
    );
    // A new object with the same content drops the stale id of its list.
    db.put(b"z", &block.repeat(2), Expect::Any).unwrap();
    let st = db.stats().unwrap();
    let rep = verify_ok(&db, true, "dedupe");
    assert_eq!(
        (st.objects, st.hash_candidates, candidate_ids(&st), rep.stale_candidates),
        (1, 2, 2, 1),
        "the rewritten list holds only the new object"
    );
    let mut db = db;
    let gc = db.gc().unwrap();
    assert_eq!(
        gc,
        GcReport {
            stale_candidates_removed: 1,
            ..GcReport::default()
        }
    );
    let st = db.stats().unwrap();
    assert_eq!((st.objects, st.hash_candidates), (1, 1), "gc drops the stale row");
    verify_ok(&db, true, "dedupe");
}

#[test]
fn babel_pure_and_raw_only_never_share_objects() {
    let raw_only = Config {
        block_size: BLOCK,
        inline_max: INLINE,
        ..Config::raw_only()
    };
    for (label, cfg, codec) in [
        ("babel-pure", small_config(Mode::BabelPure), "BabelAffineV1"),
        ("raw-only", raw_only, "RawV1"),
    ] {
        let db = mem_db(cfg);
        let zeros = vec![0u8; 3 * BS];
        db.put(b"z1", &zeros, Expect::Any).unwrap();
        db.put(b"z2", &zeros, Expect::Any).unwrap();
        db.put(b"small", b"tiny", Expect::Any).unwrap();
        db.put(b"empty", b"", Expect::Any).unwrap();
        for key in [&b"z1"[..], b"z2", b"small", b"empty"] {
            let insp = db.inspect(key).unwrap().expect("live key");
            for u in &insp.units {
                assert_eq!(u.codec, codec, "{label}: codec of {}", key_str(key));
                assert_eq!(
                    u.body_len, u.raw_len,
                    "{label}: {codec} body has raw_len bytes"
                );
                if insp.kind == "chunks" {
                    assert_eq!(u.refcount, Some(1), "{label}: no sharing without dedupe");
                }
            }
        }
        let st = db.stats().unwrap();
        assert_eq!(
            st.objects, 6,
            "{label}: identical blocks are stored separately"
        );
        assert_eq!(st.hash_candidates, 0, "{label}: no dedupe index");
        assert_eq!(db.get(b"z2").unwrap(), Some(zeros), "{label}");
        assert_eq!(db.get(b"empty").unwrap(), Some(Vec::new()), "{label}");
        verify_ok(&db, true, label);
    }
}

#[test]
fn deferred_group_commit_is_visible_then_durable_after_sync() {
    let dir = temp_dir("babeldb-deferred-");
    let path = dir.path().join("deferred.redb");
    let cfg = small_config(Mode::Adaptive);
    let v = pattern_bytes(Pattern::Mixed, 4 * BS + 3, 11);
    {
        let db = open_redb(&path, cfg.clone());
        let res = db
            .write_batch_each(
                &[
                    BatchOp::Put {
                        key: b"a",
                        value: &v,
                        expect: Expect::Any,
                    },
                    BatchOp::Put {
                        key: b"b",
                        value: b"small",
                        expect: Expect::Absent,
                    },
                ],
                Durability::Deferred,
            )
            .unwrap();
        assert!(res.iter().all(|r| matches!(r, Ok(Some(_)))), "{res:?}");
        assert_eq!(
            db.get(b"a").unwrap(),
            Some(v.clone()),
            "deferred commit is visible at once"
        );
        db.sync().unwrap();
    }
    let db = open_redb(&path, cfg);
    assert_eq!(
        db.get(b"a").unwrap(),
        Some(v),
        "synced deferred commit survives reopen"
    );
    assert_eq!(db.get(b"b").unwrap(), Some(b"small".to_vec()));
    verify_ok(&db, true, "deferred + sync");
}
