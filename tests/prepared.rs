//! Prepared writes: `Db::prepare` / `Db::prepare_batch` + `Db::write_prepared_each`
//! (same contract as `write_batch_each`), and the group committer preparing
//! requests in the submitting thread (`BatchSink::prepare`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use babeldb::config::Config;
use babeldb::engine::PreparedOp;
use babeldb::planner::TrainOptions;
use babeldb::scale::group_commit::{GroupCommitConfig, GroupCommitter, TrySubmitError};
use babeldb::scale::{BatchSink, OpResult, OwnedOp, SinkOps};
use babeldb::store::{Durability, Store};
use babeldb::{BatchOp, Db, Error, Expect, MemStore, Result, ScanOptions};
use proptest::prelude::*;

fn small_cfg(keep_history: bool) -> Config {
    Config { inline_max: 64, block_size: 512, keep_history, ..Config::adaptive() }
}

fn mem_db(cfg: Config) -> Db<MemStore> {
    Db::with_store(MemStore::new(), cfg).unwrap()
}

/// Deterministic values: inline and chunked sizes, repeated contents
/// (dedupe), text-like and binary.
fn value(i: u8) -> Vec<u8> {
    match i % 9 {
        0 => Vec::new(),
        1 => b"hello".to_vec(),
        2 => vec![b'a'; 64],
        3 => vec![b'b'; 65],
        4 => (0..300u32).map(|j| (j * 7 % 251) as u8).collect(),
        5 => b"{\"id\":1,\"content\":\"the deploy worked\"} ".repeat(20),
        6 => (0..1300u32).map(|j| (j / 3) as u8).collect(),
        7 => (0..513u32).map(|j| (j * 31 % 256) as u8 ^ i).collect(),
        _ => format!("value {i} ").into_bytes(),
    }
}

/// Comparable form of one op result: the revision, or the error variant.
fn outcome(r: &Result<Option<u64>>) -> String {
    match r {
        Ok(v) => format!("ok {v:?}"),
        Err(e) => {
            let s = format!("{e:?}");
            let end = s.find([' ', '(', '{']).unwrap_or(s.len());
            format!("err {}", &s[..end])
        }
    }
}

#[derive(Clone, Debug)]
enum Spec {
    Put { key: u8, value: u8, expect: u8 },
    Delete { key: u8, expect: u8 },
}

fn spec_strategy() -> impl Strategy<Value = Spec> {
    prop_oneof![
        3 => (0u8..7, any::<u8>(), 0u8..12).prop_map(|(key, value, expect)| Spec::Put { key, value, expect }),
        1 => (0u8..7, 0u8..12).prop_map(|(key, expect)| Spec::Delete { key, expect }),
    ]
}

/// Key 6 is empty (invalid for puts and deletes).
fn key_bytes(k: u8) -> Vec<u8> {
    if k == 6 { Vec::new() } else { format!("key{k}").into_bytes() }
}

/// 0..=1 Any, 2 Absent, else Revision(n - 2).
fn expect_of(e: u8) -> Expect {
    match e {
        0 | 1 => Expect::Any,
        2 => Expect::Absent,
        n => Expect::Revision(u64::from(n) - 2),
    }
}

struct Owned {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    expect: Expect,
}

fn owned(specs: &[Spec]) -> Vec<Owned> {
    specs
        .iter()
        .map(|s| match *s {
            Spec::Put { key, value: v, expect } => Owned { key: key_bytes(key), value: Some(value(v)), expect: expect_of(expect) },
            Spec::Delete { key, expect } => Owned { key: key_bytes(key), value: None, expect: expect_of(expect) },
        })
        .collect()
}

fn batch_ops(ops: &[Owned]) -> Vec<BatchOp<'_>> {
    ops.iter()
        .map(|o| match &o.value {
            Some(v) => BatchOp::Put { key: &o.key, value: v, expect: o.expect },
            None => BatchOp::Delete { key: &o.key, expect: o.expect },
        })
        .collect()
}

/// Table-level accounting (records, objects, candidates, history, bytes).
fn shape(db: &Db<MemStore>) -> [u64; 8] {
    let s = db.stats().unwrap();
    [s.records, s.tombstones, s.objects, s.hash_candidates, s.history_entries, s.logical_bytes, s.manifest_bytes, s.object_bytes]
}

fn dump(db: &Db<MemStore>) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
    let mut out: Vec<_> = db
        .scan(&ScanOptions::all().with_values(true))
        .unwrap()
        .into_iter()
        .map(|it| (it.key, it.revision, it.value))
        .collect();
    for k in 0..6 {
        let key = key_bytes(k);
        for h in db.history(&key).unwrap() {
            out.push((key.clone(), h.revision, db.get_at(&key, h.revision).unwrap()));
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    /// `write_prepared_each(prepare_batch(ops))` = `write_batch_each(ops)`,
    /// op by op and in the resulting state, even when every batch is
    /// prepared before the batches in front of it are written (expectations
    /// are evaluated when the op is written, not when it is prepared).
    #[test]
    fn prepared_writes_match_write_batch_each(
        batches in prop::collection::vec(prop::collection::vec(spec_strategy(), 1..8), 1..6),
        keep_history in any::<bool>(),
        deferred in any::<bool>(),
    ) {
        let durability = if deferred { Durability::Deferred } else { Durability::Immediate };
        let a = mem_db(small_cfg(keep_history));
        let b = mem_db(small_cfg(keep_history));
        let owned_batches: Vec<Vec<Owned>> = batches.iter().map(|s| owned(s)).collect();
        // Everything is prepared up front, then written in order.
        let prepared: Vec<Vec<PreparedOp>> = owned_batches.iter().map(|o| b.prepare_batch(&batch_ops(o))).collect();
        for (i, (ops, p)) in owned_batches.iter().zip(prepared).enumerate() {
            let expected = a.write_batch_each(&batch_ops(ops), durability).unwrap();
            let got = b.write_prepared_each(p, durability).unwrap();
            let expected: Vec<String> = expected.iter().map(outcome).collect();
            let got: Vec<String> = got.iter().map(outcome).collect();
            prop_assert_eq!(&got, &expected, "batch {}", i);
        }
        prop_assert_eq!(dump(&b), dump(&a));
        prop_assert!(b.verify(true).unwrap().ok());
        prop_assert_eq!(shape(&b), shape(&a));
    }
}

#[test]
fn single_prepare_matches_put_and_delete() {
    let db = mem_db(small_cfg(false));
    let big = value(6);
    let p = vec![
        db.prepare(&BatchOp::Put { key: b"a", value: b"small", expect: Expect::Absent }),
        db.prepare(&BatchOp::Put { key: b"b", value: &big, expect: Expect::Any }),
    ];
    let r = db.write_prepared_each(p, Durability::Immediate).unwrap();
    assert!(r.iter().all(|r| r.is_ok()), "{r:?}");
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"small"[..]));
    assert_eq!(db.get(b"b").unwrap(), Some(big));
    let del = db.prepare(&BatchOp::Delete { key: b"a", expect: Expect::Revision(1) });
    assert_eq!(del.key(), b"a");
    assert!(!del.is_put());
    let r = db.write_prepared_each(vec![del], Durability::Immediate).unwrap();
    assert_eq!(r.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>(), vec![Some(1)]);
    assert_eq!(db.get(b"a").unwrap(), None);
    assert!(db.write_prepared_each(Vec::new(), Durability::Immediate).unwrap().is_empty());
}

/// The expectation is checked when the op is written.
#[test]
fn expectation_is_checked_when_written() {
    let db = mem_db(small_cfg(false));
    let p = db.prepare_batch(&[
        BatchOp::Put { key: b"k", value: b"first", expect: Expect::Absent },
        BatchOp::Put { key: b"j", value: b"other", expect: Expect::Absent },
    ]);
    let rev = db.put(b"k", b"meanwhile", Expect::Any).unwrap();
    let r = db.write_prepared_each(p, Durability::Immediate).unwrap();
    match &r[0] {
        Err(Error::RevisionConflict { actual, .. }) => assert_eq!(*actual, Some(rev)),
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert!(r[1].is_ok());
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"meanwhile"[..]));
}

/// Chunked prepared values are deduplicated inside the transaction, by byte
/// comparison, against objects written after they were prepared too.
#[test]
fn dedupe_is_decided_when_written() {
    let db = mem_db(small_cfg(false));
    let big = value(6);
    let p1 = db.prepare_batch(&[BatchOp::Put { key: b"x", value: &big, expect: Expect::Any }]);
    let p2 = db.prepare_batch(&[BatchOp::Put { key: b"y", value: &big, expect: Expect::Any }]);
    db.write_prepared_each(p1, Durability::Immediate).unwrap().pop().unwrap().unwrap();
    let written = db.stats().unwrap().counters.objects_written;
    db.write_prepared_each(p2, Durability::Immediate).unwrap().pop().unwrap().unwrap();
    let s = db.stats().unwrap();
    assert_eq!(s.counters.objects_written, written, "the second copy reuses every block");
    assert!(s.counters.dedupe_hits >= big.len().div_ceil(512) as u64);
    assert_eq!(db.get(b"y").unwrap(), Some(big));
}

#[test]
fn invalid_and_foreign_ops_are_reported_in_their_slot() {
    let cfg = Config { max_key_len: 8, max_value_len: 100, ..small_cfg(false) };
    let db = mem_db(cfg.clone());
    let other = mem_db(cfg);
    let long = vec![1u8; 101];
    let p = db.prepare_batch(&[
        BatchOp::Put { key: b"", value: b"v", expect: Expect::Any },
        BatchOp::Put { key: b"too-long-key", value: b"v", expect: Expect::Any },
        BatchOp::Put { key: b"k", value: &long, expect: Expect::Any },
        BatchOp::Delete { key: b"", expect: Expect::Any },
        BatchOp::Put { key: b"ok", value: b"v", expect: Expect::Any },
    ]);
    assert!(p[0].error().is_some() && p[4].error().is_none());
    let r = db.write_prepared_each(p, Durability::Immediate).unwrap();
    assert!(matches!(r[0], Err(Error::InvalidArgument(_))));
    assert!(matches!(r[1], Err(Error::LimitExceeded(_))));
    assert!(matches!(r[2], Err(Error::LimitExceeded(_))));
    assert!(matches!(r[3], Err(Error::InvalidArgument(_))));
    assert!(matches!(r[4], Ok(Some(_))));
    // Only invalid ops: nothing is committed.
    let commits = db.stats().unwrap().counters.commits;
    let r = db.write_prepared_each(db.prepare_batch(&[BatchOp::Delete { key: b"", expect: Expect::Any }]), Durability::Immediate).unwrap();
    assert!(r[0].is_err());
    assert_eq!(db.stats().unwrap().counters.commits, commits);
    // Ops prepared by another handle are refused, the others applied.
    let mut mixed = other.prepare_batch(&[BatchOp::Put { key: b"f", value: b"foreign", expect: Expect::Any }]);
    mixed.extend(db.prepare_batch(&[BatchOp::Put { key: b"g", value: b"mine", expect: Expect::Any }]));
    let r = db.write_prepared_each(mixed, Durability::Immediate).unwrap();
    assert!(matches!(r[0], Err(Error::InvalidArgument(_))), "{:?}", r[0]);
    assert!(r[1].is_ok());
    assert_eq!(db.get(b"f").unwrap(), None);
    let only_foreign = other.prepare_batch(&[BatchOp::Delete { key: b"g", expect: Expect::Any }]);
    assert!(matches!(db.write_prepared_each(only_foreign, Durability::Immediate).unwrap()[0], Err(Error::InvalidArgument(_))));
    assert!(db.get(b"g").unwrap().is_some());
}

fn templated(i: usize) -> Vec<u8> {
    format!(
        r#"{{"id":"11900000000{i:05}","channel_id":"1180000000000000123","author":{{"id":"2200{i}","username":"user{}"}},"content":"message number {i}","pinned":false,"tts":false}}"#,
        i % 17
    )
    .into_bytes()
}

fn force() -> TrainOptions {
    TrainOptions { require_gain: false, ..TrainOptions::default() }
}

/// A dependency installed between prepare and write: the prepared values
/// keep the representation they were prepared with and stay exact.
#[test]
fn planner_changes_between_prepare_and_write_keep_values_exact() {
    let db = mem_db(Config { inline_max: 1024, ..Config::adaptive() });
    let samples: Vec<Vec<u8>> = (0..400).map(templated).collect();
    let before = db.prepare_batch(&[BatchOp::Put { key: b"before", value: &templated(1000), expect: Expect::Any }]);
    assert!(db.train_template(&samples, &force()).unwrap().installed);
    let with_t1 = db.prepare_batch(&[BatchOp::Put { key: b"t1", value: &templated(1001), expect: Expect::Any }]);
    assert!(db.train_dictionary(&samples, &force()).unwrap().installed);
    for p in [before, with_t1] {
        assert!(db.write_prepared_each(p, Durability::Immediate).unwrap()[0].is_ok());
    }
    assert_eq!(db.get(b"before").unwrap(), Some(templated(1000)));
    assert_eq!(db.get(b"t1").unwrap(), Some(templated(1001)));
    assert!(db.verify(true).unwrap().ok());
}

/// `gc` between prepare and write removed the template a prepared value
/// needs: that op fails with MissingDependency, nothing unreadable is stored.
#[test]
fn a_param_removed_by_gc_fails_only_the_ops_that_need_it() {
    let mut db = mem_db(Config { inline_max: 1024, ..Config::adaptive() });
    let samples: Vec<Vec<u8>> = (0..400).map(templated).collect();
    let t1 = db.train_template(&samples, &force()).unwrap().param_id.unwrap();
    let values: Vec<Vec<u8>> = (2000..2010).map(templated).collect();
    let keys: Vec<Vec<u8>> = (0..values.len()).map(|i| format!("m{i}").into_bytes()).collect();
    let mut ops: Vec<BatchOp<'_>> = keys
        .iter()
        .zip(&values)
        .map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any })
        .collect();
    ops.push(BatchOp::Put { key: b"plain", value: b"x", expect: Expect::Any });
    let p = db.prepare_batch(&ops);
    // A newer template becomes active; nothing references t1, gc drops it.
    let t2 = db.train_template(&samples[..200], &force()).unwrap().param_id.unwrap();
    assert_ne!(t1, t2);
    assert!(db.gc().unwrap().params_removed >= 1);
    let r = db.write_prepared_each(p, Durability::Immediate).unwrap();
    let missing = r
        .iter()
        .filter(|r| matches!(r, Err(Error::MissingDependency { param_id }) if *param_id == t1))
        .count();
    assert!(missing > 0, "some values were prepared with the removed template: {r:?}");
    for (i, res) in r.iter().enumerate().take(values.len()) {
        match res {
            Ok(_) => assert_eq!(db.get(&keys[i]).unwrap().as_ref(), Some(&values[i])),
            Err(_) => assert_eq!(db.get(&keys[i]).unwrap(), None),
        }
    }
    assert!(r[values.len()].is_ok());
    assert!(db.verify(true).unwrap().ok());
    // Prepared again, they are written.
    let r = db.write_prepared_each(db.prepare_batch(&ops[..values.len()]), Durability::Immediate).unwrap();
    assert!(r.iter().all(|r| r.is_ok()));
    for (k, v) in keys.iter().zip(&values) {
        assert_eq!(db.get(k).unwrap().as_ref(), Some(v));
    }
}

// ---------------------------------------------------------------------------
// Group committer
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum Event {
    /// Thread name, first key.
    Prepared(String, Vec<u8>),
    /// Keys of an `apply` / `apply_prepared` call.
    Applied(Vec<Vec<u8>>),
    AppliedPrepared(Vec<Vec<u8>>),
}

/// A database sink that records calls. Requests whose first key starts with
/// `o` are not prepared; a key `panic` panics in `prepare`.
struct Recorder {
    db: Db<MemStore>,
    events: Mutex<Vec<Event>>,
    prepares: AtomicUsize,
}

impl Recorder {
    fn new() -> Recorder {
        Recorder { db: mem_db(small_cfg(false)), events: Mutex::new(Vec::new()), prepares: AtomicUsize::new(0) }
    }

    fn events(&self) -> std::sync::MutexGuard<'_, Vec<Event>> {
        self.events.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl BatchSink for Recorder {
    fn apply(&self, ops: &[OwnedOp], durability: Durability) -> Result<Vec<OpResult>> {
        self.events().push(Event::Applied(ops.iter().map(|o| o.key().to_vec()).collect()));
        BatchSink::apply(&self.db, ops, durability)
    }

    fn sync(&self) -> Result<()> {
        self.db.sync()
    }

    fn prepare(&self, ops: Vec<OwnedOp>) -> SinkOps {
        self.prepares.fetch_add(1, Ordering::SeqCst);
        let first = ops[0].key().to_vec();
        let name = thread::current().name().unwrap_or("").to_string();
        self.events().push(Event::Prepared(name, first.clone()));
        if first == b"panic" {
            panic!("injected prepare panic");
        }
        if first.starts_with(b"o") { SinkOps::Owned(ops) } else { BatchSink::prepare(&self.db, ops) }
    }

    fn apply_prepared(&self, ops: Vec<PreparedOp>, durability: Durability) -> Result<Vec<OpResult>> {
        self.events().push(Event::AppliedPrepared(ops.iter().map(|o| o.key().to_vec()).collect()));
        BatchSink::apply_prepared(&self.db, ops, durability)
    }
}

fn put(key: &str, v: &str) -> Vec<OwnedOp> {
    vec![OwnedOp::put(key.as_bytes(), v.as_bytes(), Expect::Any)]
}

#[test]
fn committer_prepares_in_the_submitting_thread() {
    let sink = Arc::new(Recorder::new());
    let committer = GroupCommitter::new(sink.clone(), GroupCommitConfig::default()).unwrap();
    let rev = thread::scope(|scope| {
        thread::Builder::new()
            .name("submitter-1".into())
            .spawn_scoped(scope, || committer.put(b"p1".to_vec(), b"v1".to_vec(), Expect::Absent).unwrap())
            .unwrap()
            .join()
            .unwrap()
    });
    assert_eq!(sink.db.get(b"p1").unwrap().as_deref(), Some(&b"v1"[..]));
    committer.shutdown().unwrap();
    let events = sink.events();
    assert_eq!(events[0], Event::Prepared("submitter-1".into(), b"p1".to_vec()));
    assert_eq!(events[1], Event::AppliedPrepared(vec![b"p1".to_vec()]));
    assert!(rev > 0);
}

/// Prepared and unprepared requests are never mixed in one batch, and the
/// submission order is kept across them.
#[test]
fn batches_never_mix_prepared_and_unprepared_requests() {
    let sink = Arc::new(Recorder::new());
    let committer = GroupCommitter::new(sink.clone(), GroupCommitConfig::default()).unwrap();
    let keys = ["a1", "a2", "o1", "o2", "a3", "o3", "a4"];
    let tickets: Vec<_> = keys.iter().map(|k| committer.submit(put(k, "v")).unwrap()).collect();
    for t in tickets {
        assert!(t.wait().unwrap()[0].is_ok());
    }
    committer.shutdown().unwrap();
    let mut applied = Vec::new();
    for e in sink.events().iter() {
        match e {
            Event::Applied(keys) => {
                assert!(keys.iter().all(|k| k.starts_with(b"o")), "{keys:?}");
                applied.extend(keys.iter().cloned());
            }
            Event::AppliedPrepared(keys) => {
                assert!(keys.iter().all(|k| k.starts_with(b"a")), "{keys:?}");
                applied.extend(keys.iter().cloned());
            }
            Event::Prepared(..) => {}
        }
    }
    let expected: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
    assert_eq!(applied, expected);
    // Revisions follow the submission order.
    let revs: Vec<u64> = keys.iter().map(|k| sink.db.head(k.as_bytes()).unwrap().unwrap().0).collect();
    assert!(revs.windows(2).all(|w| w[0] < w[1]), "{revs:?}");
}

#[test]
fn a_panic_while_preparing_fails_that_request_only() {
    let sink = Arc::new(Recorder::new());
    let committer = GroupCommitter::new(sink.clone(), GroupCommitConfig::default()).unwrap();
    let err = committer.apply(put("panic", "v")).unwrap_err();
    assert!(err.to_string().contains("injected prepare panic"), "{err}");
    match committer.try_submit(put("panic", "v")) {
        Err(TrySubmitError::Closed(e)) => assert!(e.to_string().contains("panicked")),
        other => panic!("unexpected {other:?}"),
    }
    committer.put(b"after".to_vec(), b"v".to_vec(), Expect::Any).unwrap();
    assert!(committer.is_running());
    let s = committer.stats();
    assert_eq!((s.queued_ops, s.queued_bytes), (0, 0));
    committer.shutdown().unwrap();
    assert_eq!(sink.db.get(b"panic").unwrap(), None);
}

/// A full queue hands the operations back untouched: nothing was prepared.
#[test]
fn try_submit_on_a_full_queue_does_not_prepare() {
    let sink = Arc::new(Recorder::new());
    let cfg = GroupCommitConfig { queue_max_ops: 1, ..GroupCommitConfig::default() };
    let committer = GroupCommitter::new(sink.clone(), cfg).unwrap();
    // Hold the store's write lock: the writer thread blocks in its commit.
    let held = sink.db.store().begin_write().unwrap();
    let first = committer.submit(put("a1", "v")).unwrap();
    // The writer may have taken a1 already: fill the queue until it is full.
    let mut queued = vec![first];
    let mut handed_back = None;
    for i in 0..3 {
        match committer.try_submit(put(&format!("a{}", i + 2), "v")) {
            Ok(t) => queued.push(t),
            Err(TrySubmitError::Full(ops)) => {
                handed_back = Some(ops);
                break;
            }
            Err(e) => panic!("{e}"),
        }
    }
    let prepares = sink.prepares.load(Ordering::SeqCst);
    let ops = handed_back.expect("the queue fills up");
    assert_eq!(ops.len(), 1);
    assert_eq!(sink.prepares.load(Ordering::SeqCst), prepares);
    drop(held);
    for t in queued {
        t.wait().unwrap();
    }
    committer.shutdown().unwrap();
}

/// After `shutdown` the committer holds no reference to its sink.
#[test]
fn shutdown_releases_the_sink() {
    let db = Arc::new(mem_db(small_cfg(false)));
    let committer = GroupCommitter::new(db.clone(), GroupCommitConfig::default()).unwrap();
    committer.put(b"k".to_vec(), b"v".to_vec(), Expect::Any).unwrap();
    assert!(Arc::strong_count(&db) > 1);
    committer.shutdown().unwrap();
    assert_eq!(Arc::strong_count(&db), 1);
    assert!(committer.put(b"k2".to_vec(), b"v".to_vec(), Expect::Any).is_err());
    drop(committer);
    assert!(Arc::try_unwrap(db).is_ok());
}

/// Many threads through a database committer: every result and value is
/// exact, conflicting `Absent` puts on one key let exactly one win.
#[test]
fn concurrent_prepared_writes_through_a_db_committer() {
    let db = Arc::new(mem_db(small_cfg(false)));
    let committer = Arc::new(GroupCommitter::new(db.clone(), GroupCommitConfig::default()).unwrap());
    let threads = 8;
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let committer = committer.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let mut wins = 0;
                for i in 0..40u8 {
                    let key = format!("t{t}-{i}").into_bytes();
                    committer.put(key, value(i.wrapping_add(t as u8)), Expect::Absent).unwrap();
                    if committer.apply_one(OwnedOp::put(b"contended".to_vec(), vec![t as u8], Expect::Absent)).is_ok() {
                        wins += 1;
                    }
                }
                wins
            })
        })
        .collect();
    let wins: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(wins, 1);
    committer.shutdown().unwrap();
    for t in 0..threads {
        for i in 0..40u8 {
            let key = format!("t{t}-{i}").into_bytes();
            assert_eq!(db.get(&key).unwrap(), Some(value(i.wrapping_add(t as u8))));
        }
    }
    assert!(db.verify(true).unwrap().ok());
}
