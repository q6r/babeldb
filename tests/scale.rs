//! Tests of the Discord-scale layer (`babeldb::scale`).
//!
//! Everything except the `engine_*` tests runs against in-process fakes, so it
//! does not depend on the storage engine. The `engine_*` tests exercise the
//! same paths on `Db<MemStore>` and redb and are ignored until the engine is
//! merged.

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use babeldb::engine::{Expect, Revision, ScanItem, ScanOptions};
use babeldb::error::{Error, Result};
use babeldb::scale::{
    BatchSink, ChatOptions, ChatStore, Coalescer, GroupCommitConfig, GroupCommitter, OpResult,
    OwnedOp, ReadSource, Router, ScanRoute, ShardBackend, ShardMeta, ShardedDb, Snowflake,
    SnowflakeParts, TrySubmitError, WriteDurability, message_key, parse_message_key,
    shard_file_name, shard_index, stable_hash,
};
use babeldb::store::Durability;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run `f` on a thread and fail the test instead of hanging.
fn within<T: Send + 'static>(secs: u64, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(v) => v,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("timed out after {secs} s: possible hang"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("the checked closure panicked"),
    }
}

fn wait_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        thread::sleep(Duration::from_millis(1));
    }
}

/// Closed until opened; `pass` blocks while closed.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.cv.notify_all();
    }

    fn pass(&self) {
        let mut g = self.open.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
    }
}

/// Deterministic xorshift generator.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

// ---------------------------------------------------------------------------
// Fake database: a BTreeMap with revisions, write_batch_each semantics,
// durability tracking and fault injection.
// ---------------------------------------------------------------------------

/// (revision, value) of a live key.
type Entry = (Revision, Vec<u8>);

#[derive(Default)]
struct FakeState {
    map: BTreeMap<Vec<u8>, Entry>,
    last_rev: Revision,
    durable_rev: Revision,
    batches: Vec<(usize, Durability)>,
    syncs: usize,
    applies_started: usize,
    applies_finished: usize,
    gets: usize,
    scans: usize,
}

#[derive(Default)]
struct FakeKv {
    state: Mutex<FakeState>,
    apply_delay: Duration,
    apply_gate: Option<Arc<Gate>>,
    read_gate: Option<Arc<Gate>>,
    fail_on: Option<Vec<u8>>,
    panic_on: Option<Vec<u8>>,
    fail_sync: AtomicBool,
}

impl FakeKv {
    fn st(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl BatchSink for FakeKv {
    fn apply(&self, ops: &[OwnedOp], durability: Durability) -> Result<Vec<OpResult>> {
        self.st().applies_started += 1;
        if let Some(g) = &self.apply_gate {
            g.pass();
        }
        if !self.apply_delay.is_zero() {
            thread::sleep(self.apply_delay);
        }
        if let Some(k) = &self.panic_on
            && ops.iter().any(|o| o.key() == k.as_slice())
        {
            panic!("fake sink: injected panic");
        }
        let mut st = self.st();
        if let Some(k) = &self.fail_on
            && ops.iter().any(|o| o.key() == k.as_slice())
        {
            st.applies_finished += 1;
            return Err(Error::Backend("fake sink: injected commit failure".into()));
        }
        let mut out = Vec::with_capacity(ops.len());
        for op in ops {
            let current = st.map.get(op.key()).map(|(r, _)| *r);
            let holds = match op.expect() {
                Expect::Any => true,
                Expect::Absent => current.is_none(),
                Expect::Revision(r) => current == Some(r),
            };
            if !holds {
                out.push(Err(Error::RevisionConflict {
                    key: op.key().to_vec(),
                    expected: format!("{:?}", op.expect()),
                    actual: current,
                }));
                continue;
            }
            match op {
                OwnedOp::Put { key, value, .. } => {
                    st.last_rev += 1;
                    let rev = st.last_rev;
                    st.map.insert(key.clone(), (rev, value.clone()));
                    out.push(Ok(Some(rev)));
                }
                OwnedOp::Delete { key, .. } => {
                    if st.map.remove(key).is_some() {
                        st.last_rev += 1;
                        out.push(Ok(Some(st.last_rev)));
                    } else {
                        out.push(Ok(None));
                    }
                }
            }
        }
        st.batches.push((ops.len(), durability));
        if durability == Durability::Immediate {
            st.durable_rev = st.last_rev;
        }
        st.applies_finished += 1;
        Ok(out)
    }

    fn sync(&self) -> Result<()> {
        if self.fail_sync.load(Ordering::SeqCst) {
            return Err(Error::Io(std::io::Error::other(
                "fake sink: injected sync failure",
            )));
        }
        let mut st = self.st();
        st.syncs += 1;
        st.durable_rev = st.last_rev;
        Ok(())
    }
}

fn slice_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn empty_range(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => {
            s > e
                || (s == e
                    && !(matches!(start, Bound::Included(_)) && matches!(end, Bound::Included(_))))
        }
        _ => false,
    }
}

impl ReadSource for FakeKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.st().gets += 1;
        if let Some(g) = &self.read_gate {
            g.pass();
        }
        if key == b"bad" {
            return Err(Error::Backend("fake source: injected read failure".into()));
        }
        if key == b"panic" {
            panic!("fake source: injected read panic");
        }
        Ok(self.st().map.get(key).map(|(_, v)| v.clone()))
    }

    fn scan(&self, opts: &ScanOptions) -> Result<Vec<ScanItem>> {
        self.st().scans += 1;
        if let Some(g) = &self.read_gate {
            g.pass();
        }
        let st = self.st();
        if empty_range(&opts.start, &opts.end) {
            return Ok(Vec::new());
        }
        let range = st
            .map
            .range::<[u8], _>((slice_bound(&opts.start), slice_bound(&opts.end)));
        let items: Box<dyn Iterator<Item = (&Vec<u8>, &Entry)>> = if opts.reverse {
            Box::new(range.rev())
        } else {
            Box::new(range)
        };
        let limit = if opts.limit == 0 {
            usize::MAX
        } else {
            opts.limit
        };
        Ok(items
            .take(limit)
            .map(|(k, (rev, v))| ScanItem {
                key: k.clone(),
                revision: *rev,
                logical_len: v.len() as u64,
                value: opts.with_values.then(|| v.clone()),
            })
            .collect())
    }
}

impl ShardBackend for FakeKv {
    fn head(&self, key: &[u8]) -> Result<Option<(Revision, u64)>> {
        Ok(self.st().map.get(key).map(|(r, v)| (*r, v.len() as u64)))
    }

    fn get_range(&self, key: &[u8], offset: u64, len: u64) -> Result<Option<Vec<u8>>> {
        Ok(self.st().map.get(key).map(|(_, v)| {
            let start = (offset as usize).min(v.len());
            let end = start.saturating_add(len as usize).min(v.len());
            v[start..end].to_vec()
        }))
    }
}

fn committer(sink: &Arc<FakeKv>, cfg: GroupCommitConfig) -> Arc<GroupCommitter> {
    Arc::new(GroupCommitter::new(sink.clone(), cfg).unwrap())
}

fn buffered(interval: Duration, max_pending_bytes: usize) -> GroupCommitConfig {
    GroupCommitConfig::from(WriteDurability::Buffered {
        flush_interval: interval,
        max_pending_bytes,
    })
}

// ---------------------------------------------------------------------------
// Group commit
// ---------------------------------------------------------------------------

#[test]
fn group_commit_batches_concurrent_submitters() {
    let sink = Arc::new(FakeKv {
        apply_delay: Duration::from_millis(5),
        ..FakeKv::default()
    });
    let c = committer(&sink, GroupCommitConfig::default());
    let threads: Vec<_> = (0..16)
        .map(|t| {
            let c = c.clone();
            thread::spawn(move || {
                for i in 0..25 {
                    c.put(
                        format!("t{t}-{i}").into_bytes(),
                        b"v".as_slice(),
                        Expect::Absent,
                    )
                    .unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let stats = c.stats();
    assert_eq!(stats.ops, 400);
    assert!(
        stats.batches < 200,
        "expected grouping, got {} commits for 400 puts",
        stats.batches
    );
    assert!(stats.max_batch_ops > 1);
    let st = sink.st();
    assert_eq!(st.batches.iter().map(|b| b.0).sum::<usize>(), 400);
    assert!(st.batches.iter().all(|b| b.1 == Durability::Immediate));
    assert_eq!(st.map.len(), 400);
}

#[test]
fn per_op_results_reach_the_right_caller() {
    let sink = Arc::new(FakeKv {
        apply_delay: Duration::from_millis(2),
        ..FakeKv::default()
    });
    let c = committer(&sink, GroupCommitConfig::default());
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let c = c.clone();
            thread::spawn(move || {
                let mut mine = Vec::new();
                for i in 0..20 {
                    let (a, b) = (format!("{t}/{i}/a"), format!("{t}/{i}/b"));
                    let r = c
                        .apply(vec![
                            OwnedOp::put(a.as_bytes(), a.as_bytes(), Expect::Any),
                            OwnedOp::put(b.as_bytes(), b.as_bytes(), Expect::Any),
                            OwnedOp::delete(format!("{t}/missing").into_bytes(), Expect::Any),
                        ])
                        .unwrap();
                    assert_eq!(r.len(), 3);
                    assert!(matches!(r[2], Ok(None)));
                    mine.push((a, r[0].as_ref().unwrap().unwrap()));
                    mine.push((b, r[1].as_ref().unwrap().unwrap()));
                }
                mine
            })
        })
        .collect();
    let all: Vec<(String, Revision)> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    let st = sink.st();
    for (key, rev) in all {
        let (stored_rev, value) = &st.map[key.as_bytes()];
        assert_eq!(
            *stored_rev, rev,
            "revision of {key} went to the wrong caller"
        );
        assert_eq!(value.as_slice(), key.as_bytes());
    }
}

#[test]
fn failing_expectation_only_fails_its_op() {
    let gate = Arc::new(Gate::default());
    let sink = Arc::new(FakeKv {
        apply_gate: Some(gate.clone()),
        ..FakeKv::default()
    });
    let c = committer(&sink, GroupCommitConfig::default());
    let first = c
        .submit(vec![OwnedOp::put(
            b"a".as_slice(),
            b"1".as_slice(),
            Expect::Absent,
        )])
        .unwrap();
    wait_until("first batch in apply", || sink.st().applies_started == 1);
    // These three queue up behind the blocked commit and form one batch.
    let b1 = c
        .submit(vec![OwnedOp::put(
            b"x".as_slice(),
            b"1".as_slice(),
            Expect::Absent,
        )])
        .unwrap();
    let b2 = c
        .submit(vec![OwnedOp::put(
            b"x".as_slice(),
            b"2".as_slice(),
            Expect::Absent,
        )])
        .unwrap();
    let b3 = c
        .submit(vec![OwnedOp::put(
            b"y".as_slice(),
            b"1".as_slice(),
            Expect::Absent,
        )])
        .unwrap();
    assert_eq!(c.stats().queued_ops, 3);
    gate.open();
    assert!(first.wait().unwrap()[0].is_ok());
    assert!(b1.wait().unwrap()[0].is_ok());
    assert!(matches!(
        b2.wait().unwrap()[0],
        Err(Error::RevisionConflict { .. })
    ));
    assert!(b3.wait().unwrap()[0].is_ok());
    let st = sink.st();
    assert_eq!(
        st.batches,
        vec![(1, Durability::Immediate), (3, Durability::Immediate)]
    );
    assert_eq!(st.map[b"x".as_slice()].1, b"1");
}

#[test]
fn immediate_mode_returns_only_after_apply() {
    let sink = Arc::new(FakeKv {
        apply_delay: Duration::from_millis(30),
        ..FakeKv::default()
    });
    let c = committer(&sink, GroupCommitConfig::default());
    let rev = c
        .put(b"k".as_slice(), b"v".as_slice(), Expect::Any)
        .unwrap();
    let st = sink.st();
    assert_eq!(
        st.applies_finished, 1,
        "put returned before the commit finished"
    );
    assert!(st.durable_rev >= rev);
    assert_eq!(st.batches[0].1, Durability::Immediate);
}

#[test]
fn buffered_mode_is_visible_but_not_durable_until_flush() {
    let sink = Arc::new(FakeKv::default());
    let c = committer(&sink, buffered(Duration::from_secs(3600), 1 << 30));
    let rev = c
        .put(b"k".as_slice(), b"v".as_slice(), Expect::Any)
        .unwrap();
    {
        let st = sink.st();
        assert_eq!(st.batches[0].1, Durability::Deferred);
        assert!(
            st.map.contains_key(b"k".as_slice()),
            "acknowledged write must be visible"
        );
        assert!(
            st.durable_rev < rev,
            "buffered write must not be durable yet"
        );
    }
    assert!(c.stats().unsynced_bytes > 0);
    c.flush().unwrap();
    let st = sink.st();
    assert!(st.durable_rev >= rev);
    assert_eq!(st.syncs, 1);
    drop(st);
    assert_eq!(c.stats().unsynced_bytes, 0);
    // Nothing pending: a second flush does not sync again.
    c.flush().unwrap();
    assert_eq!(sink.st().syncs, 1);
}

#[test]
fn buffered_mode_syncs_on_its_interval() {
    let sink = Arc::new(FakeKv::default());
    let c = committer(&sink, buffered(Duration::from_millis(30), 1 << 30));
    let rev = c
        .put(b"k".as_slice(), b"v".as_slice(), Expect::Any)
        .unwrap();
    wait_until("periodic sync", || sink.st().syncs >= 1);
    assert!(sink.st().durable_rev >= rev);
    assert_eq!(c.stats().syncs, 1);
}

#[test]
fn buffered_mode_bounds_pending_bytes() {
    let sink = Arc::new(FakeKv::default());
    let c = committer(&sink, buffered(Duration::from_secs(3600), 1000));
    let value = vec![7u8; 400];
    for i in 0..3u8 {
        c.put(vec![i], value.clone(), Expect::Any).unwrap();
        assert!(c.stats().unsynced_bytes <= 1000);
    }
    let st = sink.st();
    // 401 + 401 stay buffered; the third would cross 1000 bytes and is
    // committed durably instead (no separate sync needed).
    let kinds: Vec<Durability> = st.batches.iter().map(|b| b.1).collect();
    assert_eq!(
        kinds,
        [
            Durability::Deferred,
            Durability::Deferred,
            Durability::Immediate
        ]
    );
    assert_eq!(st.durable_rev, st.last_rev);
    assert_eq!(st.syncs, 0);
}

#[test]
fn failed_sync_forces_the_next_commit_to_be_durable() {
    let sink = Arc::new(FakeKv::default());
    let c = committer(&sink, buffered(Duration::from_secs(3600), 1 << 30));
    c.put(b"a".as_slice(), b"1".as_slice(), Expect::Any)
        .unwrap();
    sink.fail_sync.store(true, Ordering::SeqCst);
    assert!(matches!(c.flush(), Err(Error::Io(_))));
    sink.fail_sync.store(false, Ordering::SeqCst);
    let rev = c
        .put(b"b".as_slice(), b"2".as_slice(), Expect::Any)
        .unwrap();
    let st = sink.st();
    assert_eq!(st.batches.last().unwrap().1, Durability::Immediate);
    assert!(st.durable_rev >= rev);
    drop(st);
    assert_eq!(c.stats().failed_syncs, 1);
}

#[test]
fn backpressure_blocks_and_try_submit_hands_ops_back() {
    let gate = Arc::new(Gate::default());
    let sink = Arc::new(FakeKv {
        apply_gate: Some(gate.clone()),
        ..FakeKv::default()
    });
    let cfg = GroupCommitConfig {
        queue_max_ops: 4,
        max_batch_ops: 1,
        ..GroupCommitConfig::default()
    };
    let c = committer(&sink, cfg);
    let put = |i: u8| vec![OwnedOp::put(vec![i], b"v".as_slice(), Expect::Any)];
    let mut tickets = vec![c.submit(put(0)).unwrap()];
    wait_until("first batch in apply", || sink.st().applies_started == 1);
    for i in 1..=4 {
        tickets.push(c.submit(put(i)).unwrap());
    }
    match c.try_submit(put(5)) {
        Err(TrySubmitError::Full(ops)) => assert_eq!(ops, put(5)),
        other => panic!("expected Full, got {other:?}"),
    }
    let blocked = {
        let c = c.clone();
        thread::spawn(move || {
            c.submit(vec![OwnedOp::put(vec![6], b"v".as_slice(), Expect::Any)])
                .map(|t| t.wait())
        })
    };
    wait_until("submitter blocked on a full queue", || {
        c.stats().blocked_submits == 1
    });
    thread::sleep(Duration::from_millis(20));
    assert!(
        !blocked.is_finished(),
        "submit must block while the queue is full"
    );
    gate.open();
    let late = blocked.join().unwrap().unwrap().unwrap();
    assert!(late[0].is_ok());
    for t in tickets {
        assert!(t.wait().unwrap()[0].is_ok());
    }
    let keys: Vec<Vec<u8>> = sink.st().map.keys().cloned().collect();
    assert_eq!(
        keys,
        vec![vec![0], vec![1], vec![2], vec![3], vec![4], vec![6]]
    );
}

#[test]
fn shutdown_drains_the_queue_and_rejects_new_work() {
    let gate = Arc::new(Gate::default());
    let sink = Arc::new(FakeKv {
        apply_gate: Some(gate.clone()),
        ..FakeKv::default()
    });
    let c = committer(&sink, buffered(Duration::from_secs(3600), 1 << 30));
    let tickets: Vec<_> = (0..10u8)
        .map(|i| {
            c.submit(vec![OwnedOp::put(vec![i], b"v".as_slice(), Expect::Any)])
                .unwrap()
        })
        .collect();
    c.close();
    assert!(
        c.submit(vec![OwnedOp::put(
            b"late".as_slice(),
            b"v".as_slice(),
            Expect::Any
        )])
        .is_err()
    );
    assert!(matches!(
        c.try_submit(vec![OwnedOp::put(
            b"late".as_slice(),
            b"v".as_slice(),
            Expect::Any
        )]),
        Err(TrySubmitError::Closed(_))
    ));
    gate.open();
    let c2 = c.clone();
    within(10, move || c2.shutdown()).unwrap();
    for t in tickets {
        assert!(t.wait().unwrap()[0].is_ok());
    }
    let st = sink.st();
    assert_eq!(st.map.len(), 10);
    assert_eq!(
        st.durable_rev, st.last_rev,
        "shutdown must leave buffered writes durable"
    );
    drop(st);
    assert!(!c.is_running());
    assert!(c.shutdown().is_ok(), "shutdown is idempotent");
    assert!(c.flush().is_err());
}

#[test]
fn sink_error_fails_every_waiter_of_that_batch_only() {
    let gate = Arc::new(Gate::default());
    let sink = Arc::new(FakeKv {
        apply_gate: Some(gate.clone()),
        fail_on: Some(b"poison".to_vec()),
        ..FakeKv::default()
    });
    let c = committer(&sink, GroupCommitConfig::default());
    let put = |k: &[u8]| vec![OwnedOp::put(k, b"v".as_slice(), Expect::Any)];
    let a = c.submit(put(b"a")).unwrap();
    wait_until("first batch in apply", || sink.st().applies_started == 1);
    let batch2: Vec<_> = [b"b".as_slice(), b"poison", b"c"]
        .iter()
        .map(|k| c.submit(put(k)).unwrap())
        .collect();
    gate.open();
    assert!(a.wait().unwrap()[0].is_ok());
    for t in batch2 {
        assert!(matches!(t.wait(), Err(Error::Backend(_))));
    }
    assert!(c.put(b"d".as_slice(), b"v".as_slice(), Expect::Any).is_ok());
    let keys: Vec<Vec<u8>> = sink.st().map.keys().cloned().collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"d".to_vec()]);
    assert_eq!(c.stats().failed_batches, 1);
}

#[test]
fn writer_panic_fails_callers_without_hanging() {
    let gate = Arc::new(Gate::default());
    let sink = Arc::new(FakeKv {
        apply_gate: Some(gate.clone()),
        panic_on: Some(b"boom".to_vec()),
        ..FakeKv::default()
    });
    let c = committer(&sink, GroupCommitConfig::default());
    let put = |k: &[u8]| vec![OwnedOp::put(k, b"v".as_slice(), Expect::Any)];
    let boom = c.submit(put(b"boom")).unwrap();
    wait_until("panicking batch in apply", || {
        sink.st().applies_started == 1
    });
    let queued = c.submit(put(b"x")).unwrap();
    gate.open();
    let results = within(10, move || (boom.wait(), queued.wait()));
    assert!(results.0.is_err(), "in-flight request must fail");
    assert!(results.1.is_err(), "queued request must fail");
    let c2 = c.clone();
    let later = within(10, move || {
        c2.put(b"y".as_slice(), b"v".as_slice(), Expect::Any)
    });
    assert!(later.is_err());
    match c.shutdown() {
        Err(Error::Backend(msg)) => assert!(msg.contains("panicked"), "{msg}"),
        other => panic!("expected a panic report, got {other:?}"),
    }
}

#[test]
fn submission_order_is_preserved() {
    let sink = Arc::new(FakeKv::default());
    let c = committer(
        &sink,
        GroupCommitConfig {
            max_batch_ops: 7,
            ..GroupCommitConfig::default()
        },
    );
    let tickets: Vec<_> = (0..500u32)
        .map(|i| {
            c.submit(vec![OwnedOp::put(
                b"k".as_slice(),
                i.to_be_bytes(),
                Expect::Any,
            )])
            .unwrap()
        })
        .collect();
    let revs: Vec<Revision> = tickets
        .into_iter()
        .map(|t| t.wait().unwrap()[0].as_ref().unwrap().unwrap())
        .collect();
    assert!(revs.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(sink.st().map[b"k".as_slice()].1, 499u32.to_be_bytes());
    assert!(sink.st().batches.iter().all(|b| b.0 <= 7));
}

// ---------------------------------------------------------------------------
// Routing and sharding
// ---------------------------------------------------------------------------

#[test]
fn router_is_deterministic_and_spreads_channels() {
    let (a, b) = (Router::FirstBytes(8), Router::FirstBytes(8));
    for i in 0..1000u64 {
        let key = message_key(i, i * 7);
        assert_eq!(a.shard_of(&key, 16), b.shard_of(&key, 16));
    }
    // Golden values (independent Python implementation of the documented hash).
    assert_eq!(a.shard_of(&message_key(0, 99), 16), 7);
    assert_eq!(a.shard_of(&message_key(1, 5), 16), 0);
    assert_eq!(shard_index(stable_hash(&0u64.to_be_bytes()), 8), 3);

    let clock = Arc::new(AtomicU64::new(1_700_000_000_000));
    let ids = {
        let clock = clock.clone();
        Snowflake::with_clock(1, 2, move || clock.fetch_add(1, Ordering::Relaxed)).unwrap()
    };
    for (name, channels) in [
        ("sequential ids", (0..100_000u64).collect::<Vec<_>>()),
        (
            "snowflake ids",
            (0..100_000).map(|_| ids.next_id().unwrap()).collect(),
        ),
    ] {
        let mut counts = [0usize; 8];
        for ch in channels {
            counts[a.shard_of(&message_key(ch, 0), 8)] += 1;
        }
        for c in counts {
            assert!(
                (11_875..=13_125).contains(&c),
                "{name}: uneven shard counts {counts:?}"
            );
        }
    }
}

fn model_scan(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    prefix: &[u8],
    reverse: bool,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .filter(|(k, _)| k.starts_with(prefix))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if reverse {
        v.reverse();
    }
    if limit != 0 {
        v.truncate(limit);
    }
    v
}

#[test]
fn sharded_scans_match_a_model_and_route_when_possible() {
    let db = ShardedDb::new(
        (0..4).map(|_| FakeKv::default()).collect(),
        Router::FirstBytes(2),
        GroupCommitConfig::default(),
    )
    .unwrap();
    let mut model = BTreeMap::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for i in 0..400u32 {
        let len = 1 + (rng.next() % 6) as usize;
        let key: Vec<u8> = (0..len)
            .map(|_| b"abc\xff"[(rng.next() % 4) as usize])
            .collect();
        let value = i.to_le_bytes().to_vec();
        db.put(&key, &value, Expect::Any).unwrap();
        model.insert(key, value);
    }
    let per_shard: Vec<usize> = (0..4)
        .map(|i| db.shard(i).unwrap().st().map.len())
        .collect();
    assert_eq!(per_shard.iter().sum::<usize>(), model.len());
    assert!(
        per_shard.iter().filter(|n| **n > 0).count() > 1,
        "keys should spread: {per_shard:?}"
    );
    for prefix in [
        &b""[..],
        b"a",
        b"\xff",
        b"ab",
        b"\xff\xff",
        b"abc",
        b"ca\xff",
    ] {
        for reverse in [false, true] {
            for limit in [0, 1, 7, 1000] {
                let got: Vec<(Vec<u8>, Vec<u8>)> = db
                    .scan_prefix(prefix, reverse, limit, true)
                    .unwrap()
                    .into_iter()
                    .map(|it| (it.key, it.value.unwrap()))
                    .collect();
                assert_eq!(
                    got,
                    model_scan(&model, prefix, reverse, limit),
                    "prefix {prefix:?} rev {reverse} limit {limit}"
                );
            }
        }
        let route = db.scan_route(&ScanOptions::prefix(prefix));
        if prefix.len() >= 2 {
            let expected = db.shard_for_key(prefix);
            assert_eq!(route, ScanRoute::Single(expected));
            let before: Vec<usize> = (0..4).map(|i| db.shard(i).unwrap().st().scans).collect();
            db.scan_prefix(prefix, true, 3, false).unwrap();
            for (i, scans_before) in before.into_iter().enumerate() {
                let scans = db.shard(i).unwrap().st().scans;
                assert_eq!(
                    scans - scans_before,
                    usize::from(i == expected),
                    "routed scan touched shard {i}"
                );
            }
        } else {
            assert_eq!(route, ScanRoute::FanOut);
        }
    }
    for key in model.keys().take(50) {
        assert_eq!(db.get(key).unwrap().as_ref(), model.get(key));
        assert_eq!(db.head(key).unwrap().map(|h| h.1), Some(4));
        assert_eq!(
            db.get_range(key, 1, 2).unwrap(),
            Some(model[key][1..3].to_vec())
        );
    }
}

#[test]
fn sharded_write_batch_reports_per_op_in_input_order() {
    let db = ShardedDb::new(
        (0..3).map(|_| FakeKv::default()).collect(),
        Router::FirstBytes(1),
        GroupCommitConfig::default(),
    )
    .unwrap();
    db.put(b"b-existing", b"0", Expect::Any).unwrap();
    let results = db.write_owned(vec![
        OwnedOp::put(b"a1".as_slice(), b"1".as_slice(), Expect::Absent),
        OwnedOp::put(b"b-existing".as_slice(), b"2".as_slice(), Expect::Absent),
        OwnedOp::put(b"c1".as_slice(), b"3".as_slice(), Expect::Any),
        OwnedOp::delete(b"zz".as_slice(), Expect::Any),
        OwnedOp::delete(b"a1".as_slice(), Expect::Any),
    ]);
    assert!(matches!(results[0], Ok(Some(_))));
    assert!(matches!(results[1], Err(Error::RevisionConflict { .. })));
    assert!(matches!(results[2], Ok(Some(_))));
    assert!(matches!(results[3], Ok(None)));
    assert!(
        matches!(results[4], Ok(Some(_))),
        "same-shard ops apply in input order"
    );
    assert_eq!(db.get(b"b-existing").unwrap(), Some(b"0".to_vec()));
    assert_eq!(db.get(b"a1").unwrap(), None);
    assert!(db.delete(b"c1", Expect::Any).unwrap());
    db.flush().unwrap();
    db.shutdown().unwrap();
    assert!(
        db.put(b"x", b"y", Expect::Any).is_err(),
        "writes fail after shutdown"
    );
    assert_eq!(
        db.get(b"b-existing").unwrap(),
        Some(b"0".to_vec()),
        "reads keep working"
    );
}

#[test]
fn shard_meta_is_checked_before_touching_shards() {
    let dir = tempfile::tempdir().unwrap();
    ShardMeta::new(4, &Router::FirstBytes(8))
        .write(dir.path())
        .unwrap();
    assert_eq!(
        ShardMeta::read(dir.path()).unwrap(),
        Some(ShardMeta::new(4, &Router::FirstBytes(8)))
    );
    let wrong_count = ShardedDb::open(
        dir.path(),
        8,
        Default::default(),
        Router::FirstBytes(8),
        WriteDurability::Immediate,
    );
    assert!(matches!(wrong_count, Err(Error::InvalidArgument(_))));
    let wrong_router = ShardedDb::open(
        dir.path(),
        4,
        Default::default(),
        Router::FirstBytes(4),
        WriteDurability::Immediate,
    );
    assert!(matches!(wrong_router, Err(Error::InvalidArgument(_))));

    let orphan = tempfile::tempdir().unwrap();
    std::fs::write(orphan.path().join(shard_file_name(0)), b"").unwrap();
    let no_meta = ShardedDb::open(
        orphan.path(),
        1,
        Default::default(),
        Router::FirstBytes(8),
        WriteDurability::Immediate,
    );
    assert!(matches!(no_meta, Err(Error::Format(_))));
}

// ---------------------------------------------------------------------------
// Coalescing
// ---------------------------------------------------------------------------

#[test]
fn coalescer_shares_one_read_among_concurrent_callers() {
    let gate = Arc::new(Gate::default());
    let source = Arc::new(FakeKv {
        read_gate: Some(gate.clone()),
        ..FakeKv::default()
    });
    source
        .st()
        .map
        .insert(b"hot".to_vec(), (1, b"value".to_vec()));
    let co = Arc::new(Coalescer::new(source.clone(), Router::FirstBytes(8)));
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let co = co.clone();
            thread::spawn(move || co.get(b"hot").unwrap())
        })
        .collect();
    wait_until("7 callers joined the leader's read", || {
        co.stats().joined == 7
    });
    gate.open();
    let values: Vec<_> = readers.into_iter().map(|r| r.join().unwrap()).collect();
    assert!(
        values
            .iter()
            .all(|v| v.as_deref() == Some(b"value".as_slice()))
    );
    assert!(
        values.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])),
        "followers share the leader's Arc"
    );
    assert_eq!(source.st().gets, 1);
    // Nothing is cached after completion.
    co.get(b"hot").unwrap();
    assert_eq!(source.st().gets, 2);
    assert_eq!(co.stats().source_reads, 2);

    // Scans coalesce on (prefix, limit, reverse).
    let gate2 = Arc::new(Gate::default());
    let source2 = Arc::new(FakeKv {
        read_gate: Some(gate2.clone()),
        ..FakeKv::default()
    });
    for i in 0..10u64 {
        source2
            .st()
            .map
            .insert(message_key(5, i).to_vec(), (i + 1, vec![i as u8]));
    }
    let co2 = Arc::new(Coalescer::new(source2.clone(), Router::FirstBytes(8)));
    let scanners: Vec<_> = (0..4)
        .map(|_| {
            let co2 = co2.clone();
            thread::spawn(move || co2.scan_prefix(&5u64.to_be_bytes(), 3, true).unwrap())
        })
        .collect();
    wait_until("3 scans joined", || co2.stats().joined == 3);
    gate2.open();
    for s in scanners {
        let page = s.join().unwrap();
        let ids: Vec<u64> = page
            .iter()
            .map(|it| parse_message_key(&it.key).unwrap().1)
            .collect();
        assert_eq!(ids, [9, 8, 7]);
    }
    assert_eq!(source2.st().scans, 1);
}

#[test]
fn coalescer_never_joins_a_read_older_than_a_noted_write() {
    let gate = Arc::new(Gate::default());
    let source = Arc::new(FakeKv {
        read_gate: Some(gate.clone()),
        ..FakeKv::default()
    });
    let co = Arc::new(Coalescer::new(source.clone(), Router::FirstBytes(8)));
    let first = {
        let co = co.clone();
        thread::spawn(move || co.get(b"key").unwrap())
    };
    wait_until("leader in the source", || source.st().gets == 1);
    // A write is acknowledged after the leader's snapshot may have been taken.
    source
        .st()
        .map
        .insert(b"key".to_vec(), (1, b"new".to_vec()));
    co.note_write(b"key");
    let second = {
        let co = co.clone();
        thread::spawn(move || co.get(b"key").unwrap())
    };
    wait_until("second caller issued its own read", || {
        source.st().gets == 2
    });
    gate.open();
    first.join().unwrap();
    assert_eq!(second.join().unwrap().as_deref(), Some(b"new".as_slice()));
    assert_eq!(co.stats().joined, 0);
}

#[test]
fn coalescer_shares_errors_and_survives_a_panicking_leader() {
    for key in [b"bad".as_slice(), b"panic".as_slice()] {
        let gate = Arc::new(Gate::default());
        let source = Arc::new(FakeKv {
            read_gate: Some(gate.clone()),
            ..FakeKv::default()
        });
        let co = Arc::new(Coalescer::new(source.clone(), Router::FirstBytes(8)));
        let leader = {
            let co = co.clone();
            thread::spawn(move || co.get(key).map(|_| ()))
        };
        wait_until("leader in the source", || source.st().gets == 1);
        let follower = {
            let co = co.clone();
            thread::spawn(move || co.get(key).map(|_| ()))
        };
        wait_until("follower joined", || co.stats().joined == 1);
        gate.open();
        let follower_result = within(10, move || follower.join().unwrap());
        assert!(
            follower_result.is_err(),
            "{key:?}: follower must get an error"
        );
        let leader_result = leader.join();
        if key == b"bad" {
            assert!(leader_result.unwrap().is_err());
        } else {
            assert!(
                leader_result.is_err(),
                "the leader's panic propagates to the leader only"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Snowflakes and keys
// ---------------------------------------------------------------------------

#[test]
fn snowflakes_are_unique_and_monotonic_across_threads() {
    let ids = Arc::new(Snowflake::new(3, 17).unwrap());
    let start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let ids = ids.clone();
            thread::spawn(move || {
                (0..20_000)
                    .map(|_| ids.next_id().unwrap())
                    .collect::<Vec<u64>>()
            })
        })
        .collect();
    let per_thread: Vec<Vec<u64>> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut all = HashSet::new();
    for seq in &per_thread {
        assert!(seq.windows(2).all(|w| w[0] < w[1]));
        for id in seq {
            assert!(all.insert(*id), "duplicate id {id}");
            let p = SnowflakeParts::decode(*id);
            assert_eq!((p.worker_id, p.process_id), (3, 17));
            // Bursts above 4096 ids/ms borrow up to 160000/4096 ms ahead.
            assert!(
                p.timestamp_ms >= start && p.timestamp_ms <= end + 100,
                "{p:?}"
            );
        }
    }
    assert_eq!(all.len(), 160_000);
}

#[test]
fn snowflakes_stay_monotonic_when_the_clock_misbehaves() {
    let now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let ids = {
        let now = now.clone();
        Snowflake::with_clock(0, 0, move || now.load(Ordering::SeqCst)).unwrap()
    };
    let a = ids.next_id().unwrap();
    now.store(1_600_000_000_000, Ordering::SeqCst); // backwards
    let b = ids.next_id().unwrap();
    assert!(b > a);
    assert_eq!(SnowflakeParts::decode(b).timestamp_ms, 1_700_000_000_000);
    // Frozen clock: 10 000 ids in "one" millisecond carry into the next ones.
    now.store(1_800_000_000_000, Ordering::SeqCst);
    let burst: Vec<u64> = (0..10_000).map(|_| ids.next_id().unwrap()).collect();
    assert!(burst.windows(2).all(|w| w[0] < w[1]));
    let last = SnowflakeParts::decode(*burst.last().unwrap());
    assert_eq!(last.timestamp_ms, 1_800_000_000_002);
    assert_eq!(SnowflakeParts::decode(burst[0]).increment, 0);
}

#[test]
fn message_keys_sort_like_channel_then_message() {
    let mut rng = Rng(42);
    let mut pairs: Vec<(u64, u64)> = (0..2000).map(|_| (rng.next() % 50, rng.next())).collect();
    pairs.extend([
        (0, 0),
        (0, u64::MAX),
        (u64::MAX, 0),
        (u64::MAX, u64::MAX),
        (1, 0),
    ]);
    let mut by_tuple = pairs.clone();
    by_tuple.sort();
    let mut by_key = pairs;
    by_key.sort_by_key(|(c, m)| message_key(*c, *m));
    assert_eq!(by_tuple, by_key);
    for (c, m) in by_tuple {
        assert_eq!(parse_message_key(&message_key(c, m)), Some((c, m)));
    }
}

// ---------------------------------------------------------------------------
// Chat store over fake shards
// ---------------------------------------------------------------------------

fn fake_chat(opts: ChatOptions) -> ChatStore<FakeKv> {
    let db = ShardedDb::new(
        (0..4).map(|_| FakeKv::default()).collect(),
        Router::FirstBytes(8),
        GroupCommitConfig::default(),
    )
    .unwrap();
    let clock = AtomicU64::new(1_700_000_000_000);
    let ids = Snowflake::with_clock(opts.worker_id, opts.process_id, move || {
        clock.fetch_add(1, Ordering::Relaxed)
    })
    .unwrap();
    ChatStore::with_ids(db, opts, ids).unwrap()
}

type ChatModel = BTreeMap<(u64, u64), Vec<u8>>;

fn model_latest(
    model: &ChatModel,
    channel: u64,
    before: Option<u64>,
    n: usize,
) -> Vec<(u64, Vec<u8>)> {
    model
        .range((channel, 0)..=(channel, u64::MAX))
        .rev()
        .filter(|((_, id), _)| before.is_none_or(|b| *id < b))
        .take(n)
        .map(|((_, id), p)| (*id, p.clone()))
        .collect()
}

#[test]
fn chat_store_matches_a_model_in_every_configuration() {
    for (coalesce, cache) in [(true, 1 << 20), (false, 0), (true, 0), (false, 1 << 20)] {
        let opts = ChatOptions {
            coalesce_reads: coalesce,
            recent_cache_bytes: cache,
            recent_page_len: 60,
            ..Default::default()
        };
        let chat = fake_chat(opts);
        let mut model = ChatModel::new();
        let check = |model: &ChatModel| {
            for ch in [7u64, 8, 99] {
                for n in [0usize, 1, 10, 50, 60, 200] {
                    assert_eq!(
                        chat.latest(ch, n).unwrap(),
                        model_latest(model, ch, None, n),
                        "latest({ch},{n})"
                    );
                }
            }
        };
        for i in 0..150u32 {
            let ch = if i % 5 == 4 { 8 } else { 7 };
            let payload = [&i.to_le_bytes()[..], b"\x00\xff exact bytes"].concat();
            let id = chat.send(ch, &payload).unwrap();
            model.insert((ch, id), payload);
            if i % 37 == 0 {
                check(&model);
            }
        }
        check(&model);
        // Pagination.
        let page1 = chat.latest(7, 50).unwrap();
        let oldest = page1.last().unwrap().0;
        assert_eq!(
            chat.before(7, oldest, 50).unwrap(),
            model_latest(&model, 7, Some(oldest), 50)
        );
        assert_eq!(chat.before(7, 0, 50).unwrap(), Vec::new());
        // Edit inside and outside the cached window.
        for (ch, id) in [
            (7u64, page1[3].0),
            (7, model_latest(&model, 7, None, 100)[90].0),
        ] {
            assert!(chat.edit(ch, id, b"edited").unwrap());
            model.insert((ch, id), b"edited".to_vec());
            assert_eq!(chat.get(ch, id).unwrap(), Some(b"edited".to_vec()));
        }
        assert!(!chat.edit(7, 12345, b"nope").unwrap());
        check(&model);
        // Delete the newest and one in the middle.
        for id in [page1[0].0, page1[20].0] {
            assert!(chat.delete(7, id).unwrap());
            model.remove(&(7, id));
            assert!(!chat.delete(7, id).unwrap());
        }
        check(&model);
        // Explicit ids (imports) interleave correctly, duplicates are rejected.
        chat.insert(7, page1[20].0, b"reinserted").unwrap();
        model.insert((7, page1[20].0), b"reinserted".to_vec());
        assert!(matches!(
            chat.insert(7, page1[20].0, b"dup"),
            Err(Error::RevisionConflict { .. })
        ));
        check(&model);
        let stats = chat.stats();
        if cache > 0 {
            assert!(
                stats.recent.hits > 0,
                "cache should serve repeated reads: {stats:?}"
            );
        } else {
            assert_eq!(stats.recent.hits, 0);
        }
        chat.flush().unwrap();
        chat.shutdown().unwrap();
    }
}

#[test]
fn chat_readers_always_see_their_own_messages() {
    let opts = ChatOptions {
        recent_page_len: 50,
        ..Default::default()
    };
    let chat = Arc::new(fake_chat(opts));
    let threads: Vec<_> = (0..6)
        .map(|t| {
            let chat = chat.clone();
            thread::spawn(move || {
                for i in 0..150u32 {
                    let ch = u64::from(i % 3);
                    let id = chat.send(ch, format!("{t}:{i}").as_bytes()).unwrap();
                    let page = chat.latest(ch, 50).unwrap();
                    assert!(
                        page.windows(2).all(|w| w[0].0 > w[1].0),
                        "page must be newest first"
                    );
                    let pushed_out = page.len() == 50 && page.last().unwrap().0 > id;
                    assert!(
                        pushed_out
                            || page
                                .iter()
                                .any(|(m, p)| *m == id && p == format!("{t}:{i}").as_bytes()),
                        "thread {t} did not read its own message {id}"
                    );
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let stats = chat.stats();
    assert!(stats.recent.hits + stats.recent.misses > 0);
}

// ---------------------------------------------------------------------------
// Engine-backed tests (real engine, MemStore and redb)
// ---------------------------------------------------------------------------

mod engine {
    use super::*;
    use babeldb::{Config, Db, MemStore};

    #[test]
    fn engine_group_commit_over_mem_db() {
        let db = Arc::new(Db::with_store(MemStore::new(), Config::default()).unwrap());
        let c = Arc::new(GroupCommitter::new(db.clone(), GroupCommitConfig::default()).unwrap());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let c = c.clone();
                thread::spawn(move || {
                    for i in 0..50u32 {
                        c.put(
                            format!("{t}/{i}").into_bytes(),
                            i.to_le_bytes(),
                            Expect::Absent,
                        )
                        .unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(db.get(b"3/7").unwrap(), Some(7u32.to_le_bytes().to_vec()));
        let r = c
            .apply(vec![
                OwnedOp::put(b"3/7".as_slice(), b"x".as_slice(), Expect::Absent),
                OwnedOp::put(b"new".as_slice(), b"y".as_slice(), Expect::Absent),
            ])
            .unwrap();
        assert!(r[0].is_err() && r[1].is_ok());
        c.shutdown().unwrap();
    }

    #[test]
    fn engine_sharded_mem_matches_model() {
        let stores = (0..4).map(|_| MemStore::new()).collect();
        let db = ShardedDb::with_stores(
            stores,
            Config::default(),
            Router::FirstBytes(8),
            WriteDurability::buffered(Duration::from_millis(20)),
        )
        .unwrap();
        let mut model = BTreeMap::new();
        for ch in 0..20u64 {
            for m in 0..30u64 {
                let key = message_key(ch, m).to_vec();
                let value = format!("{ch}:{m}").into_bytes();
                db.put(&key, &value, Expect::Absent).unwrap();
                model.insert(key, value);
            }
        }
        db.flush().unwrap();
        for prefix in [&5u64.to_be_bytes()[..], &[0u8; 4][..], &[][..]] {
            let got: Vec<(Vec<u8>, Vec<u8>)> = db
                .scan_prefix(prefix, true, 25, true)
                .unwrap()
                .into_iter()
                .map(|i| (i.key, i.value.unwrap()))
                .collect();
            assert_eq!(got, model_scan(&model, prefix, true, 25));
        }
        let stats = db.stats().unwrap();
        assert_eq!(stats.records, 600);
        db.shutdown().unwrap();
    }

    #[test]
    fn engine_sharded_redb_reopens_and_checks_meta() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = ShardedDb::open(
                dir.path(),
                4,
                Config::default(),
                Router::FirstBytes(8),
                WriteDurability::Immediate,
            )
            .unwrap();
            for ch in 0..40u64 {
                db.put(&message_key(ch, 1), &ch.to_le_bytes(), Expect::Absent)
                    .unwrap();
            }
            let stats = db.stats().unwrap();
            assert_eq!(stats.records, 40);
            assert!(stats.meta_file.is_some());
            db.shutdown().unwrap();
        }
        for i in 0..4 {
            assert!(dir.path().join(shard_file_name(i)).exists());
        }
        let db = ShardedDb::open(
            dir.path(),
            4,
            Config::default(),
            Router::FirstBytes(8),
            WriteDurability::Immediate,
        )
        .unwrap();
        for ch in 0..40u64 {
            assert_eq!(
                db.get(&message_key(ch, 1)).unwrap(),
                Some(ch.to_le_bytes().to_vec())
            );
        }
        db.shutdown().unwrap();
        drop(db);
        assert!(
            ShardedDb::open(
                dir.path(),
                2,
                Config::default(),
                Router::FirstBytes(8),
                WriteDurability::Immediate
            )
            .is_err()
        );
    }

    #[test]
    fn engine_chat_store_redb_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let ids = {
            let chat = ChatStore::open(
                dir.path(),
                2,
                Config::default(),
                WriteDurability::Immediate,
                ChatOptions::default(),
            )
            .unwrap();
            let ids: Vec<u64> = (0..120)
                .map(|i| chat.send(9, format!("msg {i}").as_bytes()).unwrap())
                .collect();
            let latest = chat.latest(9, 50).unwrap();
            assert_eq!(latest.len(), 50);
            assert_eq!(latest[0], (ids[119], b"msg 119".to_vec()));
            let older = chat.before(9, latest[49].0, 50).unwrap();
            assert_eq!(older[0].0, ids[69]);
            assert!(chat.edit(9, ids[100], b"edited").unwrap());
            assert!(chat.delete(9, ids[119]).unwrap());
            assert_eq!(chat.latest(9, 1).unwrap()[0].0, ids[118]);
            chat.shutdown().unwrap();
            ids
        };
        let chat = ChatStore::open(
            dir.path(),
            2,
            Config::default(),
            WriteDurability::Immediate,
            ChatOptions::default(),
        )
        .unwrap();
        assert_eq!(chat.get(9, ids[100]).unwrap(), Some(b"edited".to_vec()));
        assert_eq!(chat.get(9, ids[119]).unwrap(), None);
        assert_eq!(chat.latest(9, 200).unwrap().len(), 119);
        chat.shutdown().unwrap();
    }
}
