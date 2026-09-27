//! Crash recovery of `Db::open_wal` with a real child process (the pattern of
//! `tests/recovery.rs`).
//!
//! Each test re-invokes this test binary (`std::env::current_exe()`) with
//! `--exact child_process_entry --nocapture --ignored --test-threads=1` and
//! `BABEL_WAL_CHILD_DB`, `BABEL_WAL_CHILD_KIND`, `BABEL_WAL_CHILD_ROUND` set. The
//! child opens the database with a small WAL (128 KiB, so checkpoints and log
//! restarts happen while it runs), writes a deterministic workload and prints
//! one flushed line per acknowledged step. The parent kills it (`Child::kill`:
//! TerminateProcess on Windows, SIGKILL on Unix) at a pseudo-random point,
//! reopens with `open_wal` and checks:
//!
//! - every acknowledged step is present with its exact bytes and the revision
//!   the child was given;
//! - the recovered content is the state after a unit boundary between the
//!   acknowledged prefix and the in-flight unit: an in-flight put, batch or
//!   group is entirely present or entirely absent;
//! - revisions allocated after recovery never reuse a durable one;
//! - `verify(true)` is ok and `gc()` has nothing to collect.
//!
//! Kinds: `puts` (`put`/`delete`, Immediate), `batch` (`write_batch`),
//! `deferred` (`write_batch_each(.., Deferred)`; `VISIBLE` after each deferred
//! commit, `COMMITTED` only after the `sync()` that follows every 4 units: the
//! recovered state must be a prefix of the unit sequence), and `group`: 8
//! threads write disjoint keys through a `GroupCommitter` (one WAL write per
//! batch) and each acknowledges its own steps.
//!
//! IMPORTANT: killing a process is NOT a power-loss test (see tests/recovery.rs).
//!
//! Knobs: `BABEL_WAL_RECOVERY_ROUNDS` (kill/reopen rounds per kind, default 3),
//! `BABEL_RECOVERY_SEED` (fixes the kill points; printed in every failure).

mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use babeldb::config::WalConfig;
use babeldb::engine::WalDb;
use babeldb::maintenance::GcReport;
use babeldb::scale::{GroupCommitConfig, GroupCommitter, WriteDurability};
use babeldb::store::Durability;
use babeldb::{BatchOp, Config, Db, Expect, Mode};
use common::{
    Pattern, Rng, State, assert_state_eq, delete_everything_and_check_no_leaks, env_u64, key_str,
    mix, pattern_bytes, small_config, snapshot, state_diff, temp_dir, values_of, verify_ok,
};

const CHILD_TEST: &str = "child_process_entry";
const ENV_DB: &str = "BABEL_WAL_CHILD_DB";
const ENV_KIND: &str = "BABEL_WAL_CHILD_KIND";
const ENV_ROUND: &str = "BABEL_WAL_CHILD_ROUND";

/// Keys `k000..k047` are rewritten over and over (overwrites, deletes, dedupe).
const KEYSPACE: u64 = 48;
const MAX_STEPS: u64 = 50_000;
const DEFERRED_SYNC_EVERY: u64 = 4;
const GROUP_THREADS: u64 = 8;
/// Keys per group thread.
const GROUP_KEYS: u64 = 6;
const CHILD_TIMEOUT: Duration = Duration::from_secs(240);
const REOPEN_TIMEOUT: Duration = Duration::from_secs(30);

const SIZES: [usize; 16] = [
    0, 1, 17, 63, 64, 65, 200, 511, 512, 513, 1000, 1024, 1500, 2048, 2600, 4101,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Puts,
    Batch,
    Deferred,
    Group,
}

impl Kind {
    const ALL: [Kind; 4] = [Kind::Puts, Kind::Batch, Kind::Deferred, Kind::Group];

    fn name(self) -> &'static str {
        match self {
            Kind::Puts => "puts",
            Kind::Batch => "batch",
            Kind::Deferred => "deferred",
            Kind::Group => "group",
        }
    }

    fn parse(s: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|k| k.name() == s)
    }
}

fn config() -> Config {
    small_config(Mode::Adaptive)
}

/// Small enough that the child checkpoints and restarts the log many times.
fn wal_config() -> WalConfig {
    WalConfig {
        segment_bytes: 128 << 10,
        max_pending_bytes: 16 << 10,
        max_record_bytes: 32 << 10,
        ..WalConfig::default()
    }
}

fn open_wal(path: &Path) -> babeldb::Result<WalDb> {
    Db::open_wal_with(path, config(), wal_config())
}

/// Reopen a database that a killed process had open (Windows may take a
/// moment to release the handles and locks of a terminated process).
#[track_caller]
fn open_wal_retry(path: &Path) -> WalDb {
    let start = Instant::now();
    loop {
        match open_wal(path) {
            Ok(db) => return db,
            Err(_) if start.elapsed() < REOPEN_TIMEOUT => thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!(
                "Db::open_wal({}) still failing after {REOPEN_TIMEOUT:?}: {e}",
                path.display()
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Deterministic workload shared by parent and child
// ---------------------------------------------------------------------------

fn step_key(i: u64) -> Vec<u8> {
    format!("k{:03}", i % KEYSPACE).into_bytes()
}

fn base_value(round: u64, i: u64) -> Vec<u8> {
    let h = mix(round.wrapping_mul(0x9E37) ^ 0xC0FF_EE00, i);
    let len = SIZES[((h >> 16) % SIZES.len() as u64) as usize];
    let pattern = Pattern::ALL[(h % Pattern::ALL.len() as u64) as usize];
    pattern_bytes(pattern, len, h)
}

/// Step `i` of `round`: `Some(value)` = put, `None` = delete. About a quarter
/// of the values copy (or edit) a recent one, so blocks are shared (dedupe).
fn step_value(round: u64, i: u64) -> Option<Vec<u8>> {
    let h = mix(round ^ 0x5EED, i);
    if i >= KEYSPACE && h.is_multiple_of(10) {
        return None;
    }
    let source = |h: u64| i - 1 - (h >> 24) % i.min(12);
    match (h >> 8) % 8 {
        6 if i > 0 => Some(base_value(round, source(h))),
        7 if i > 0 => {
            let mut v = base_value(round, source(h));
            if !v.is_empty() {
                let p = (h >> 40) as usize % v.len();
                v[p] ^= 0xA5;
            }
            Some(v)
        }
        _ => Some(base_value(round, i)),
    }
}

/// Steps of commit unit `unit` (one put, one batch).
fn unit_len(kind: Kind, round: u64, unit: u64) -> u64 {
    match kind {
        Kind::Puts => 1,
        _ => 1 + mix(round ^ 0xBA7C, unit) % 5,
    }
}

/// Step counts at unit boundaries, up to the first boundary past `min_steps`.
fn unit_ends(kind: Kind, round: u64, min_steps: u64) -> Vec<u64> {
    let mut ends = vec![0];
    let mut unit = 0;
    while *ends.last().expect("non-empty") <= min_steps {
        let next = ends.last().expect("non-empty") + unit_len(kind, round, unit);
        ends.push(next);
        unit += 1;
    }
    ends
}

fn apply_steps(state: &mut State, round: u64, steps: Range<u64>) {
    for i in steps {
        match step_value(round, i) {
            Some(v) => {
                state.insert(step_key(i), v);
            }
            None => {
                state.remove(&step_key(i));
            }
        }
    }
}

/// Group kind: thread `t` rewrites its own `GROUP_KEYS` keys of this round.
fn group_key(round: u64, t: u64, i: u64) -> Vec<u8> {
    format!("g{round}/t{t}/k{}", i % GROUP_KEYS).into_bytes()
}

fn group_value(round: u64, t: u64, i: u64) -> Option<Vec<u8>> {
    let h = mix(round ^ (t << 32) ^ 0x6A0B, i);
    if i >= GROUP_KEYS && h.is_multiple_of(8) {
        return None;
    }
    let len = SIZES[((h >> 12) % SIZES.len() as u64) as usize];
    Some(pattern_bytes(Pattern::ALL[(h % 6) as usize], len, h))
}

fn group_state(round: u64, t: u64, steps: u64) -> State {
    let mut s = State::new();
    for i in 0..steps {
        match group_value(round, t, i) {
            Some(v) => {
                s.insert(group_key(round, t, i), v);
            }
            None => {
                s.remove(&group_key(round, t, i));
            }
        }
    }
    s
}

// ---------------------------------------------------------------------------
// Child side
// ---------------------------------------------------------------------------

fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{line}").expect("child stdout");
    out.flush().expect("child stdout flush");
}

/// Entry point of the child process; a no-op in a normal test run.
#[test]
#[ignore = "child-process entry point of the WAL crash tests (no-op unless BABEL_WAL_CHILD_DB is set)"]
fn child_process_entry() {
    let Some(db) = std::env::var_os(ENV_DB) else {
        return;
    };
    let kind = std::env::var(ENV_KIND)
        .ok()
        .and_then(|k| Kind::parse(&k))
        .expect("BABEL_WAL_CHILD_KIND must name a known kind");
    let round = env_u64(ENV_ROUND, 0);
    say("");
    let path = PathBuf::from(db);
    match kind {
        Kind::Group => child_group(&path, round),
        _ => child_write(&path, kind, round),
    }
}

fn child_write(path: &Path, kind: Kind, round: u64) {
    let db = open_wal(path).unwrap_or_else(|e| panic!("child: open_wal failed: {e}"));
    say("CHILD_READY");
    let mut step = 0u64;
    let mut unit = 0u64;
    let mut unsynced: Vec<(u64, u64)> = Vec::new();
    while step < MAX_STEPS {
        let steps: Vec<u64> = (step..step + unit_len(kind, round, unit)).collect();
        let keys: Vec<Vec<u8>> = steps.iter().map(|&i| step_key(i)).collect();
        let values: Vec<Option<Vec<u8>>> = steps.iter().map(|&i| step_value(round, i)).collect();
        let ops: Vec<BatchOp<'_>> = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| match value {
                Some(v) => BatchOp::Put {
                    key,
                    value: v,
                    expect: Expect::Any,
                },
                None => BatchOp::Delete {
                    key,
                    expect: Expect::Any,
                },
            })
            .collect();
        let results: Vec<Option<u64>> = match kind {
            Kind::Puts => vec![match &values[0] {
                Some(v) => Some(
                    db.put(&keys[0], v, Expect::Any)
                        .unwrap_or_else(|e| panic!("child: put failed: {e}")),
                ),
                None => {
                    db.delete(&keys[0], Expect::Any)
                        .unwrap_or_else(|e| panic!("child: delete failed: {e}"));
                    None
                }
            }],
            Kind::Batch => db
                .write_batch(&ops)
                .unwrap_or_else(|e| panic!("child: write_batch failed: {e}")),
            _ => db
                .write_batch_each(&ops, Durability::Deferred)
                .unwrap_or_else(|e| panic!("child: write_batch_each failed: {e}"))
                .into_iter()
                .map(|r| r.unwrap_or_else(|e| panic!("child: deferred op failed: {e}")))
                .collect(),
        };
        // Revision 0 marks a delete.
        let acked: Vec<(u64, u64)> = steps
            .iter()
            .zip(&values)
            .zip(results)
            .map(|((&s, v), r)| (s, if v.is_some() { r.unwrap_or(0) } else { 0 }))
            .collect();
        if kind == Kind::Deferred {
            for (s, r) in &acked {
                say(&format!("VISIBLE {s} {r}"));
            }
            unsynced.extend(acked);
            if (unit + 1).is_multiple_of(DEFERRED_SYNC_EVERY) {
                db.sync().unwrap_or_else(|e| panic!("child: sync failed: {e}"));
                for (s, r) in unsynced.drain(..) {
                    say(&format!("COMMITTED {s} {r}"));
                }
            }
        } else {
            for (s, r) in &acked {
                say(&format!("COMMITTED {s} {r}"));
            }
        }
        step += steps.len() as u64;
        unit += 1;
    }
    say("CHILD_DONE");
}

fn child_group(path: &Path, round: u64) {
    let db = Arc::new(open_wal(path).unwrap_or_else(|e| panic!("child: open_wal failed: {e}")));
    let committer = GroupCommitter::new(db, GroupCommitConfig::from(WriteDurability::Immediate))
        .unwrap_or_else(|e| panic!("child: committer failed: {e}"));
    say("CHILD_READY");
    thread::scope(|s| {
        for t in 0..GROUP_THREADS {
            let committer = &committer;
            s.spawn(move || {
                for i in 0..MAX_STEPS {
                    let key = group_key(round, t, i);
                    let rev = match group_value(round, t, i) {
                        Some(v) => committer
                            .put(key, v, Expect::Any)
                            .unwrap_or_else(|e| panic!("child: group put failed: {e}")),
                        None => {
                            committer
                                .delete(key, Expect::Any)
                                .unwrap_or_else(|e| panic!("child: group delete failed: {e}"));
                            0
                        }
                    };
                    say(&format!("COMMITTED {t} {i} {rev}"));
                }
            });
        }
    });
    say("CHILD_DONE");
}

// ---------------------------------------------------------------------------
// Parent side
// ---------------------------------------------------------------------------

const MARKERS: [&str; 4] = ["CHILD_READY", "CHILD_DONE", "COMMITTED", "VISIBLE"];

#[derive(Default)]
struct Progress {
    ready: bool,
    done: bool,
    /// Durable acknowledgements: the numbers after `COMMITTED`, in order.
    committed: Vec<Vec<u64>>,
    /// Deferred (visible, not durable) acknowledgements.
    visible: Vec<Vec<u64>>,
    tail: Vec<String>,
}

impl Progress {
    fn feed(&mut self, raw: &str) {
        let line = raw.trim_end_matches(['\n', '\r']);
        self.tail.push(line.to_string());
        if self.tail.len() > 40 {
            self.tail.remove(0);
        }
        let toks: Vec<&str> = line.split_whitespace().collect();
        let Some(pos) = toks.iter().position(|t| MARKERS.contains(t)) else {
            return;
        };
        let nums: Vec<u64> = toks[pos + 1..]
            .iter()
            .map_while(|t| t.parse::<u64>().ok())
            .collect();
        match toks[pos] {
            "CHILD_READY" => self.ready = true,
            "CHILD_DONE" => self.done = true,
            "COMMITTED" => self.committed.push(nums),
            "VISIBLE" => self.visible.push(nums),
            _ => {}
        }
    }
}

struct ChildRun {
    progress: Progress,
    killed: bool,
    status: Option<ExitStatus>,
    stderr: String,
}

impl ChildRun {
    #[track_caller]
    fn expect_progress(&self, ctx: &str) {
        if !self.progress.ready || (!self.killed && !self.progress.done) {
            panic!(
                "{ctx}: the child did not reach the kill point (ready: {}, killed: {}, finished: {}, \
                 exit status: {:?})\n--- child stdout (last lines) ---\n{}\n--- child stderr ---\n{}",
                self.progress.ready,
                self.killed,
                self.progress.done,
                self.status,
                self.progress.tail.join("\n"),
                self.stderr
            );
        }
    }
}

fn run_child(kind: Kind, db_path: &Path, round: u64, mut should_kill: impl FnMut(&Progress) -> bool) -> ChildRun {
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(exe)
        .args(["--exact", CHILD_TEST, "--nocapture", "--ignored", "--test-threads=1"])
        .env(ENV_DB, db_path)
        .env(ENV_KIND, kind.name())
        .env(ENV_ROUND, round.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the child test process");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let (tx, rx) = mpsc::channel::<String>();
    let out_reader = thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) if line.ends_with('\n') => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Ok(_) => {}
            }
        }
    });
    let err_reader = thread::spawn(move || {
        let mut s = String::new();
        let _ = BufReader::new(stderr).read_to_string(&mut s);
        s
    });
    let deadline = Instant::now() + CHILD_TIMEOUT;
    let mut progress = Progress::default();
    let mut killed = false;
    let mut timed_out = false;
    loop {
        match rx.recv_timeout(Duration::from_millis(5)) {
            Ok(line) => progress.feed(&line),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if should_kill(&progress) {
            let _ = child.kill();
            killed = true;
            break;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            timed_out = true;
            break;
        }
    }
    // Everything the child printed before it died still counts.
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < drain_deadline {
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(line) => progress.feed(&line),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    let status = child.wait().ok();
    if out_reader.is_finished() {
        let _ = out_reader.join();
    }
    let join_deadline = Instant::now() + Duration::from_secs(10);
    while !err_reader.is_finished() && Instant::now() < join_deadline {
        thread::sleep(Duration::from_millis(10));
    }
    let stderr = if err_reader.is_finished() {
        err_reader.join().unwrap_or_default()
    } else {
        "<stderr not available>".to_string()
    };
    if timed_out {
        panic!(
            "child ({}, round {round}) did not reach the kill point within {CHILD_TIMEOUT:?}\n\
             --- child stdout (last lines) ---\n{}\n--- child stderr ---\n{stderr}",
            kind.name(),
            progress.tail.join("\n")
        );
    }
    ChildRun {
        progress,
        killed,
        status,
        stderr,
    }
}

fn run_seed(kind: Kind) -> u64 {
    match std::env::var("BABEL_RECOVERY_SEED") {
        Ok(s) => s.trim().parse().expect("BABEL_RECOVERY_SEED must be a u64"),
        Err(_) => {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            mix(nanos, kind as u64 ^ 0x3A1)
        }
    }
}

/// `(step, revision)` pairs of `COMMITTED`/`VISIBLE` lines of the sequential kinds.
fn pairs(lines: &[Vec<u64>], ctx: &str) -> Vec<(u64, u64)> {
    lines
        .iter()
        .map(|n| match n.as_slice() {
            [s, r] => (*s, *r),
            other => panic!("{ctx}: malformed acknowledgement {other:?}"),
        })
        .collect()
}

fn crash_writer(kind: Kind) {
    let seed = run_seed(kind);
    let mut rng = Rng::new(seed);
    let rounds = env_u64("BABEL_WAL_RECOVERY_ROUNDS", 3);
    let dir = temp_dir("babeldb-wal-crash-");
    let db_path = dir.path().join("crash.redb");
    let mut base: State = BTreeMap::new();
    let mut max_rev = 0u64;
    for round in 0..rounds {
        let kill_after = rng.range(40, 400);
        let ctx = format!(
            "[wal {} round {round}, BABEL_RECOVERY_SEED={seed}, kill after {kill_after} acknowledged steps]",
            kind.name()
        );
        let run = run_child(kind, &db_path, round, |p| {
            let acked = if kind == Kind::Deferred {
                p.visible.len()
            } else {
                p.committed.len()
            };
            acked as u64 >= kill_after
        });
        run.expect_progress(&ctx);
        let p = &run.progress;
        let committed = pairs(&p.committed, &ctx);
        let visible_acks = pairs(&p.visible, &ctx);
        for (list, name) in [(&committed, "COMMITTED"), (&visible_acks, "VISIBLE")] {
            for (n, (s, _)) in list.iter().enumerate() {
                assert_eq!(*s, n as u64, "{ctx}: {name} acknowledgements out of step order");
            }
        }
        let durable = committed.len() as u64;
        let visible = if kind == Kind::Deferred {
            visible_acks.len() as u64
        } else {
            durable
        };
        assert!(visible >= durable, "{ctx}: durable acknowledgement of a step never made visible");
        let acked: HashMap<u64, u64> = committed.iter().chain(&visible_acks).copied().collect();
        for (&s, &r) in &acked {
            if step_value(round, s).is_some() {
                assert!(r > max_rev, "{ctx}: step {s} got revision {r}, not above {max_rev} (revision reused after recovery)");
            }
        }
        // The child prints one line per step, and only after the unit's commit (or the sync
        // covering it) returned, so a kill can land between the lines of one unit: that unit
        // is complete. Round both prefixes up to the end of their unit.
        let ends = unit_ends(kind, round, visible);
        let round_up = |n: u64| {
            ends.iter()
                .copied()
                .find(|&e| e >= n)
                .expect("unit_ends covers every acknowledged step")
        };
        let (durable, visible) = (round_up(durable), round_up(visible));
        let ends = unit_ends(kind, round, visible);
        let hi = if p.done {
            visible
        } else {
            ends.iter()
                .copied()
                .find(|&e| e > visible)
                .expect("unit_ends covers the in-flight unit")
        };

        let mut db = open_wal_retry(&db_path);
        let rec = db.store().recovery().clone();
        let snap = snapshot(&db, &ctx);
        let actual = values_of(&snap);
        let mut model = base.clone();
        apply_steps(&mut model, round, 0..durable);
        let acknowledged = model.clone();
        let mut at = durable;
        let matched = loop {
            if model == actual {
                break Some(at);
            }
            match ends.iter().copied().find(|&e| e > at) {
                Some(next) if next <= hi => {
                    apply_steps(&mut model, round, at..next);
                    at = next;
                }
                _ => break None,
            }
        };
        let Some(at) = matched else {
            panic!(
                "{ctx}: recovered content is not the state after any unit boundary in [{durable}, {hi}] \
                 (killed: {}, recovery: {rec:?}): lost acknowledged data or a partial unit.\n\
                 Differences from the acknowledged state:\n{}",
                run.killed,
                state_diff(&actual, &acknowledged)
            );
        };
        println!(
            "{ctx}: acknowledged {durable} (visible {visible}), recovered state after {at} steps; replayed {} WAL records, chain end {:?}",
            rec.records_replayed, rec.chain_end
        );
        let mut last_writer: HashMap<Vec<u8>, u64> = HashMap::new();
        for s in 0..at {
            last_writer.insert(step_key(s), s);
        }
        // A step in [at, hi) may have been applied without changing any value (it put
        // the value its key already had): such a key may carry that newer revision.
        let maybe_rewritten: HashSet<Vec<u8>> = (at..hi)
            .filter(|&s| step_value(round, s).is_some())
            .map(step_key)
            .collect();
        for (key, &s) in &last_writer {
            if maybe_rewritten.contains(key) {
                continue;
            }
            if let (Some(&rev), Some(_)) = (acked.get(&s), step_value(round, s)) {
                let got = snap.get(key).map(|(r, _)| *r);
                assert_eq!(got, Some(rev), "{ctx}: {} was last written by acknowledged step {s} (revision {rev})", key_str(key));
            }
        }
        verify_ok(&db, true, &format!("{ctx} after recovery"));
        let gc = db.gc().unwrap_or_else(|e| panic!("{ctx}: gc failed: {e}"));
        assert_eq!(gc, GcReport::default(), "{ctx}: a killed writer must leave nothing to collect");
        max_rev = max_rev.max(committed.iter().map(|&(_, r)| r).chain(snap.values().map(|(r, _)| *r)).max().unwrap_or(0));
        base = actual;
        drop(db);
    }
    let mut db = open_wal_retry(&db_path);
    delete_everything_and_check_no_leaks(&mut db, &format!("[wal {} final drain, seed {seed}]", kind.name()));
}

#[test]
fn wal_crash_during_puts() {
    crash_writer(Kind::Puts);
}

#[test]
fn wal_crash_during_write_batch() {
    crash_writer(Kind::Batch);
}

#[test]
fn wal_crash_with_deferred_commits_recovers_a_prefix() {
    crash_writer(Kind::Deferred);
}

#[test]
fn wal_crash_during_group_commit() {
    let seed = run_seed(Kind::Group);
    let mut rng = Rng::new(seed);
    let rounds = env_u64("BABEL_WAL_RECOVERY_ROUNDS", 3);
    let dir = temp_dir("babeldb-wal-crash-group-");
    let db_path = dir.path().join("group.redb");
    let mut base: State = BTreeMap::new();
    let mut max_rev = 0u64;
    for round in 0..rounds {
        let kill_after = rng.range(100, 1500);
        let ctx = format!("[wal group round {round}, BABEL_RECOVERY_SEED={seed}, kill after {kill_after} acks]");
        let run = run_child(Kind::Group, &db_path, round, |p| p.committed.len() as u64 >= kill_after);
        run.expect_progress(&ctx);
        // acknowledged steps per thread: (step, revision), in order
        let mut per_thread: Vec<Vec<(u64, u64)>> = vec![Vec::new(); GROUP_THREADS as usize];
        for n in &run.progress.committed {
            match n.as_slice() {
                [t, i, r] if *t < GROUP_THREADS => per_thread[*t as usize].push((*i, *r)),
                other => panic!("{ctx}: malformed acknowledgement {other:?}"),
            }
        }
        for (t, acks) in per_thread.iter().enumerate() {
            for (n, (i, _)) in acks.iter().enumerate() {
                assert_eq!(*i, n as u64, "{ctx}: thread {t} acknowledgements out of order");
            }
        }
        let mut db = open_wal_retry(&db_path);
        let rec = db.store().recovery().clone();
        println!(
            "{ctx}: {} acknowledgements; replayed {} WAL records, chain end {:?}",
            run.progress.committed.len(),
            rec.records_replayed,
            rec.chain_end
        );
        let snap = snapshot(&db, &ctx);
        let actual = values_of(&snap);
        let prefix = format!("g{round}/");
        let (mine, others): (State, State) = actual.clone().into_iter().partition(|(k, _)| k.starts_with(prefix.as_bytes()));
        assert_state_eq(&others, &base, &format!("{ctx}: records of earlier rounds"));
        for (t, acks) in per_thread.iter().enumerate() {
            let t = t as u64;
            let a = acks.len() as u64;
            let thread_prefix = format!("g{round}/t{t}/");
            let got: State = mine.iter().filter(|(k, _)| k.starts_with(thread_prefix.as_bytes())).map(|(k, v)| (k.clone(), v.clone())).collect();
            // the in-flight step of each thread may or may not have been applied
            let matching: Vec<u64> = [a, a + 1]
                .into_iter()
                .filter(|&n| group_state(round, t, n) == got)
                .collect();
            let Some(&n) = matching.first() else {
                panic!(
                    "{ctx}: thread {t} acknowledged {a} steps but its keys match neither {a} nor {} steps:\n{}",
                    a + 1,
                    state_diff(&got, &group_state(round, t, a))
                );
            };
            // An in-flight step that put the value its key already had leaves both states
            // equal; if it was applied, that key carries the newer revision.
            let rewritten = (matching.len() == 2).then(|| group_key(round, t, a));
            // records last written by an acknowledged put keep its revision
            let mut last: HashMap<Vec<u8>, u64> = HashMap::new();
            for i in 0..n {
                last.insert(group_key(round, t, i), i);
            }
            for (key, &i) in &last {
                if rewritten.as_ref() == Some(key) {
                    continue;
                }
                if let (Some(&(_, rev)), Some(_)) = (acks.get(i as usize), group_value(round, t, i)) {
                    assert!(rev > max_rev, "{ctx}: revision {rev} reused");
                    assert_eq!(snap.get(key).map(|(r, _)| *r), Some(rev), "{ctx}: revision of {}", key_str(key));
                }
            }
        }
        verify_ok(&db, true, &format!("{ctx} after recovery"));
        let gc = db.gc().unwrap_or_else(|e| panic!("{ctx}: gc failed: {e}"));
        assert_eq!(gc, GcReport::default(), "{ctx}: a killed writer must leave nothing to collect");
        max_rev = max_rev.max(snap.values().map(|(r, _)| *r).max().unwrap_or(0));
        max_rev = max_rev.max(per_thread.iter().flatten().map(|&(_, r)| r).max().unwrap_or(0));
        base = actual;
        drop(db);
    }
    let mut db = open_wal_retry(&db_path);
    delete_everything_and_check_no_leaks(&mut db, &format!("[wal group final drain, seed {seed}]"));
}
