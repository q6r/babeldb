//! Crash recovery with a real child process.
//!
//! Each test re-invokes this test binary (`std::env::current_exe()`) with
//! `--exact child_process_entry --nocapture --ignored --test-threads=1` and
//! `BABEL_CHILD_DB`, `BABEL_CHILD_KIND`, `BABEL_CHILD_ROUND` set. The child
//! writes a deterministic workload and prints one line per acknowledged step
//! (flushed); the parent kills it (`Child::kill`: TerminateProcess on Windows,
//! SIGKILL on Unix) at a pseudo-random point, reopens the database and checks:
//!
//! - every acknowledged step is present with exact bytes and the revision the
//!   child was given;
//! - the recovered content equals the state after some unit boundary between
//!   the acknowledged prefix and the in-flight unit: an in-flight single put,
//!   batch or group is entirely present or entirely absent, never partial;
//! - revisions allocated after recovery never reuse a durable revision;
//! - `verify(true)` is ok; `gc()` finds nothing to do for plain writers (each
//!   commit is atomic) and collects abandoned imports; verify stays ok.
//!
//! Durability modes: `puts` (`put`/`delete`) and `batch` (`write_batch`) use
//! `Durability::Immediate` and acknowledge after the call returns; `group`
//! uses `write_batch_each(.., Deferred)` + `sync()` and acknowledges after
//! `sync` returns; `deferred` acknowledges `VISIBLE` after each deferred
//! commit but `COMMITTED` only after the `sync()` that follows every
//! `DEFERRED_SYNC_EVERY` units, and the recovered state must be a prefix of the
//! unit sequence; `import` / `import_file` are killed in the middle of a
//! multi-transaction import (8 MiB, 1 KiB blocks, 32 KiB import batches).
//!
//! IMPORTANT: killing a process is NOT a power-loss test. The operating system
//! still writes back its page cache, so these tests validate atomicity of each
//! commit and durability of acknowledged commits against process death; they
//! say nothing about power failure or disks that lie about flushes.
//!
//! Knobs: `BABEL_RECOVERY_ROUNDS` (kill/reopen rounds per writer kind,
//! default 2), `BABEL_RECOVERY_IMPORT_ROUNDS` (default 2), `BABEL_RECOVERY_SEED`
//! (fixes the kill points; printed in every failure message).
//!
//! Run: `cargo test --test recovery` (debug builds take tens of seconds; the
//! child entry point is `#[ignore]` and a no-op without `BABEL_CHILD_DB`).

mod common;

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use babeldb::format::source_kind;
use babeldb::ingest::file::FileSource;
use babeldb::ingest::{ByteSource, ImportOptions};
use babeldb::maintenance::GcReport;
use babeldb::store::Durability;
use babeldb::{BatchOp, Config, Db, Expect, Mode};
use common::{
    Pattern, Rng, State, assert_state_eq, delete_everything_and_check_no_leaks, describe_diff,
    env_u64, key_str, mix, open_redb_retry, pattern_bytes, small_config, snapshot, state_diff,
    temp_dir, values_of, verify_ok,
};

const CHILD_TEST: &str = "child_process_entry";
const ENV_DB: &str = "BABEL_CHILD_DB";
const ENV_KIND: &str = "BABEL_CHILD_KIND";
const ENV_ROUND: &str = "BABEL_CHILD_ROUND";

/// Keys `k000..k047` are rewritten over and over (overwrites, deletes, dedupe).
const KEYSPACE: u64 = 48;
/// Safety bound: the child is always killed long before.
const MAX_STEPS: u64 = 50_000;
const DEFERRED_SYNC_EVERY: u64 = 4;
const CHILD_TIMEOUT: Duration = Duration::from_secs(240);
const REOPEN_TIMEOUT: Duration = Duration::from_secs(30);

const IMPORT_LEN: usize = (8 << 20) - 777;
const IMPORT_REGION: usize = 64 << 10;
const READ_REPORT_EVERY: u64 = 64 << 10;

const SIZES: [usize; 16] = [
    0, 1, 17, 63, 64, 65, 200, 511, 512, 513, 1000, 1024, 1500, 2048, 2600, 4101,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Puts,
    Batch,
    Group,
    Deferred,
    Import,
    ImportFile,
}

impl Kind {
    const ALL: [Kind; 6] = [
        Kind::Puts,
        Kind::Batch,
        Kind::Group,
        Kind::Deferred,
        Kind::Import,
        Kind::ImportFile,
    ];

    fn name(self) -> &'static str {
        match self {
            Kind::Puts => "puts",
            Kind::Batch => "batch",
            Kind::Group => "group",
            Kind::Deferred => "deferred",
            Kind::Import => "import",
            Kind::ImportFile => "import_file",
        }
    }

    fn parse(s: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|k| k.name() == s)
    }
}

fn writer_config() -> Config {
    small_config(Mode::Adaptive)
}

fn import_config() -> Config {
    Config {
        block_size: 1024,
        inline_max: 64,
        import_batch_bytes: 32 << 10,
        ..Config::adaptive()
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

/// Step `i` of `round`: `Some(value)` = put, `None` = delete. Sizes span
/// inline and multi-block values; about a quarter are copies (or edited
/// copies) of recent values, so blocks are shared through dedupe.
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

/// Number of steps of commit unit `unit` (one put, one batch, one group).
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

fn import_key(round: u64) -> Vec<u8> {
    format!("import/r{round}").into_bytes()
}

fn previous_value(round: u64) -> Vec<u8> {
    pattern_bytes(Pattern::Text, 3000, 0xAB00 + round)
}

fn import_file_name(round: u64) -> String {
    format!("import-r{round}.bin")
}

fn import_file_path(db: &Path, round: u64) -> PathBuf {
    db.with_file_name(import_file_name(round))
}

/// 8 MiB of 64 KiB regions: random, zeros, motifs, arithmetic, text, mixed,
/// and copies of earlier regions (dedupe inside the import). Not block aligned.
fn import_content(round: u64) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(IMPORT_LEN + IMPORT_REGION);
    let mut region = 0u64;
    while out.len() < IMPORT_LEN {
        let h = mix(round ^ 0x1A2B, region);
        let bytes = match h % 8 {
            6 | 7 if region > 0 => {
                let src = ((h >> 8) % region) as usize * IMPORT_REGION;
                out[src..src + IMPORT_REGION].to_vec()
            }
            n => pattern_bytes(Pattern::ALL[(n % 6) as usize], IMPORT_REGION, h),
        };
        out.extend_from_slice(&bytes);
        region += 1;
    }
    out.truncate(IMPORT_LEN);
    out
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
#[ignore = "child-process entry point of the crash tests (no-op unless BABEL_CHILD_DB is set)"]
fn child_process_entry() {
    let Some(db) = std::env::var_os(ENV_DB) else {
        return;
    };
    let kind = std::env::var(ENV_KIND)
        .ok()
        .and_then(|k| Kind::parse(&k))
        .expect("BABEL_CHILD_KIND must name a known kind");
    let round = env_u64(ENV_ROUND, 0);
    // Terminates the harness' "test child_process_entry ... " line.
    say("");
    let path = PathBuf::from(db);
    match kind {
        Kind::Import | Kind::ImportFile => child_import(&path, kind, round),
        _ => child_write(&path, kind, round),
    }
}

fn child_write(path: &Path, kind: Kind, round: u64) {
    let db = Db::open(path, writer_config()).unwrap_or_else(|e| panic!("child: open failed: {e}"));
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
                .map(|r| r.unwrap_or_else(|e| panic!("child: group op failed: {e}")))
                .collect(),
        };
        // Revision 0 marks a delete (its result is not a record revision).
        let acked: Vec<(u64, u64)> = steps
            .iter()
            .zip(&values)
            .zip(results)
            .map(|((&s, v), r)| (s, if v.is_some() { r.unwrap_or(0) } else { 0 }))
            .collect();
        match kind {
            Kind::Group => {
                db.sync()
                    .unwrap_or_else(|e| panic!("child: sync failed: {e}"));
                for (s, r) in &acked {
                    say(&format!("COMMITTED {s} {r}"));
                }
            }
            Kind::Deferred => {
                for (s, r) in &acked {
                    say(&format!("VISIBLE {s} {r}"));
                }
                unsynced.extend(acked);
                if (unit + 1).is_multiple_of(DEFERRED_SYNC_EVERY) {
                    db.sync()
                        .unwrap_or_else(|e| panic!("child: sync failed: {e}"));
                    for (s, r) in unsynced.drain(..) {
                        say(&format!("COMMITTED {s} {r}"));
                    }
                }
            }
            _ => {
                for (s, r) in &acked {
                    say(&format!("COMMITTED {s} {r}"));
                }
            }
        }
        step += steps.len() as u64;
        unit += 1;
    }
    say("CHILD_DONE");
}

/// Reports progress while the importer reads the file.
struct ProgressSource {
    inner: FileSource,
    total: u64,
    next_report: u64,
}

impl ByteSource for ProgressSource {
    fn read_block(&mut self, buf: &mut [u8]) -> babeldb::Result<usize> {
        let n = self.inner.read_block(buf)?;
        self.total += n as u64;
        if self.total >= self.next_report {
            say(&format!("READ {}", self.total));
            self.next_report = self.total + READ_REPORT_EVERY;
        }
        Ok(n)
    }
}

fn child_import(path: &Path, kind: Kind, round: u64) {
    let db = Db::open(path, import_config()).unwrap_or_else(|e| panic!("child: open failed: {e}"));
    say("CHILD_READY");
    let key = import_key(round);
    let rev = db
        .put(&key, &previous_value(round), Expect::Any)
        .unwrap_or_else(|e| panic!("child: put of the previous value failed: {e}"));
    say(&format!("PREVIOUS_COMMITTED {rev}"));
    let file = import_file_path(path, round);
    std::fs::write(&file, import_content(round)).expect("child: write the import file");
    say("IMPORT_STARTED");
    let rev = match kind {
        Kind::Import => {
            let inner = FileSource::open(&file).unwrap_or_else(|e| panic!("child: open file: {e}"));
            let mut src = ProgressSource {
                inner,
                total: 0,
                next_report: READ_REPORT_EVERY,
            };
            db.import(&key, &mut src, &ImportOptions::default())
        }
        _ => db.import_file(&key, &file, &ImportOptions::default()),
    }
    .unwrap_or_else(|e| panic!("child: import failed: {e}"));
    say(&format!("IMPORT_DONE {rev}"));
    say("CHILD_DONE");
}

// ---------------------------------------------------------------------------
// Parent side: run, observe and kill the child
// ---------------------------------------------------------------------------

/// Words that start a child report (a line may be prefixed by harness output).
const MARKERS: [&str; 8] = [
    "CHILD_READY",
    "CHILD_DONE",
    "COMMITTED",
    "VISIBLE",
    "PREVIOUS_COMMITTED",
    "IMPORT_STARTED",
    "READ",
    "IMPORT_DONE",
];

/// What the child reported before it died.
#[derive(Default)]
struct Progress {
    ready: bool,
    done: bool,
    /// Durable acknowledgements `(step, revision)`, in order.
    committed: Vec<(u64, u64)>,
    /// Deferred (visible, not yet durable) acknowledgements, in order.
    visible: Vec<(u64, u64)>,
    previous_rev: Option<u64>,
    import_started: Option<Instant>,
    read_bytes: u64,
    import_done: Option<u64>,
    /// Last lines, for diagnostics.
    tail: Vec<String>,
}

impl Progress {
    /// Only complete lines are fed (a line cut by the kill is ignored).
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
        let num = |k: usize| toks.get(pos + k).and_then(|t| t.parse::<u64>().ok());
        match toks[pos] {
            "CHILD_READY" => self.ready = true,
            "CHILD_DONE" => self.done = true,
            "COMMITTED" => {
                if let (Some(s), Some(r)) = (num(1), num(2)) {
                    self.committed.push((s, r));
                }
            }
            "VISIBLE" => {
                if let (Some(s), Some(r)) = (num(1), num(2)) {
                    self.visible.push((s, r));
                }
            }
            "PREVIOUS_COMMITTED" => self.previous_rev = num(1),
            "IMPORT_STARTED" => self.import_started = Some(Instant::now()),
            "READ" => self.read_bytes = num(1).unwrap_or(self.read_bytes),
            "IMPORT_DONE" => self.import_done = num(1),
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

/// Spawn the child, feed its stdout lines to `Progress`, and kill it as soon
/// as `should_kill` says so (or let it finish).
fn run_child(
    kind: Kind,
    db_path: &Path,
    round: u64,
    mut should_kill: impl FnMut(&Progress) -> bool,
) -> ChildRun {
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(exe)
        .args([
            "--exact",
            CHILD_TEST,
            "--nocapture",
            "--ignored",
            "--test-threads=1",
        ])
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

/// `BABEL_RECOVERY_SEED`, or a fresh seed per run (printed on failure).
fn run_seed(kind: Kind) -> u64 {
    match std::env::var("BABEL_RECOVERY_SEED") {
        Ok(s) => s.trim().parse().expect("BABEL_RECOVERY_SEED must be a u64"),
        Err(_) => {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            mix(nanos, kind as u64)
        }
    }
}

// ---------------------------------------------------------------------------
// Writer crashes
// ---------------------------------------------------------------------------

fn crash_writer(kind: Kind) {
    let seed = run_seed(kind);
    let mut rng = Rng::new(seed);
    let rounds = env_u64("BABEL_RECOVERY_ROUNDS", 2);
    let dir = temp_dir("babeldb-crash-");
    let db_path = dir.path().join("crash.redb");
    let cfg = writer_config();
    // Verified content after the previous round, and the highest durable revision.
    let mut base: State = BTreeMap::new();
    let mut max_rev = 0u64;
    for round in 0..rounds {
        let kill_after = rng.range(30, 150);
        let ctx = format!(
            "[{} round {round}, BABEL_RECOVERY_SEED={seed}, kill after {kill_after} acknowledged steps]",
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
        for (list, name) in [(&p.committed, "COMMITTED"), (&p.visible, "VISIBLE")] {
            for (n, (s, _)) in list.iter().enumerate() {
                assert_eq!(
                    *s, n as u64,
                    "{ctx}: {name} acknowledgements out of step order"
                );
            }
        }
        let durable = p.committed.len() as u64;
        let visible = if kind == Kind::Deferred {
            p.visible.len() as u64
        } else {
            durable
        };
        assert!(
            visible >= durable,
            "{ctx}: durable acknowledgement of a step never made visible"
        );
        let acked: HashMap<u64, u64> = p.committed.iter().chain(&p.visible).copied().collect();
        for (&s, &r) in &acked {
            if step_value(round, s).is_some() {
                assert!(
                    r > max_rev,
                    "{ctx}: step {s} got revision {r}, not above {max_rev} which was durable \
                     before this round (revision reused after recovery)"
                );
            }
        }
        let ends = unit_ends(kind, round, visible);
        assert!(
            ends.contains(&durable),
            "{ctx}: acknowledged prefix {durable} is not a unit boundary"
        );
        assert!(
            ends.contains(&visible),
            "{ctx}: visible prefix {visible} is not a unit boundary"
        );
        let hi = if p.done {
            visible
        } else {
            ends.iter()
                .copied()
                .find(|&e| e > visible)
                .expect("unit_ends covers the in-flight unit")
        };

        let mut db = open_redb_retry(&db_path, &cfg, REOPEN_TIMEOUT);
        let snap = snapshot(&db, &ctx);
        let actual = values_of(&snap);

        // The recovered state must be the state after one unit boundary in [durable, hi].
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
                "{ctx}: recovered content is not the state after any unit boundary in \
                 [{durable}, {hi}] steps (durable acks {durable}, visible acks {visible}, killed: {}): \
                 lost acknowledged data or a partially applied unit.\nDifferences from the \
                 acknowledged state:\n{}",
                run.killed,
                state_diff(&actual, &acknowledged)
            );
        };
        // Records last written by an acknowledged put keep that put's revision.
        let mut last_writer: HashMap<Vec<u8>, u64> = HashMap::new();
        for s in 0..at {
            last_writer.insert(step_key(s), s);
        }
        for (key, &s) in &last_writer {
            if let (Some(&rev), Some(_)) = (acked.get(&s), step_value(round, s)) {
                let got = snap.get(key).map(|(r, _)| *r);
                assert_eq!(
                    got,
                    Some(rev),
                    "{ctx}: {} was last written by acknowledged step {s} (revision {rev})",
                    key_str(key)
                );
            }
        }
        verify_ok(&db, true, &format!("{ctx} after recovery"));
        let gc = db.gc().unwrap_or_else(|e| panic!("{ctx}: gc failed: {e}"));
        assert_eq!(
            gc,
            GcReport::default(),
            "{ctx}: a killed writer must leave no orphan, dangling candidate or refcount \
             drift (every commit is atomic)"
        );
        let rep = verify_ok(&db, true, &format!("{ctx} after gc"));
        assert_eq!(rep.pending_imports, 0, "{ctx}: pending imports");
        assert_eq!(rep.orphan_objects, 0, "{ctx}: orphan objects after gc");
        assert_state_eq(
            &values_of(&snapshot(&db, &ctx)),
            &actual,
            &format!("{ctx}: gc changed content"),
        );
        let durable_revs = p.committed.iter().map(|&(_, r)| r);
        let recovered_revs = snap.values().map(|(r, _)| *r);
        max_rev = max_rev.max(durable_revs.chain(recovered_revs).max().unwrap_or(0));
        base = actual;
        drop(db);
    }
    let mut db = open_redb_retry(&db_path, &cfg, REOPEN_TIMEOUT);
    delete_everything_and_check_no_leaks(
        &mut db,
        &format!("[{} final drain, seed {seed}]", kind.name()),
    );
}

#[test]
fn crash_during_puts() {
    crash_writer(Kind::Puts);
}

#[test]
fn crash_during_write_batch() {
    crash_writer(Kind::Batch);
}

#[test]
fn crash_during_group_commit() {
    crash_writer(Kind::Group);
}

#[test]
fn crash_with_deferred_commits_recovers_a_prefix() {
    crash_writer(Kind::Deferred);
}

// ---------------------------------------------------------------------------
// Import crashes
// ---------------------------------------------------------------------------

fn crash_import(kind: Kind) {
    let seed = run_seed(kind);
    let mut rng = Rng::new(seed);
    let rounds = env_u64("BABEL_RECOVERY_IMPORT_ROUNDS", 2);
    let dir = temp_dir("babeldb-crash-import-");
    let db_path = dir.path().join("import.redb");
    let cfg = import_config();
    let mut base: State = BTreeMap::new();
    for round in 0..rounds {
        // `import`: kill once the importer has read this many bytes;
        // `import_file`: kill this long after the import started.
        let read_threshold = rng.range(256 << 10, 3 << 20);
        let delay = Duration::from_millis(rng.range(50, 1500));
        let ctx = format!(
            "[{} round {round}, BABEL_RECOVERY_SEED={seed}, kill after {read_threshold} bytes read / {delay:?}]",
            kind.name()
        );
        let run = run_child(kind, &db_path, round, |p| {
            p.import_done.is_none()
                && match kind {
                    Kind::Import => p.read_bytes >= read_threshold,
                    _ => p.import_started.is_some_and(|t| t.elapsed() >= delay),
                }
        });
        run.expect_progress(&ctx);
        assert!(
            run.progress.import_started.is_some(),
            "{ctx}: the child never started the import"
        );
        let previous_rev = run
            .progress
            .previous_rev
            .unwrap_or_else(|| panic!("{ctx}: no PREVIOUS_COMMITTED line"));
        let key = import_key(round);
        let previous = previous_value(round);
        let content = import_content(round);

        let mut db = open_redb_retry(&db_path, &cfg, REOPEN_TIMEOUT);
        let snap = snapshot(&db, &ctx);
        let actual = values_of(&snap);
        let mut others = actual.clone();
        let got = others
            .remove(&key)
            .unwrap_or_else(|| panic!("{ctx}: the value written before the import is gone"));
        assert_state_eq(&others, &base, &format!("{ctx}: records of earlier rounds"));
        let published = if got == content {
            true
        } else if got == previous {
            false
        } else {
            panic!(
                "{ctx}: an interrupted import exposed a partial or corrupted value \
                 (vs imported file: {}; vs previous value: {})",
                describe_diff(&got, &content),
                describe_diff(&got, &previous)
            );
        };
        let rev = snap[&key].0;
        if let Some(done) = run.progress.import_done {
            assert!(
                published,
                "{ctx}: import acknowledged (revision {done}) but not durable"
            );
            assert_eq!(rev, done, "{ctx}: revision of the acknowledged import");
        }
        if !published {
            assert_eq!(
                rev, previous_rev,
                "{ctx}: an unpublished import changed the revision"
            );
        }
        if kind == Kind::ImportFile {
            let file_name = import_file_name(round);
            let sources = db
                .sources()
                .unwrap_or_else(|e| panic!("{ctx}: sources failed: {e}"));
            let mine: Vec<_> = sources
                .iter()
                .filter(|(_, d)| d.location.contains(&file_name))
                .collect();
            assert!(mine.len() <= 1, "{ctx}: one source per location: {mine:?}");
            match mine.first() {
                Some((_, d)) => {
                    assert_eq!(d.kind, source_kind::LOCAL_FILE, "{ctx}: source kind");
                    match (&d.last_import, published) {
                        (Some(li), true) => {
                            assert_eq!(li.revision, rev, "{ctx}: last_import.revision");
                            assert_eq!(li.bytes, content.len() as u64, "{ctx}: last_import.bytes");
                            assert_eq!(
                                li.digest,
                                *blake3::hash(&content).as_bytes(),
                                "{ctx}: digest"
                            );
                        }
                        (None, false) => {}
                        (li, _) => panic!(
                            "{ctx}: last_import {li:?} inconsistent with published={published}"
                        ),
                    }
                }
                None => assert!(
                    !published,
                    "{ctx}: a published import_file must reference its source"
                ),
            }
        }
        let before = verify_ok(
            &db,
            true,
            &format!("{ctx} before gc (pending imports allowed)"),
        );
        let gc = db.gc().unwrap_or_else(|e| panic!("{ctx}: gc failed: {e}"));
        if before.pending_imports > 0 {
            assert!(
                gc.abandoned_imports > 0,
                "{ctx}: verify saw {} pending imports but gc abandoned none: {gc:?}",
                before.pending_imports
            );
        }
        let after = verify_ok(&db, true, &format!("{ctx} after gc"));
        assert_eq!(
            after.pending_imports, 0,
            "{ctx}: pending imports survive gc"
        );
        assert_eq!(after.orphan_objects, 0, "{ctx}: orphan objects survive gc");
        let st = db
            .stats()
            .unwrap_or_else(|e| panic!("{ctx}: stats failed: {e}"));
        assert_eq!(
            st.pending_imports, 0,
            "{ctx}: stats.pending_imports after gc"
        );
        assert_state_eq(
            &values_of(&snapshot(&db, &ctx)),
            &actual,
            &format!("{ctx}: gc changed content"),
        );
        base = actual;
        drop(db);
    }
    // Every object prepared by an abandoned import must be gone: deleting all
    // records must leave no object behind.
    let mut db = open_redb_retry(&db_path, &cfg, REOPEN_TIMEOUT);
    delete_everything_and_check_no_leaks(
        &mut db,
        &format!("[{} final drain, seed {seed}]", kind.name()),
    );
}

#[test]
fn crash_during_import() {
    crash_import(Kind::Import);
}

#[test]
fn crash_during_import_file() {
    crash_import(Kind::ImportFile);
}
