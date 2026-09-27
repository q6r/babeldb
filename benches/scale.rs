//! Discord-scale benchmarks (custom harness, not criterion).
//!
//! ```text
//! cargo bench --bench scale
//! SCALE_BENCH_SECS=2          seconds per measured run (default 2)
//! SCALE_BENCH_ONLY=sim,a,b    sections to run (default: all)
//! SCALE_BENCH_DIR=<dir>       where database files go (default: a temp dir)
//! ```
//!
//! Sections:
//! - `sim`: group-commit mechanics against a SIMULATED fsync (the sink sleeps
//!   `SIM_FSYNC` per durable commit). Measures batching, not a database.
//! - `a`: durable single puts (one commit + fsync each) vs group commit, with
//!   1/4/16/64 writer threads (redb).
//! - `b`: buffered group commit (Deferred commits + periodic sync).
//! - `c`: `ShardedDb` write throughput with 1/2/4/8 shards, 64 writers.
//! - `d`: Zipf-hot point reads with and without coalescing.
//! - `e`: `ChatStore`: `latest(channel, 50)` and `send` latencies over 1000
//!   Zipf-skewed channels, 95/5 and 50/50 read/write mixes.
//!
//! Sections a–e need the storage engine; when it is not available (the
//! engine is still a stub) they are skipped with a message. Every run prints
//! one JSON line; a summary table follows at the end.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use babeldb::engine::{Db, Expect};
use babeldb::error::Result;
use babeldb::scale::{
    BatchSink, ChatOptions, ChatStore, Coalescer, GroupCommitConfig, GroupCommitter, OpResult,
    OwnedOp, Router, ShardedDb, Snowflake, WriteDurability, message_key,
};
use babeldb::store::Durability;
use babeldb::{Config, MemStore};

const SIM_FSYNC: Duration = Duration::from_millis(1);
const VALUE_LEN: usize = 200;

// ---------------------------------------------------------------------------
// Measurement helpers
// ---------------------------------------------------------------------------

/// splitmix64
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Zipf(s) over `0..n` by inverse CDF.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Zipf {
        let mut acc = 0.0;
        let mut cdf: Vec<f64> = (1..=n)
            .map(|k| {
                acc += 1.0 / (k as f64).powf(s);
                acc
            })
            .collect();
        for c in &mut cdf {
            *c /= acc;
        }
        Zipf { cdf }
    }

    fn sample(&self, rng: &mut Rng) -> usize {
        let u = rng.unit();
        self.cdf.partition_point(|c| *c < u).min(self.cdf.len() - 1)
    }
}

fn value(seed: u64) -> Vec<u8> {
    let mut v = vec![0u8; VALUE_LEN];
    let mut r = Rng(seed);
    for chunk in v.chunks_mut(8) {
        let x = r.next().to_le_bytes();
        chunk.copy_from_slice(&x[..chunk.len()]);
    }
    v
}

struct Sampled {
    /// Sorted per-operation latencies in ns, per operation class.
    lat: Vec<Vec<u64>>,
    secs: f64,
}

fn pct(sorted: &[u64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = ((sorted.len() as f64 * q) as usize).min(sorted.len() - 1);
    sorted[i] as f64 / 1000.0
}

/// Run `op(thread, rng) -> class` on `threads` threads for `secs` seconds,
/// timing every call. Returns the sorted latencies per class.
fn run_threads(
    threads: usize,
    secs: f64,
    classes: usize,
    op: impl Fn(usize, &mut Rng) -> usize + Sync,
) -> Sampled {
    let barrier = Barrier::new(threads + 1);
    let deadline = Mutex::new(None::<Instant>);
    let per_thread: Vec<Vec<Vec<u64>>> = thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let (barrier, deadline, op) = (&barrier, &deadline, &op);
                s.spawn(move || {
                    let mut rng = Rng(0xC0FFEE ^ ((t as u64) << 32));
                    let mut lat = vec![Vec::new(); classes];
                    barrier.wait();
                    let end = deadline.lock().unwrap().expect("deadline set");
                    while Instant::now() < end {
                        let t0 = Instant::now();
                        let class = op(t, &mut rng);
                        lat[class].push(t0.elapsed().as_nanos() as u64);
                    }
                    lat
                })
            })
            .collect();
        *deadline.lock().unwrap() = Some(Instant::now() + Duration::from_secs_f64(secs));
        barrier.wait();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut lat = vec![Vec::new(); classes];
    for t in per_thread {
        for (c, v) in t.into_iter().enumerate() {
            lat[c].extend(v);
        }
    }
    for v in &mut lat {
        v.sort_unstable();
    }
    Sampled { lat, secs }
}

struct Row {
    bench: String,
    config: String,
    threads: usize,
    ops_per_sec: f64,
    p50_us: f64,
    p99_us: f64,
    note: String,
}

#[derive(Default)]
struct Report {
    rows: Vec<Row>,
}

impl Report {
    /// Print one JSON line and keep a table row. `class` selects the latency
    /// class; `ops` overrides the op count (for mixed runs).
    #[allow(clippy::too_many_arguments)]
    fn add(
        &mut self,
        bench: &str,
        config: &str,
        threads: usize,
        s: &Sampled,
        class: usize,
        extra: &[(&str, f64)],
        note: &str,
    ) {
        let ops = s.lat[class].len() as u64;
        let ops_per_sec = ops as f64 / s.secs;
        let (p50, p99, p999) = (
            pct(&s.lat[class], 0.50),
            pct(&s.lat[class], 0.99),
            pct(&s.lat[class], 0.999),
        );
        let mut json = format!(
            "{{\"bench\":\"{bench}\",\"config\":\"{config}\",\"threads\":{threads},\"ops\":{ops},\"secs\":{:.3},\"ops_per_sec\":{ops_per_sec:.1},\"p50_us\":{p50:.1},\"p99_us\":{p99:.1},\"p999_us\":{p999:.1}",
            s.secs
        );
        for (k, v) in extra {
            let _ = write!(json, ",\"{k}\":{v:.3}");
        }
        json.push('}');
        println!("{json}");
        self.rows.push(Row {
            bench: bench.to_string(),
            config: config.to_string(),
            threads,
            ops_per_sec,
            p50_us: p50,
            p99_us: p99,
            note: note.to_string(),
        });
    }

    fn print_table(&self) {
        println!();
        println!(
            "{:<22} {:<34} {:>7} {:>12} {:>10} {:>10}  note",
            "bench", "config", "threads", "ops/s", "p50 us", "p99 us"
        );
        for r in &self.rows {
            println!(
                "{:<22} {:<34} {:>7} {:>12.0} {:>10.1} {:>10.1}  {}",
                r.bench, r.config, r.threads, r.ops_per_sec, r.p50_us, r.p99_us, r.note
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Simulated sink (section `sim`)
// ---------------------------------------------------------------------------

/// Commits instantly but sleeps `fsync` for every durable commit or sync.
struct SimSink {
    fsync: Duration,
    rev: AtomicU64,
}

impl BatchSink for SimSink {
    fn apply(&self, ops: &[OwnedOp], durability: Durability) -> Result<Vec<OpResult>> {
        if durability == Durability::Immediate {
            thread::sleep(self.fsync);
        }
        Ok(ops
            .iter()
            .map(|_| Ok(Some(self.rev.fetch_add(1, Ordering::Relaxed) + 1)))
            .collect())
    }

    fn sync(&self) -> Result<()> {
        thread::sleep(self.fsync);
        Ok(())
    }
}

fn section_sim(rep: &mut Report, secs: f64) {
    for threads in [1, 4, 16, 64] {
        let sink = SimSink {
            fsync: SIM_FSYNC,
            rev: AtomicU64::new(0),
        };
        let writer = Mutex::new(());
        let s = run_threads(threads, secs, 1, |t, rng| {
            let op = OwnedOp::put(rng.next().to_be_bytes(), value(t as u64), Expect::Any);
            let _one_writer = writer.lock().unwrap();
            sink.apply(std::slice::from_ref(&op), Durability::Immediate)
                .unwrap();
            0
        });
        rep.add(
            "sim-direct",
            "1 simulated fsync per put",
            threads,
            &s,
            0,
            &[],
            "simulation",
        );

        let c = GroupCommitter::new(
            SimSink {
                fsync: SIM_FSYNC,
                rev: AtomicU64::new(0),
            },
            GroupCommitConfig::default(),
        )
        .unwrap();
        let s = run_threads(threads, secs, 1, |t, rng| {
            c.put(rng.next().to_be_bytes(), value(t as u64), Expect::Any)
                .unwrap();
            0
        });
        let st = c.stats();
        rep.add(
            "sim-group",
            "group commit, simulated fsync",
            threads,
            &s,
            0,
            &[
                ("avg_batch_ops", st.avg_batch_ops()),
                ("commits", st.batches as f64),
            ],
            &format!("avg batch {:.1}", st.avg_batch_ops()),
        );
    }
}

// ---------------------------------------------------------------------------
// Engine sections
// ---------------------------------------------------------------------------

/// Whether the engine (Db over MemStore and redb) works in this build.
fn engine_available(dir: &Path) -> bool {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let path = dir.join("probe.redb");
    let ok = std::panic::catch_unwind(|| -> Result<bool> {
        let mem = Db::with_store(MemStore::new(), Config::default())?;
        mem.put(b"probe", b"value", Expect::Any)?;
        let redb = Db::open(&path, Config::default())?;
        redb.put(b"probe", b"value", Expect::Any)?;
        Ok(mem.get(b"probe")?.as_deref() == Some(b"value".as_slice())
            && redb.get(b"probe")?.as_deref() == Some(b"value".as_slice()))
    });
    std::panic::set_hook(prev);
    let _ = std::fs::remove_file(&path);
    matches!(ok, Ok(Ok(true)))
}

/// A benchmark section over the engine.
type Section = fn(&mut Report, f64, &Dirs);

struct Dirs {
    root: PathBuf,
    n: AtomicU64,
}

impl Dirs {
    fn fresh(&self, name: &str) -> PathBuf {
        let p = self
            .root
            .join(format!("{name}-{}", self.n.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}

fn key(thread: usize, i: u64) -> Vec<u8> {
    message_key(thread as u64, i).to_vec()
}

fn section_a(rep: &mut Report, secs: f64, dirs: &Dirs) {
    for threads in [1, 4, 16, 64] {
        let db = Db::open(dirs.fresh("a-direct").join("db.redb"), Config::default()).unwrap();
        let counter = AtomicU64::new(0);
        let s = run_threads(threads, secs, 1, |t, _| {
            db.put(
                &key(t, counter.fetch_add(1, Ordering::Relaxed)),
                &value(t as u64),
                Expect::Any,
            )
            .unwrap();
            0
        });
        rep.add(
            "a-durable-put",
            "Db::put, 1 commit+fsync each",
            threads,
            &s,
            0,
            &[],
            "",
        );
        drop(db);

        let db =
            Arc::new(Db::open(dirs.fresh("a-group").join("db.redb"), Config::default()).unwrap());
        let c = GroupCommitter::new(db.clone(), GroupCommitConfig::default()).unwrap();
        let s = run_threads(threads, secs, 1, |t, _| {
            c.put(
                key(t, counter.fetch_add(1, Ordering::Relaxed)),
                value(t as u64),
                Expect::Any,
            )
            .unwrap();
            0
        });
        let st = c.stats();
        rep.add(
            "a-group-commit",
            "GroupCommitter, Immediate",
            threads,
            &s,
            0,
            &[("avg_batch_ops", st.avg_batch_ops())],
            &format!("avg batch {:.1}", st.avg_batch_ops()),
        );
        c.shutdown().unwrap();
    }
}

fn section_b(rep: &mut Report, secs: f64, dirs: &Dirs) {
    for threads in [1, 16, 64] {
        let db = Arc::new(Db::open(dirs.fresh("b").join("db.redb"), Config::default()).unwrap());
        let cfg = GroupCommitConfig::from(WriteDurability::Buffered {
            flush_interval: Duration::from_millis(100),
            max_pending_bytes: 16 << 20,
        });
        let c = GroupCommitter::new(db.clone(), cfg).unwrap();
        let counter = AtomicU64::new(0);
        let s = run_threads(threads, secs, 1, |t, _| {
            c.put(
                key(t, counter.fetch_add(1, Ordering::Relaxed)),
                value(t as u64),
                Expect::Any,
            )
            .unwrap();
            0
        });
        let t0 = Instant::now();
        c.flush().unwrap();
        let flush_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let st = c.stats();
        rep.add(
            "b-buffered",
            "Buffered 100ms / 16MiB",
            threads,
            &s,
            0,
            &[
                ("avg_batch_ops", st.avg_batch_ops()),
                ("durable_commits", (st.immediate_batches + st.syncs) as f64),
                ("final_flush_ms", flush_ms),
            ],
            &format!("{} durable commits", st.immediate_batches + st.syncs),
        );
        c.shutdown().unwrap();
    }
}

fn shard_config(shards: usize) -> Config {
    let base = Config::default();
    Config {
        cache_bytes: base.cache_bytes / shards,
        backend_cache_bytes: base.backend_cache_bytes / shards,
        ..base
    }
}

fn section_c(rep: &mut Report, secs: f64, dirs: &Dirs) {
    let threads = 64;
    for shards in [1, 2, 4, 8] {
        let db = ShardedDb::open(
            dirs.fresh("c"),
            shards,
            shard_config(shards),
            Router::FirstBytes(8),
            WriteDurability::Immediate,
        )
        .unwrap();
        let counter = AtomicU64::new(0);
        let s = run_threads(threads, secs, 1, |t, rng| {
            let channel = rng.next() % 10_000;
            let k = message_key(channel, counter.fetch_add(1, Ordering::Relaxed));
            db.put(&k, &value(t as u64), Expect::Any).unwrap();
            0
        });
        let commits: u64 = db.commit_stats().iter().map(|c| c.batches).sum();
        rep.add(
            "c-sharded-writes",
            &format!("{shards} shard(s), Immediate"),
            threads,
            &s,
            0,
            &[("shards", shards as f64), ("commits", commits as f64)],
            &format!("{commits} commits"),
        );
        db.shutdown().unwrap();
    }
}

fn section_d(rep: &mut Report, secs: f64, dirs: &Dirs) {
    const KEYS: u64 = 100_000;
    let db = Arc::new(Db::open(dirs.fresh("d").join("db.redb"), Config::default()).unwrap());
    let ops: Vec<OwnedOp> = (0..KEYS)
        .map(|i| OwnedOp::put(key(0, i), value(i), Expect::Any))
        .collect();
    for chunk in ops.chunks(5000) {
        db.apply(chunk, Durability::Immediate).unwrap();
    }
    let zipf = Zipf::new(KEYS as usize, 1.1);
    for threads in [16, 64] {
        let s = run_threads(threads, secs, 1, |_, rng| {
            db.get(&key(0, zipf.sample(rng) as u64)).unwrap();
            0
        });
        rep.add(
            "d-reads",
            "direct Db::get, Zipf 1.1",
            threads,
            &s,
            0,
            &[],
            "",
        );

        let co = Coalescer::new(db.clone(), Router::FirstBytes(8));
        let s = run_threads(threads, secs, 1, |_, rng| {
            co.get(&key(0, zipf.sample(rng) as u64)).unwrap();
            0
        });
        let st = co.stats();
        let joined = st.joined as f64 / (st.joined + st.source_reads).max(1) as f64;
        rep.add(
            "d-reads",
            "Coalescer::get, Zipf 1.1",
            threads,
            &s,
            0,
            &[("joined_ratio", joined)],
            &format!("{:.1}% joined", joined * 100.0),
        );
    }
}

fn section_e(rep: &mut Report, secs: f64, dirs: &Dirs) {
    const CHANNELS: u64 = 1000;
    const PRELOAD: u64 = 100;
    let threads = 32;
    let runs = [
        (0.95, true, "95/5, coalesce+cache"),
        (0.50, true, "50/50, coalesce+cache"),
        (0.95, false, "95/5, no coalesce/cache"),
    ];
    for (read_ratio, fast, label) in runs {
        let opts = if fast {
            ChatOptions::default()
        } else {
            ChatOptions {
                coalesce_reads: false,
                recent_cache_bytes: 0,
                ..ChatOptions::default()
            }
        };
        let chat = ChatStore::open(
            dirs.fresh("e"),
            4,
            shard_config(4),
            WriteDurability::Immediate,
            opts,
        )
        .unwrap();
        let ids = Snowflake::new(31, 31).unwrap();
        let preload: Vec<OwnedOp> = (0..CHANNELS)
            .flat_map(|ch| (0..PRELOAD).map(move |i| (ch, i)))
            .map(|(ch, i)| {
                OwnedOp::put(
                    message_key(ch, ids.next_id().unwrap()),
                    value(ch ^ i),
                    Expect::Absent,
                )
            })
            .collect();
        for chunk in preload.chunks(5000) {
            assert!(
                chat.db()
                    .write_owned(chunk.to_vec())
                    .iter()
                    .all(|r| r.is_ok())
            );
        }
        let zipf = Zipf::new(CHANNELS as usize, 1.1);
        let s = run_threads(threads, secs, 2, |t, rng| {
            let ch = zipf.sample(rng) as u64;
            if rng.unit() < read_ratio {
                chat.latest(ch, 50).unwrap();
                0
            } else {
                chat.send(ch, &value(t as u64)).unwrap();
                1
            }
        });
        let st = chat.stats();
        let hit = st.recent.hits as f64 / (st.recent.hits + st.recent.misses).max(1) as f64;
        let extra = [
            ("cache_hit_ratio", hit),
            ("coalesced_reads", st.coalesced_reads as f64),
            ("page_reads", st.page_reads as f64),
        ];
        let note = format!("hit {:.1}%, {} joined", hit * 100.0, st.coalesced_reads);
        rep.add("e-chat-latest50", label, threads, &s, 0, &extra, &note);
        rep.add("e-chat-send", label, threads, &s, 1, &[], "");
        chat.shutdown().unwrap();
    }
}

fn main() {
    let secs: f64 = std::env::var("SCALE_BENCH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2.0);
    let only: Option<Vec<String>> = std::env::var("SCALE_BENCH_ONLY")
        .ok()
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect());
    let want = |s: &str| only.as_ref().is_none_or(|o| o.iter().any(|x| x == s));
    let temp = tempfile::tempdir().expect("temp dir");
    let root = std::env::var("SCALE_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| temp.path().to_path_buf());
    std::fs::create_dir_all(&root).unwrap();
    let dirs = Dirs {
        root,
        n: AtomicU64::new(0),
    };

    println!(
        "# babeldb scale bench: {} {} | {} logical CPUs | {secs} s per run | dir {}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        dirs.root.display()
    );
    let mut rep = Report::default();
    if want("sim") {
        println!(
            "# sim: simulated fsync of {SIM_FSYNC:?} per durable commit (committer mechanics only)"
        );
        section_sim(&mut rep, secs);
    }
    let engine_sections: [(&str, Section); 5] = [
        ("a", section_a),
        ("b", section_b),
        ("c", section_c),
        ("d", section_d),
        ("e", section_e),
    ];
    if engine_sections.iter().any(|(name, _)| want(name)) {
        if engine_available(&dirs.root) {
            for (name, section) in engine_sections {
                if want(name) {
                    section(&mut rep, secs, &dirs);
                }
            }
        } else {
            println!(
                "# sections a-e skipped: the storage engine is not available in this build (stubs)"
            );
        }
    }
    rep.print_table();
}
