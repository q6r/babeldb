//! Raw store microbenchmark: the `Store` trait alone, without the engine format.
//!
//! ```text
//! cargo bench --bench store -- [FLAGS]       run the benchmark (--help lists the flags)
//! ```
//!
//! Cargo passes `--bench` to bench binaries. Without it (under `cargo test --benches` or
//! `--all-targets`) the binary does a small smoke run on a temporary database instead.
//!
//! Records are the S3 chat messages of `babeldb::datasets` (the dataset of the engine
//! benchmark): keys `ch/{channel:08}/msg/{snowflake:020}`, inserted in dataset order, so
//! consecutive records land in random channels (random places of the key space).
//!
//! Phases, in this order:
//! - `load` (always): `--records` records in write transactions of `--batch` records, each
//!   committed with `--durability`; `--api put` calls `WriteTxn::put` per record (what the
//!   engine benchmark's raw-backend variant does), `--api put-many` calls
//!   `WriteTxn::put_many` once per transaction. Reports records/s over the whole load and
//!   the split between building the transaction (puts) and committing it.
//! - `get`: for each `--threads` count, every thread does `--ops` point gets of uniformly
//!   drawn loaded keys, one read transaction per get (the pattern of `Db::get`).
//! - `latest`: the same with reverse prefix scans returning the `--latest-limit` newest
//!   messages of the channel of a uniformly drawn loaded record (a busy channel is drawn
//!   more often), one read transaction per scan, keys and values copied out.
//! - `begin`: `begin_read` + drop alone (the transaction overhead of the two phases above).
//! - `compact`: file size, `Store::compact`, file size again.
//!
//! Timing: each operation is timed alone (`Instant`); ops/s = operations of all threads /
//! (barrier release -> last thread done). Every thread first runs `--warmup` untimed
//! operations. Percentiles are nearest-rank.

use std::fmt::Write as _;
use std::hint::black_box;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::time::{Duration, Instant};

use babeldb::datasets::{self, DEFAULT_SEED, Scenario, SplitMix64};
use babeldb::engine::prefix_successor;
use babeldb::store::mem::MemStore;
use babeldb::store::redb::RedbStore;
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Res<T> = Result<T, BoxError>;

const HELP: &str = "\
babeldb raw store benchmark (the Store trait alone, S3 chat records)

USAGE:
    cargo bench --bench store -- [FLAGS]

Sizes accept k/m/g (KiB/MiB/GiB); counts accept k/m (10^3/10^6); lists are comma-separated.

FLAGS:
    --backend redb|mem            store [redb]; mem is a smoke test only (not a benchmark)
    --records N                   records loaded [200k]
    --value-size B                value size (S3 values are at least ~220 B) [512]
    --batch N                     records per write transaction [1000]
    --durability immediate|deferred
                                  commit of each load transaction [immediate]; deferred ends
                                  with one empty immediate commit, included in the load time
    --api put|put-many            load through WriteTxn::put per record or put_many per batch [put]
    --cache-bytes B               redb page cache [64m] (the engine's Config default)
    --threads LIST                reader threads of get/latest/begin [1,4,8,16]
    --ops N                       timed operations per thread and phase [100k]
    --warmup N                    untimed operations per thread before each phase [50k]
    --prewarm                     read every record once before the read phases (loads the
                                  page cache as far as it holds them)
    --latest-limit N              messages per latest scan [50]
    --phases LIST                 get,latest,begin,compact [all]; load always runs
                                  (--phases load: nothing else)
    --no-reopen                   keep the store open after the load (default: close and
                                  reopen it, so reads start with an empty page cache)
    --seed N                      dataset seed [42]
    --dir PATH                    directory of the database [bench-results/tmp/store]
    --keep                        keep the database after the run
    --help
";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backend {
    Redb,
    Mem,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Api {
    Put,
    PutMany,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Get,
    Latest,
    Begin,
    Compact,
}

impl Phase {
    const ALL: [Phase; 4] = [Phase::Get, Phase::Latest, Phase::Begin, Phase::Compact];

    fn name(self) -> &'static str {
        match self {
            Phase::Get => "get",
            Phase::Latest => "latest",
            Phase::Begin => "begin",
            Phase::Compact => "compact",
        }
    }
}

#[derive(Clone, Debug)]
struct Opts {
    backend: Backend,
    records: u64,
    value_size: usize,
    batch: usize,
    durability: Durability,
    api: Api,
    cache_bytes: usize,
    threads: Vec<usize>,
    ops: usize,
    warmup: usize,
    prewarm: bool,
    latest_limit: usize,
    phases: Vec<Phase>,
    reopen: bool,
    seed: u64,
    dir: PathBuf,
    keep: bool,
}

impl Default for Opts {
    fn default() -> Opts {
        Opts {
            backend: Backend::Redb,
            records: 200_000,
            value_size: 512,
            batch: 1000,
            durability: Durability::Immediate,
            api: Api::Put,
            cache_bytes: 64 << 20,
            threads: vec![1, 4, 8, 16],
            ops: 100_000,
            warmup: 50_000,
            prewarm: false,
            latest_limit: 50,
            phases: Phase::ALL.to_vec(),
            reopen: true,
            seed: DEFAULT_SEED,
            dir: PathBuf::from("bench-results/tmp/store"),
            keep: false,
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !args.iter().any(|a| a == "--bench") {
        if let Err(e) = smoke() {
            eprintln!("store bench smoke run failed: {e}");
            std::process::exit(1);
        }
        return;
    }
    let code = match parse(&args) {
        Ok(None) => {
            print!("{HELP}");
            0
        }
        Ok(Some(o)) => match run(&o) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("error: {e}");
                1
            }
        },
        Err(e) => {
            eprintln!("error: {e}\n\n{HELP}");
            2
        }
    };
    std::process::exit(code);
}

/// `cargo test --benches`: every phase on tiny data, both backends.
fn smoke() -> Res<()> {
    let dir = tempfile::tempdir()?;
    for (backend, api) in [(Backend::Redb, Api::PutMany), (Backend::Mem, Api::Put)] {
        let o = Opts {
            backend,
            api,
            records: 3000,
            batch: 700,
            threads: vec![1, 3],
            ops: 300,
            warmup: 50,
            prewarm: true,
            dir: dir.path().join(format!("{backend:?}")),
            ..Opts::default()
        };
        run(&o)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------------

fn parse_scaled(s: &str, unit: u64) -> Result<u64, String> {
    let s = s.trim();
    let (digits, mult) = match s.chars().last().map(|c| c.to_ascii_lowercase()) {
        Some('k') => (&s[..s.len() - 1], unit),
        Some('m') => (&s[..s.len() - 1], unit * unit),
        Some('g') => (&s[..s.len() - 1], unit * unit * unit),
        _ => (s, 1),
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
        .ok_or_else(|| format!("bad number {s:?}"))
}

fn parse_count(s: &str) -> Result<u64, String> {
    parse_scaled(s, 1000)
}

fn parse_bytes(s: &str) -> Result<u64, String> {
    parse_scaled(s, 1024)
}

fn to_usize(n: u64, what: &str) -> Result<usize, String> {
    usize::try_from(n).map_err(|_| format!("{what} is too large"))
}

/// `Ok(None)` = help.
fn parse(args: &[String]) -> Result<Option<Opts>, String> {
    let mut o = Opts::default();
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--bench" => {}
            "--help" | "-h" => return Ok(None),
            "--backend" => {
                o.backend = match value()?.as_str() {
                    "redb" => Backend::Redb,
                    "mem" => Backend::Mem,
                    other => return Err(format!("unknown backend {other:?}")),
                }
            }
            "--records" => o.records = parse_count(&value()?)?,
            "--value-size" => o.value_size = to_usize(parse_bytes(&value()?)?, "--value-size")?,
            "--batch" => o.batch = to_usize(parse_count(&value()?)?, "--batch")?,
            "--durability" => {
                o.durability = match value()?.as_str() {
                    "immediate" => Durability::Immediate,
                    "deferred" => Durability::Deferred,
                    other => return Err(format!("unknown durability {other:?}")),
                }
            }
            "--api" => {
                o.api = match value()?.as_str() {
                    "put" => Api::Put,
                    "put-many" => Api::PutMany,
                    other => return Err(format!("unknown api {other:?}")),
                }
            }
            "--cache-bytes" => o.cache_bytes = to_usize(parse_bytes(&value()?)?, "--cache-bytes")?,
            "--threads" => {
                o.threads = value()?
                    .split(',')
                    .map(|s| parse_count(s).and_then(|n| to_usize(n, "--threads")))
                    .collect::<Result<_, _>>()?
            }
            "--ops" => o.ops = to_usize(parse_count(&value()?)?, "--ops")?,
            "--warmup" => o.warmup = to_usize(parse_count(&value()?)?, "--warmup")?,
            "--latest-limit" => {
                o.latest_limit = to_usize(parse_count(&value()?)?, "--latest-limit")?
            }
            "--phases" => {
                o.phases = value()?
                    .split(',')
                    .filter(|s| s.trim() != "load")
                    .map(|s| {
                        Phase::ALL
                            .into_iter()
                            .find(|p| p.name() == s.trim())
                            .ok_or_else(|| format!("unknown phase {s:?}"))
                    })
                    .collect::<Result<_, _>>()?
            }
            "--no-reopen" => o.reopen = false,
            "--prewarm" => o.prewarm = true,
            "--seed" => o.seed = parse_count(&value()?)?,
            "--dir" => o.dir = PathBuf::from(value()?),
            "--keep" => o.keep = true,
            other => return Err(format!("unknown flag {other:?}")),
        }
    }
    if o.records == 0 || o.batch == 0 || o.ops == 0 || o.latest_limit == 0 {
        return Err("--records, --batch, --ops and --latest-limit must be >= 1".into());
    }
    if o.threads.is_empty() || o.threads.contains(&0) {
        return Err("--threads needs thread counts >= 1".into());
    }
    Ok(Some(o))
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

fn run(o: &Opts) -> Res<()> {
    let profile = if cfg!(debug_assertions) {
        "DEBUG build"
    } else {
        "release build"
    };
    let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
    println!(
        "store bench ({profile}, {cpus} logical CPUs): backend {:?}, {} S3 records x {} B, \
         batch {}, {:?} commits, api {:?}, cache {} MiB, seed {}",
        o.backend,
        o.records,
        o.value_size,
        o.batch,
        o.durability,
        o.api,
        o.cache_bytes >> 20,
        o.seed
    );
    match o.backend {
        Backend::Redb => {
            std::fs::create_dir_all(&o.dir)?;
            let path = o.dir.join("store.redb");
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            let open = || RedbStore::open(&path, o.cache_bytes);
            let mut store = open()?;
            load(&store, o)?;
            print_files(&store, "after load")?;
            if o.reopen {
                drop(store);
                let t = Instant::now();
                store = open()?;
                println!("reopen: {:.1} ms", ms(t.elapsed()));
            }
            reads(&store, o)?;
            if o.phases.contains(&Phase::Compact) {
                compact(&mut store)?;
            }
            drop(store);
            let size = babeldb::sys::file_size(&path)?;
            println!(
                "file after close: apparent {:.2} MB, allocated {}",
                mb(size.apparent_bytes),
                size.allocated_bytes
                    .map_or_else(|| "unknown".to_string(), |b| format!("{:.2} MB", mb(b)))
            );
            if !o.keep {
                std::fs::remove_file(&path)?;
            }
        }
        Backend::Mem => {
            let mut store = MemStore::new();
            load(&store, o)?;
            reads(&store, o)?;
            if o.phases.contains(&Phase::Compact) {
                compact(&mut store)?;
            }
        }
    }
    Ok(())
}

fn reads<S: Store>(store: &S, o: &Opts) -> Res<()> {
    let keys: Vec<Vec<u8>> = (0..o.records)
        .map(|i| datasets::key(Scenario::ChatJson, o.seed, i))
        .collect();
    if o.prewarm {
        let t = Instant::now();
        let r = store.begin_read()?;
        let mut bytes = 0u64;
        r.scan(
            Table::Records,
            Bound::Unbounded,
            Bound::Unbounded,
            false,
            &mut |k, v| {
                bytes += (k.len() + v.len()) as u64;
                Ok(true)
            },
        )?;
        println!(
            "prewarm: scanned {:.1} MB in {:.2} s",
            mb(bytes),
            t.elapsed().as_secs_f64()
        );
    }
    for phase in [Phase::Get, Phase::Latest, Phase::Begin] {
        if o.phases.contains(&phase) {
            for &threads in &o.threads {
                read_phase(store, o, &keys, phase, threads)?;
            }
        }
    }
    Ok(())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / 1e6
}

/// Nearest rank: the smallest sample with at least `p` % of the samples <= it.
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn latency_summary(lat: &mut [Duration]) -> String {
    lat.sort_unstable();
    let mut s = String::new();
    for (label, p) in [("p50", 50.0), ("p99", 99.0), ("p99.9", 99.9)] {
        let _ = write!(s, "{label} {:.1} us ", us(percentile(lat, p)));
    }
    let max = lat.last().copied().unwrap_or_default();
    let _ = write!(s, "max {:.1} us", us(max));
    s
}

// ---------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------

fn load<S: Store>(store: &S, o: &Opts) -> Res<()> {
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(o.batch);
    let mut commits = Vec::new();
    let (mut build, mut commit) = (Duration::ZERO, Duration::ZERO);
    let mut next = 0u64;
    let mut bytes = 0u64;
    let io_before = babeldb::sys::process_metrics();
    while next < o.records {
        let end = (next + o.batch as u64).min(o.records);
        batch.clear();
        batch.extend(
            (next..end).map(|i| datasets::record(Scenario::ChatJson, o.seed, i, o.value_size)),
        );
        bytes += batch
            .iter()
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum::<u64>();
        let t0 = Instant::now();
        let mut w = store.begin_write()?;
        match o.api {
            Api::Put => {
                for (k, v) in &batch {
                    w.put(Table::Records, k, v)?;
                }
            }
            Api::PutMany => w.put_many(
                Table::Records,
                &mut batch.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
            )?,
        }
        let t1 = Instant::now();
        w.commit(o.durability)?;
        let t2 = Instant::now();
        build += t1 - t0;
        commit += t2 - t1;
        commits.push(t2 - t1);
        next = end;
    }
    if o.durability == Durability::Deferred {
        let t = Instant::now();
        store.begin_write()?.commit(Durability::Immediate)?;
        commit += t.elapsed();
    }
    let io = babeldb::sys::process_metrics();
    let total = build + commit;
    let n = o.records as f64;
    println!(
        "load: {} records ({:.1} MB of keys+values) in {:.2} s = {:.0} records/s; \
         building txns {:.2} s ({:.2} us/record), commits {:.2} s",
        o.records,
        mb(bytes),
        total.as_secs_f64(),
        n / total.as_secs_f64(),
        build.as_secs_f64(),
        us(build) / n,
        commit.as_secs_f64(),
    );
    let written = io.io_write_bytes.saturating_sub(io_before.io_write_bytes);
    let writes = io.io_write_ops.saturating_sub(io_before.io_write_ops);
    println!(
        "  process wrote {:.1} MB in {} write calls: {:.0} B and {:.2} calls per record ({:.1}x)",
        mb(written),
        writes,
        written as f64 / n,
        writes as f64 / n,
        written as f64 / bytes.max(1) as f64
    );
    commits.sort_unstable();
    println!(
        "  commit of one {}-record txn: p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms ({} commits)",
        o.batch,
        ms(percentile(&commits, 50.0)),
        ms(percentile(&commits, 99.0)),
        ms(commits.last().copied().unwrap_or_default()),
        commits.len()
    );
    let r = store.begin_read()?;
    let len = r.len(Table::Records)?;
    if len != o.records {
        return Err(format!("{len} records after the load, want {}", o.records).into());
    }
    Ok(())
}

fn print_files<S: Store>(store: &S, when: &str) -> Res<()> {
    for f in store.files() {
        let size = babeldb::sys::file_size(&f)?;
        println!(
            "file {when}: {} apparent {:.2} MB, allocated {}",
            display_name(&f),
            mb(size.apparent_bytes),
            size.allocated_bytes
                .map_or_else(|| "unknown".to_string(), |b| format!("{:.2} MB", mb(b)))
        );
    }
    Ok(())
}

fn display_name(p: &Path) -> String {
    p.file_name().map_or_else(
        || p.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    )
}

fn compact<S: Store>(store: &mut S) -> Res<()> {
    print_files(store, "before compact")?;
    let t = Instant::now();
    let ran = store.compact()?;
    println!("compact: ran={ran} in {:.2} s", t.elapsed().as_secs_f64());
    print_files(store, "after compact")?;
    let t = Instant::now();
    let n = store.begin_read()?.len(Table::Records)?;
    println!(
        "  {n} records readable after compact ({:.1} ms to count)",
        ms(t.elapsed())
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Concurrent reads
// ---------------------------------------------------------------------------

struct ThreadResult {
    lat: Vec<Duration>,
    end: Instant,
    items: u64,
}

/// One timed read operation; returns the entries it produced.
fn read_op<S: Store>(
    store: &S,
    o: &Opts,
    keys: &[Vec<u8>],
    phase: Phase,
    rng: &mut SplitMix64,
    verify: bool,
    lat: Option<&mut Vec<Duration>>,
) -> Res<u64> {
    let i = rng.below(keys.len() as u64);
    let key = keys[i as usize].as_slice();
    match phase {
        Phase::Get => {
            let t = Instant::now();
            let value = store.begin_read()?.get(Table::Records, key)?;
            let elapsed = t.elapsed();
            let value = value.ok_or("a loaded key is missing")?;
            if verify && value != datasets::value(Scenario::ChatJson, o.seed, i, o.value_size) {
                return Err(format!("wrong value for record {i}").into());
            }
            if let Some(lat) = lat {
                lat.push(elapsed);
            }
            Ok(1)
        }
        Phase::Latest => {
            let prefix = datasets::channel_prefix(datasets::channel_of(i, o.seed));
            let end = prefix_successor(&prefix);
            let end = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
            let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(o.latest_limit);
            let t = Instant::now();
            let r = store.begin_read()?;
            r.scan(
                Table::Records,
                Bound::Included(&prefix),
                end,
                true,
                &mut |k, v| {
                    out.push((k.to_vec(), v.to_vec()));
                    Ok(out.len() < o.latest_limit)
                },
            )?;
            drop(r);
            let elapsed = t.elapsed();
            if out.is_empty() || !out.windows(2).all(|w| w[0].0 > w[1].0) {
                return Err("latest scan: empty or not in descending key order".into());
            }
            if let Some(lat) = lat {
                lat.push(elapsed);
            }
            Ok(out.len() as u64)
        }
        Phase::Begin => {
            let t = Instant::now();
            black_box(store.begin_read()?);
            let elapsed = t.elapsed();
            if let Some(lat) = lat {
                lat.push(elapsed);
            }
            Ok(0)
        }
        Phase::Compact => Err("compact is not a read phase".into()),
    }
}

fn read_phase<S: Store>(
    store: &S,
    o: &Opts,
    keys: &[Vec<u8>],
    phase: Phase,
    threads: usize,
) -> Res<()> {
    let barrier = Barrier::new(threads + 1);
    let (start, results) = std::thread::scope(|scope| -> Res<(Instant, Vec<ThreadResult>)> {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let barrier = &barrier;
                scope.spawn(move || -> Res<ThreadResult> {
                    let seed = o.seed ^ ((t as u64 + 1) << 32) ^ phase as u64;
                    let mut rng = SplitMix64::new(datasets::splitmix64(seed));
                    let warm = (0..o.warmup).try_for_each(|_| {
                        read_op(store, o, keys, phase, &mut rng, false, None).map(drop)
                    });
                    // always reach the barrier, or the other threads would wait forever
                    barrier.wait();
                    warm?;
                    let mut lat = Vec::with_capacity(o.ops);
                    let mut items = 0;
                    for n in 0..o.ops {
                        let verify = n % 1024 == 0;
                        items += read_op(store, o, keys, phase, &mut rng, verify, Some(&mut lat))?;
                    }
                    Ok(ThreadResult {
                        lat,
                        end: Instant::now(),
                        items,
                    })
                })
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        let mut results = Vec::with_capacity(threads);
        for h in handles {
            results.push(h.join().map_err(|_| "reader thread panicked")??);
        }
        Ok((start, results))
    })?;
    let end = results.iter().map(|r| r.end).max().unwrap_or(start);
    let wall = end.duration_since(start);
    let ops = (o.ops * threads) as f64;
    let items: u64 = results.iter().map(|r| r.items).sum();
    let mut lat: Vec<Duration> = results.into_iter().flat_map(|r| r.lat).collect();
    let extra = match phase {
        Phase::Latest => format!(", {:.1} messages/scan", items as f64 / ops),
        _ => String::new(),
    };
    let rate = if wall.is_zero() {
        f64::NAN
    } else {
        ops / wall.as_secs_f64()
    };
    println!(
        "{:<6} x{threads:<2}: {:>9.0} ops/s ({} ops in {:.3} s{extra}); {}",
        phase.name(),
        rate,
        ops,
        wall.as_secs_f64(),
        latency_summary(&mut lat)
    );
    Ok(())
}
