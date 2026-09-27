//! Engine benchmark harness: per-operation latency percentiles, throughput,
//! space accounting and process metrics for every storage variant (spec §11;
//! method and reproduction commands in `docs/benchmarks.md`).
//!
//! ```text
//! cargo bench --bench engine -- [FLAGS]           run the benchmark matrix (--help)
//! cargo bench --bench engine -- gen-manifest      write benchdata/manifest.json
//! cargo bench --bench engine -- --list            list variants, scenarios, backends
//! ```
//!
//! Cargo passes `--bench` to bench binaries. Without it (i.e. under
//! `cargo test --benches` / `--all-targets`) the binary runs its self-tests
//! (percentiles, JSON, argument parsing, samplers) and a smoke run of the
//! whole harness on the in-memory store instead of benchmarking.

use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::ops::Bound;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use babeldb::cache::CacheStats;
use babeldb::config::{CodecPolicy, Config};
use babeldb::datasets::{
    self, DATASETS_VERSION, DEFAULT_SEED, S3_CHANNELS, Scenario, SplitMix64, Zipf,
};
use babeldb::engine::{BatchOp, Db, Expect, ScanOptions, prefix_successor};
use babeldb::stats::{EngineCountersSnapshot, FileSize, Stats};
#[cfg(feature = "lmdb")]
use babeldb::store::heed::HeedStore;
use babeldb::store::mem::MemStore;
use babeldb::store::redb::RedbStore;
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use babeldb::sys::{self, ProcessMetrics};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Res<T> = Result<T, BoxError>;

const SCHEMA: &str = "babeldb-bench-engine/1";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !args.iter().any(|a| a == "--bench") {
        self_test::run();
        return;
    }
    let code = match parse_command(&args) {
        Ok(Command::Help) => {
            print!("{HELP}");
            0
        }
        Ok(Command::List) => {
            print_list();
            0
        }
        Ok(Command::GenManifest(m)) => match gen_manifest(&m) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("error: {e}");
                1
            }
        },
        Ok(Command::Run(o)) => match run_matrix(&o) {
            Ok(true) => 0,
            Ok(false) => 1,
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

const HELP: &str = "\
babeldb engine benchmark (spec section 11; method in docs/benchmarks.md)

USAGE:
    cargo bench --bench engine -- [FLAGS]            run the benchmark matrix
    cargo bench --bench engine -- gen-manifest [..]  write benchdata/manifest.json
    cargo bench --bench engine -- --list             list variants, scenarios, backends

Sizes accept k/m/g (KiB/MiB/GiB); counts accept k/m (10^3/10^6); lists are comma-separated.

FLAGS:
    --backend redb|lmdb|mem       storage backend [redb]; lmdb needs --features lmdb;
                                  mem = harness smoke test only (not a benchmark)
    --variant LIST|all            raw-backend,engine-raw,babel-pure,lz4,zstd,
                                  adaptive-nodedupe,adaptive [all]
    --scenario LIST|all           s1..s6 [all]
    --records N                   records loaded [20k]
    --value-size B                target value size [1024]
    --block-size B                engine block size [16k]
    --inline-max B                engine inline threshold [1024]
    --cache-bytes B               engine block cache [64m]
    --backend-cache-bytes B       backend page cache (redb) [64m]
    --lmdb-map-size B             LMDB map size [4 x estimated data + 64m]
    --batch N                     records per write_batch during the load [1000]
    --durable-puts N              single puts, one Immediate commit each [200]
    --ops N                       measured ops per read phase and per reader thread [10k]
    --warmup N                    unrecorded ops before each read phase [= --ops]
    --dist uniform|zipf|latest    key popularity; zipf is scrambled [uniform]
    --zipf-theta F                Zipf exponent in (0,1) [0.99]
    --readers LIST                threads of the mt-get phase, e.g. 1,4,16 [logical CPUs]
    --mix read-only|95-5|50-50    mixed reads + durable puts phase [read-only = skipped]
    --mixed-ops N                 ops per thread in the mixed phase [1000]
    --range-len B                 bytes per get_range [100]
    --latest-limit N              messages per latest-in-channel scan (s3) [50]
    --phases LIST                 put,get,miss,range,latest,mt-get,mixed [all];
                                  load and space always run
    --verify-every N              fully compare every N-th value read (0 = lengths only) [64]
    --no-reopen                   keep the database open between phases
                                  (default: close and reopen before each read phase)
    --seed N                      dataset seed [42]
    --dir PATH                    parent directory of the databases [bench-results/tmp]
    --keep                        keep the databases after the run
    --out PATH                    JSON lines output, appended [bench-results/engine.jsonl]
    --no-out                      do not write JSON lines
    --tag TEXT                    label stored in every JSON line (e.g. rep1)
    --note TEXT                   free text stored in env (e.g. the disk model)
    --mem-limit B                 cap the process commit charge with a job object (Windows)
    --list | --help

gen-manifest FLAGS:
    --out PATH [benchdata/manifest.json]   --seed N [42]   --records N [10000]
    --sizes LIST [64,256,1k,4k,16k,64k]
";

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backend {
    Redb,
    Lmdb,
    Mem,
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Backend::Redb => "redb",
            Backend::Lmdb => "lmdb",
            Backend::Mem => "mem",
        }
    }

    fn parse(s: &str) -> Option<Backend> {
        [Backend::Redb, Backend::Lmdb, Backend::Mem]
            .into_iter()
            .find(|b| b.name() == s)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    RawBackend,
    EngineRaw,
    BabelPure,
    Lz4,
    Zstd,
    AdaptiveNoDedupe,
    Adaptive,
}

impl Variant {
    const ALL: [Variant; 7] = [
        Variant::RawBackend,
        Variant::EngineRaw,
        Variant::BabelPure,
        Variant::Lz4,
        Variant::Zstd,
        Variant::AdaptiveNoDedupe,
        Variant::Adaptive,
    ];

    fn name(self) -> &'static str {
        match self {
            Variant::RawBackend => "raw-backend",
            Variant::EngineRaw => "engine-raw",
            Variant::BabelPure => "babel-pure",
            Variant::Lz4 => "lz4",
            Variant::Zstd => "zstd",
            Variant::AdaptiveNoDedupe => "adaptive-nodedupe",
            Variant::Adaptive => "adaptive",
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Variant::RawBackend => {
                "keys/values written directly to Table::Records through the Store trait, no engine format (backend reference)"
            }
            Variant::EngineRaw => {
                "engine with RawV1 only, no dedupe (Config::raw_only): format overhead baseline"
            }
            Variant::BabelPure => {
                "Mode::BabelPure: every unit stored as its BabelAffineV1 seed (no dedupe)"
            }
            Variant::Lz4 => "Adaptive restricted to Lz4V1 + RawV1, no dedupe",
            Variant::Zstd => "Adaptive restricted to ZstdV1 (no dictionary) + RawV1, no dedupe",
            Variant::AdaptiveNoDedupe => "Adaptive with the default codec policy, dedupe off",
            Variant::Adaptive => "Adaptive with the default codec policy and byte-verified dedupe",
        }
    }

    fn parse(s: &str) -> Option<Variant> {
        Variant::ALL.into_iter().find(|v| v.name() == s)
    }

    /// Engine configuration (None for the raw backend reference).
    fn config(self, o: &Opts) -> Option<Config> {
        let mut c = match self {
            Variant::RawBackend => return None,
            Variant::EngineRaw => Config::raw_only(),
            Variant::BabelPure => Config::babel_pure(),
            Variant::Lz4 => Config {
                codecs: CodecPolicy {
                    lz4: true,
                    ..CodecPolicy::raw_only()
                },
                dedupe: false,
                ..Config::adaptive()
            },
            Variant::Zstd => Config {
                codecs: CodecPolicy {
                    zstd: true,
                    ..CodecPolicy::raw_only()
                },
                dedupe: false,
                ..Config::adaptive()
            },
            Variant::AdaptiveNoDedupe => Config {
                dedupe: false,
                ..Config::adaptive()
            },
            Variant::Adaptive => Config::adaptive(),
        };
        c.block_size = o.block_size;
        c.inline_max = o.inline_max;
        c.cache_bytes = o.cache_bytes;
        c.backend_cache_bytes = o.backend_cache_bytes;
        Some(c)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dist {
    Uniform,
    Zipf,
    Latest,
}

impl Dist {
    fn name(self) -> &'static str {
        match self {
            Dist::Uniform => "uniform",
            Dist::Zipf => "zipf",
            Dist::Latest => "latest",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mix {
    ReadOnly,
    R95W5,
    R50W50,
}

impl Mix {
    fn name(self) -> &'static str {
        match self {
            Mix::ReadOnly => "read-only",
            Mix::R95W5 => "95-5",
            Mix::R50W50 => "50-50",
        }
    }

    fn write_per_mille(self) -> u64 {
        match self {
            Mix::ReadOnly => 0,
            Mix::R95W5 => 50,
            Mix::R50W50 => 500,
        }
    }
}

/// Optional phases (load and space always run).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Put,
    Get,
    Miss,
    Range,
    Latest,
    MtGet,
    Mixed,
}

impl Phase {
    const ALL: [Phase; 7] = [
        Phase::Put,
        Phase::Get,
        Phase::Miss,
        Phase::Range,
        Phase::Latest,
        Phase::MtGet,
        Phase::Mixed,
    ];

    /// Name used in the JSON lines.
    fn name(self) -> &'static str {
        match self {
            Phase::Put => "put-durable",
            Phase::Get => "get",
            Phase::Miss => "miss",
            Phase::Range => "range",
            Phase::Latest => "latest",
            Phase::MtGet => "mt-get",
            Phase::Mixed => "mixed",
        }
    }

    fn parse(s: &str) -> Option<Phase> {
        match s {
            "put" => Some(Phase::Put),
            "mt" => Some(Phase::MtGet),
            _ => Phase::ALL.into_iter().find(|p| p.name() == s),
        }
    }
}

#[derive(Clone, Debug)]
struct Opts {
    backend: Backend,
    variants: Vec<Variant>,
    scenarios: Vec<Scenario>,
    records: u64,
    value_size: usize,
    block_size: u32,
    inline_max: u32,
    cache_bytes: usize,
    backend_cache_bytes: usize,
    lmdb_map_size: Option<usize>,
    batch: usize,
    durable_puts: u64,
    ops: u64,
    warmup: u64,
    dist: Dist,
    zipf_theta: f64,
    readers: Vec<usize>,
    mix: Mix,
    mixed_ops: u64,
    range_len: u64,
    latest_limit: usize,
    phases: Vec<Phase>,
    verify_every: u64,
    reopen: bool,
    seed: u64,
    dir: PathBuf,
    keep: bool,
    out: Option<PathBuf>,
    tag: String,
    note: String,
    mem_limit: Option<u64>,
}

fn logical_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

impl Default for Opts {
    fn default() -> Opts {
        let c = Config::default();
        Opts {
            backend: Backend::Redb,
            variants: Variant::ALL.to_vec(),
            scenarios: Scenario::ALL.to_vec(),
            records: 20_000,
            value_size: 1024,
            block_size: c.block_size,
            inline_max: c.inline_max,
            cache_bytes: c.cache_bytes,
            backend_cache_bytes: c.backend_cache_bytes,
            lmdb_map_size: None,
            batch: 1000,
            durable_puts: 200,
            ops: 10_000,
            warmup: 10_000,
            dist: Dist::Uniform,
            zipf_theta: 0.99,
            readers: vec![logical_cpus()],
            mix: Mix::ReadOnly,
            mixed_ops: 1000,
            range_len: 100,
            latest_limit: 50,
            phases: Phase::ALL.to_vec(),
            verify_every: 64,
            reopen: true,
            seed: DEFAULT_SEED,
            dir: PathBuf::from("bench-results/tmp"),
            keep: false,
            out: Some(PathBuf::from("bench-results/engine.jsonl")),
            tag: String::new(),
            note: String::new(),
            mem_limit: None,
        }
    }
}

impl Opts {
    fn has(&self, p: Phase) -> bool {
        self.phases.contains(&p)
    }
}

#[derive(Clone, Debug)]
struct ManifestOpts {
    out: PathBuf,
    seed: u64,
    records: u64,
    sizes: Vec<usize>,
}

impl Default for ManifestOpts {
    fn default() -> ManifestOpts {
        ManifestOpts {
            out: PathBuf::from("benchdata/manifest.json"),
            seed: DEFAULT_SEED,
            records: 10_000,
            sizes: vec![64, 256, 1024, 4096, 16 * 1024, 64 * 1024],
        }
    }
}

#[derive(Debug)]
enum Command {
    Run(Box<Opts>),
    List,
    Help,
    GenManifest(ManifestOpts),
}

/// Bytes with optional binary suffix: `64`, `4k`, `4KiB`, `16m`, `1g`, `1_000`.
fn parse_bytes(s: &str) -> Res<u64> {
    parse_scaled(s, 1024).map_err(|e| format!("invalid size {s:?}: {e}").into())
}

/// Counts with optional decimal suffix: `100k` = 100 000, `2m` = 2 000 000.
fn parse_count(s: &str) -> Res<u64> {
    parse_scaled(s, 1000).map_err(|e| format!("invalid count {s:?}: {e}").into())
}

fn parse_scaled(s: &str, base: u64) -> Result<u64, String> {
    let t = s.trim().to_ascii_lowercase().replace('_', "");
    let digits_end = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, suffix) = t.split_at(digits_end);
    if num.is_empty() {
        return Err("expected a number".into());
    }
    let n: u64 = num.parse().map_err(|e| format!("{e}"))?;
    let mult = match suffix {
        "" | "b" => 1,
        "k" | "kb" | "kib" => base,
        "m" | "mb" | "mib" => base * base,
        "g" | "gb" | "gib" => base * base * base,
        other => return Err(format!("unknown suffix {other:?}")),
    };
    n.checked_mul(mult).ok_or_else(|| "overflow".to_string())
}

fn parse_list<T>(s: &str, what: &str, all: &[T], one: impl Fn(&str) -> Option<T>) -> Res<Vec<T>>
where
    T: Copy + PartialEq,
{
    if s.trim() == "all" {
        return Ok(all.to_vec());
    }
    let mut out = Vec::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let v = one(part).ok_or_else(|| format!("unknown {what} {part:?}"))?;
        if !out.contains(&v) {
            out.push(v);
        }
    }
    if out.is_empty() {
        return Err(format!("empty {what} list").into());
    }
    Ok(out)
}

fn usize_of(v: u64, what: &str) -> Res<usize> {
    usize::try_from(v).map_err(|_| format!("{what} too large").into())
}

fn parse_command(args: &[String]) -> Res<Command> {
    let mut args: Vec<String> = args
        .iter()
        .filter(|a| a.as_str() != "--bench")
        .cloned()
        .collect();
    let manifest = match args.first().map(String::as_str) {
        Some("gen-manifest") => {
            args.remove(0);
            true
        }
        Some("run") => {
            args.remove(0);
            false
        }
        Some("list") => return Ok(Command::List),
        Some("help") => return Ok(Command::Help),
        _ => false,
    };
    let mut o = Opts::default();
    let mut m = ManifestOpts::default();
    let mut warmup: Option<u64> = None;
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) if arg.starts_with("--") => (f.to_string(), Some(v.to_string())),
            _ => (arg, None),
        };
        let mut value = || -> Res<String> {
            match inline.clone() {
                Some(v) => Ok(v),
                None => it
                    .next()
                    .ok_or_else(|| format!("{flag} needs a value").into()),
            }
        };
        if manifest {
            match flag.as_str() {
                "--out" => m.out = PathBuf::from(value()?),
                "--seed" => m.seed = parse_count(&value()?)?,
                "--records" => m.records = parse_count(&value()?)?,
                "--sizes" => {
                    let list = value()?;
                    m.sizes = list
                        .split(',')
                        .map(|s| parse_bytes(s).and_then(|b| usize_of(b, "size")))
                        .collect::<Res<Vec<usize>>>()?;
                }
                other => return Err(format!("unknown gen-manifest argument {other:?}").into()),
            }
            continue;
        }
        match flag.as_str() {
            "--help" | "-h" => return Ok(Command::Help),
            "--list" => return Ok(Command::List),
            "--backend" => {
                let v = value()?;
                o.backend = Backend::parse(&v).ok_or_else(|| format!("unknown backend {v:?}"))?;
            }
            "--variant" | "--variants" => {
                o.variants = parse_list(&value()?, "variant", &Variant::ALL, Variant::parse)?
            }
            "--scenario" | "--scenarios" => {
                o.scenarios = parse_list(&value()?, "scenario", &Scenario::ALL, Scenario::parse)?
            }
            "--records" => o.records = parse_count(&value()?)?,
            "--value-size" => o.value_size = usize_of(parse_bytes(&value()?)?, "--value-size")?,
            "--block-size" => o.block_size = u32::try_from(parse_bytes(&value()?)?)?,
            "--inline-max" => o.inline_max = u32::try_from(parse_bytes(&value()?)?)?,
            "--cache-bytes" => o.cache_bytes = usize_of(parse_bytes(&value()?)?, "--cache-bytes")?,
            "--backend-cache-bytes" => {
                o.backend_cache_bytes = usize_of(parse_bytes(&value()?)?, "--backend-cache-bytes")?
            }
            "--lmdb-map-size" => {
                o.lmdb_map_size = Some(usize_of(parse_bytes(&value()?)?, "--lmdb-map-size")?)
            }
            "--batch" => o.batch = usize_of(parse_count(&value()?)?, "--batch")?,
            "--durable-puts" => o.durable_puts = parse_count(&value()?)?,
            "--ops" => o.ops = parse_count(&value()?)?,
            "--warmup" => warmup = Some(parse_count(&value()?)?),
            "--dist" => {
                let v = value()?;
                o.dist = [Dist::Uniform, Dist::Zipf, Dist::Latest]
                    .into_iter()
                    .find(|d| d.name() == v)
                    .ok_or_else(|| format!("unknown distribution {v:?}"))?;
            }
            "--zipf-theta" => o.zipf_theta = value()?.parse()?,
            "--readers" => {
                o.readers = value()?
                    .split(',')
                    .map(|s| parse_count(s).and_then(|n| usize_of(n, "--readers")))
                    .collect::<Res<Vec<usize>>>()?;
            }
            "--mix" => {
                let v = value()?;
                o.mix = [Mix::ReadOnly, Mix::R95W5, Mix::R50W50]
                    .into_iter()
                    .find(|x| x.name() == v)
                    .ok_or_else(|| format!("unknown mix {v:?}"))?;
            }
            "--mixed-ops" => o.mixed_ops = parse_count(&value()?)?,
            "--range-len" => o.range_len = parse_bytes(&value()?)?,
            "--latest-limit" => {
                o.latest_limit = usize_of(parse_count(&value()?)?, "--latest-limit")?
            }
            "--phases" => o.phases = parse_list(&value()?, "phase", &Phase::ALL, Phase::parse)?,
            "--verify-every" => o.verify_every = parse_count(&value()?)?,
            "--no-reopen" => o.reopen = false,
            "--seed" => o.seed = parse_count(&value()?)?,
            "--dir" => o.dir = PathBuf::from(value()?),
            "--keep" => o.keep = true,
            "--out" => o.out = Some(PathBuf::from(value()?)),
            "--no-out" => o.out = None,
            "--tag" => o.tag = value()?,
            "--note" => o.note = value()?,
            "--mem-limit" => o.mem_limit = Some(parse_bytes(&value()?)?),
            other => return Err(format!("unknown argument {other:?}").into()),
        }
    }
    if manifest {
        if m.sizes.is_empty() || m.records == 0 {
            return Err("gen-manifest needs --records >= 1 and at least one size".into());
        }
        return Ok(Command::GenManifest(m));
    }
    o.warmup = warmup.unwrap_or(o.ops);
    if o.records == 0 || o.batch == 0 || o.latest_limit == 0 {
        return Err("--records, --batch and --latest-limit must be >= 1".into());
    }
    if o.readers.is_empty() || o.readers.contains(&0) {
        return Err("--readers needs thread counts >= 1".into());
    }
    if !(o.zipf_theta > 0.0 && o.zipf_theta < 1.0) {
        return Err("--zipf-theta must be in (0, 1)".into());
    }
    if o.inline_max > o.block_size {
        return Err("--inline-max must be <= --block-size".into());
    }
    Ok(Command::Run(Box::new(o)))
}

fn print_list() {
    println!("variants:");
    for v in Variant::ALL {
        println!("  {:<18} {}", v.name(), v.describe());
    }
    println!("scenarios:");
    for s in Scenario::ALL {
        println!("  {} {:<16} {}", s.code(), s.name(), s.description());
    }
    println!("backends:");
    println!(
        "  redb   single-file B-tree, Durability::Immediate = fsync (FlushFileBuffers on Windows)"
    );
    println!(
        "  lmdb   heed/LMDB (compiled in: {})",
        if cfg!(feature = "lmdb") {
            "yes"
        } else {
            "no, rebuild with --features lmdb"
        }
    );
    println!("  mem    in-memory reference store: harness smoke test only, not a benchmark");
    println!(
        "phases: load, space (always); put-durable, get, miss, range, latest (s3), mt-get, mixed; space-final"
    );
    println!(
        "dists: uniform, zipf (scrambled, theta 0.99), latest (zipf over recency); mixes: read-only, 95-5, 50-50"
    );
}

// ---------------------------------------------------------------------------
// JSON (hand-written, no serde)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum J {
    Null,
    Bool(bool),
    U(u64),
    I(i64),
    F(f64),
    S(String),
    A(Vec<J>),
    O(Vec<(String, J)>),
}

impl From<bool> for J {
    fn from(v: bool) -> J {
        J::Bool(v)
    }
}

impl From<u64> for J {
    fn from(v: u64) -> J {
        J::U(v)
    }
}

impl From<u32> for J {
    fn from(v: u32) -> J {
        J::U(v.into())
    }
}

impl From<usize> for J {
    fn from(v: usize) -> J {
        J::U(v as u64)
    }
}

impl From<i64> for J {
    fn from(v: i64) -> J {
        J::I(v)
    }
}

impl From<f64> for J {
    fn from(v: f64) -> J {
        J::F(v)
    }
}

impl From<&str> for J {
    fn from(v: &str) -> J {
        J::S(v.to_string())
    }
}

impl From<String> for J {
    fn from(v: String) -> J {
        J::S(v)
    }
}

impl From<Vec<J>> for J {
    fn from(v: Vec<J>) -> J {
        J::A(v)
    }
}

impl<T: Into<J>> From<Option<T>> for J {
    fn from(v: Option<T>) -> J {
        v.map_or(J::Null, Into::into)
    }
}

impl J {
    fn obj() -> J {
        J::O(Vec::new())
    }

    fn with(mut self, key: &str, value: impl Into<J>) -> J {
        match &mut self {
            J::O(fields) => fields.push((key.to_string(), value.into())),
            _ => panic!("J::with on a non-object"),
        }
        self
    }

    /// Append the fields of `other` (both must be objects).
    fn merge(mut self, other: J) -> J {
        if let (J::O(a), J::O(b)) = (&mut self, other) {
            a.extend(b);
        }
        self
    }

    fn get(&self, key: &str) -> Option<&J> {
        match self {
            J::O(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    fn compact(&self) -> String {
        let mut s = String::new();
        self.write(&mut s, None, 0);
        s
    }

    fn pretty(&self) -> String {
        let mut s = String::new();
        self.write(&mut s, Some(2), 0);
        s.push('\n');
        s
    }

    fn write(&self, out: &mut String, indent: Option<usize>, depth: usize) {
        match self {
            J::Null => out.push_str("null"),
            J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            J::U(v) => {
                let _ = write!(out, "{v}");
            }
            J::I(v) => {
                let _ = write!(out, "{v}");
            }
            J::F(v) if v.is_finite() => {
                let _ = write!(out, "{v}");
            }
            J::F(_) => out.push_str("null"),
            J::S(s) => write_json_string(out, s),
            J::A(items) => {
                write_container(out, ('[', ']'), items.len(), indent, depth, |out, i| {
                    items[i].write(out, indent, depth + 1);
                })
            }
            J::O(fields) => {
                write_container(out, ('{', '}'), fields.len(), indent, depth, |out, i| {
                    write_json_string(out, &fields[i].0);
                    out.push(':');
                    if indent.is_some() {
                        out.push(' ');
                    }
                    fields[i].1.write(out, indent, depth + 1);
                })
            }
        }
    }
}

fn write_container(
    out: &mut String,
    (open, close): (char, char),
    len: usize,
    indent: Option<usize>,
    depth: usize,
    mut item: impl FnMut(&mut String, usize),
) {
    out.push(open);
    if len > 0 {
        for i in 0..len {
            if i > 0 {
                out.push(',');
            }
            if let Some(w) = indent {
                out.push('\n');
                out.extend(std::iter::repeat_n(' ', w * (depth + 1)));
            }
            item(out, i);
        }
        if let Some(w) = indent {
            out.push('\n');
            out.extend(std::iter::repeat_n(' ', w * depth));
        }
    }
    out.push(close);
}

fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Latency summaries and process metrics
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq)]
struct Summary {
    count: u64,
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    p999: u64,
    max: u64,
    mean: f64,
    sum: u64,
}

/// Nearest-rank percentile of ascending samples, `q` in parts per 100 000
/// (p99.9 = 99 900): the smallest sample with at least q% of the samples at
/// or below it. Integer arithmetic, so p99.9 of 1000 samples is the 999th.
fn percentile(sorted: &[u64], q: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let n = sorted.len() as u128;
    let rank = ((n * u128::from(q)).div_ceil(100_000)).clamp(1, n);
    sorted[rank as usize - 1]
}

fn summarize(mut v: Vec<u64>) -> Summary {
    if v.is_empty() {
        return Summary::default();
    }
    v.sort_unstable();
    let sum: u64 = v.iter().sum();
    Summary {
        count: v.len() as u64,
        min: v[0],
        p50: percentile(&v, 50_000),
        p95: percentile(&v, 95_000),
        p99: percentile(&v, 99_000),
        p999: percentile(&v, 99_900),
        max: v[v.len() - 1],
        mean: sum as f64 / v.len() as f64,
        sum,
    }
}

impl Summary {
    fn json(&self) -> J {
        J::obj()
            .with("count", self.count)
            .with("min", self.min)
            .with("p50", self.p50)
            .with("p95", self.p95)
            .with("p99", self.p99)
            .with("p999", self.p999)
            .with("max", self.max)
            .with("mean", self.mean)
            .with("sum", self.sum)
    }

    fn line(&self) -> String {
        format!(
            "p50 {} p95 {} p99 {} p99.9 {} max {}",
            fmt_ns(self.p50),
            fmt_ns(self.p95),
            fmt_ns(self.p99),
            fmt_ns(self.p999),
            fmt_ns(self.max)
        )
    }

    /// Operations per second of one thread: count / sum of latencies.
    fn ops_per_s(&self) -> f64 {
        rate(self.count as f64, self.sum)
    }
}

fn ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// `amount` per second over `elapsed_ns` (0 when nothing elapsed).
fn rate(amount: f64, elapsed_ns: u64) -> f64 {
    if elapsed_ns == 0 {
        0.0
    } else {
        amount * 1e9 / elapsed_ns as f64
    }
}

fn ratio(a: u64, b: u64) -> Option<f64> {
    (b > 0).then(|| a as f64 / b as f64)
}

fn fmt_ns(v: u64) -> String {
    match v {
        v if v >= 1_000_000_000 => format!("{:.2}s", v as f64 / 1e9),
        v if v >= 1_000_000 => format!("{:.2}ms", v as f64 / 1e6),
        v if v >= 1_000 => format!("{:.1}us", v as f64 / 1e3),
        v => format!("{v}ns"),
    }
}

/// Difference of the cumulative process counters between two snapshots.
#[derive(Clone, Debug, Default, PartialEq)]
struct ProcDelta {
    user_cpu_ms: f64,
    kernel_cpu_ms: f64,
    page_faults: u64,
    io_read_bytes: u64,
    io_write_bytes: u64,
    io_read_ops: u64,
    io_write_ops: u64,
}

impl ProcDelta {
    fn between(a: &ProcessMetrics, b: &ProcessMetrics) -> ProcDelta {
        ProcDelta {
            user_cpu_ms: (b.user_cpu_ms - a.user_cpu_ms).max(0.0),
            kernel_cpu_ms: (b.kernel_cpu_ms - a.kernel_cpu_ms).max(0.0),
            page_faults: b.page_faults.saturating_sub(a.page_faults),
            io_read_bytes: b.io_read_bytes.saturating_sub(a.io_read_bytes),
            io_write_bytes: b.io_write_bytes.saturating_sub(a.io_write_bytes),
            io_read_ops: b.io_read_ops.saturating_sub(a.io_read_ops),
            io_write_ops: b.io_write_ops.saturating_sub(a.io_write_ops),
        }
    }

    fn add(&mut self, o: &ProcDelta) {
        self.user_cpu_ms += o.user_cpu_ms;
        self.kernel_cpu_ms += o.kernel_cpu_ms;
        self.page_faults += o.page_faults;
        self.io_read_bytes += o.io_read_bytes;
        self.io_write_bytes += o.io_write_bytes;
        self.io_read_ops += o.io_read_ops;
        self.io_write_ops += o.io_write_ops;
    }

    fn json(&self) -> J {
        J::obj()
            .with("user_cpu_ms", self.user_cpu_ms)
            .with("kernel_cpu_ms", self.kernel_cpu_ms)
            .with("page_faults", self.page_faults)
            .with("io_read_bytes", self.io_read_bytes)
            .with("io_write_bytes", self.io_write_bytes)
            .with("io_read_ops", self.io_read_ops)
            .with("io_write_ops", self.io_write_ops)
    }
}

fn metrics_json(m: &ProcessMetrics) -> J {
    J::obj()
        .with("working_set_bytes", m.working_set_bytes)
        .with("peak_working_set_bytes", m.peak_working_set_bytes)
        .with("private_bytes", m.private_bytes)
        .with("page_faults", m.page_faults)
        .with("user_cpu_ms", m.user_cpu_ms)
        .with("kernel_cpu_ms", m.kernel_cpu_ms)
        .with("io_read_bytes", m.io_read_bytes)
        .with("io_write_bytes", m.io_write_bytes)
        .with("io_read_ops", m.io_read_ops)
        .with("io_write_ops", m.io_write_ops)
}

fn file_json(f: &FileSize) -> J {
    J::obj()
        .with("path", f.path.display().to_string())
        .with("apparent_bytes", f.apparent_bytes)
        .with("allocated_bytes", f.allocated_bytes)
}

fn counters_json(c: &EngineCountersSnapshot) -> J {
    J::obj()
        .with("gets", c.gets)
        .with("puts", c.puts)
        .with("deletes", c.deletes)
        .with("commits", c.commits)
        .with("bytes_requested", c.bytes_requested)
        .with("bytes_reconstructed", c.bytes_reconstructed)
        .with("units_decoded", c.units_decoded)
        .with("dedupe_hits", c.dedupe_hits)
        .with("objects_written", c.objects_written)
}

fn cache_json(c: &CacheStats) -> J {
    J::obj()
        .with("capacity_bytes", c.capacity_bytes)
        .with("used_bytes", c.used_bytes)
        .with("entries", c.entries)
        .with("hits", c.hits)
        .with("misses", c.misses)
        .with("insertions", c.insertions)
        .with("evictions", c.evictions)
}

fn stats_json(s: &Stats) -> J {
    let per_codec: Vec<J> = s
        .per_codec
        .iter()
        .map(|c| {
            J::obj()
                .with("codec", c.codec.as_str())
                .with("units", c.units)
                .with("raw_bytes", c.raw_bytes)
                .with("body_bytes", c.body_bytes)
                .with("envelope_bytes", c.envelope_bytes)
        })
        .collect();
    let chosen = J::O(
        s.planner
            .chosen
            .iter()
            .map(|(k, v)| (k.clone(), J::U(*v)))
            .collect(),
    );
    J::obj()
        .with("backend", s.backend)
        .with("mode", s.mode.as_str())
        .with("block_size", s.block_size)
        .with("inline_max", s.inline_max)
        .with("records", s.records)
        .with("tombstones", s.tombstones)
        .with("objects", s.objects)
        .with("hash_candidates", s.hash_candidates)
        .with("params", s.params)
        .with("history_entries", s.history_entries)
        .with("sources", s.sources)
        .with("pending_imports", s.pending_imports)
        .with("logical_bytes", s.logical_bytes)
        .with("key_bytes", s.key_bytes)
        .with("manifest_bytes", s.manifest_bytes)
        .with("inline_envelope_bytes", s.inline_envelope_bytes)
        .with("object_bytes", s.object_bytes)
        .with("candidate_bytes", s.candidate_bytes)
        .with("refcount_bytes", s.refcount_bytes)
        .with("param_bytes", s.param_bytes)
        .with("history_bytes", s.history_bytes)
        .with("source_bytes", s.source_bytes)
        .with("payload_bytes", s.payload_bytes())
        .with(
            "payload_over_logical",
            ratio(s.payload_bytes(), s.logical_bytes),
        )
        .with("file_apparent_bytes", s.file_apparent_bytes())
        .with("file_allocated_bytes", s.file_allocated_bytes())
        .with("files", J::A(s.files.iter().map(file_json).collect()))
        .with("per_codec", per_codec)
        .with(
            "planner",
            J::obj()
                .with("chosen", chosen)
                .with("budget_fallbacks", s.planner.budget_fallbacks)
                .with("roundtrip_failures", s.planner.roundtrip_failures),
        )
        .with("cache", cache_json(&s.cache))
        .with("counters", counters_json(&s.counters))
}

fn config_json(c: &Config) -> J {
    J::obj()
        .with("mode", c.mode.as_str())
        .with("block_size", c.block_size)
        .with("inline_max", c.inline_max)
        .with("dedupe", c.dedupe)
        .with("effective_dedupe", c.effective_dedupe(c.mode))
        .with("verify_on_read", c.verify_on_read)
        .with("keep_history", c.keep_history)
        .with("cache_bytes", c.cache_bytes)
        .with("backend_cache_bytes", c.backend_cache_bytes)
        .with(
            "codecs",
            J::obj()
                .with("recipes", c.codecs.recipes)
                .with("lz4", c.codecs.lz4)
                .with("zstd", c.codecs.zstd)
                .with("zstd_level", i64::from(c.codecs.zstd_level))
                .with("zstd_dictionary", c.codecs.zstd_dictionary)
                .with("template", c.codecs.template)
                .with("decode_budget_ns", c.codecs.decode_budget_ns),
        )
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

struct Env {
    run_id: String,
    json: J,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// stdout of a command, trimmed (None when it cannot run or fails).
fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(program)
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn cpu_name() -> Option<String> {
    if cfg!(windows) {
        let out = command_output(
            "reg",
            &[
                "query",
                r"HKLM\HARDWARE\DESCRIPTION\System\CentralProcessor\0",
                "/v",
                "ProcessorNameString",
            ],
        );
        if let Some(name) = out
            .as_deref()
            .and_then(|o| o.split("REG_SZ").nth(1))
            .map(str::trim)
        {
            return Some(name.to_string());
        }
        return std::env::var("PROCESSOR_IDENTIFIER").ok();
    }
    let info = fs::read_to_string("/proc/cpuinfo").ok()?;
    info.lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split(':').nth(1))
        .map(|s| s.trim().to_string())
}

fn os_version() -> Option<String> {
    if cfg!(windows) {
        // "Microsoft Windows [Version 10.0.26200.6584]" (localized): keep the number.
        let out = command_output("cmd", &["/C", "ver"])?;
        let token = out.split_whitespace().next_back()?.trim_end_matches(']');
        return Some(token.to_string());
    }
    fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .map(|s| s.trim().to_string())
}

fn collect_env(o: &Opts) -> Env {
    let run_id = format!("{}-{}", now_ms(), std::process::id());
    let mem = sys::memory_status();
    let vol = sys::volume_info(&o.dir).ok();
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let lock = fs::read(manifest_dir.join("Cargo.lock"))
        .ok()
        .map(|b| hex(blake3::hash(&b).as_bytes()));
    let dirty = command_output("git", &["status", "--porcelain", "--untracked-files=no"])
        .map(|s| !s.is_empty());
    let mut features = Vec::new();
    if cfg!(feature = "lmdb") {
        features.push(J::from("lmdb"));
    }
    let json = J::obj()
        .with("os", std::env::consts::OS)
        .with("os_version", os_version())
        .with("arch", std::env::consts::ARCH)
        .with("cpu", cpu_name())
        .with("logical_cpus", logical_cpus())
        .with("ram_total_bytes", mem.as_ref().map(|m| m.total_bytes))
        .with(
            "ram_available_bytes_at_start",
            mem.as_ref().map(|m| m.available_bytes),
        )
        .with(
            "fs",
            vol.map(|v| {
                J::obj()
                    .with("root", v.root.display().to_string())
                    .with("file_system", v.file_system)
                    .with("cluster_bytes", v.cluster_bytes)
                    .with("total_bytes", v.total_bytes)
                    .with("free_bytes", v.free_bytes)
            }),
        )
        .with("rustc", command_output("rustc", &["--version"]))
        .with(
            "profile",
            if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        )
        .with("features", features)
        .with("git_commit", command_output("git", &["rev-parse", "HEAD"]))
        .with("git_dirty", dirty)
        .with("cargo_lock_blake3", lock)
        .with("datasets_version", DATASETS_VERSION)
        .with("zstd", zstd::zstd_safe::version_string())
        .with(
            "bench_exe_bytes",
            std::env::current_exe()
                .and_then(fs::metadata)
                .ok()
                .map(|m| m.len()),
        )
        .with("note", o.note.as_str());
    Env { run_id, json }
}

fn params_json(o: &Opts, variant: Variant) -> J {
    let mut p = J::obj()
        .with("records", o.records)
        .with("value_size", o.value_size)
        .with("seed", o.seed)
        .with("block_size", o.block_size)
        .with("inline_max", o.inline_max)
        .with("cache_bytes", o.cache_bytes)
        .with("backend_cache_bytes", o.backend_cache_bytes)
        .with(
            "lmdb_map_size",
            (o.backend == Backend::Lmdb).then(|| lmdb_map_size(o)),
        )
        .with("batch", o.batch)
        .with("durable_puts", o.durable_puts)
        .with("ops", o.ops)
        .with("warmup", o.warmup)
        .with("dist", o.dist.name())
        .with("zipf_theta", o.zipf_theta)
        .with(
            "readers",
            J::A(o.readers.iter().map(|&r| J::from(r)).collect()),
        )
        .with("mix", o.mix.name())
        .with("mixed_ops", o.mixed_ops)
        .with("range_len", o.range_len)
        .with("latest_limit", o.latest_limit)
        .with("verify_every", o.verify_every)
        .with("reopen", o.reopen)
        .with("mem_limit", o.mem_limit);
    if let Some(c) = variant.config(o) {
        p = p.with("engine_config", config_json(&c));
    }
    p
}

fn base_json(o: &Opts, env: &Env, variant: Variant, scenario: Scenario, phase: &str) -> J {
    J::obj()
        .with("schema", SCHEMA)
        .with("run_id", env.run_id.as_str())
        .with("ts_unix_ms", now_ms())
        .with("tag", o.tag.as_str())
        .with("backend", o.backend.name())
        .with("variant", variant.name())
        .with("scenario", scenario.name())
        .with("phase", phase)
        .with("params", params_json(o, variant))
        .with("env", env.json.clone())
}

// ---------------------------------------------------------------------------
// Benchmark targets: the engine or the raw backend behind one interface
// ---------------------------------------------------------------------------

type Record = (Vec<u8>, Vec<u8>);

trait Target: Send + Sync {
    /// One atomic, durable commit with every record.
    fn load_batch(&self, recs: &[Record]) -> Res<()>;
    /// One record, one durable (Immediate) commit.
    fn put_durable(&self, key: &[u8], value: &[u8]) -> Res<()>;
    fn get(&self, key: &[u8]) -> Res<Option<Vec<u8>>>;
    fn get_range(&self, key: &[u8], offset: u64, len: u64) -> Res<Option<Vec<u8>>>;
    /// Up to `limit` records under `prefix`, newest (largest key) first, with values.
    fn latest(&self, prefix: &[u8], limit: usize) -> Res<Vec<Record>>;
    /// Engine statistics (None for the raw backend).
    fn stats(&self) -> Res<Option<Stats>>;
}

struct RawTarget<S: Store> {
    store: S,
}

impl<S: Store> Target for RawTarget<S> {
    fn load_batch(&self, recs: &[Record]) -> Res<()> {
        let mut w = self.store.begin_write()?;
        for (k, v) in recs {
            w.put(Table::Records, k, v)?;
        }
        w.commit(Durability::Immediate)?;
        Ok(())
    }

    fn put_durable(&self, key: &[u8], value: &[u8]) -> Res<()> {
        let mut w = self.store.begin_write()?;
        w.put(Table::Records, key, value)?;
        w.commit(Durability::Immediate)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Res<Option<Vec<u8>>> {
        Ok(self.store.begin_read()?.get(Table::Records, key)?)
    }

    fn get_range(&self, key: &[u8], offset: u64, len: u64) -> Res<Option<Vec<u8>>> {
        Ok(self.get(key)?.map(|v| {
            let start = usize::try_from(offset).unwrap_or(usize::MAX).min(v.len());
            let end = start
                .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
                .min(v.len());
            v[start..end].to_vec()
        }))
    }

    fn latest(&self, prefix: &[u8], limit: usize) -> Res<Vec<Record>> {
        let r = self.store.begin_read()?;
        let end = prefix_successor(prefix);
        let end_bound = match &end {
            Some(e) => Bound::Excluded(e.as_slice()),
            None => Bound::Unbounded,
        };
        let mut out = Vec::with_capacity(limit);
        r.scan(
            Table::Records,
            Bound::Included(prefix),
            end_bound,
            true,
            &mut |k, v| {
                out.push((k.to_vec(), v.to_vec()));
                Ok(out.len() < limit)
            },
        )?;
        Ok(out)
    }

    fn stats(&self) -> Res<Option<Stats>> {
        Ok(None)
    }
}

struct EngineTarget<S: Store> {
    db: Db<S>,
}

impl<S: Store> Target for EngineTarget<S>
where
    Db<S>: Send + Sync,
{
    fn load_batch(&self, recs: &[Record]) -> Res<()> {
        let ops: Vec<BatchOp<'_>> = recs
            .iter()
            .map(|(k, v)| BatchOp::Put {
                key: k.as_slice(),
                value: v.as_slice(),
                expect: Expect::Any,
            })
            .collect();
        self.db.write_batch(&ops)?;
        Ok(())
    }

    fn put_durable(&self, key: &[u8], value: &[u8]) -> Res<()> {
        self.db.put(key, value, Expect::Any)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Res<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    fn get_range(&self, key: &[u8], offset: u64, len: u64) -> Res<Option<Vec<u8>>> {
        Ok(self.db.get_range(key, offset, len)?)
    }

    fn latest(&self, prefix: &[u8], limit: usize) -> Res<Vec<Record>> {
        let items = self.db.scan(
            &ScanOptions::prefix(prefix)
                .reverse(true)
                .limit(limit)
                .with_values(true),
        )?;
        Ok(items
            .into_iter()
            .map(|it| (it.key, it.value.unwrap_or_default()))
            .collect())
    }

    fn stats(&self) -> Res<Option<Stats>> {
        Ok(Some(self.db.stats()?))
    }
}

/// Opens (or reopens) the store of one run and wraps it for its variant.
struct Opener {
    backend: Backend,
    cfg: Option<Config>,
    dir: PathBuf,
    backend_cache_bytes: usize,
    lmdb_map_size: usize,
}

impl Opener {
    fn open(&self) -> Res<Box<dyn Target>> {
        match self.backend {
            Backend::Redb => self.wrap(RedbStore::open(
                self.dir.join("babel.redb"),
                self.backend_cache_bytes,
            )?),
            Backend::Lmdb => self.open_lmdb(),
            Backend::Mem => self.wrap(MemStore::new()),
        }
    }

    #[cfg(feature = "lmdb")]
    fn open_lmdb(&self) -> Res<Box<dyn Target>> {
        let dir = self.dir.join("lmdb");
        fs::create_dir_all(&dir)?;
        self.wrap(HeedStore::open(&dir, self.lmdb_map_size)?)
    }

    #[cfg(not(feature = "lmdb"))]
    fn open_lmdb(&self) -> Res<Box<dyn Target>> {
        let _ = self.lmdb_map_size;
        Err("the lmdb backend needs `cargo bench --features lmdb`".into())
    }

    fn wrap<S: Store>(&self, store: S) -> Res<Box<dyn Target>>
    where
        Db<S>: Send + Sync,
    {
        Ok(match &self.cfg {
            None => Box::new(RawTarget { store }),
            Some(cfg) => Box::new(EngineTarget {
                db: Db::with_store(store, cfg.clone())?,
            }),
        })
    }

    /// The in-memory store cannot be reopened: its data lives in the handle.
    fn reopenable(&self) -> bool {
        self.backend != Backend::Mem
    }

    /// Every file under the run directory (redb file, LMDB data + lock).
    fn files(&self) -> Vec<FileSize> {
        let mut out = Vec::new();
        let mut stack = vec![self.dir.clone()];
        while let Some(d) = stack.pop() {
            let Ok(entries) = fs::read_dir(&d) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(s) = sys::file_size(&p) {
                    out.push(s);
                }
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        out
    }
}

fn lmdb_map_size(o: &Opts) -> usize {
    o.lmdb_map_size.unwrap_or_else(|| {
        let readers = o.readers.iter().copied().max().unwrap_or(1) as u64;
        let records = o.records + o.durable_puts + o.mixed_ops * readers;
        let estimate = records
            .saturating_mul(o.value_size as u64 + 256)
            .saturating_mul(4)
            .saturating_add(64 << 20);
        let mib = 1u64 << 20;
        usize::try_from(estimate.div_ceil(mib) * mib).unwrap_or(usize::MAX)
    })
}

/// Removes the run directory when the run ends (unless `--keep`).
struct DirGuard {
    path: PathBuf,
    keep: bool,
}

impl Drop for DirGuard {
    fn drop(&mut self) {
        if !self.keep
            && let Err(e) = fs::remove_dir_all(&self.path)
        {
            eprintln!("warning: could not remove {}: {e}", self.path.display());
        }
    }
}

// ---------------------------------------------------------------------------
// Run state, samplers, key lists
// ---------------------------------------------------------------------------

/// What the harness knows about the stored dataset (to verify reads).
struct DataState {
    /// Records `[0, loaded)` come from the load phase; read phases sample them.
    loaded: u64,
    /// First record index never written.
    next_index: u64,
    records_total: u64,
    /// Value length of each loaded record.
    lengths: Vec<u32>,
    /// S3: messages and newest record index per channel (every write).
    channel_counts: Vec<u64>,
    channel_newest: Vec<Option<u64>>,
    user_key_bytes: u64,
    user_value_bytes: u64,
}

impl DataState {
    fn new(records: u64) -> DataState {
        DataState {
            loaded: 0,
            next_index: 0,
            records_total: 0,
            lengths: Vec::with_capacity(usize::try_from(records).unwrap_or(0)),
            channel_counts: vec![0; S3_CHANNELS as usize],
            channel_newest: vec![None; S3_CHANNELS as usize],
            user_key_bytes: 0,
            user_value_bytes: 0,
        }
    }

    fn note(
        &mut self,
        scenario: Scenario,
        seed: u64,
        i: u64,
        key_len: usize,
        value_len: usize,
        loaded: bool,
    ) {
        if loaded {
            debug_assert_eq!(i, self.loaded);
            self.lengths
                .push(u32::try_from(value_len).unwrap_or(u32::MAX));
            self.loaded += 1;
        }
        self.next_index = self.next_index.max(i + 1);
        self.records_total += 1;
        self.user_key_bytes += key_len as u64;
        self.user_value_bytes += value_len as u64;
        if scenario == Scenario::ChatJson {
            let c = datasets::channel_of(i, seed) as usize;
            self.channel_counts[c] += 1;
            self.channel_newest[c] = Some(self.channel_newest[c].map_or(i, |n| n.max(i)));
        }
    }

    fn check_len(&self, i: u64, len: usize) -> Res<()> {
        let want = self.lengths.get(i as usize).copied();
        if want != Some(u32::try_from(len).unwrap_or(u32::MAX)) {
            return Err(format!("record {i}: read {len} bytes, stored {want:?}").into());
        }
        Ok(())
    }
}

/// Record indices in `[0, n)` following the configured popularity.
struct Sampler<'z> {
    dist: Dist,
    n: u64,
    rng: SplitMix64,
    zipf: Option<&'z Zipf>,
}

impl<'z> Sampler<'z> {
    fn new(dist: Dist, n: u64, zipf: Option<&'z Zipf>, seed: u64) -> Sampler<'z> {
        debug_assert!(zipf.is_none_or(|z| z.n() == n));
        Sampler {
            dist,
            n,
            rng: SplitMix64::new(seed),
            zipf,
        }
    }

    fn next(&mut self) -> u64 {
        match (self.dist, self.zipf) {
            (Dist::Zipf, Some(z)) => datasets::scramble(z.sample(self.rng.next_f64()), self.n),
            (Dist::Latest, Some(z)) => self.n - 1 - z.sample(self.rng.next_f64()),
            _ => self.rng.below(self.n),
        }
    }
}

/// Stream seed of a phase/thread (reproducible, independent streams).
fn stream_seed(seed: u64, phase: u64, thread: u64) -> u64 {
    datasets::splitmix64(
        datasets::splitmix64(seed ^ phase.wrapping_mul(0x9E37_79B9_7F4A_7C15)) ^ thread,
    )
}

const PH_GET: u64 = 1;
const PH_MISS: u64 = 2;
const PH_RANGE: u64 = 3;
const PH_LATEST: u64 = 4;
const PH_MT: u64 = 5;
const PH_MIXED: u64 = 6;

/// Keys prepared before a timed loop, packed in one buffer.
#[derive(Default)]
struct KeyList {
    buf: Vec<u8>,
    ends: Vec<usize>,
    idx: Vec<u64>,
    extra: Vec<u64>,
}

impl KeyList {
    fn push(&mut self, key: &[u8], idx: u64, extra: u64) {
        self.buf.extend_from_slice(key);
        self.ends.push(self.buf.len());
        self.idx.push(idx);
        self.extra.push(extra);
    }

    fn len(&self) -> usize {
        self.ends.len()
    }

    /// (key, record index, extra) in insertion order.
    fn iter(&self) -> impl Iterator<Item = (&[u8], u64, u64)> + '_ {
        (0..self.len()).map(move |j| {
            let start = if j == 0 { 0 } else { self.ends[j - 1] };
            (&self.buf[start..self.ends[j]], self.idx[j], self.extra[j])
        })
    }

    fn sample(
        sampler: &mut Sampler<'_>,
        count: u64,
        mut make: impl FnMut(u64) -> (Vec<u8>, u64),
    ) -> KeyList {
        let mut l = KeyList::default();
        for _ in 0..count {
            let i = sampler.next();
            let (k, extra) = make(i);
            l.push(&k, i, extra);
        }
        l
    }
}

// ---------------------------------------------------------------------------
// Matrix runner
// ---------------------------------------------------------------------------

/// One line of the final summary table.
#[derive(Clone, Debug, Default)]
struct Row {
    variant: &'static str,
    scenario: &'static str,
    load_mb_s: Option<f64>,
    put_p99_ms: Option<f64>,
    get_p50_us: Option<f64>,
    get_p99_us: Option<f64>,
    miss_p99_us: Option<f64>,
    range_p99_us: Option<f64>,
    latest_p99_us: Option<f64>,
    mt_kops: Option<f64>,
    mt_p99_us: Option<f64>,
    alloc_mb: Option<f64>,
    amp: Option<f64>,
    error: Option<String>,
}

struct Sink {
    file: Option<fs::File>,
    lines: u64,
}

impl Sink {
    fn open(path: Option<&Path>) -> Res<Sink> {
        let file = match path {
            Some(p) => {
                if let Some(parent) = p.parent().filter(|d| !d.as_os_str().is_empty()) {
                    fs::create_dir_all(parent)?;
                }
                Some(fs::OpenOptions::new().create(true).append(true).open(p)?)
            }
            None => None,
        };
        Ok(Sink { file, lines: 0 })
    }

    fn emit(&mut self, line: &J) -> Res<()> {
        if let Some(f) = &mut self.file {
            let mut s = line.compact();
            s.push('\n');
            f.write_all(s.as_bytes())?;
            f.flush()?;
        }
        self.lines += 1;
        Ok(())
    }
}

struct Ctx<'a> {
    o: &'a Opts,
    env: &'a Env,
    variant: Variant,
    scenario: Scenario,
    label: String,
}

impl Ctx<'_> {
    fn line(&self, phase: &str, fields: J) -> J {
        base_json(self.o, self.env, self.variant, self.scenario, phase).merge(fields)
    }

    fn verify(&self, j: usize) -> bool {
        self.o.verify_every > 0 && (j as u64).is_multiple_of(self.o.verify_every)
    }

    fn expected(&self, i: u64) -> Vec<u8> {
        datasets::value(self.scenario, self.o.seed, i, self.o.value_size)
    }

    fn key(&self, i: u64) -> Vec<u8> {
        datasets::key(self.scenario, self.o.seed, i)
    }

    /// Compare sampled values with the regenerated dataset (outside timing).
    fn verify_values(&self, values: &[(u64, Vec<u8>)]) -> Res<u64> {
        for (i, v) in values {
            if *v != self.expected(*i) {
                return Err(format!("record {i}: value differs from the dataset").into());
            }
        }
        Ok(values.len() as u64)
    }
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        format!("panic: {s}")
    } else if let Some(s) = p.downcast_ref::<String>() {
        format!("panic: {s}")
    } else {
        "panic".to_string()
    }
}

fn run_matrix(o: &Opts) -> Res<bool> {
    if let Some(limit) = o.mem_limit {
        sys::limit_process_memory(limit)?;
        println!("process commit charge limited to {limit} bytes (job object)");
    }
    fs::create_dir_all(&o.dir)?;
    let env = collect_env(o);
    let mut sink = Sink::open(o.out.as_deref())?;
    println!(
        "babeldb engine bench run {} ({})",
        env.run_id,
        env.json.compact()
    );
    let mut rows = Vec::new();
    let mut all_ok = true;
    for &variant in &o.variants {
        for &scenario in &o.scenarios {
            let mut row = Row {
                variant: variant.name(),
                scenario: scenario.code(),
                ..Row::default()
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                run_one(o, &env, variant, scenario, &mut sink, &mut row)
            }));
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(e.to_string()),
                Err(p) => Some(panic_message(&*p)),
            };
            if let Some(msg) = error {
                all_ok = false;
                eprintln!(
                    "[{} {} {}] FAILED: {msg}",
                    o.backend.name(),
                    variant.name(),
                    scenario.name()
                );
                sink.emit(
                    &base_json(o, &env, variant, scenario, "error").with("error", msg.as_str()),
                )?;
                row.error = Some(msg);
            }
            rows.push(row);
        }
    }
    print_summary(o, &rows);
    if let Some(p) = &o.out {
        println!("{} JSON lines appended to {}", sink.lines, p.display());
    }
    Ok(all_ok)
}

fn cur(target: &Option<Box<dyn Target>>) -> Res<&dyn Target> {
    target
        .as_deref()
        .ok_or_else(|| "database is not open".into())
}

/// Close and reopen the database (fresh in-process caches). Returns the open
/// time, or None when the target is kept open.
fn reopen(opener: &Opener, o: &Opts, target: &mut Option<Box<dyn Target>>) -> Res<Option<u64>> {
    if !(o.reopen && opener.reopenable()) {
        return Ok(None);
    }
    drop(target.take());
    let t0 = Instant::now();
    *target = Some(opener.open()?);
    Ok(Some(ns(t0.elapsed())))
}

fn run_one(
    o: &Opts,
    env: &Env,
    variant: Variant,
    scenario: Scenario,
    sink: &mut Sink,
    row: &mut Row,
) -> Res<()> {
    let run_dir = o.dir.join(format!(
        "{}-{}-{}-{}",
        env.run_id,
        o.backend.name(),
        variant.name(),
        scenario.code()
    ));
    if run_dir.exists() {
        fs::remove_dir_all(&run_dir)?;
    }
    fs::create_dir_all(&run_dir)?;
    let _guard = DirGuard {
        path: run_dir.clone(),
        keep: o.keep,
    };
    let opener = Opener {
        backend: o.backend,
        cfg: variant.config(o),
        dir: run_dir.clone(),
        backend_cache_bytes: o.backend_cache_bytes,
        lmdb_map_size: lmdb_map_size(o),
    };
    let ctx = Ctx {
        o,
        env,
        variant,
        scenario,
        label: format!(
            "[{} {} {}]",
            o.backend.name(),
            variant.name(),
            scenario.name()
        ),
    };
    println!("{} start, dir {}", ctx.label, run_dir.display());

    let mut st = DataState::new(o.records);
    let zipf_records = (o.dist != Dist::Uniform).then(|| Zipf::new(o.records, o.zipf_theta));
    let zipf_channels = Zipf::new(S3_CHANNELS, o.zipf_theta);

    let t0 = Instant::now();
    let mut target = Some(opener.open()?);
    let create_ns = ns(t0.elapsed());

    let fields = phase_load(&ctx, cur(&target)?, &mut st, row)?.with("open_ns", create_ns);
    sink.emit(&ctx.line("load", fields))?;
    let fields = phase_space(&ctx, cur(&target)?, &opener, &st, Some(row))?;
    sink.emit(&ctx.line("space", fields))?;

    if o.durable_puts > 0 && o.has(Phase::Put) {
        let fields = phase_put(&ctx, cur(&target)?, &mut st, row)?;
        sink.emit(&ctx.line(Phase::Put.name(), fields))?;
    }

    let mut read_phases: Vec<(Phase, usize)> = Vec::new();
    for p in [Phase::Get, Phase::Miss, Phase::Range, Phase::Latest] {
        if o.has(p) && (p != Phase::Latest || scenario == Scenario::ChatJson) {
            read_phases.push((p, 1));
        }
    }
    if o.has(Phase::MtGet) {
        read_phases.extend(o.readers.iter().map(|&t| (Phase::MtGet, t)));
    }
    if o.has(Phase::Mixed) && o.mix != Mix::ReadOnly {
        read_phases.push((Phase::Mixed, o.readers.iter().copied().max().unwrap_or(1)));
    }

    for (phase, threads) in read_phases {
        let open_ns = reopen(&opener, o, &mut target)?;
        let t = cur(&target)?;
        let zr = zipf_records.as_ref();
        let fields = match phase {
            Phase::Get => phase_get(&ctx, t, &st, zr, row)?,
            Phase::Miss => phase_miss(&ctx, t, &st, zr, row)?,
            Phase::Range => phase_range(&ctx, t, &st, zr, row)?,
            Phase::Latest => phase_latest(&ctx, t, &st, &zipf_channels, row)?,
            Phase::MtGet => phase_mt(&ctx, t, &st, zr, threads, row)?,
            Phase::Mixed => phase_mixed(&ctx, t, &mut st, zr, threads)?,
            Phase::Put => unreachable!("put is not a read phase"),
        };
        let mut fields = fields
            .with("reopened", open_ns.is_some())
            .with("open_ns", open_ns);
        // Counters since the reopen = this phase (warmup included). Skipped
        // without a reopen: stats() scans every table and would pollute the
        // caches of the next phase.
        if open_ns.is_some()
            && let Some(s) = t.stats()?
        {
            fields = fields
                .with("engine_counters", counters_json(&s.counters))
                .with(
                    "read_amplification",
                    ratio(s.counters.bytes_reconstructed, s.counters.bytes_requested),
                )
                .with("engine_cache", cache_json(&s.cache));
        }
        sink.emit(&ctx.line(phase.name(), fields))?;
    }

    let fields = phase_space(&ctx, cur(&target)?, &opener, &st, None)?;
    drop(target.take());
    let closed = opener.files();
    let fields = fields
        .with(
            "files_after_close",
            J::A(closed.iter().map(file_json).collect()),
        )
        .with(
            "file_allocated_bytes_after_close",
            closed
                .iter()
                .map(|f| f.allocated_bytes)
                .sum::<Option<u64>>(),
        );
    sink.emit(&ctx.line("space-final", fields))?;
    println!("{} done", ctx.label);
    Ok(())
}

// ---------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------

/// (a) Load `records` records with `write_batch` (one durable commit per batch).
fn phase_load(ctx: &Ctx, t: &dyn Target, st: &mut DataState, row: &mut Row) -> Res<J> {
    let o = ctx.o;
    let n = o.records;
    let mut batch_lat =
        Vec::with_capacity(usize::try_from(n.div_ceil(o.batch as u64)).unwrap_or(0));
    let mut delta = ProcDelta::default();
    let mut hasher = datasets::DatasetHasher::new();
    let (mut write_ns, mut gen_ns) = (0u64, 0u64);
    let wall = Instant::now();
    let mut start = 0u64;
    while start < n {
        let end = (start + o.batch as u64).min(n);
        let g = Instant::now();
        let recs: Vec<Record> = (start..end)
            .map(|i| datasets::record(ctx.scenario, o.seed, i, o.value_size))
            .collect();
        gen_ns += ns(g.elapsed());
        let before = sys::process_metrics();
        let t0 = Instant::now();
        t.load_batch(&recs)?;
        let dt = ns(t0.elapsed());
        delta.add(&ProcDelta::between(&before, &sys::process_metrics()));
        write_ns += dt;
        batch_lat.push(dt);
        for (i, (k, v)) in (start..end).zip(&recs) {
            hasher.add(k, v);
            st.note(ctx.scenario, o.seed, i, k.len(), v.len(), true);
        }
        start = end;
    }
    let wall_ns = ns(wall.elapsed());
    let after = sys::process_metrics();
    let bytes = st.user_key_bytes + st.user_value_bytes;
    let s = summarize(batch_lat);
    let mb_s = rate(bytes as f64 / 1e6, write_ns);
    row.load_mb_s = Some(mb_s);
    println!(
        "{} load: {n} records, {:.1} MB in {:.2}s of commits -> {:.0} rec/s, {mb_s:.1} MB/s; batch {}",
        ctx.label,
        bytes as f64 / 1e6,
        write_ns as f64 / 1e9,
        rate(n as f64, write_ns),
        s.line()
    );
    Ok(J::obj()
        .with("records", n)
        .with("batch", o.batch)
        .with("batches", s.count)
        .with("key_bytes", st.user_key_bytes)
        .with("value_bytes", st.user_value_bytes)
        .with("write_seconds", write_ns as f64 / 1e9)
        .with("generation_seconds", gen_ns as f64 / 1e9)
        .with("wall_seconds", wall_ns as f64 / 1e9)
        .with("records_per_s", rate(n as f64, write_ns))
        .with("mb_per_s", mb_s)
        .with(
            "value_mb_per_s",
            rate(st.user_value_bytes as f64 / 1e6, write_ns),
        )
        .with("batch_latency_ns", s.json())
        .with("dataset_blake3", hex(&hasher.finish()))
        .with("process_delta", delta.json())
        .with("process_after", metrics_json(&after)))
}

/// Space accounting: files (apparent + allocated), user bytes, engine stats.
fn phase_space(
    ctx: &Ctx,
    t: &dyn Target,
    opener: &Opener,
    st: &DataState,
    row: Option<&mut Row>,
) -> Res<J> {
    let files = opener.files();
    let apparent: u64 = files.iter().map(|f| f.apparent_bytes).sum();
    // No files (in-memory store) or any unmeasurable file: unknown, not 0.
    let allocated: Option<u64> = if files.is_empty() {
        None
    } else {
        files.iter().map(|f| f.allocated_bytes).sum()
    };
    let user = st.user_key_bytes + st.user_value_bytes;
    let mut j = J::obj()
        .with("records_total", st.records_total)
        .with("user_key_bytes", st.user_key_bytes)
        .with("user_value_bytes", st.user_value_bytes)
        .with("user_bytes", user)
        .with("files", J::A(files.iter().map(file_json).collect()))
        .with("file_apparent_bytes", apparent)
        .with("file_allocated_bytes", allocated)
        .with("amplification_apparent", ratio(apparent, user))
        .with(
            "amplification_allocated",
            allocated.and_then(|a| ratio(a, user)),
        );
    let stats = t.stats()?;
    if let Some(s) = &stats {
        j = j.with("engine", stats_json(s));
    }
    if let Some(row) = row {
        row.alloc_mb = allocated.map(|a| a as f64 / 1e6);
        row.amp = allocated.and_then(|a| ratio(a, user));
        println!(
            "{} space: user {:.2} MB, files {:.2} MB apparent / {} allocated{}",
            ctx.label,
            user as f64 / 1e6,
            apparent as f64 / 1e6,
            allocated.map_or("n/a".to_string(), |a| format!("{:.2} MB", a as f64 / 1e6)),
            stats.as_ref().map_or(String::new(), |s| format!(
                ", engine payload {:.2} MB",
                s.payload_bytes() as f64 / 1e6
            )),
        );
    }
    Ok(j)
}

/// (b) Durable single puts of new records, one Immediate commit each.
fn phase_put(ctx: &Ctx, t: &dyn Target, st: &mut DataState, row: &mut Row) -> Res<J> {
    let o = ctx.o;
    let mut lat = Vec::with_capacity(usize::try_from(o.durable_puts).unwrap_or(0));
    let mut delta = ProcDelta::default();
    let mut bytes = 0u64;
    for _ in 0..o.durable_puts {
        let i = st.next_index;
        let (k, v) = datasets::record(ctx.scenario, o.seed, i, o.value_size);
        let before = sys::process_metrics();
        let t0 = Instant::now();
        t.put_durable(&k, &v)?;
        let dt = ns(t0.elapsed());
        delta.add(&ProcDelta::between(&before, &sys::process_metrics()));
        lat.push(dt);
        bytes += (k.len() + v.len()) as u64;
        st.note(ctx.scenario, o.seed, i, k.len(), v.len(), false);
    }
    let s = summarize(lat);
    row.put_p99_ms = Some(s.p99 as f64 / 1e6);
    println!("{} put-durable: {} ops {}", ctx.label, s.count, s.line());
    Ok(J::obj()
        .with("ops", s.count)
        .with("bytes_written", bytes)
        .with("latency_ns", s.json())
        .with("ops_per_s", s.ops_per_s())
        .with("process_delta", delta.json())
        .with("process_after", metrics_json(&sys::process_metrics())))
}

/// Common fields of a single-threaded read phase.
fn read_fields(
    s: &Summary,
    wall_ns: u64,
    bytes: u64,
    before: &ProcessMetrics,
    after: &ProcessMetrics,
    verified: u64,
) -> J {
    J::obj()
        .with("ops", s.count)
        .with("latency_ns", s.json())
        .with("ops_per_s", s.ops_per_s())
        .with("wall_seconds", wall_ns as f64 / 1e9)
        .with("bytes_returned", bytes)
        .with("verified_values", verified)
        .with("process_delta", ProcDelta::between(before, after).json())
        .with("process_after", metrics_json(after))
}

/// Unrecorded reads of the same distribution before a measured phase.
fn warm_up(ctx: &Ctx, t: &dyn Target, sampler: &mut Sampler<'_>, count: u64) -> Res<()> {
    for _ in 0..count {
        let i = sampler.next();
        std::hint::black_box(t.get(&ctx.key(i))?);
    }
    Ok(())
}

/// (c) Point gets of loaded records.
fn phase_get(
    ctx: &Ctx,
    t: &dyn Target,
    st: &DataState,
    zipf: Option<&Zipf>,
    row: &mut Row,
) -> Res<J> {
    let o = ctx.o;
    let mut sampler = Sampler::new(o.dist, st.loaded, zipf, stream_seed(o.seed, PH_GET, 0));
    warm_up(ctx, t, &mut sampler, o.warmup)?;
    let keys = KeyList::sample(&mut sampler, o.ops, |i| (ctx.key(i), 0));
    let mut lat = Vec::with_capacity(keys.len());
    let mut sampled = Vec::new();
    let mut bytes = 0u64;
    let before = sys::process_metrics();
    let wall = Instant::now();
    for (j, (key, i, _)) in keys.iter().enumerate() {
        let t0 = Instant::now();
        let v = t.get(key)?;
        lat.push(ns(t0.elapsed()));
        let v = v.ok_or_else(|| format!("get: record {i} is missing"))?;
        st.check_len(i, v.len())?;
        bytes += v.len() as u64;
        if ctx.verify(j) {
            sampled.push((i, v));
        }
    }
    let wall_ns = ns(wall.elapsed());
    let after = sys::process_metrics();
    let verified = ctx.verify_values(&sampled)?;
    let s = summarize(lat);
    row.get_p50_us = Some(s.p50 as f64 / 1e3);
    row.get_p99_us = Some(s.p99 as f64 / 1e3);
    println!(
        "{} get: {} ops {} ({:.0} ops/s)",
        ctx.label,
        s.count,
        s.line(),
        s.ops_per_s()
    );
    Ok(read_fields(&s, wall_ns, bytes, &before, &after, verified))
}

/// (d) Gets of absent keys that sort next to real keys.
fn phase_miss(
    ctx: &Ctx,
    t: &dyn Target,
    st: &DataState,
    zipf: Option<&Zipf>,
    row: &mut Row,
) -> Res<J> {
    let o = ctx.o;
    let mut sampler = Sampler::new(o.dist, st.loaded, zipf, stream_seed(o.seed, PH_MISS, 0));
    warm_up(ctx, t, &mut sampler, o.warmup)?;
    let keys = KeyList::sample(&mut sampler, o.ops, |i| {
        (datasets::miss_key(ctx.scenario, o.seed, i), 0)
    });
    let mut lat = Vec::with_capacity(keys.len());
    let before = sys::process_metrics();
    let wall = Instant::now();
    for (key, i, _) in keys.iter() {
        let t0 = Instant::now();
        let v = t.get(key)?;
        lat.push(ns(t0.elapsed()));
        if v.is_some() {
            return Err(format!("miss: the absent key next to record {i} was found").into());
        }
    }
    let wall_ns = ns(wall.elapsed());
    let after = sys::process_metrics();
    let s = summarize(lat);
    row.miss_p99_us = Some(s.p99 as f64 / 1e3);
    println!("{} miss: {} ops {}", ctx.label, s.count, s.line());
    Ok(read_fields(&s, wall_ns, 0, &before, &after, 0))
}

/// (e) Short range reads (`range_len` bytes at a random offset).
fn phase_range(
    ctx: &Ctx,
    t: &dyn Target,
    st: &DataState,
    zipf: Option<&Zipf>,
    row: &mut Row,
) -> Res<J> {
    let o = ctx.o;
    let mut sampler = Sampler::new(o.dist, st.loaded, zipf, stream_seed(o.seed, PH_RANGE, 0));
    warm_up(ctx, t, &mut sampler, o.warmup)?;
    let mut offsets = SplitMix64::new(stream_seed(o.seed, PH_RANGE, 1));
    let keys = KeyList::sample(&mut sampler, o.ops, |i| {
        let len = u64::from(st.lengths[i as usize]);
        (
            ctx.key(i),
            offsets.below(len.saturating_sub(o.range_len) + 1),
        )
    });
    let mut lat = Vec::with_capacity(keys.len());
    let mut sampled = Vec::new();
    let mut bytes = 0u64;
    let before = sys::process_metrics();
    let wall = Instant::now();
    for (j, (key, i, off)) in keys.iter().enumerate() {
        let t0 = Instant::now();
        let v = t.get_range(key, off, o.range_len)?;
        lat.push(ns(t0.elapsed()));
        let v = v.ok_or_else(|| format!("range: record {i} is missing"))?;
        let want = o.range_len.min(u64::from(st.lengths[i as usize]) - off);
        if v.len() as u64 != want {
            return Err(format!(
                "range: record {i} offset {off}: {} bytes, expected {want}",
                v.len()
            )
            .into());
        }
        bytes += v.len() as u64;
        if ctx.verify(j) {
            sampled.push((i, off, v));
        }
    }
    let wall_ns = ns(wall.elapsed());
    let after = sys::process_metrics();
    for (i, off, v) in &sampled {
        let full = ctx.expected(*i);
        let start = *off as usize;
        if full[start..start + v.len()] != v[..] {
            return Err(
                format!("range: record {i} offset {off}: bytes differ from the dataset").into(),
            );
        }
    }
    let s = summarize(lat);
    row.range_p99_us = Some(s.p99 as f64 / 1e3);
    println!("{} range: {} ops {}", ctx.label, s.count, s.line());
    Ok(
        read_fields(&s, wall_ns, bytes, &before, &after, sampled.len() as u64)
            .with("range_len", o.range_len),
    )
}

/// (f) S3: latest `latest_limit` messages of a channel (reverse prefix scan).
fn phase_latest(
    ctx: &Ctx,
    t: &dyn Target,
    st: &DataState,
    zipf_channels: &Zipf,
    row: &mut Row,
) -> Res<J> {
    let o = ctx.o;
    let limit = o.latest_limit;
    let mut rng = SplitMix64::new(stream_seed(o.seed, PH_LATEST, 0));
    let mut channel = || match o.dist {
        Dist::Uniform => rng.below(S3_CHANNELS),
        // Hot channels in the reads are the hot channels of the data.
        Dist::Zipf | Dist::Latest => {
            datasets::channel_for_rank(zipf_channels.sample(rng.next_f64()))
        }
    };
    for _ in 0..o.warmup {
        std::hint::black_box(t.latest(&datasets::channel_prefix(channel()), limit)?);
    }
    let mut prefixes = KeyList::default();
    for _ in 0..o.ops {
        let c = channel();
        prefixes.push(&datasets::channel_prefix(c), c, 0);
    }
    let mut lat = Vec::with_capacity(prefixes.len());
    let (mut bytes, mut items_total, mut checked) = (0u64, 0u64, 0u64);
    let before = sys::process_metrics();
    let wall = Instant::now();
    for (j, (prefix, c, _)) in prefixes.iter().enumerate() {
        let t0 = Instant::now();
        let items = t.latest(prefix, limit)?;
        lat.push(ns(t0.elapsed()));
        let want = (limit as u64).min(st.channel_counts[c as usize]);
        if items.len() as u64 != want {
            return Err(format!(
                "latest: channel {c} returned {} items, expected {want}",
                items.len()
            )
            .into());
        }
        if ctx.verify(j) {
            let ordered = items.windows(2).all(|w| w[0].0 > w[1].0);
            let in_prefix = items.iter().all(|(k, _)| k.starts_with(prefix));
            let newest = st.channel_newest[c as usize].map(|i| ctx.key(i));
            if !ordered || !in_prefix || items.first().map(|(k, _)| k) != newest.as_ref() {
                return Err(
                    format!("latest: channel {c}: wrong order, prefix or newest message").into(),
                );
            }
            checked += 1;
        }
        items_total += items.len() as u64;
        bytes += items.iter().map(|(_, v)| v.len() as u64).sum::<u64>();
    }
    let wall_ns = ns(wall.elapsed());
    let after = sys::process_metrics();
    let s = summarize(lat);
    row.latest_p99_us = Some(s.p99 as f64 / 1e3);
    println!(
        "{} latest{limit}: {} scans {} ({:.1} items/scan)",
        ctx.label,
        s.count,
        s.line(),
        items_total as f64 / s.count.max(1) as f64
    );
    Ok(read_fields(&s, wall_ns, bytes, &before, &after, checked)
        .with("limit", limit)
        .with("items_returned", items_total)
        .with("items_per_scan", items_total as f64 / s.count.max(1) as f64))
}

#[derive(Default)]
struct ThreadOut {
    lat: Vec<u64>,
    write_lat: Vec<u64>,
    bytes: u64,
    end: Option<Instant>,
    sampled: Vec<(u64, Vec<u8>)>,
    written: Vec<(u64, usize, usize)>,
}

/// Run `prepare` (not timed), wait for every thread, then run `work` (timed).
/// A panic or error in `prepare` still reaches the barrier, so no thread waits forever.
fn barrier_worker<P>(
    barrier: &Barrier,
    prepare: impl FnOnce() -> Res<P>,
    work: impl FnOnce(P) -> Res<ThreadOut>,
) -> Res<ThreadOut> {
    let prepared =
        catch_unwind(AssertUnwindSafe(prepare)).unwrap_or_else(|p| Err(panic_message(&*p).into()));
    barrier.wait();
    work(prepared?)
}

/// Join worker threads; returns their outputs, the release instant and the
/// process metrics at release.
fn run_threads<P>(
    threads: usize,
    prepare: impl Fn(usize) -> Res<P> + Sync,
    work: impl Fn(usize, P) -> Res<ThreadOut> + Sync,
) -> (Vec<Res<ThreadOut>>, Instant, ProcessMetrics) {
    let barrier = Barrier::new(threads + 1);
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|tid| {
                let (barrier, prepare, work) = (&barrier, &prepare, &work);
                s.spawn(move || barrier_worker(barrier, || prepare(tid), |p| work(tid, p)))
            })
            .collect();
        barrier.wait();
        let before = sys::process_metrics();
        let start = Instant::now();
        let outs = handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|p| Err(panic_message(&*p).into())))
            .collect();
        (outs, start, before)
    })
}

/// (g) `threads` concurrent readers doing point gets.
fn phase_mt(
    ctx: &Ctx,
    t: &dyn Target,
    st: &DataState,
    zipf: Option<&Zipf>,
    threads: usize,
    row: &mut Row,
) -> Res<J> {
    let o = ctx.o;
    let warm_each = o.warmup.div_ceil(threads as u64);
    let prepare = |tid: usize| -> Res<KeyList> {
        let mut sampler = Sampler::new(
            o.dist,
            st.loaded,
            zipf,
            stream_seed(o.seed, PH_MT, tid as u64),
        );
        warm_up(ctx, t, &mut sampler, warm_each)?;
        Ok(KeyList::sample(&mut sampler, o.ops, |i| (ctx.key(i), 0)))
    };
    let work = |_tid: usize, keys: KeyList| -> Res<ThreadOut> {
        let mut out = ThreadOut {
            lat: Vec::with_capacity(keys.len()),
            ..ThreadOut::default()
        };
        for (j, (key, i, _)) in keys.iter().enumerate() {
            let t0 = Instant::now();
            let v = t.get(key)?;
            out.lat.push(ns(t0.elapsed()));
            let v = v.ok_or_else(|| format!("mt-get: record {i} is missing"))?;
            st.check_len(i, v.len())?;
            out.bytes += v.len() as u64;
            if ctx.verify(j) {
                out.sampled.push((i, v));
            }
        }
        out.end = Some(Instant::now());
        Ok(out)
    };
    let (outs, start, before) = run_threads(threads, prepare, work);
    let after = sys::process_metrics();
    let outs = outs.into_iter().collect::<Res<Vec<ThreadOut>>>()?;
    let wall_ns = outs
        .iter()
        .filter_map(|x| x.end)
        .max()
        .map_or(0, |e| ns(e.duration_since(start)));
    let per_thread: Vec<J> = outs
        .iter()
        .map(|x| {
            J::from(x.end.map_or(0.0, |e| {
                rate(x.lat.len() as f64, ns(e.duration_since(start)))
            }))
        })
        .collect();
    let mut verified = 0;
    for x in &outs {
        verified += ctx.verify_values(&x.sampled)?;
    }
    let bytes: u64 = outs.iter().map(|x| x.bytes).sum();
    let s = summarize(outs.into_iter().flat_map(|x| x.lat).collect());
    let total_rate = rate(s.count as f64, wall_ns);
    row.mt_kops = Some(total_rate / 1e3);
    row.mt_p99_us = Some(s.p99 as f64 / 1e3);
    println!(
        "{} mt-get x{threads}: {} ops, {:.0} ops/s aggregate, {}",
        ctx.label,
        s.count,
        total_rate,
        s.line()
    );
    Ok(J::obj()
        .with("threads", threads)
        .with("ops", s.count)
        .with("ops_per_thread", o.ops)
        .with("latency_ns", s.json())
        .with("ops_per_s", total_rate)
        .with("per_thread_ops_per_s", per_thread)
        .with("wall_seconds", wall_ns as f64 / 1e9)
        .with("bytes_returned", bytes)
        .with("verified_values", verified)
        .with("process_delta", ProcDelta::between(&before, &after).json())
        .with("process_after", metrics_json(&after)))
}

enum MixedOp {
    Read,
    Write(u64),
}

/// (h) Mixed reads and durable single-put inserts from `threads` threads.
fn phase_mixed(
    ctx: &Ctx,
    t: &dyn Target,
    st: &mut DataState,
    zipf: Option<&Zipf>,
    threads: usize,
) -> Res<J> {
    let o = ctx.o;
    let per_mille = o.mix.write_per_mille();
    let base = st.next_index;
    let shared: &DataState = st;
    let prepare = |tid: usize| -> Res<(Vec<MixedOp>, KeyList, Vec<Record>)> {
        let mut sampler = Sampler::new(
            o.dist,
            shared.loaded,
            zipf,
            stream_seed(o.seed, PH_MIXED, tid as u64),
        );
        let mut coin = SplitMix64::new(stream_seed(o.seed, PH_MIXED, 1000 + tid as u64));
        let mut ops = Vec::with_capacity(usize::try_from(o.mixed_ops).unwrap_or(0));
        let (mut reads, mut writes) = (KeyList::default(), Vec::new());
        let mut next_write = base + tid as u64 * o.mixed_ops;
        for _ in 0..o.mixed_ops {
            if coin.below(1000) < per_mille {
                writes.push(datasets::record(
                    ctx.scenario,
                    o.seed,
                    next_write,
                    o.value_size,
                ));
                ops.push(MixedOp::Write(next_write));
                next_write += 1;
            } else {
                let i = sampler.next();
                reads.push(&ctx.key(i), i, 0);
                ops.push(MixedOp::Read);
            }
        }
        Ok((ops, reads, writes))
    };
    let work = |_tid: usize,
                (ops, reads, writes): (Vec<MixedOp>, KeyList, Vec<Record>)|
     -> Res<ThreadOut> {
        let mut out = ThreadOut::default();
        let mut read_keys = reads.iter();
        let mut write_recs = writes.iter();
        for op in &ops {
            match op {
                MixedOp::Read => {
                    let (key, i, _) = read_keys.next().ok_or("mixed: read list exhausted")?;
                    let t0 = Instant::now();
                    let v = t.get(key)?;
                    out.lat.push(ns(t0.elapsed()));
                    let v = v.ok_or_else(|| format!("mixed: record {i} is missing"))?;
                    shared.check_len(i, v.len())?;
                    out.bytes += v.len() as u64;
                }
                MixedOp::Write(i) => {
                    let (k, v) = write_recs.next().ok_or("mixed: write list exhausted")?;
                    let t0 = Instant::now();
                    t.put_durable(k, v)?;
                    out.write_lat.push(ns(t0.elapsed()));
                    out.written.push((*i, k.len(), v.len()));
                }
            }
        }
        out.end = Some(Instant::now());
        Ok(out)
    };
    let (outs, start, before) = run_threads(threads, prepare, work);
    let after = sys::process_metrics();
    let outs = outs.into_iter().collect::<Res<Vec<ThreadOut>>>()?;
    let wall_ns = outs
        .iter()
        .filter_map(|x| x.end)
        .max()
        .map_or(0, |e| ns(e.duration_since(start)));
    for x in &outs {
        for &(i, kl, vl) in &x.written {
            st.note(ctx.scenario, o.seed, i, kl, vl, false);
        }
    }
    st.next_index = st.next_index.max(base + threads as u64 * o.mixed_ops);
    let reads = summarize(outs.iter().flat_map(|x| x.lat.iter().copied()).collect());
    let writes = summarize(
        outs.iter()
            .flat_map(|x| x.write_lat.iter().copied())
            .collect(),
    );
    let total = reads.count + writes.count;
    println!(
        "{} mixed {} x{threads}: {:.0} ops/s; reads {} | writes {}",
        ctx.label,
        o.mix.name(),
        rate(total as f64, wall_ns),
        reads.line(),
        writes.line()
    );
    Ok(J::obj()
        .with("threads", threads)
        .with("mix", o.mix.name())
        .with("write_per_mille", per_mille)
        .with("ops", total)
        .with("ops_per_s", rate(total as f64, wall_ns))
        .with("reads_per_s", rate(reads.count as f64, wall_ns))
        .with("writes_per_s", rate(writes.count as f64, wall_ns))
        .with("read_latency_ns", reads.json())
        .with("write_latency_ns", writes.json())
        .with("wall_seconds", wall_ns as f64 / 1e9)
        .with("bytes_returned", outs.iter().map(|x| x.bytes).sum::<u64>())
        .with("process_delta", ProcDelta::between(&before, &after).json())
        .with("process_after", metrics_json(&after)))
}

fn print_summary(o: &Opts, rows: &[Row]) {
    const WIDTHS: [usize; 13] = [18, 4, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 6];
    fn line<S: AsRef<str>>(cells: &[S]) -> String {
        let mut s = String::new();
        for (i, (c, w)) in cells.iter().zip(WIDTHS).enumerate() {
            let c = c.as_ref();
            let _ = if i < 2 {
                write!(s, "{c:<w$} ")
            } else {
                write!(s, "{c:>w$} ")
            };
        }
        s.trim_end().to_string()
    }
    fn cell(v: Option<f64>, decimals: usize) -> String {
        v.map_or("-".to_string(), |x| format!("{x:.decimals$}"))
    }
    println!();
    println!(
        "== summary: backend {}, {} records x {} B, dist {}, readers {:?}, seed {} ==",
        o.backend.name(),
        o.records,
        o.value_size,
        o.dist.name(),
        o.readers,
        o.seed
    );
    println!(
        "{}",
        line(&[
            "variant",
            "scen",
            "load MB/s",
            "put p99",
            "get p50",
            "get p99",
            "miss p99",
            "rng p99",
            "lat p99",
            "mt kop/s",
            "mt p99",
            "alloc MB",
            "amp"
        ])
    );
    println!(
        "{}",
        line(&[
            "", "", "", "ms", "us", "us", "us", "us", "us", "", "us", "", ""
        ])
    );
    for r in rows {
        if let Some(e) = &r.error {
            let short: String = e.chars().take(90).collect();
            println!("{} ERROR {short}", line(&[r.variant, r.scenario]));
            continue;
        }
        println!(
            "{}",
            line(&[
                r.variant.to_string(),
                r.scenario.to_string(),
                cell(r.load_mb_s, 1),
                cell(r.put_p99_ms, 2),
                cell(r.get_p50_us, 1),
                cell(r.get_p99_us, 1),
                cell(r.miss_p99_us, 1),
                cell(r.range_p99_us, 1),
                cell(r.latest_p99_us, 1),
                cell(r.mt_kops, 1),
                cell(r.mt_p99_us, 1),
                cell(r.alloc_mb, 2),
                cell(r.amp, 2),
            ])
        );
    }
}

// ---------------------------------------------------------------------------
// gen-manifest
// ---------------------------------------------------------------------------

fn scenario_json(sc: Scenario) -> J {
    let params = match sc {
        Scenario::Repetitive => J::obj().with("zero_records", "even i").with("motif_bytes", "2..=64 (odd i)"),
        Scenario::Sequences => J::obj().with("start", "< 2^48").with("step", "1..=65536").with("element", "u64 little-endian"),
        Scenario::ChatJson => J::obj()
            .with("channels", S3_CHANNELS)
            .with("users", datasets::S3_USERS)
            .with("discord_epoch_unix_ms", datasets::DISCORD_EPOCH_MS)
            .with("first_message_unix_ms", datasets::S3_BASE_MS)
            .with("mean_gap_ms", datasets::S3_GAP_MS)
            .with("channel_popularity", "Zipf exponent 1, integer weights 2^32/(rank+1), rank -> channel (rank*337+211) mod 1000")
            .with("vocabulary_words", 256u64)
            .with("template_bytes_record0", datasets::s3_template_len(DEFAULT_SEED, 0))
            .with("latest_query", "ScanOptions::prefix(channel_prefix(c)).reverse(true).limit(50).with_values(true)"),
        Scenario::Duplicates => J::obj()
            .with("duplicates_per_10_records", datasets::S4_DUPLICATES_PER_10)
            .with("source", "uniform in [0, i)")
            .with("base_values", "BLAKE3 XOF"),
        Scenario::HighEntropy => J::obj().with("generator", "BLAKE3 derive_key XOF over (seed, i)"),
        Scenario::Compressed => J::obj()
            .with("zstd_level", i64::from(datasets::S6_ZSTD_LEVEL))
            .with("size_search", "closest frame to value_size in at most 8 compressions, stop within 2 %")
            .with("text", "JSON lines of S3-like messages of one channel"),
    };
    J::obj()
        .with("id", sc.code())
        .with("name", sc.name())
        .with("key_format", sc.key_format())
        .with("description", sc.description())
        .with("hypothesis", sc.hypothesis())
        .with("parameters", params)
}

fn gen_manifest(m: &ManifestOpts) -> Res<()> {
    let mut digests = Vec::new();
    for sc in Scenario::ALL {
        for &size in &m.sizes {
            let t0 = Instant::now();
            let mut h = datasets::DatasetHasher::new();
            let (mut key_bytes, mut value_bytes, mut min_len, mut max_len) =
                (0u64, 0u64, u64::MAX, 0u64);
            let mut channels = vec![0u64; S3_CHANNELS as usize];
            let mut duplicates = 0u64;
            for i in 0..m.records {
                let (k, v) = datasets::record(sc, m.seed, i, size);
                h.add(&k, &v);
                key_bytes += k.len() as u64;
                value_bytes += v.len() as u64;
                min_len = min_len.min(v.len() as u64);
                max_len = max_len.max(v.len() as u64);
                match sc {
                    Scenario::ChatJson => channels[datasets::channel_of(i, m.seed) as usize] += 1,
                    Scenario::Duplicates if datasets::duplicate_source(m.seed, i).is_some() => {
                        duplicates += 1
                    }
                    _ => {}
                }
            }
            let mut e = J::obj()
                .with("scenario", sc.name())
                .with("seed", m.seed)
                .with("records", m.records)
                .with("value_size", size)
                .with("blake3", hex(&h.finish()))
                .with("key_bytes", key_bytes)
                .with("value_bytes", value_bytes)
                .with("min_value_len", min_len)
                .with("max_value_len", max_len);
            if sc == Scenario::ChatJson {
                e = e
                    .with("channels_used", channels.iter().filter(|&&c| c > 0).count())
                    .with(
                        "top_channel_records",
                        channels.iter().copied().max().unwrap_or(0),
                    );
            }
            if sc == Scenario::Duplicates {
                e = e.with("duplicate_records", duplicates);
            }
            digests.push(e);
            eprintln!(
                "{} value_size {size}: {:.1}s",
                sc.name(),
                t0.elapsed().as_secs_f64()
            );
        }
    }
    let variants: Vec<J> = Variant::ALL
        .iter()
        .map(|v| {
            J::obj()
                .with("name", v.name())
                .with("description", v.describe())
        })
        .collect();
    let d = Opts::default();
    let manifest = J::obj()
        .with("schema", "babeldb-benchdata-manifest/1")
        .with("datasets_version", DATASETS_VERSION)
        .with("generator", "src/datasets.rs; regenerate with: cargo bench --bench engine -- gen-manifest")
        .with(
            "note",
            "Synthetic scenarios are explicit hypotheses standing in for a production corpus, which is not \
             available (spec section 11). Results on them do not predict results on real data.",
        )
        .with(
            "determinism",
            "Every record is a pure function of (scenario, seed, i, value_size) using integer arithmetic, \
             splitmix64 and BLAKE3; S6 also depends on libzstd level-3 output. The first n records of a \
             larger dataset are the n-record dataset.",
        )
        .with("zstd_library", zstd::zstd_safe::version_string())
        .with("digest", "BLAKE3 over, for i in 0..records: u64 LE key length, key, u64 LE value length, value")
        .with("default_seed", DEFAULT_SEED)
        .with("scenarios", J::A(Scenario::ALL.iter().map(|&s| scenario_json(s)).collect()))
        .with("variants", variants)
        .with(
            "benchmark_defaults",
            J::obj()
                .with("records", d.records)
                .with("value_size", d.value_size)
                .with("block_size", d.block_size)
                .with("inline_max", d.inline_max)
                .with("cache_bytes", d.cache_bytes)
                .with("backend_cache_bytes", d.backend_cache_bytes)
                .with("batch", d.batch)
                .with("durable_puts", d.durable_puts)
                .with("ops", d.ops)
                .with("dist", d.dist.name())
                .with("zipf_theta", d.zipf_theta)
                .with("readers", "logical CPUs")
                .with("range_len", d.range_len)
                .with("latest_limit", d.latest_limit)
                .with("verify_every", d.verify_every),
        )
        .with("digests", digests);
    if let Some(parent) = m.out.parent().filter(|d| !d.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(&m.out, manifest.pretty())?;
    println!("wrote {}", m.out.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Self-tests (run when the binary is started without `--bench`)
// ---------------------------------------------------------------------------

mod self_test {
    use super::*;

    pub fn run() {
        let tests: [(&str, fn()); 7] = [
            ("percentiles", percentiles),
            ("json", json),
            ("argument_parsing", argument_parsing),
            ("variant_configs", variant_configs),
            ("samplers", samplers),
            ("key_list", key_list),
            ("smoke_run_on_mem_store", smoke_run_on_mem_store),
        ];
        println!("engine bench: no --bench argument, running the harness self-tests");
        for (name, test) in tests {
            println!("test {name} ...");
            test();
            println!("test {name} ... ok");
        }
        println!("engine bench self-tests: {} passed", tests.len());
    }

    fn percentiles() {
        let s = summarize((1..=100).rev().collect());
        assert_eq!(
            (s.count, s.min, s.p50, s.p95, s.p99, s.p999, s.max),
            (100, 1, 50, 95, 99, 100, 100)
        );
        assert!((s.mean - 50.5).abs() < 1e-9);
        assert_eq!(s.sum, 5050);
        let s = summarize((1..=1000).collect());
        assert_eq!((s.p50, s.p95, s.p99, s.p999), (500, 950, 990, 999));
        let s = summarize((1..=10).collect());
        assert_eq!((s.p50, s.p95, s.p99, s.p999), (5, 10, 10, 10));
        let s = summarize(vec![7]);
        assert_eq!((s.min, s.p50, s.p99, s.max), (7, 7, 7, 7));
        assert_eq!(summarize(Vec::new()), Summary::default());
        assert_eq!(percentile(&[1, 2, 3, 4], 25_000), 1);
        assert_eq!(percentile(&[1, 2, 3, 4], 25_001), 2);
        assert!(
            (Summary {
                count: 4,
                sum: 2_000,
                ..Summary::default()
            }
            .ops_per_s()
                - 2e6)
                .abs()
                < 1e-6
        );
        assert_eq!(rate(10.0, 0), 0.0);
        assert_eq!(fmt_ns(999), "999ns");
        assert_eq!(fmt_ns(1_500), "1.5us");
        assert_eq!(fmt_ns(2_500_000), "2.50ms");
    }

    fn json() {
        let j = J::obj()
            .with("s", "a\"b\\c\n\t\u{1}é")
            .with("u", 18_446_744_073_709_551_615u64)
            .with("i", -5i64)
            .with("f", 1.5)
            .with("nan", f64::NAN)
            .with("big", 1e21)
            .with("none", Option::<u64>::None)
            .with("b", true)
            .with("a", J::A(vec![J::U(1), J::Null, J::obj()]))
            .with("e", J::A(Vec::new()));
        assert_eq!(
            j.compact(),
            r#"{"s":"a\"b\\c\n\t\u0001é","u":18446744073709551615,"i":-5,"f":1.5,"nan":null,"big":1000000000000000000000,"none":null,"b":true,"a":[1,null,{}],"e":[]}"#
        );
        let p = J::obj()
            .with("k", J::A(vec![J::U(1), J::obj().with("x", 0.25)]))
            .with("z", J::obj());
        assert_eq!(
            p.pretty(),
            "{\n  \"k\": [\n    1,\n    {\n      \"x\": 0.25\n    }\n  ],\n  \"z\": {}\n}\n"
        );
        let merged = J::obj().with("a", 1u64).merge(J::obj().with("b", 2u64));
        assert_eq!(merged.compact(), r#"{"a":1,"b":2}"#);
        assert_eq!(merged.get("b"), Some(&J::U(2)));
        assert_eq!(hex(&[0, 15, 255]), "000fff");
    }

    fn args(list: &[&str]) -> Res<Command> {
        parse_command(&list.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    fn argument_parsing() {
        assert_eq!(parse_bytes("64").unwrap(), 64);
        assert_eq!(parse_bytes("4k").unwrap(), 4096);
        assert_eq!(parse_bytes("4KiB").unwrap(), 4096);
        assert_eq!(parse_bytes("16m").unwrap(), 16 << 20);
        assert_eq!(parse_bytes("1g").unwrap(), 1 << 30);
        assert_eq!(parse_bytes("1_000").unwrap(), 1000);
        assert!(
            parse_bytes("x").is_err() && parse_bytes("4q").is_err() && parse_bytes("").is_err()
        );
        assert_eq!(parse_count("100k").unwrap(), 100_000);
        assert_eq!(parse_count("2m").unwrap(), 2_000_000);

        let Command::Run(o) = args(&[
            "--variant",
            "lz4,zstd",
            "--scenario=s3",
            "--records",
            "5k",
            "--readers",
            "1,4",
            "--dist",
            "zipf",
            "--mix",
            "95-5",
            "--value-size",
            "4k",
            "--phases",
            "get,mt",
            "--no-out",
            "--bench",
        ])
        .unwrap() else {
            panic!("expected a run command")
        };
        assert_eq!(o.variants, vec![Variant::Lz4, Variant::Zstd]);
        assert_eq!(o.scenarios, vec![Scenario::ChatJson]);
        assert_eq!((o.records, o.value_size, o.warmup), (5000, 4096, o.ops));
        assert_eq!(o.readers, vec![1, 4]);
        assert_eq!((o.dist, o.mix), (Dist::Zipf, Mix::R95W5));
        assert_eq!(o.phases, vec![Phase::Get, Phase::MtGet]);
        assert!(o.out.is_none());

        let Command::Run(o) = args(&["--variant", "all", "--warmup", "0", "--bench"]).unwrap()
        else {
            panic!()
        };
        assert_eq!(o.variants.len(), 7);
        assert_eq!(o.warmup, 0);
        assert!(matches!(
            args(&["--list", "--bench"]).unwrap(),
            Command::List
        ));
        assert!(matches!(args(&["--help"]).unwrap(), Command::Help));
        assert!(args(&["--bogus", "--bench"]).is_err());
        assert!(args(&["--records"]).is_err());
        assert!(args(&["--readers", "0"]).is_err());
        assert!(args(&["--inline-max", "64k", "--block-size", "16k"]).is_err());
        let Command::GenManifest(m) = args(&[
            "gen-manifest",
            "--records",
            "10",
            "--sizes",
            "64,1k",
            "--bench",
        ])
        .unwrap() else {
            panic!("expected gen-manifest")
        };
        assert_eq!((m.records, m.sizes.clone()), (10, vec![64, 1024]));
        assert_eq!(m.out, PathBuf::from("benchdata/manifest.json"));
    }

    fn variant_configs() {
        let o = Opts::default();
        assert!(Variant::RawBackend.config(&o).is_none());
        let raw = Variant::EngineRaw.config(&o).unwrap();
        assert!(!raw.codecs.lz4 && !raw.codecs.zstd && !raw.codecs.recipes && !raw.dedupe);
        let lz4 = Variant::Lz4.config(&o).unwrap();
        assert!(lz4.codecs.lz4 && !lz4.codecs.zstd && !lz4.codecs.recipes && !lz4.dedupe);
        let zstd = Variant::Zstd.config(&o).unwrap();
        assert!(
            zstd.codecs.zstd && !zstd.codecs.zstd_dictionary && !zstd.codecs.lz4 && !zstd.dedupe
        );
        assert!(!Variant::AdaptiveNoDedupe.config(&o).unwrap().dedupe);
        let a = Variant::Adaptive.config(&o).unwrap();
        assert!(a.dedupe && a.codecs.lz4 && a.codecs.zstd && a.codecs.recipes);
        assert_eq!(
            Variant::BabelPure.config(&o).unwrap().mode,
            babeldb::Mode::BabelPure
        );
        for v in Variant::ALL {
            assert_eq!(Variant::parse(v.name()), Some(v));
            if let Some(c) = v.config(&o) {
                c.validate().unwrap();
            }
        }
    }

    fn samplers() {
        let n = 1000u64;
        let zipf = Zipf::new(n, 0.99);
        for dist in [Dist::Uniform, Dist::Zipf, Dist::Latest] {
            let z = (dist != Dist::Uniform).then_some(&zipf);
            let mut s = Sampler::new(dist, n, z, 7);
            let mut counts = vec![0u32; n as usize];
            for _ in 0..50_000 {
                counts[s.next() as usize] += 1;
            }
            let (hot, &max) = counts.iter().enumerate().max_by_key(|(_, c)| **c).unwrap();
            match dist {
                Dist::Uniform => assert!(max < 120, "uniform max {max}"),
                Dist::Zipf => {
                    assert_eq!(hot as u64, datasets::scramble(0, n));
                    assert!(max > 5000, "zipf max {max}");
                }
                Dist::Latest => assert_eq!(hot as u64, n - 1),
            }
        }
        assert_ne!(stream_seed(1, PH_GET, 0), stream_seed(1, PH_MISS, 0));
        assert_ne!(stream_seed(1, PH_MT, 0), stream_seed(1, PH_MT, 1));
    }

    fn key_list() {
        let mut l = KeyList::default();
        l.push(b"a", 1, 10);
        l.push(b"", 2, 20);
        l.push(b"ccc", 3, 30);
        let got: Vec<(Vec<u8>, u64, u64)> = l.iter().map(|(k, i, e)| (k.to_vec(), i, e)).collect();
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), 1, 10),
                (Vec::new(), 2, 20),
                (b"ccc".to_vec(), 3, 30)
            ]
        );
        let mut s = Sampler::new(Dist::Uniform, 5, None, 1);
        let l = KeyList::sample(&mut s, 4, |i| (vec![i as u8], i * 2));
        assert_eq!(l.len(), 4);
        assert!(
            l.iter()
                .all(|(k, i, e)| k == [i as u8] && e == i * 2 && i < 5)
        );
    }

    /// The whole harness end to end on the in-memory store with the raw
    /// backend variant (the only variant that needs no engine code).
    fn smoke_run_on_mem_store() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("out.jsonl");
        let o = Opts {
            backend: Backend::Mem,
            variants: vec![Variant::RawBackend],
            scenarios: Scenario::ALL.to_vec(),
            records: 300,
            value_size: 256,
            batch: 64,
            durable_puts: 20,
            ops: 200,
            warmup: 50,
            readers: vec![2],
            mix: Mix::R95W5,
            mixed_ops: 40,
            verify_every: 7,
            dir: tmp.path().join("db"),
            out: Some(out.clone()),
            tag: "self-test".into(),
            ..Opts::default()
        };
        assert!(run_matrix(&o).unwrap(), "smoke run reported failures");
        let o2 = Opts {
            scenarios: vec![Scenario::ChatJson],
            dist: Dist::Latest,
            mix: Mix::R50W50,
            readers: vec![1, 3],
            ..o.clone()
        };
        assert!(run_matrix(&o2).unwrap());

        let text = fs::read_to_string(&out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        // 9 lines per scenario (load, space, put-durable, get, miss, range,
        // mt-get, mixed, space-final), +1 for latest on s3; second run: s3
        // with two mt-get lines.
        assert_eq!(lines.len(), 6 * 9 + 1 + 11, "{text}");
        assert!(lines.iter().all(|l| l.starts_with(r#"{"schema":"babeldb-bench-engine/1""#) && l.ends_with('}')));
        assert!(!text.contains(r#""phase":"error""#));
        for phase in [
            "load",
            "space",
            "put-durable",
            "get",
            "miss",
            "range",
            "latest",
            "mt-get",
            "mixed",
            "space-final",
        ] {
            assert!(
                text.contains(&format!(r#""phase":"{phase}""#)),
                "missing phase {phase}"
            );
        }
        assert!(text.contains(r#""tag":"self-test""#) && text.contains(r#""p999":"#));
        // The database directories are removed after each run.
        assert_eq!(fs::read_dir(tmp.path().join("db")).unwrap().count(), 0);
    }
}
