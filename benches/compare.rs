//! Fair comparison of babeldb with local PostgreSQL and MongoDB on a
//! Discord-like chat workload (dataset S3 of `babeldb::datasets`).
//!
//! ```text
//! BABEL_PG_URL="host=127.0.0.1 user=postgres password=... dbname=postgres" \
//!   cargo bench --bench compare --features compare -- [flags]
//! ```
//! Credentials are read from the environment only and never written anywhere.
//! Each run drops and recreates an isolated `babeldb_bench` database on the
//! PostgreSQL and MongoDB servers; nothing else on them is touched.
//!
//! Systems (same messages, same operations):
//! - `babel-raw`, `babel-adaptive`, `babel-dict`: `Db<RedbStore>` in-process
//!   (no network; `Config::raw_only()`, `Config::adaptive()`, and adaptive
//!   with a zstd dictionary trained by `Db::train_dictionary` on the first
//!   2,000 messages before the load), writes through a `GroupCommitter` (one
//!   commit per batch of concurrent writes).
//! - `babel-tcp`, `babel-tcp-raw` (and `babel-tcp-dict`): the same engines
//!   behind the babeldb TCP server (`cli::server`, localhost, binary protocol;
//!   every connection's writes share the server's group committer): the
//!   like-for-like comparison with the client/server databases.
//! - `babel-fjall-wal`, `babel-fjall-wal-raw`, `babel-fjall-wal-dict` and their `-tcp`
//!   counterparts (`babel-fjall-wal-tcp`, ...; feature `fjall`, i.e. `--features compare,fjall`):
//!   the same engines on fjall, an LSM-tree (`Db::open_fjall_wal`, `FjallOptions::for_wal`;
//!   `BABEL_FJALL_*` environment variables override the tuning, see `fjall_options`), behind the
//!   same write-through WAL as `babel-wal*`. Their `space` first flushes the memtables to tables,
//!   the counterpart of the CHECKPOINT / fsync the PostgreSQL and MongoDB targets run; fjall's
//!   journal, a log like the WAL, is excluded and reported next to it.
//! - `postgres`: `messages(channel_id bigint, id bigint, payload bytea,
//!   primary key (channel_id, id))`, prepared statements, one connection per thread.
//! - `mongo`: collection `messages`, `_id = {c, m}` (both int64), `p` = BinData
//!   (exact bytes), one pooled client.
//! - `tcp-floor` (not in the default list): the loopback floor of the
//!   babeldb client, i.e. `cli::server::Client` against a trivial server that
//!   answers every GET / SCAN_PREFIX with a pre-encoded reply of the same size
//!   (no engine, no dispatch); only the read phases run.
//!
//! Durability, default `durable`: babeldb `Immediate` commits (one
//! FlushFileBuffers per commit), PostgreSQL `synchronous_commit = on` (WAL
//! flush per commit, group commit across connections), MongoDB write concern
//! `{w: 1, j: true}` (journal flush). `--relaxed` runs the non-durable
//! counterparts instead: babeldb `Buffered` group commit (Deferred commits,
//! periodic sync; the TCP server acknowledges the same way), PostgreSQL
//! `synchronous_commit = off`, MongoDB `{w: 1, j: false}`.
//!
//! Phases: `load` (bulk: `write_batch` / `COPY BINARY` / `insert_many`, 1000 per
//! batch), `space` (bytes on disk after load), `space-compacted` (babeldb only:
//! the same after `Db::compact`, with the database no longer shared; the
//! following phases run on the compacted file; `--no-compact` skips it), `put`
//! (sequential single-message commits), `put-mt` (T writer threads), `get` /
//! `get-mt` (point reads by (channel, id); uniform over loaded messages),
//! `latest` / `latest-mt` (newest 50 messages of a channel; channels weighted
//! by message count). With `--pipeline N` the TCP systems also run `put-pipeN`
//! and `get-pipeN`: one connection sends N requests before reading their N
//! replies (the protocol answers in order); ops/s counts requests, latencies
//! are per batch of N. Every 64th read is compared byte for byte with the
//! expected payload.

use std::error::Error as StdError;
use std::fs::OpenOptions;
use std::io::{BufReader, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use babeldb::cli::protocol::{self, Reply, op};
use babeldb::cli::server::{self, Client as BabelClient, ServerConfig, ServerHandle};
use babeldb::datasets::{self, Scenario, SplitMix64};
use babeldb::planner::TrainOptions;
use babeldb::scale::chat::message_key;
use babeldb::scale::group_commit::{GroupCommitConfig, GroupCommitter, WriteDurability};
use babeldb::config::{WalConfig, WalSync};
use babeldb::store::Store;
#[cfg(feature = "fjall")]
use babeldb::store::{fjall::{DeferredPersist, FjallCompression, FjallOptions, FjallStore}, wal::WalStore};
use babeldb::{BatchOp, Config, Db, Expect, ScanItem, ScanOptions};
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, Document, doc};
use mongodb::options::{Acknowledgment, CollectionOptions, WriteConcern};

type R<T> = Result<T, Box<dyn StdError + Send + Sync>>;

const LOAD_BATCH: usize = 1000;
const LATEST_N: usize = 50;
const VERIFY_EVERY: usize = 64;
/// `babel-dict`: messages the zstd dictionary is trained on, before the load.
const DICT_SAMPLES: u64 = 2000;

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Msg {
    channel: u64,
    id: u64,
    payload: Vec<u8>,
}

fn msg(seed: u64, i: u64, value_size: usize) -> Msg {
    Msg {
        channel: datasets::channel_of(i, seed),
        id: datasets::s3_snowflake(seed, i),
        payload: datasets::value(Scenario::ChatJson, seed, i, value_size),
    }
}

#[derive(Clone, Debug)]
struct Params {
    records: u64,
    value_size: usize,
    seed: u64,
    systems: Vec<String>,
    put_ops: usize,
    put_threads: Vec<usize>,
    put_mt_ops: usize,
    read_ops: usize,
    read_threads: Vec<usize>,
    relaxed: bool,
    /// Compact babeldb after the load (`space-compacted`).
    compact: bool,
    /// Depth of the pipelined TCP phases (0 = not run).
    pipeline: usize,
    dir: PathBuf,
    out: Option<PathBuf>,
    pg_url: Option<String>,
    mongo_url: String,
    tag: String,
}

impl Default for Params {
    fn default() -> Self {
        Params {
            records: 100_000,
            value_size: 512,
            seed: datasets::DEFAULT_SEED,
            systems: vec![
                "babel-raw".into(),
                "babel-adaptive".into(),
                "babel-dict".into(),
                "babel-tcp".into(),
                "babel-tcp-raw".into(),
                "postgres".into(),
                "mongo".into(),
            ],
            put_ops: 300,
            put_threads: vec![1, 4, 16, 64],
            put_mt_ops: 200,
            read_ops: 20_000,
            read_threads: vec![1, 4, 16],
            relaxed: false,
            compact: true,
            pipeline: 0,
            dir: PathBuf::from("bench-results/tmp-compare"),
            out: Some(PathBuf::from("bench-results/compare.jsonl")),
            pg_url: std::env::var("BABEL_PG_URL").ok(),
            mongo_url: std::env::var("BABEL_MONGO_URL")
                .unwrap_or_else(|_| "mongodb://127.0.0.1:27017/?maxPoolSize=256".into()),
            tag: String::new(),
        }
    }
}

fn parse_count(s: &str) -> R<u64> {
    let s = s.trim().to_ascii_lowercase();
    let (num, mul) = match s.strip_suffix('k') {
        Some(n) => (n, 1_000),
        None => match s.strip_suffix('m') {
            Some(n) => (n, 1_000_000),
            None => (s.as_str(), 1),
        },
    };
    Ok(num.parse::<u64>()? * mul)
}

fn parse_list(s: &str) -> R<Vec<usize>> {
    s.split(',').map(|x| Ok(parse_count(x)? as usize)).collect()
}

fn parse_args() -> R<Params> {
    let mut p = Params::default();
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--bench").collect();
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        let mut val = || -> R<String> {
            i += 1;
            args.get(i).cloned().ok_or_else(|| format!("{a} needs a value").into())
        };
        match a {
            "--records" => p.records = parse_count(&val()?)?,
            "--value-size" => p.value_size = parse_count(&val()?)? as usize,
            "--seed" => p.seed = parse_count(&val()?)?,
            "--systems" => p.systems = val()?.split(',').map(str::to_string).collect(),
            "--put-ops" => p.put_ops = parse_count(&val()?)? as usize,
            "--put-threads" => p.put_threads = parse_list(&val()?)?,
            "--put-mt-ops" => p.put_mt_ops = parse_count(&val()?)? as usize,
            "--read-ops" => p.read_ops = parse_count(&val()?)? as usize,
            "--read-threads" => p.read_threads = parse_list(&val()?)?,
            "--relaxed" => p.relaxed = true,
            "--no-compact" => p.compact = false,
            "--pipeline" => p.pipeline = parse_count(&val()?)? as usize,
            "--dir" => p.dir = PathBuf::from(val()?),
            "--out" => p.out = Some(PathBuf::from(val()?)),
            "--no-out" => p.out = None,
            "--mongo-url" => p.mongo_url = val()?,
            "--tag" => p.tag = val()?,
            "--help" | "-h" => {
                println!(
                    "flags: --records N [100k] --value-size B [512] --systems LIST \
                     [babel-raw,babel-adaptive,babel-dict,babel-tcp,babel-tcp-raw,postgres,mongo; \
                     also babel-tcp-dict, tcp-floor] --put-ops N [300] \
                     --put-threads LIST [1,4,16,64] --put-mt-ops N [200] --read-ops N [20k] \
                     --read-threads LIST [1,4,16] --relaxed --no-compact --pipeline N [off] \
                     --dir PATH --out PATH --no-out \
                     --mongo-url URL --tag TEXT; env BABEL_PG_URL (libpq key=value string)"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown flag {other}").into()),
        }
        i += 1;
    }
    Ok(p)
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

struct Sample {
    phase: String,
    threads: usize,
    ops: usize,
    elapsed: Duration,
    lat_ns: Vec<u64>,
}

impl Sample {
    fn pct(&self, q: f64) -> f64 {
        if self.lat_ns.is_empty() {
            return 0.0;
        }
        let i = ((self.lat_ns.len() - 1) as f64 * q).round() as usize;
        self.lat_ns[i] as f64 / 1000.0
    }

    fn ops_per_s(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64().max(1e-9)
    }
}

fn fmt_us(us: f64) -> String {
    if us >= 1000.0 { format!("{:.2}ms", us / 1000.0) } else { format!("{us:.1}us") }
}

struct Report {
    rows: Vec<(String, Sample)>,
    /// (system, phase, bytes, what)
    space: Vec<(String, &'static str, u64, String)>,
    out: Option<std::fs::File>,
    params: Params,
}

impl Report {
    fn add(&mut self, system: &str, mut s: Sample) {
        s.lat_ns.sort_unstable();
        println!(
            "[{system}] {:<10} x{:<3} {:>7} ops {:>12.0} ops/s  p50 {:>9} p95 {:>9} p99 {:>9} max {:>9}",
            s.phase,
            s.threads,
            s.ops,
            s.ops_per_s(),
            fmt_us(s.pct(0.50)),
            fmt_us(s.pct(0.95)),
            fmt_us(s.pct(0.99)),
            fmt_us(s.pct(1.0)),
        );
        if let Some(f) = self.out.as_mut() {
            let _ = writeln!(
                f,
                "{{\"tag\":\"{}\",\"system\":\"{system}\",\"durability\":\"{}\",\"records\":{},\"value_size\":{},\"phase\":\"{}\",\"threads\":{},\"ops\":{},\"ops_per_s\":{:.1},\"p50_us\":{:.2},\"p95_us\":{:.2},\"p99_us\":{:.2},\"max_us\":{:.2}}}",
                self.params.tag,
                if self.params.relaxed { "relaxed" } else { "durable" },
                self.params.records,
                self.params.value_size,
                s.phase,
                s.threads,
                s.ops,
                s.ops_per_s(),
                s.pct(0.5),
                s.pct(0.95),
                s.pct(0.99),
                s.pct(1.0)
            );
        }
        self.rows.push((system.to_string(), s));
    }

    /// `phase`: "space" or "space-compacted".
    fn add_space(&mut self, system: &str, phase: &'static str, bytes: u64, what: String) {
        println!("[{system}] {phase:<10} {:>10.2} MB  ({what})", bytes as f64 / 1e6);
        if let Some(f) = self.out.as_mut() {
            let _ = writeln!(
                f,
                "{{\"tag\":\"{}\",\"system\":\"{system}\",\"records\":{},\"value_size\":{},\"phase\":\"{phase}\",\"bytes\":{bytes},\"what\":\"{what}\"}}",
                self.params.tag, self.params.records, self.params.value_size
            );
        }
        self.space.push((system.to_string(), phase, bytes, what));
    }
}

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------

/// One connection (or in-process handle) used by a single thread.
trait Session: Send {
    fn load(&mut self, batch: &[Msg]) -> R<()>;
    fn put(&mut self, m: &Msg) -> R<()>;
    fn get(&mut self, channel: u64, id: u64) -> R<Option<Vec<u8>>>;
    fn latest(&mut self, channel: u64, n: usize) -> R<Vec<(u64, Vec<u8>)>>;

    /// Many single-message puts; pipelined where the protocol allows it.
    fn put_many(&mut self, ms: &[Msg]) -> R<()> {
        ms.iter().try_for_each(|m| self.put(m))
    }

    /// Many point reads (channel, id); pipelined where the protocol allows it.
    fn get_many(&mut self, keys: &[(u64, u64)]) -> R<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|&(c, id)| self.get(c, id)).collect()
    }
}

trait Target: Sync {
    fn session(&self) -> R<Box<dyn Session>>;
    /// Bytes on disk used by the loaded data, and what they include.
    fn space(&self) -> R<(u64, String)>;
    /// Compact the storage and measure it again (`None`: not supported).
    fn compact(&mut self) -> R<Option<(u64, String)>> {
        Ok(None)
    }
    /// Whether sessions pipeline `put_many` / `get_many` (TCP systems).
    fn pipelines(&self) -> bool {
        false
    }
    fn close(self: Box<Self>) -> R<()>;
}

// --- babeldb in-process and behind its TCP server ----------------------------

/// Engine flavour of a `babel-*` system.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BabelEngine {
    Raw,
    Adaptive,
    /// Adaptive + a zstd dictionary trained before the load.
    Dict,
}

/// `babel-*` system names: (engine, behind the TCP server, WAL mode).
/// `babel-wal*` systems use `Db::open_wal_with` (write-ahead log in front of
/// redb): `wal` = `WalSync::WriteThrough` (PostgreSQL's default guarantee on
/// Windows), `wal-strict` = `WriteThroughUnbuffered` (FUA requested),
/// `wal-flush` = `Flush` (FlushFileBuffers, redb's own guarantee).
fn babel_system(name: &str) -> Option<(BabelEngine, bool, Option<WalSync>)> {
    let wt = Some(WalSync::WriteThrough);
    Some(match name {
        "babel-raw" => (BabelEngine::Raw, false, None),
        "babel-adaptive" => (BabelEngine::Adaptive, false, None),
        "babel-dict" => (BabelEngine::Dict, false, None),
        "babel-tcp" => (BabelEngine::Adaptive, true, None),
        "babel-tcp-raw" => (BabelEngine::Raw, true, None),
        "babel-tcp-dict" => (BabelEngine::Dict, true, None),
        "babel-wal-raw" => (BabelEngine::Raw, false, wt),
        "babel-wal" => (BabelEngine::Adaptive, false, wt),
        "babel-wal-dict" => (BabelEngine::Dict, false, wt),
        "babel-wal-tcp" => (BabelEngine::Adaptive, true, wt),
        "babel-wal-tcp-raw" => (BabelEngine::Raw, true, wt),
        "babel-wal-tcp-dict" => (BabelEngine::Dict, true, wt),
        "babel-wal-strict-raw" => (BabelEngine::Raw, false, Some(WalSync::WriteThroughUnbuffered)),
        "babel-wal-strict-tcp-raw" => (BabelEngine::Raw, true, Some(WalSync::WriteThroughUnbuffered)),
        "babel-wal-flush-raw" => (BabelEngine::Raw, false, Some(WalSync::Flush)),
        _ => return None,
    })
}

/// The database is owned here; the in-process committer (or the TCP server
/// and its committer) hold clones of the `Arc` only while running, so
/// `compact` can stop them and get the database back exclusively.
struct BabelTarget<S: Store> {
    db: Arc<Db<S>>,
    durability: WriteDurability,
    /// `Some(workers)`: sessions go through the TCP server.
    tcp_threads: Option<usize>,
    /// In-process group committer (in-process systems, while running).
    committer: Option<Arc<GroupCommitter>>,
    /// TCP server and its address (TCP systems, while running).
    server: Option<(ServerHandle, SocketAddr)>,
    space: SpaceProbe<S>,
}

fn babel_open(
    p: &Params,
    name: &str,
    engine: BabelEngine,
    tcp: bool,
    wal: Option<WalSync>,
    tcp_threads: usize,
) -> R<Box<dyn Target>> {
    let dir = p.dir.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let cfg = match engine {
        BabelEngine::Raw => Config::raw_only(),
        BabelEngine::Adaptive | BabelEngine::Dict => Config::adaptive(),
    };
    let path = dir.join("babel.redb");
    Ok(match wal {
        None => Box::new(babel_target(p, name, engine, tcp, tcp_threads, Db::open(path, cfg)?, SpaceProbe::redb())?),
        Some(sync) => {
            let wal_cfg = WalConfig { sync, ..WalConfig::default() };
            let db = Db::open_wal_with(path, cfg, wal_cfg)?;
            Box::new(babel_target(p, name, engine, tcp, tcp_threads, db, SpaceProbe::redb())?)
        }
    })
}

/// What `space` measures for a babeldb backend: the label of the number, and what runs first so
/// that the data files hold every loaded record (the PostgreSQL and MongoDB targets force a
/// checkpoint for the same reason; its duration is printed, it is not part of `load`).
struct SpaceProbe<S: Store> {
    label: &'static str,
    settle: Option<Settle<S>>,
}

/// Brings a database's data files up to date before they are measured.
type Settle<S> = fn(&Db<S>) -> babeldb::Result<()>;

impl<S: Store> SpaceProbe<S> {
    /// redb writes its pages into the file at every commit: nothing to settle.
    fn redb() -> Self {
        SpaceProbe { label: "redb file allocated", settle: None }
    }
}

/// `babel-fjall-wal*` systems: (engine, behind the TCP server).
#[cfg(feature = "fjall")]
fn fjall_system(name: &str) -> Option<(BabelEngine, bool)> {
    Some(match name {
        "babel-fjall-wal-raw" => (BabelEngine::Raw, false),
        "babel-fjall-wal" => (BabelEngine::Adaptive, false),
        "babel-fjall-wal-dict" => (BabelEngine::Dict, false),
        "babel-fjall-wal-tcp" => (BabelEngine::Adaptive, true),
        "babel-fjall-wal-tcp-raw" => (BabelEngine::Raw, true),
        "babel-fjall-wal-tcp-dict" => (BabelEngine::Dict, true),
        _ => return None,
    })
}

/// `FjallOptions::for_wal()`, overridden by the environment (tuning runs):
/// `BABEL_FJALL_COMPRESSION` = `lz4` | `none` | `default` (fjall's: LZ4 from level 2 on),
/// `BABEL_FJALL_BLOCK` = data block bytes, `BABEL_FJALL_MEMTABLE_MB`,
/// `BABEL_FJALL_POINT_HITS` = `1` (no last-level filters), `BABEL_FJALL_PIN` = `1` | `0` (index and
/// filter blocks pinned), `BABEL_FJALL_HASH` = data block hash index percent,
/// `BABEL_FJALL_RESTART` = data block restart interval,
/// `BABEL_FJALL_DEFERRED` = `os` | `buffer`.
#[cfg(feature = "fjall")]
fn fjall_options() -> R<FjallOptions> {
    let env = |k: &str| std::env::var(k).ok();
    let mut o = FjallOptions::for_wal();
    if let Some(v) = env("BABEL_FJALL_COMPRESSION") {
        o.compression = match v.as_str() {
            "lz4" => FjallCompression::Lz4,
            "none" => FjallCompression::None,
            "default" => FjallCompression::FjallDefault,
            other => return Err(format!("BABEL_FJALL_COMPRESSION={other}").into()),
        };
    }
    if let Some(v) = env("BABEL_FJALL_BLOCK") {
        o.data_block_bytes = parse_count(&v)? as u32;
    }
    if let Some(v) = env("BABEL_FJALL_MEMTABLE_MB") {
        o.memtable_bytes = parse_count(&v)? << 20;
    }
    if let Some(v) = env("BABEL_FJALL_POINT_HITS") {
        o.expect_point_read_hits = v == "1";
    }
    if let Some(v) = env("BABEL_FJALL_PIN") {
        o.pin_index_and_filters = v == "1";
    }
    if let Some(v) = env("BABEL_FJALL_HASH") {
        o.data_block_hash_percent = u8::try_from(parse_count(&v)?)?;
    }
    if let Some(v) = env("BABEL_FJALL_RESTART") {
        o.data_block_restart_interval = u8::try_from(parse_count(&v)?)?;
    }
    if let Some(v) = env("BABEL_FJALL_DEFERRED") {
        o.deferred = match v.as_str() {
            "os" => DeferredPersist::WriteToOs,
            "buffer" => DeferredPersist::JournalBuffer,
            other => return Err(format!("BABEL_FJALL_DEFERRED={other}").into()),
        };
    }
    Ok(o)
}

/// The `babel-fjall-wal*` target called `name` (`None` for other names): fjall in
/// `<dir>/<name>/babel.fjall`, its WAL (`WalSync::WriteThrough`, as `babel-wal*`) next to it.
#[cfg(feature = "fjall")]
fn fjall_open(p: &Params, name: &str, tcp_threads: usize) -> R<Option<Box<dyn Target>>> {
    let Some((engine, tcp)) = fjall_system(name) else {
        return Ok(None);
    };
    let dir = p.dir.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let cfg = match engine {
        BabelEngine::Raw => Config::raw_only(),
        BabelEngine::Adaptive | BabelEngine::Dict => Config::adaptive(),
    };
    let opts = fjall_options()?;
    println!("[{name}] fjall      {opts:?}");
    let wal_cfg = WalConfig { sync: WalSync::WriteThrough, ..WalConfig::default() };
    let db = Db::open_fjall_wal_with(dir.join("babel.fjall"), cfg, wal_cfg, &opts)?;
    let space: SpaceProbe<WalStore<FjallStore>> = SpaceProbe {
        label: "fjall tables after a memtable flush (its checkpoint)",
        settle: Some(|db| db.store().inner().flush_memtables()),
    };
    Ok(Some(Box::new(babel_target(p, name, engine, tcp, tcp_threads, db, space)?)))
}

#[cfg(not(feature = "fjall"))]
fn fjall_open(_: &Params, _: &str, _: usize) -> R<Option<Box<dyn Target>>> {
    Ok(None)
}

fn babel_target<S: Store>(
    p: &Params,
    name: &str,
    engine: BabelEngine,
    tcp: bool,
    tcp_threads: usize,
    db: Db<S>,
    space: SpaceProbe<S>,
) -> R<BabelTarget<S>> {
    if engine == BabelEngine::Dict {
        // Trained on the first messages of the workload, before anything is
        // measured (the load below then encodes every message with it).
        let samples: Vec<Vec<u8>> = (0..DICT_SAMPLES.min(p.records)).map(|i| msg(p.seed, i, p.value_size).payload).collect();
        let opts = TrainOptions { expected_uses: p.records.max(1), ..TrainOptions::default() };
        let t0 = Instant::now();
        let rep = db.train_dictionary(&samples, &opts)?;
        println!(
            "[{name}] dict       {} B zstd dictionary from {} samples in {:.2}s (validation {} -> {} B, installed: {})",
            rep.param_bytes,
            rep.train_samples,
            t0.elapsed().as_secs_f64(),
            rep.validation_bytes_without,
            rep.validation_bytes_with,
            rep.installed
        );
    }
    let durability = if p.relaxed {
        WriteDurability::Buffered { flush_interval: Duration::from_millis(100), max_pending_bytes: 64 << 20 }
    } else {
        WriteDurability::Immediate
    };
    let mut target = BabelTarget {
        db: Arc::new(db),
        durability,
        tcp_threads: tcp.then_some(tcp_threads),
        committer: None,
        server: None,
        space,
    };
    target.start()?;
    Ok(target)
}

impl<S: Store> BabelTarget<S> {
    /// Start the committer, or the TCP server (which has its own).
    fn start(&mut self) -> R<()> {
        match self.tcp_threads {
            Some(threads) => {
                let listener = server::bind("127.0.0.1:0")?;
                let addr = listener.local_addr()?;
                let cfg = ServerConfig::new(threads).with_durability(self.durability);
                self.server = Some((server::serve_with(self.db.clone(), listener, cfg)?, addr));
            }
            None => {
                let cfg = GroupCommitConfig::from(self.durability);
                self.committer = Some(Arc::new(GroupCommitter::new(self.db.clone(), cfg)?));
            }
        }
        Ok(())
    }

    /// Stop the server / committer: every accepted write is committed (and
    /// made durable) and their `Arc<Db>` clones are released.
    fn stop(&mut self) -> R<()> {
        if let Some((handle, _)) = self.server.take() {
            handle.shutdown()?;
        }
        if let Some(committer) = self.committer.take() {
            committer.shutdown()?;
        }
        Ok(())
    }

    /// Data files only (the WAL file, like PostgreSQL's WAL and MongoDB's
    /// journal, is reported separately and excluded from the number; so is
    /// fjall's journal).
    fn describe_space(&self, what: &str) -> R<(u64, String)> {
        let s = self.db.stats()?;
        let (data, wal, journal) = split_logs(&s.files, |f| f.allocated_bytes.unwrap_or(f.apparent_bytes));
        Ok((
            data,
            format!(
                "{what}: data files allocated, {}; engine payload {:.2} MB)",
                logs_note(wal, journal),
                s.payload_bytes() as f64 / 1e6
            ),
        ))
    }

    /// After `Db::compact` the file is shorter, but while it stays open NTFS
    /// still reports the allocation of the larger file redb grew during the
    /// compaction (measured: 104 MB apparent, 209 MB allocated; both 104 MB
    /// once closed). The settled size is the smaller of the two.
    fn describe_compacted(&self, took: Duration) -> R<(u64, String)> {
        let s = self.db.stats()?;
        let settled = |f: &babeldb::stats::FileSize| f.allocated_bytes.map_or(f.apparent_bytes, |a| a.min(f.apparent_bytes));
        let (data, wal, journal) = split_logs(&s.files, settled);
        Ok((
            data,
            format!(
                "data files after Db::compact in {:.2}s: min(apparent, allocated), {}; engine payload {:.2} MB)",
                took.as_secs_f64(),
                logs_note(wal, journal),
                s.payload_bytes() as f64 / 1e6
            ),
        ))
    }
}

/// (data bytes, WAL bytes, fjall journal bytes) of a store's files, sized by `size`.
fn split_logs(files: &[babeldb::stats::FileSize], size: impl Fn(&babeldb::stats::FileSize) -> u64) -> (u64, u64, u64) {
    let ext = |f: &babeldb::stats::FileSize, e: &str| f.path.extension().is_some_and(|x| x == e);
    let sum = |keep: &dyn Fn(&babeldb::stats::FileSize) -> bool| files.iter().filter(|f| keep(f)).map(&size).sum::<u64>();
    let wal = sum(&|f| ext(f, "wal"));
    let journal = sum(&|f| ext(f, "jnl"));
    let data = sum(&|f| !ext(f, "wal") && !ext(f, "jnl"));
    (data, wal, journal)
}

/// The excluded logs, up to the engine payload. Backends without a journal of their own keep
/// the text of the earlier rounds.
fn logs_note(wal: u64, journal: u64) -> String {
    if journal == 0 {
        format!("WAL file excluded (+{:.2} MB WAL", wal as f64 / 1e6)
    } else {
        format!(
            "WAL file and fjall journal excluded (+{:.2} MB WAL, +{:.2} MB journal",
            wal as f64 / 1e6,
            journal as f64 / 1e6
        )
    }
}

struct BabelSession<S: Store> {
    db: Arc<Db<S>>,
    committer: Arc<GroupCommitter>,
}

impl<S: Store> Session for BabelSession<S> {
    fn load(&mut self, batch: &[Msg]) -> R<()> {
        let keys: Vec<[u8; 16]> = batch.iter().map(|m| message_key(m.channel, m.id)).collect();
        let ops: Vec<BatchOp<'_>> = batch
            .iter()
            .zip(&keys)
            .map(|(m, k)| BatchOp::Put { key: k, value: &m.payload, expect: Expect::Any })
            .collect();
        self.db.write_batch(&ops)?;
        Ok(())
    }

    fn put(&mut self, m: &Msg) -> R<()> {
        self.committer.put(message_key(m.channel, m.id).to_vec(), m.payload.clone(), Expect::Any)?;
        Ok(())
    }

    fn get(&mut self, channel: u64, id: u64) -> R<Option<Vec<u8>>> {
        Ok(self.db.get(&message_key(channel, id))?)
    }

    fn latest(&mut self, channel: u64, n: usize) -> R<Vec<(u64, Vec<u8>)>> {
        let items = self.db.scan(
            &ScanOptions::prefix(&channel.to_be_bytes()).reverse(true).limit(n).with_values(true),
        )?;
        Ok(items
            .into_iter()
            .map(|it| (u64::from_be_bytes(it.key[8..16].try_into().unwrap()), it.value.unwrap_or_default()))
            .collect())
    }
}

struct BabelTcpSession {
    client: BabelClient,
}

impl Session for BabelTcpSession {
    fn load(&mut self, batch: &[Msg]) -> R<()> {
        let keys: Vec<[u8; 16]> = batch.iter().map(|m| message_key(m.channel, m.id)).collect();
        let items: Vec<(&[u8], &[u8])> =
            batch.iter().zip(&keys).map(|(m, k)| (&k[..], &m.payload[..])).collect();
        self.client.put_batch(&items)?;
        Ok(())
    }

    fn put(&mut self, m: &Msg) -> R<()> {
        self.client.put(&message_key(m.channel, m.id), &m.payload)?;
        Ok(())
    }

    fn get(&mut self, channel: u64, id: u64) -> R<Option<Vec<u8>>> {
        Ok(self.client.get(&message_key(channel, id))?)
    }

    fn latest(&mut self, channel: u64, n: usize) -> R<Vec<(u64, Vec<u8>)>> {
        let items = self.client.scan_prefix(&channel.to_be_bytes(), n as u32, true, true)?;
        Ok(latest_pairs(items))
    }

    fn put_many(&mut self, ms: &[Msg]) -> R<()> {
        let keys: Vec<[u8; 16]> = ms.iter().map(|m| message_key(m.channel, m.id)).collect();
        let items: Vec<(&[u8], &[u8])> = ms.iter().zip(&keys).map(|(m, k)| (&k[..], &m.payload[..])).collect();
        self.client.put_many(&items)?;
        Ok(())
    }

    fn get_many(&mut self, keys: &[(u64, u64)]) -> R<Vec<Option<Vec<u8>>>> {
        let keys: Vec<[u8; 16]> = keys.iter().map(|&(c, id)| message_key(c, id)).collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| &k[..]).collect();
        Ok(self.client.get_many(&refs)?)
    }
}

/// (message id, payload) of scanned `channel BE || id BE` items.
fn latest_pairs(items: Vec<ScanItem>) -> Vec<(u64, Vec<u8>)> {
    items
        .into_iter()
        .map(|it| (u64::from_be_bytes(it.key[8..16].try_into().unwrap()), it.value.unwrap_or_default()))
        .collect()
}

impl<S: Store> Target for BabelTarget<S> {
    fn session(&self) -> R<Box<dyn Session>> {
        if let Some((_, addr)) = &self.server {
            return Ok(Box::new(BabelTcpSession { client: BabelClient::connect(addr)? }));
        }
        let committer = self.committer.clone().ok_or("babeldb target is stopped")?;
        Ok(Box::new(BabelSession { db: self.db.clone(), committer }))
    }

    fn space(&self) -> R<(u64, String)> {
        if let Some((handle, _)) = &self.server {
            handle.flush()?;
        }
        if let Some(committer) = &self.committer {
            committer.flush()?;
        }
        match self.space.settle {
            None => self.describe_space(self.space.label),
            Some(settle) => {
                let t0 = Instant::now();
                settle(&self.db)?;
                self.describe_space(&format!("{} in {:.2}s", self.space.label, t0.elapsed().as_secs_f64()))
            }
        }
    }

    fn compact(&mut self) -> R<Option<(u64, String)>> {
        self.stop()?;
        let t0 = Instant::now();
        // No session, committer or server holds a clone any more.
        let compacted: R<babeldb::maintenance::CompactReport> = match Arc::get_mut(&mut self.db) {
            Some(db) => db.compact().map_err(Into::into),
            None => Err("the database is still shared: Db::compact needs it exclusively".into()),
        };
        let took = t0.elapsed();
        let restarted = self.start();
        let report = compacted?;
        restarted?;
        if !report.supported {
            return Ok(None);
        }
        Ok(Some(self.describe_compacted(took)?))
    }

    fn pipelines(&self) -> bool {
        self.server.is_some()
    }

    fn close(mut self: Box<Self>) -> R<()> {
        self.stop()
    }
}

// --- PostgreSQL ------------------------------------------------------------

struct PgTarget {
    url: String,
    relaxed: bool,
}

fn pg_bench_url(base: &str) -> String {
    // Replace (or add) dbname in a libpq key=value string.
    let mut parts: Vec<String> = base.split_whitespace().filter(|kv| !kv.starts_with("dbname=")).map(str::to_string).collect();
    parts.push("dbname=babeldb_bench".into());
    parts.join(" ")
}

fn pg_open(p: &Params) -> R<PgTarget> {
    let base = p.pg_url.clone().ok_or("BABEL_PG_URL is not set")?;
    let mut admin = postgres::Client::connect(&base, postgres::NoTls)?;
    admin.batch_execute("DROP DATABASE IF EXISTS babeldb_bench")?;
    admin.batch_execute("CREATE DATABASE babeldb_bench")?;
    drop(admin);
    let url = pg_bench_url(&base);
    let mut c = postgres::Client::connect(&url, postgres::NoTls)?;
    c.batch_execute(
        "CREATE TABLE messages (channel_id bigint NOT NULL, id bigint NOT NULL, payload bytea NOT NULL, PRIMARY KEY (channel_id, id))",
    )?;
    Ok(PgTarget { url, relaxed: p.relaxed })
}

struct PgSession {
    c: postgres::Client,
    ins: postgres::Statement,
    get: postgres::Statement,
    latest: postgres::Statement,
}

impl Session for PgSession {
    fn load(&mut self, batch: &[Msg]) -> R<()> {
        let mut buf = Vec::with_capacity(batch.len() * (self::LOAD_ROW_OVERHEAD + 600) + 32);
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        for m in batch {
            buf.extend_from_slice(&3i16.to_be_bytes());
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&(m.channel as i64).to_be_bytes());
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&(m.id as i64).to_be_bytes());
            buf.extend_from_slice(&(m.payload.len() as i32).to_be_bytes());
            buf.extend_from_slice(&m.payload);
        }
        buf.extend_from_slice(&(-1i16).to_be_bytes());
        let mut w = self.c.copy_in("COPY messages (channel_id, id, payload) FROM STDIN (FORMAT binary)")?;
        w.write_all(&buf)?;
        w.finish()?;
        Ok(())
    }

    fn put(&mut self, m: &Msg) -> R<()> {
        self.c.execute(&self.ins, &[&(m.channel as i64), &(m.id as i64), &m.payload])?;
        Ok(())
    }

    fn get(&mut self, channel: u64, id: u64) -> R<Option<Vec<u8>>> {
        Ok(self.c.query_opt(&self.get, &[&(channel as i64), &(id as i64)])?.map(|r| r.get::<_, Vec<u8>>(0)))
    }

    fn latest(&mut self, channel: u64, n: usize) -> R<Vec<(u64, Vec<u8>)>> {
        let rows = self.c.query(&self.latest, &[&(channel as i64), &(n as i64)])?;
        Ok(rows.into_iter().map(|r| (r.get::<_, i64>(0) as u64, r.get::<_, Vec<u8>>(1))).collect())
    }
}

const LOAD_ROW_OVERHEAD: usize = 2 + 4 + 8 + 4 + 8 + 4;

impl Target for PgTarget {
    fn session(&self) -> R<Box<dyn Session>> {
        let mut c = postgres::Client::connect(&self.url, postgres::NoTls)?;
        c.batch_execute(if self.relaxed { "SET synchronous_commit = off" } else { "SET synchronous_commit = on" })?;
        let ins = c.prepare("INSERT INTO messages (channel_id, id, payload) VALUES ($1, $2, $3)")?;
        let get = c.prepare("SELECT payload FROM messages WHERE channel_id = $1 AND id = $2")?;
        let latest = c.prepare("SELECT id, payload FROM messages WHERE channel_id = $1 ORDER BY id DESC LIMIT $2")?;
        Ok(Box::new(PgSession { c, ins, get, latest }))
    }

    fn space(&self) -> R<(u64, String)> {
        let mut c = postgres::Client::connect(&self.url, postgres::NoTls)?;
        c.batch_execute("CHECKPOINT")?;
        let row = c.query_one("SELECT pg_total_relation_size('messages')", &[])?;
        let bytes: i64 = row.get(0);
        Ok((bytes as u64, "pg_total_relation_size(messages): heap + TOAST + primary key index (WAL excluded)".into()))
    }

    fn close(self: Box<Self>) -> R<()> {
        Ok(())
    }
}

// --- MongoDB ---------------------------------------------------------------

struct MongoTarget {
    client: mongodb::sync::Client,
    coll: mongodb::sync::Collection<Document>,
}

fn mongo_open(p: &Params) -> R<MongoTarget> {
    let client = mongodb::sync::Client::with_uri_str(&p.mongo_url)?;
    let db = client.database("babeldb_bench");
    db.drop().run()?;
    let wc = WriteConcern::builder().w(Acknowledgment::Nodes(1)).journal(!p.relaxed).build();
    let coll = db.collection_with_options::<Document>("messages", CollectionOptions::builder().write_concern(wc).build());
    db.create_collection("messages").run()?;
    Ok(MongoTarget { client, coll })
}

fn mongo_doc(m: &Msg) -> Document {
    doc! {
        "_id": { "c": m.channel as i64, "m": m.id as i64 },
        "p": Binary { subtype: BinarySubtype::Generic, bytes: m.payload.clone() },
    }
}

struct MongoSession {
    coll: mongodb::sync::Collection<Document>,
}

fn doc_bytes(d: &Document) -> R<Vec<u8>> {
    match d.get("p") {
        Some(Bson::Binary(b)) => Ok(b.bytes.clone()),
        _ => Err("document without binary payload".into()),
    }
}

impl Session for MongoSession {
    fn load(&mut self, batch: &[Msg]) -> R<()> {
        self.coll.insert_many(batch.iter().map(mongo_doc)).ordered(true).run()?;
        Ok(())
    }

    fn put(&mut self, m: &Msg) -> R<()> {
        self.coll.insert_one(mongo_doc(m)).run()?;
        Ok(())
    }

    fn get(&mut self, channel: u64, id: u64) -> R<Option<Vec<u8>>> {
        let found = self.coll.find_one(doc! { "_id": { "c": channel as i64, "m": id as i64 } }).run()?;
        found.as_ref().map(doc_bytes).transpose()
    }

    fn latest(&mut self, channel: u64, n: usize) -> R<Vec<(u64, Vec<u8>)>> {
        let c = channel as i64;
        let cursor = self
            .coll
            .find(doc! { "_id": { "$gte": { "c": c, "m": i64::MIN }, "$lte": { "c": c, "m": i64::MAX } } })
            .sort(doc! { "_id": -1 })
            .limit(n as i64)
            .run()?;
        let mut out = Vec::with_capacity(n);
        for d in cursor {
            let d = d?;
            let id = d.get_document("_id")?.get_i64("m")? as u64;
            out.push((id, doc_bytes(&d)?));
        }
        Ok(out)
    }
}

fn bson_u64(d: &Document, key: &str) -> u64 {
    match d.get(key) {
        Some(Bson::Int32(v)) => *v as u64,
        Some(Bson::Int64(v)) => *v as u64,
        Some(Bson::Double(v)) => *v as u64,
        _ => 0,
    }
}

impl Target for MongoTarget {
    fn session(&self) -> R<Box<dyn Session>> {
        Ok(Box::new(MongoSession { coll: self.coll.clone() }))
    }

    fn space(&self) -> R<(u64, String)> {
        // Force a checkpoint so WiredTiger files reflect the loaded data.
        self.client.database("admin").run_command(doc! { "fsync": 1 }).run()?;
        let stats = self.client.database("babeldb_bench").run_command(doc! { "dbStats": 1, "scale": 1 }).run()?;
        let storage = bson_u64(&stats, "storageSize");
        let index = bson_u64(&stats, "indexSize");
        Ok((
            storage + index,
            format!("dbStats storageSize {storage} + indexSize {index} (WiredTiger, default snappy block compression; journal excluded)"),
        ))
    }

    fn close(self: Box<Self>) -> R<()> {
        Ok(())
    }
}

// --- loopback floor (tcp-floor) --------------------------------------------

/// A trivial server: every GET is answered with a pre-encoded VALUE reply of
/// `value_size` bytes, every SCAN_PREFIX with a pre-encoded 50-item reply,
/// anything else with DONE; replies of already buffered requests go out in
/// one write. No engine, no dispatch, no stop polling: what remains is the
/// babeldb client, the framing and the loopback.
struct FloorTarget {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    acceptor: Option<std::thread::JoinHandle<()>>,
}

/// (GET reply, SCAN_PREFIX reply, other reply), frames included.
type FloorReplies = (Vec<u8>, Vec<u8>, Vec<u8>);

fn floor_open(p: &Params) -> R<FloorTarget> {
    let payload = msg(p.seed, 0, p.value_size).payload;
    let mut get_reply = Vec::new();
    Reply::Value(payload.clone()).encode(&mut get_reply)?;
    let items: Vec<ScanItem> = (0..LATEST_N as u64)
        .map(|j| ScanItem {
            key: message_key(1, LATEST_N as u64 - j).to_vec(),
            revision: j + 1,
            logical_len: payload.len() as u64,
            value: Some(payload.clone()),
        })
        .collect();
    let mut scan_reply = Vec::new();
    Reply::Items(items).encode(&mut scan_reply)?;
    let mut other_reply = Vec::new();
    Reply::Done.encode(&mut other_reply)?;
    let replies: Arc<FloorReplies> = Arc::new((get_reply, scan_reply, other_reply));
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let acceptor = {
        let stop = stop.clone();
        std::thread::Builder::new().name("floor-acceptor".into()).spawn(move || {
            let mut conns = Vec::new();
            for stream in listener.incoming() {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let replies = replies.clone();
                conns.push(std::thread::spawn(move || floor_serve(stream, &replies)));
            }
            // Connections end when their clients (the sessions) close.
            for c in conns {
                let _ = c.join();
            }
        })?
    };
    Ok(FloorTarget { addr, stop, acceptor: Some(acceptor) })
}

/// Whether `buf` starts with a complete frame.
fn frame_complete(buf: &[u8]) -> bool {
    buf.len() >= protocol::LEN_PREFIX
        && buf.len() - protocol::LEN_PREFIX >= u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize
}

fn floor_serve(stream: TcpStream, replies: &FloorReplies) {
    let _ = stream.set_nodelay(true);
    let Ok(mut writer) = stream.try_clone() else { return };
    let mut reader = BufReader::with_capacity(64 * 1024, stream);
    let (mut body, mut out) = (Vec::new(), Vec::new());
    while let Ok(true) = protocol::read_frame(&mut reader, &mut body) {
        out.extend_from_slice(match body.first() {
            Some(&op::GET) => &replies.0,
            Some(&op::SCAN_PREFIX) => &replies.1,
            _ => &replies.2,
        });
        if !frame_complete(reader.buffer()) {
            if writer.write_all(&out).is_err() {
                break;
            }
            out.clear();
        }
    }
}

impl Target for FloorTarget {
    fn session(&self) -> R<Box<dyn Session>> {
        // GET and SCAN_PREFIX behave as against babel-tcp; writes are refused.
        Ok(Box::new(BabelTcpSession { client: BabelClient::connect(self.addr)? }))
    }

    fn space(&self) -> R<(u64, String)> {
        Err("tcp-floor stores nothing".into())
    }

    fn pipelines(&self) -> bool {
        true
    }

    fn close(mut self: Box<Self>) -> R<()> {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------

/// Run `per_thread` ops on each of `threads` sessions started together.
/// `prep` builds every input before the clock starts; only `op` is timed per
/// operation; `post` (verification) runs after each op, outside its latency.
fn run_phase<I, O>(
    target: &dyn Target,
    threads: usize,
    per_thread: usize,
    prep: impl Fn(usize, usize) -> I + Sync,
    op: impl Fn(&I, &mut dyn Session) -> R<O> + Sync,
    post: impl Fn(&I, O) -> R<()> + Sync,
) -> R<Sample>
where
    I: Send + Sync,
{
    let mut sessions = Vec::with_capacity(threads);
    for _ in 0..threads {
        sessions.push(target.session()?);
    }
    let inputs: Vec<Vec<I>> = (0..threads).map(|t| (0..per_thread).map(|k| prep(t, k)).collect()).collect();
    let barrier = Barrier::new(threads + 1);
    let results: Mutex<Vec<R<Vec<u64>>>> = Mutex::new(Vec::new());
    let (op, post) = (&op, &post);
    let started = std::thread::scope(|scope| {
        for (mut session, ins) in sessions.into_iter().zip(&inputs) {
            let (barrier, results) = (&barrier, &results);
            scope.spawn(move || {
                let mut lat = Vec::with_capacity(ins.len());
                barrier.wait();
                let mut res = Ok(());
                for input in ins {
                    let t0 = Instant::now();
                    let out = match op(input, session.as_mut()) {
                        Ok(o) => o,
                        Err(e) => {
                            res = Err(e);
                            break;
                        }
                    };
                    lat.push(t0.elapsed().as_nanos() as u64);
                    if let Err(e) = post(input, out) {
                        res = Err(e);
                        break;
                    }
                }
                results.lock().unwrap().push(res.map(|()| lat));
            });
        }
        barrier.wait();
        Instant::now()
    });
    let elapsed = started.elapsed();
    let mut lat_ns = Vec::with_capacity(threads * per_thread);
    for r in results.into_inner().unwrap() {
        lat_ns.extend(r?);
    }
    Ok(Sample { phase: String::new(), threads, ops: lat_ns.len(), elapsed, lat_ns })
}

fn check(expected: &[u8], got: Option<&[u8]>, what: &str) -> R<()> {
    match got {
        Some(g) if g == expected => Ok(()),
        Some(g) => Err(format!("{what}: wrong bytes ({} vs {} expected)", g.len(), expected.len()).into()),
        None => Err(format!("{what}: missing").into()),
    }
}

/// A point read: message index, channel, id, op number.
struct ReadIn {
    i: u64,
    channel: u64,
    id: u64,
    k: usize,
}

fn run_system(p: &Params, name: &str, report: &mut Report) -> R<()> {
    println!("== {name} ({} records x {} B, {})", p.records, p.value_size, if p.relaxed { "relaxed" } else { "durable" });
    if name == "tcp-floor" {
        let target = floor_open(p)?;
        let result = read_phases(p, name, &target, report, false);
        Box::new(target).close()?;
        return result;
    }
    let max_threads = p.put_threads.iter().chain(&p.read_threads).copied().max().unwrap_or(1);
    let mut target: Box<dyn Target> = match (babel_system(name), name) {
        (Some((engine, tcp, wal)), _) => babel_open(p, name, engine, tcp, wal, max_threads + 2)?,
        (None, "postgres") => Box::new(pg_open(p)?),
        (None, "mongo") => Box::new(mongo_open(p)?),
        (None, other) => match fjall_open(p, other, max_threads + 2)? {
            Some(target) => target,
            None => return Err(format!("unknown system {other}").into()),
        },
    };

    // bulk load, 1000 messages per batch (batch generation is not timed)
    {
        let mut s = target.session()?;
        let mut lat = Vec::new();
        let mut busy = Duration::ZERO;
        let mut i = 0u64;
        while i < p.records {
            let end = (i + LOAD_BATCH as u64).min(p.records);
            let batch: Vec<Msg> = (i..end).map(|j| msg(p.seed, j, p.value_size)).collect();
            let b0 = Instant::now();
            s.load(&batch)?;
            let d = b0.elapsed();
            busy += d;
            lat.push(d.as_nanos() as u64);
            i = end;
        }
        println!(
            "[{name}] load       {} records in {:.2}s of batches -> {:.0} rec/s",
            p.records,
            busy.as_secs_f64(),
            p.records as f64 / busy.as_secs_f64()
        );
        let sample = Sample { phase: "load-batch".into(), threads: 1, ops: lat.len(), elapsed: busy, lat_ns: lat };
        report.add(name, sample);
    }
    let (bytes, what) = target.space()?;
    report.add_space(name, "space", bytes, what);
    if p.compact {
        match target.compact() {
            Ok(Some((bytes, what))) => report.add_space(name, "space-compacted", bytes, what),
            Ok(None) => {}
            Err(e) => println!("[{name}] space-compacted ERROR: {e}"),
        }
    }

    // single-message commits: 1 thread, then T threads (fresh ids, never loaded)
    let fresh = |round: u64, t: usize, k: usize| p.records + 10_000_000 * (round + 1) + (t * 100_000 + k) as u64;
    let rounds = p.put_threads.len() + 1;
    for (round, &threads) in std::iter::once(&1usize).chain(&p.put_threads).enumerate() {
        let ops = if round == 0 { p.put_ops } else { p.put_mt_ops };
        let mut s = run_phase(
            target.as_ref(),
            threads,
            ops,
            |t, k| msg(p.seed, fresh(round as u64, t, k), p.value_size),
            |m, sess| sess.put(m),
            |_, ()| Ok(()),
        )?;
        s.phase = if round == 0 { "put".into() } else { "put-mt".into() };
        report.add(name, s);
    }
    // the same single-message commits, `depth` in flight on one connection
    if p.pipeline > 0 && target.pipelines() {
        let depth = p.pipeline;
        let batches = p.put_ops.div_ceil(depth);
        let mut s = run_phase(
            target.as_ref(),
            1,
            batches,
            |t, k| (0..depth).map(|j| msg(p.seed, fresh(rounds as u64, t, k * depth + j), p.value_size)).collect::<Vec<Msg>>(),
            |ms, sess| sess.put_many(ms),
            |_, ()| Ok(()),
        )?;
        s.ops *= depth;
        s.phase = format!("put-pipe{depth}");
        report.add(name, s);
    }

    read_phases(p, name, target.as_ref(), report, true)?;
    target.close()?;
    Ok(())
}

/// Point reads (`get`, `get-mt`, `get-pipeN`) and newest-50 scans (`latest`,
/// `latest-mt`). `verify`: every 64th value is compared with the expected
/// payload (the floor answers every read with the same bytes).
fn read_phases(p: &Params, name: &str, target: &dyn Target, report: &mut Report, verify: bool) -> R<()> {
    let read_in = |t: usize, k: usize| {
        let i = SplitMix64::new(p.seed ^ ((t as u64) << 32) ^ k as u64).next_u64() % p.records;
        ReadIn { i, channel: datasets::channel_of(i, p.seed), id: datasets::s3_snowflake(p.seed, i), k }
    };
    let check_read = |r: &ReadIn, got: Option<&[u8]>| -> R<()> {
        if verify && r.k.is_multiple_of(VERIFY_EVERY) {
            check(&msg(p.seed, r.i, p.value_size).payload, got, "get")
        } else if got.is_none() {
            Err(format!("get: message {} missing", r.i).into())
        } else {
            Ok(())
        }
    };

    // point reads, uniform over the loaded messages
    for &threads in &p.read_threads {
        let per = if threads == 1 { p.read_ops } else { (p.read_ops / 2).max(1000) };
        let mut s = run_phase(target, threads, per, read_in, |r, sess| sess.get(r.channel, r.id), |r, got| {
            check_read(r, got.as_deref())
        })?;
        s.phase = if threads == 1 { "get".into() } else { "get-mt".into() };
        report.add(name, s);
    }
    // the same reads, `depth` in flight on one connection
    if p.pipeline > 0 && target.pipelines() {
        let depth = p.pipeline;
        let batches = p.read_ops.div_ceil(depth);
        let mut s = run_phase(
            target,
            1,
            batches,
            |t, k| {
                let reads: Vec<ReadIn> = (0..depth).map(|j| read_in(t, k * depth + j)).collect();
                let keys: Vec<(u64, u64)> = reads.iter().map(|r| (r.channel, r.id)).collect();
                (reads, keys)
            },
            |(_, keys), sess| sess.get_many(keys),
            |(reads, _), got| {
                if got.len() != reads.len() {
                    return Err(format!("get-pipe: {} replies for {} reads", got.len(), reads.len()).into());
                }
                reads.iter().zip(&got).try_for_each(|(r, g)| check_read(r, g.as_deref()))
            },
        )?;
        s.ops *= depth;
        s.phase = format!("get-pipe{depth}");
        report.add(name, s);
    }

    // newest 50 messages of a channel (channels weighted by message count)
    for &threads in &p.read_threads {
        let per = if threads == 1 { p.read_ops / 4 } else { (p.read_ops / 8).max(500) };
        let mut s = run_phase(
            target,
            threads,
            per,
            |t, k| {
                let i = SplitMix64::new(!p.seed ^ ((t as u64) << 40) ^ k as u64).next_u64() % p.records;
                datasets::channel_of(i, p.seed)
            },
            |&channel, sess| sess.latest(channel, LATEST_N),
            |&channel, items| {
                if items.is_empty() {
                    return Err(format!("latest: channel {channel} empty").into());
                }
                if items.windows(2).any(|w| w[0].0 <= w[1].0) {
                    return Err(format!("latest: channel {channel} not newest-first").into());
                }
                Ok(())
            },
        )?;
        s.phase = if threads == 1 { "latest".into() } else { "latest-mt".into() };
        report.add(name, s);
    }
    Ok(())
}

fn main() -> R<()> {
    let p = parse_args()?;
    if let Some(dir) = p.out.as_ref().and_then(|o| o.parent()) {
        std::fs::create_dir_all(dir)?;
    }
    let out = match &p.out {
        Some(path) => Some(OpenOptions::new().create(true).append(true).open(path)?),
        None => None,
    };
    let mut report = Report { rows: Vec::new(), space: Vec::new(), out, params: p.clone() };
    for system in &p.systems {
        if let Err(e) = run_system(&p, system, &mut report) {
            println!("[{system}] ERROR: {e}");
        }
    }
    let _ = std::fs::remove_dir_all(&p.dir);
    println!("\n== summary (ops/s; p99)");
    let mut phases: Vec<(String, usize)> = Vec::new();
    for (_, s) in &report.rows {
        if !phases.contains(&(s.phase.clone(), s.threads)) {
            phases.push((s.phase.clone(), s.threads));
        }
    }
    for (phase, threads) in phases {
        let line: Vec<String> = report
            .rows
            .iter()
            .filter(|(_, s)| s.phase == phase && s.threads == threads)
            .map(|(sys, s)| format!("{sys}: {:.0}/s p99 {}", s.ops_per_s(), fmt_us(s.pct(0.99))))
            .collect();
        println!("{phase:<10} x{threads:<3} {}", line.join(" | "));
    }
    for (sys, phase, bytes, _) in &report.space {
        println!("{phase:<10} {sys}: {:.2} MB", *bytes as f64 / 1e6);
    }
    Ok(())
}
