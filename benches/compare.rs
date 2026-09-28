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
//!
//! `--phases LIST` selects phases (default: the ones above, i.e. `default` = `load,space,put,
//! put-mt,put-pipe,get,get-mt,get-pipe,latest,latest-mt`; `all` adds the opt-in phases below;
//! `-NAME` removes one; `load` always runs). The opt-in phases run after the others, in this order:
//! - `range` / `range-mt` (read threads as `get`): the messages of a channel between two ids,
//!   `RANGE_N` (200) from a random position, oldest first (`id BETWEEN $2 AND $3 ORDER BY id`, an
//!   `_id` range, a babeldb scan with included bounds); channels weighted by message count; checked
//!   for exact ids and order, every 64th payload compared. The babeldb TCP systems run the same scan
//!   with the protocol's SCAN_RANGE (`Client::scan`); `tcp-floor` prints `skipped`.
//! - `mixed95` / `mixed50`: `--mixed-threads` clients, each op a read with 95 / 50 % probability
//!   (a point `get` 3 times in 4, a `latest` scan otherwise) or else a durable put of a new message;
//!   rows `mixedNN` (every op: aggregate ops/s), `mixedNN-read` and `mixedNN-write` (the same run,
//!   one kind of op: its rate and latencies).
//! - `update` / `update-mt` (threads and ops as `put` / `put-mt`): overwrite loaded messages with a
//!   new payload of the same size (`UPDATE ... SET payload`, `replace_one`, a babeldb put with
//!   `Expect::Any`), a distinct message per op; every 64th is read back afterwards.
//! - `delete` / `delete-mt` (as `put`): delete distinct loaded messages (`DELETE`, `delete_one`, a
//!   babeldb delete); each must report a deleted record; every 64th is read back as missing.
//!
//! Their writes are durable like `put` (relaxed like `put` with `--relaxed`).
//!
//! Values: `--scenario` picks the payload generator of `babeldb::datasets` (`s3`, default: the chat
//! JSON padded to `--value-size`; `s5`: incompressible bytes; `s1`..`s6`), keys stay chat-shaped.
//! Sizes accept `k` / `m` (1000s) and `ki` / `mi` (1024s: `--value-size 64ki`). For large values,
//! load batches hold at most 16 MiB (the TCP frame limit is 64 MiB), and each phase reads or writes
//! at most `--budget-mb` (2048) of values: ops per thread are reduced, and the reduction printed,
//! where the configured counts would move more (never with the default 512 B values).

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fs::OpenOptions;
use std::io::{BufReader, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Bound;
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
/// `babel-dict`: at most this many bytes of training samples (large values).
const DICT_SAMPLE_BYTES: usize = 32 << 20;
/// Load batches hold at most this many bytes of values (and `LOAD_BATCH` messages).
const LOAD_BATCH_BYTES: usize = 16 << 20;
/// Messages per `range` op.
const RANGE_N: usize = 200;
/// `update` writes the payloads the generator makes with the seed xor this.
const UPDATE_SEED: u64 = 0x5EED_0F_ED17_0000;
/// Draws of the `range` and `mixed` phases (distinct from those of `get` / `latest`).
const RANGE_SALT: u64 = 0x7A4E_6E00_0000_0000;
const MIXED_SALT: u64 = 0x313D_0000_0000_0000;

/// Phases `--phases` selects from, in the order they run (`load` always runs).
const PHASES: [&str; 18] = [
    "load", "space", "put", "put-mt", "put-pipe", "get", "get-mt", "get-pipe", "latest", "latest-mt", "range",
    "range-mt", "mixed95", "mixed50", "update", "update-mt", "delete", "delete-mt",
];
/// The phases of rounds 1-3 (`--phases default`).
const DEFAULT_PHASES: [&str; 10] =
    ["load", "space", "put", "put-mt", "put-pipe", "get", "get-mt", "get-pipe", "latest", "latest-mt"];

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Msg {
    channel: u64,
    id: u64,
    payload: Vec<u8>,
}

/// Loaded message `i` (fresh ids above `--records` are never loaded).
fn msg(p: &Params, i: u64) -> Msg {
    msg_with(p, i, payload(p, i))
}

/// Message `i`'s key (channel, id) with `payload`.
fn msg_with(p: &Params, i: u64, payload: Vec<u8>) -> Msg {
    let (channel, id) = key_of(p, i);
    Msg { channel, id, payload }
}

/// (channel, id) of message `i`.
fn key_of(p: &Params, i: u64) -> (u64, u64) {
    (datasets::channel_of(i, p.seed), datasets::s3_snowflake(p.seed, i))
}

/// Payload of message `i` (`--scenario`, `--value-size`).
fn payload(p: &Params, i: u64) -> Vec<u8> {
    datasets::value(p.scenario, p.seed, i, p.value_size)
}

/// The payload `update` writes over message `i`: same generator and size, other content.
fn update_payload(p: &Params, i: u64) -> Vec<u8> {
    datasets::value(p.scenario, p.seed ^ UPDATE_SEED, i, p.value_size)
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
    /// Payload generator (`--scenario`).
    scenario: Scenario,
    /// Selected phases (`--phases`), names from `PHASES`.
    phases: Vec<&'static str>,
    /// Bytes of values one phase may read or write (`--budget-mb`).
    budget: u64,
    mixed_threads: Vec<usize>,
    /// Ops per thread of `mixed95` / `mixed50`.
    mixed_ops: usize,
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
            scenario: Scenario::ChatJson,
            phases: DEFAULT_PHASES.to_vec(),
            budget: 2048 << 20,
            mixed_threads: vec![16],
            mixed_ops: 2000,
            dir: PathBuf::from("bench-results/tmp-compare"),
            out: Some(PathBuf::from("bench-results/compare.jsonl")),
            pg_url: std::env::var("BABEL_PG_URL").ok(),
            mongo_url: std::env::var("BABEL_MONGO_URL")
                .unwrap_or_else(|_| "mongodb://127.0.0.1:27017/?maxPoolSize=256".into()),
            tag: String::new(),
        }
    }
}

impl Params {
    fn has(&self, phase: &str) -> bool {
        self.phases.contains(&phase)
    }
}

/// `k` / `m`: thousands / millions; `ki` / `kib` / `mi` / `mib`: binary multiples.
fn parse_count(s: &str) -> R<u64> {
    let s = s.trim().to_ascii_lowercase();
    let binary = |unit: &str| s.strip_suffix(&format!("{unit}ib")).or_else(|| s.strip_suffix(&format!("{unit}i")));
    let (num, mul) = if let Some(n) = binary("k") {
        (n, 1 << 10)
    } else if let Some(n) = binary("m") {
        (n, 1 << 20)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1_000_000)
    } else {
        (s.as_str(), 1)
    };
    Ok(num.parse::<u64>()? * mul)
}

fn parse_list(s: &str) -> R<Vec<usize>> {
    s.split(',').map(|x| Ok(parse_count(x)? as usize)).collect()
}

/// `--phases`: names from `PHASES`, `default`, `all`, `mixed` (both mixes); `-NAME` removes (a
/// list starting with a removal starts from `default`).
fn parse_phases(s: &str) -> R<Vec<&'static str>> {
    let mut set: Vec<&'static str> = if s.trim_start().starts_with('-') { DEFAULT_PHASES.to_vec() } else { Vec::new() };
    for item in s.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        let (remove, name) = match item.strip_prefix('-') {
            Some(n) => (true, n),
            None => (false, item),
        };
        let names: Vec<&'static str> = match name {
            "all" => PHASES.to_vec(),
            "default" => DEFAULT_PHASES.to_vec(),
            "mixed" => vec!["mixed95", "mixed50"],
            n => match PHASES.iter().find(|&&x| x == n) {
                Some(&x) => vec![x],
                None => return Err(format!("unknown phase {n} (phases: {}; default, all, mixed)", PHASES.join(",")).into()),
            },
        };
        for n in names {
            if remove {
                set.retain(|&x| x != n);
            } else if !set.contains(&n) {
                set.push(n);
            }
        }
    }
    Ok(set)
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
            "--phases" => p.phases = parse_phases(&val()?)?,
            "--scenario" => {
                let v = val()?;
                p.scenario = Scenario::parse(&v).ok_or_else(|| format!("unknown scenario {v} (s1..s6)"))?;
            }
            "--budget-mb" => p.budget = parse_count(&val()?)? << 20,
            "--mixed-threads" => p.mixed_threads = parse_list(&val()?)?,
            "--mixed-ops" => p.mixed_ops = parse_count(&val()?)? as usize,
            "--dir" => p.dir = PathBuf::from(val()?),
            "--out" => p.out = Some(PathBuf::from(val()?)),
            "--no-out" => p.out = None,
            "--mongo-url" => p.mongo_url = val()?,
            "--tag" => p.tag = val()?,
            "--help" | "-h" => {
                println!(
                    "flags: --records N [100k] --value-size B [512; k/m = 1000s, ki/mi = 1024s] \
                     --scenario s1..s6 [s3; s5 = incompressible] --systems LIST \
                     [babel-raw,babel-adaptive,babel-dict,babel-tcp,babel-tcp-raw,postgres,mongo; \
                     also babel-tcp-dict, tcp-floor] --phases LIST [default = \
                     load,space,put,put-mt,put-pipe,get,get-mt,get-pipe,latest,latest-mt; all adds \
                     range,range-mt,mixed95,mixed50,update,update-mt,delete,delete-mt; -NAME removes] \
                     --put-ops N [300] \
                     --put-threads LIST [1,4,16,64] --put-mt-ops N [200] --read-ops N [20k] \
                     --read-threads LIST [1,4,16] --mixed-threads LIST [16] --mixed-ops N [2000] \
                     --budget-mb N [2048] --relaxed --no-compact --pipeline N [off] \
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
    /// Overwrite an existing message with `m.payload` (an error where the system reports that
    /// nothing matched).
    fn update(&mut self, m: &Msg) -> R<()>;
    /// Delete a message; whether it existed.
    fn delete(&mut self, channel: u64, id: u64) -> R<bool>;

    /// The messages of `channel` with `from <= id <= to`, oldest first.
    fn range(&mut self, _channel: u64, _from: u64, _to: u64) -> R<Vec<(u64, Vec<u8>)>> {
        Err("no key-range scan on this system".into())
    }

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
    /// Whether sessions have `range` (`tcp-floor` answers no key-range scan).
    fn ranges(&self) -> bool {
        true
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
        let n = DICT_SAMPLES.min(p.records).min((DICT_SAMPLE_BYTES / p.value_size.max(1)).max(8) as u64);
        let samples: Vec<Vec<u8>> = (0..n).map(|i| payload(p, i)).collect();
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

    fn update(&mut self, m: &Msg) -> R<()> {
        self.put(m)
    }

    fn delete(&mut self, channel: u64, id: u64) -> R<bool> {
        Ok(self.committer.delete(message_key(channel, id).to_vec(), Expect::Any)?)
    }

    fn range(&mut self, channel: u64, from: u64, to: u64) -> R<Vec<(u64, Vec<u8>)>> {
        let mut opts = ScanOptions::all().with_values(true);
        opts.start = Bound::Included(message_key(channel, from).to_vec());
        opts.end = Bound::Included(message_key(channel, to).to_vec());
        Ok(latest_pairs(self.db.scan(&opts)?))
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

    fn update(&mut self, m: &Msg) -> R<()> {
        self.put(m)
    }

    fn delete(&mut self, channel: u64, id: u64) -> R<bool> {
        Ok(self.client.delete(&message_key(channel, id))?)
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

    fn range(&mut self, channel: u64, from: u64, to: u64) -> R<Vec<(u64, Vec<u8>)>> {
        let mut opts = ScanOptions::all().with_values(true);
        opts.start = Bound::Included(message_key(channel, from).to_vec());
        opts.end = Bound::Included(message_key(channel, to).to_vec());
        Ok(latest_pairs(self.client.scan(&opts)?))
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
    upd: postgres::Statement,
    del: postgres::Statement,
    range: postgres::Statement,
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

    fn update(&mut self, m: &Msg) -> R<()> {
        match self.c.execute(&self.upd, &[&(m.channel as i64), &(m.id as i64), &m.payload])? {
            1 => Ok(()),
            n => Err(format!("update: {n} rows for message ({}, {})", m.channel, m.id).into()),
        }
    }

    fn delete(&mut self, channel: u64, id: u64) -> R<bool> {
        Ok(self.c.execute(&self.del, &[&(channel as i64), &(id as i64)])? == 1)
    }

    fn range(&mut self, channel: u64, from: u64, to: u64) -> R<Vec<(u64, Vec<u8>)>> {
        let rows = self.c.query(&self.range, &[&(channel as i64), &(from as i64), &(to as i64)])?;
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
        let upd = c.prepare("UPDATE messages SET payload = $3 WHERE channel_id = $1 AND id = $2")?;
        let del = c.prepare("DELETE FROM messages WHERE channel_id = $1 AND id = $2")?;
        let range =
            c.prepare("SELECT id, payload FROM messages WHERE channel_id = $1 AND id BETWEEN $2 AND $3 ORDER BY id")?;
        Ok(Box::new(PgSession { c, ins, get, latest, upd, del, range }))
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

    fn update(&mut self, m: &Msg) -> R<()> {
        let filter = doc! { "_id": { "c": m.channel as i64, "m": m.id as i64 } };
        let r = self.coll.replace_one(filter, mongo_doc(m)).run()?;
        if r.matched_count != 1 {
            return Err(format!("update: {} documents matched message ({}, {})", r.matched_count, m.channel, m.id).into());
        }
        Ok(())
    }

    fn delete(&mut self, channel: u64, id: u64) -> R<bool> {
        let r = self.coll.delete_one(doc! { "_id": { "c": channel as i64, "m": id as i64 } }).run()?;
        Ok(r.deleted_count == 1)
    }

    fn range(&mut self, channel: u64, from: u64, to: u64) -> R<Vec<(u64, Vec<u8>)>> {
        let c = channel as i64;
        let cursor = self
            .coll
            .find(doc! { "_id": { "$gte": { "c": c, "m": from as i64 }, "$lte": { "c": c, "m": to as i64 } } })
            .sort(doc! { "_id": 1 })
            .run()?;
        let mut out = Vec::new();
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
    let payload = msg(p, 0).payload;
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

    fn ranges(&self) -> bool {
        false
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
    Ok(run_classes(target, threads, per_thread, prep, |_| 0, op, post)?.0)
}

/// `run_phase`, the latencies also split by `class(input)`: `.1[c]` holds those of class `c`.
fn run_classes<I, O>(
    target: &dyn Target,
    threads: usize,
    per_thread: usize,
    prep: impl Fn(usize, usize) -> I + Sync,
    class: impl Fn(&I) -> usize + Sync,
    op: impl Fn(&I, &mut dyn Session) -> R<O> + Sync,
    post: impl Fn(&I, O) -> R<()> + Sync,
) -> R<(Sample, Vec<Vec<u64>>)>
where
    I: Send + Sync,
{
    let mut sessions = Vec::with_capacity(threads);
    for _ in 0..threads {
        sessions.push(target.session()?);
    }
    let inputs: Vec<Vec<I>> = (0..threads).map(|t| (0..per_thread).map(|k| prep(t, k)).collect()).collect();
    let barrier = Barrier::new(threads + 1);
    let results: Mutex<Vec<R<(Vec<u64>, Vec<usize>)>>> = Mutex::new(Vec::new());
    let (op, post, class) = (&op, &post, &class);
    let started = std::thread::scope(|scope| {
        for (mut session, ins) in sessions.into_iter().zip(&inputs) {
            let (barrier, results) = (&barrier, &results);
            scope.spawn(move || {
                let mut lat = Vec::with_capacity(ins.len());
                let mut classes = Vec::with_capacity(ins.len());
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
                    classes.push(class(input));
                    if let Err(e) = post(input, out) {
                        res = Err(e);
                        break;
                    }
                }
                results.lock().unwrap().push(res.map(|()| (lat, classes)));
            });
        }
        barrier.wait();
        Instant::now()
    });
    let elapsed = started.elapsed();
    let mut lat_ns = Vec::with_capacity(threads * per_thread);
    let mut by_class: Vec<Vec<u64>> = Vec::new();
    for r in results.into_inner().unwrap() {
        let (lat, classes) = r?;
        for (&l, &c) in lat.iter().zip(&classes) {
            if by_class.len() <= c {
                by_class.resize_with(c + 1, Vec::new);
            }
            by_class[c].push(l);
        }
        lat_ns.extend(lat);
    }
    Ok((Sample { phase: String::new(), threads, ops: lat_ns.len(), elapsed, lat_ns }, by_class))
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

/// Point read `k` of thread `t`: uniform over the loaded messages.
fn read_in(p: &Params, t: usize, k: usize) -> ReadIn {
    let i = SplitMix64::new(p.seed ^ ((t as u64) << 32) ^ k as u64).next_u64() % p.records;
    let (channel, id) = key_of(p, i);
    ReadIn { i, channel, id, k }
}

/// Channel of `latest` scan `k` of thread `t` (channels weighted by message count).
fn latest_channel(p: &Params, t: usize, k: usize) -> u64 {
    let i = SplitMix64::new(!p.seed ^ ((t as u64) << 40) ^ k as u64).next_u64() % p.records;
    datasets::channel_of(i, p.seed)
}

/// A point read's result: present, and every `VERIFY_EVERY`-th compared byte for byte (`verify`).
fn check_read(p: &Params, r: &ReadIn, got: Option<&[u8]>, verify: bool) -> R<()> {
    if verify && r.k.is_multiple_of(VERIFY_EVERY) {
        check(&payload(p, r.i), got, "get")
    } else if got.is_none() {
        Err(format!("get: message {} missing", r.i).into())
    } else {
        Ok(())
    }
}

/// A `latest` result: not empty, newest first.
fn check_latest(channel: u64, items: &[(u64, Vec<u8>)]) -> R<()> {
    if items.is_empty() {
        return Err(format!("latest: channel {channel} empty").into());
    }
    if items.windows(2).any(|w| w[0].0 <= w[1].0) {
        return Err(format!("latest: channel {channel} not newest-first").into());
    }
    Ok(())
}

/// `per_thread`, reduced so that `threads` x ops x `op_bytes` (bytes of values read or written)
/// stays within `--budget-mb`; a reduction is printed.
fn budgeted(p: &Params, name: &str, phase: &str, threads: usize, per_thread: usize, op_bytes: u64) -> usize {
    let max = (p.budget / (threads.max(1) as u64 * op_bytes.max(1))).max(1);
    if per_thread as u64 <= max {
        return per_thread;
    }
    println!("[{name}] {phase:<10} x{threads:<3} {max} ops per thread, not {per_thread} (--budget-mb {})", p.budget >> 20);
    max as usize
}

/// Message number of put `k` of thread `t` in write round `round`: fresh ids, never loaded
/// (at most 100 threads x 100,000 ops per round).
fn fresh(p: &Params, round: u64, t: usize, k: usize) -> u64 {
    p.records + 10_000_000 * (round + 1) + (t * 100_000 + k) as u64
}

/// Messages per load batch: `LOAD_BATCH`, fewer for large values.
fn load_batch(p: &Params) -> u64 {
    (LOAD_BATCH_BYTES / p.value_size.max(1)).clamp(1, LOAD_BATCH) as u64
}

fn run_system(p: &Params, name: &str, report: &mut Report) -> R<()> {
    let scenario = if p.scenario == Scenario::ChatJson { String::new() } else { format!(", {}", p.scenario.name()) };
    println!(
        "== {name} ({} records x {} B, {}{scenario})",
        p.records,
        p.value_size,
        if p.relaxed { "relaxed" } else { "durable" }
    );
    if name == "tcp-floor" {
        let target = floor_open(p)?;
        let result = read_phases(p, name, &target, report, false);
        Box::new(target).close()?;
        return result;
    }
    let max_threads =
        p.put_threads.iter().chain(&p.read_threads).chain(&p.mixed_threads).copied().max().unwrap_or(1);
    let mut target: Box<dyn Target> = match (babel_system(name), name) {
        (Some((engine, tcp, wal)), _) => babel_open(p, name, engine, tcp, wal, max_threads + 2)?,
        (None, "postgres") => Box::new(pg_open(p)?),
        (None, "mongo") => Box::new(mongo_open(p)?),
        (None, other) => match fjall_open(p, other, max_threads + 2)? {
            Some(target) => target,
            None => return Err(format!("unknown system {other}").into()),
        },
    };

    // bulk load, 1000 messages per batch (fewer for large values; batch generation is not timed)
    {
        let per_batch = load_batch(p);
        let mut s = target.session()?;
        let mut lat = Vec::new();
        let mut busy = Duration::ZERO;
        let mut i = 0u64;
        while i < p.records {
            let end = (i + per_batch).min(p.records);
            let batch: Vec<Msg> = (i..end).map(|j| msg(p, j)).collect();
            let b0 = Instant::now();
            s.load(&batch)?;
            let d = b0.elapsed();
            busy += d;
            lat.push(d.as_nanos() as u64);
            i = end;
        }
        let note = if per_batch == LOAD_BATCH as u64 { String::new() } else { format!(" ({per_batch} per batch)") };
        println!(
            "[{name}] load       {} records in {:.2}s of batches -> {:.0} rec/s{note}",
            p.records,
            busy.as_secs_f64(),
            p.records as f64 / busy.as_secs_f64()
        );
        let sample = Sample { phase: "load-batch".into(), threads: 1, ops: lat.len(), elapsed: busy, lat_ns: lat };
        report.add(name, sample);
    }
    if p.has("space") {
        let (bytes, what) = target.space()?;
        report.add_space(name, "space", bytes, what);
        if p.compact {
            match target.compact() {
                Ok(Some((bytes, what))) => report.add_space(name, "space-compacted", bytes, what),
                Ok(None) => {}
                Err(e) => println!("[{name}] space-compacted ERROR: {e}"),
            }
        }
    }

    // single-message commits: 1 thread, then T threads (fresh ids, never loaded)
    let rounds = p.put_threads.len() + 1;
    for (round, &threads) in std::iter::once(&1usize).chain(&p.put_threads).enumerate() {
        let phase = if round == 0 { "put" } else { "put-mt" };
        if !p.has(phase) {
            continue;
        }
        let ops = if round == 0 { p.put_ops } else { p.put_mt_ops };
        let ops = budgeted(p, name, phase, threads, ops, p.value_size as u64);
        let mut s = run_phase(
            target.as_ref(),
            threads,
            ops,
            |t, k| msg(p, fresh(p, round as u64, t, k)),
            |m, sess| sess.put(m),
            |_, ()| Ok(()),
        )?;
        s.phase = phase.into();
        report.add(name, s);
    }
    // the same single-message commits, `depth` in flight on one connection
    if p.pipeline > 0 && target.pipelines() && p.has("put-pipe") {
        let depth = p.pipeline;
        let batches = budgeted(p, name, "put-pipe", 1, p.put_ops.div_ceil(depth), (depth * p.value_size) as u64);
        let mut s = run_phase(
            target.as_ref(),
            1,
            batches,
            |t, k| (0..depth).map(|j| msg(p, fresh(p, rounds as u64, t, k * depth + j))).collect::<Vec<Msg>>(),
            |ms, sess| sess.put_many(ms),
            |_, ()| Ok(()),
        )?;
        s.ops *= depth;
        s.phase = format!("put-pipe{depth}");
        report.add(name, s);
    }

    read_phases(p, name, target.as_ref(), report, true)?;
    range_phases(p, name, target.as_ref(), report)?;
    mixed_phases(p, name, target.as_ref(), report, rounds as u64 + 1)?;
    update_phases(p, name, target.as_ref(), report)?;
    delete_phases(p, name, target.as_ref(), report)?;
    target.close()?;
    Ok(())
}

/// Point reads (`get`, `get-mt`, `get-pipeN`) and newest-50 scans (`latest`,
/// `latest-mt`). `verify`: every 64th value is compared with the expected
/// payload (the floor answers every read with the same bytes).
fn read_phases(p: &Params, name: &str, target: &dyn Target, report: &mut Report, verify: bool) -> R<()> {
    // point reads, uniform over the loaded messages
    for &threads in &p.read_threads {
        let phase = if threads == 1 { "get" } else { "get-mt" };
        if !p.has(phase) {
            continue;
        }
        let per = if threads == 1 { p.read_ops } else { (p.read_ops / 2).max(1000) };
        let per = budgeted(p, name, phase, threads, per, p.value_size as u64);
        let mut s = run_phase(
            target,
            threads,
            per,
            |t, k| read_in(p, t, k),
            |r, sess| sess.get(r.channel, r.id),
            |r, got| check_read(p, r, got.as_deref(), verify),
        )?;
        s.phase = phase.into();
        report.add(name, s);
    }
    // the same reads, `depth` in flight on one connection
    if p.pipeline > 0 && target.pipelines() && p.has("get-pipe") {
        let depth = p.pipeline;
        let batches = budgeted(p, name, "get-pipe", 1, p.read_ops.div_ceil(depth), (depth * p.value_size) as u64);
        let mut s = run_phase(
            target,
            1,
            batches,
            |t, k| {
                let reads: Vec<ReadIn> = (0..depth).map(|j| read_in(p, t, k * depth + j)).collect();
                let keys: Vec<(u64, u64)> = reads.iter().map(|r| (r.channel, r.id)).collect();
                (reads, keys)
            },
            |(_, keys), sess| sess.get_many(keys),
            |(reads, _), got| {
                if got.len() != reads.len() {
                    return Err(format!("get-pipe: {} replies for {} reads", got.len(), reads.len()).into());
                }
                reads.iter().zip(&got).try_for_each(|(r, g)| check_read(p, r, g.as_deref(), verify))
            },
        )?;
        s.ops *= depth;
        s.phase = format!("get-pipe{depth}");
        report.add(name, s);
    }

    // newest 50 messages of a channel (channels weighted by message count)
    for &threads in &p.read_threads {
        let phase = if threads == 1 { "latest" } else { "latest-mt" };
        if !p.has(phase) {
            continue;
        }
        let per = if threads == 1 { p.read_ops / 4 } else { (p.read_ops / 8).max(500) };
        let per = budgeted(p, name, phase, threads, per, (LATEST_N * p.value_size) as u64);
        let mut s = run_phase(
            target,
            threads,
            per,
            |t, k| latest_channel(p, t, k),
            |&channel, sess| sess.latest(channel, LATEST_N),
            |&channel, items| check_latest(channel, &items),
        )?;
        s.phase = phase.into();
        report.add(name, s);
    }
    Ok(())
}

/// A `range` read: loaded messages `pos .. pos + n` of `channel` (oldest first), ids `lo ..= hi`.
struct RangeIn {
    channel: u64,
    lo: u64,
    hi: u64,
    pos: usize,
    n: usize,
    k: usize,
}

/// `range` / `range-mt` (as `get` / `get-mt` for threads): `RANGE_N` messages of a channel from a
/// random position (channels weighted by message count), selected by their first and last ids.
/// Ids grow with the message number and the write phases before only add newer ones, so the
/// result must be exactly the expected loaded messages.
fn range_phases(p: &Params, name: &str, target: &dyn Target, report: &mut Report) -> R<()> {
    let runs: Vec<(usize, &str)> = p
        .read_threads
        .iter()
        .map(|&t| (t, if t == 1 { "range" } else { "range-mt" }))
        .filter(|&(_, phase)| p.has(phase))
        .collect();
    if runs.is_empty() {
        return Ok(());
    }
    if !target.ranges() {
        println!("[{name}] range      skipped: no key-range scan on this system");
        return Ok(());
    }
    // loaded messages of each channel, oldest first
    let mut channels: HashMap<u64, Vec<u64>> = HashMap::new();
    for i in 0..p.records {
        channels.entry(datasets::channel_of(i, p.seed)).or_default().push(i);
    }
    let id = |i: u64| datasets::s3_snowflake(p.seed, i);
    let range_in = |t: usize, k: usize| {
        let mut g = SplitMix64::new(p.seed ^ RANGE_SALT ^ ((t as u64) << 32) ^ k as u64);
        let channel = datasets::channel_of(g.next_u64() % p.records, p.seed);
        let msgs = &channels[&channel];
        let n = RANGE_N.min(msgs.len());
        let pos = (g.next_u64() % (msgs.len() - n + 1) as u64) as usize;
        RangeIn { channel, lo: id(msgs[pos]), hi: id(msgs[pos + n - 1]), pos, n, k }
    };
    for (threads, phase) in runs {
        // the bytes of `latest` (50 values an op): a quarter of its ops
        let per = if threads == 1 { p.read_ops / 16 } else { (p.read_ops / 32).max(125) }.max(1);
        let per = budgeted(p, name, phase, threads, per, (RANGE_N * p.value_size) as u64);
        // channels with fewer than RANGE_N messages give shorter ranges
        let items: usize = (0..threads).flat_map(|t| (0..per).map(move |k| (t, k))).map(|(t, k)| range_in(t, k).n).sum();
        println!("[{name}] {phase:<10} x{threads:<3} {:.1} messages per range on average", items as f64 / (threads * per) as f64);
        let mut s = run_phase(
            target,
            threads,
            per,
            range_in,
            |r, sess| sess.range(r.channel, r.lo, r.hi),
            |r, items| {
                let ordered = items.windows(2).all(|w| w[0].0 < w[1].0);
                if items.len() != r.n || !ordered || items[0].0 != r.lo || items[r.n - 1].0 != r.hi {
                    return Err(format!(
                        "range: channel {} ids {}..={}: {} items, expected {} oldest first",
                        r.channel,
                        r.lo,
                        r.hi,
                        items.len(),
                        r.n
                    )
                    .into());
                }
                if r.k.is_multiple_of(VERIFY_EVERY) {
                    let msgs = &channels[&r.channel][r.pos..r.pos + r.n];
                    if items.iter().zip(msgs).any(|(item, &i)| item.0 != id(i)) {
                        return Err(format!("range: channel {} from id {}: wrong ids", r.channel, r.lo).into());
                    }
                    check(&payload(p, msgs[0]), Some(&items[0].1), "range")?;
                }
                Ok(())
            },
        )?;
        s.phase = phase.into();
        report.add(name, s);
    }
    Ok(())
}

/// One op of `mixed95` / `mixed50`.
enum MixedIn {
    Get(ReadIn),
    Latest(u64),
    Put(Msg),
}

enum MixedOut {
    Got(Option<Vec<u8>>),
    Items(Vec<(u64, Vec<u8>)>),
    Put,
}

/// `mixed95` / `mixed50`: each of `--mixed-threads` clients runs `--mixed-ops` ops, each a durable
/// put of a new message with probability 5 / 50 % (fresh ids from write round `round` + 1 on),
/// else a read: a point `get` 3 times in 4 (uniform over the loaded messages), a `latest` scan
/// otherwise. Rows: every op, then the reads and the writes of the same run on their own.
fn mixed_phases(p: &Params, name: &str, target: &dyn Target, report: &mut Report, mut round: u64) -> R<()> {
    for (phase, write_pct) in [("mixed95", 5u64), ("mixed50", 50)] {
        for &threads in &p.mixed_threads {
            round += 1;
            if !p.has(phase) {
                continue;
            }
            // bytes of values per op on average: a read moves 3/4 x 1 + 1/4 x LATEST_N values
            let v = p.value_size as u64;
            let op_bytes = (v * ((100 - write_pct) * (3 + LATEST_N as u64) + 4 * write_pct)).div_ceil(400);
            let per = budgeted(p, name, phase, threads, p.mixed_ops, op_bytes);
            if threads > 100 || per > 100_000 {
                return Err(format!("{phase}: at most 100 threads x 100k ops (fresh ids)").into());
            }
            let (mut s, mut by_class) = run_classes(
                target,
                threads,
                per,
                |t, k| {
                    let mut g = SplitMix64::new(p.seed ^ MIXED_SALT ^ ((t as u64) << 32) ^ k as u64);
                    let (draw, i) = (g.next_u64(), g.next_u64() % p.records);
                    if draw % 100 < write_pct {
                        MixedIn::Put(msg(p, fresh(p, round, t, k)))
                    } else if (draw / 100) % 4 == 0 {
                        MixedIn::Latest(datasets::channel_of(i, p.seed))
                    } else {
                        let (channel, id) = key_of(p, i);
                        MixedIn::Get(ReadIn { i, channel, id, k })
                    }
                },
                |m| usize::from(matches!(m, MixedIn::Put(_))),
                |m, sess| {
                    Ok(match m {
                        MixedIn::Get(r) => MixedOut::Got(sess.get(r.channel, r.id)?),
                        MixedIn::Latest(channel) => MixedOut::Items(sess.latest(*channel, LATEST_N)?),
                        MixedIn::Put(message) => {
                            sess.put(message)?;
                            MixedOut::Put
                        }
                    })
                },
                |m, out| match (m, out) {
                    (MixedIn::Get(r), MixedOut::Got(got)) => check_read(p, r, got.as_deref(), true),
                    (MixedIn::Latest(channel), MixedOut::Items(items)) => check_latest(*channel, &items),
                    _ => Ok(()),
                },
            )?;
            let elapsed = s.elapsed;
            s.phase = phase.into();
            report.add(name, s);
            by_class.resize_with(2, Vec::new);
            for (kind, lat_ns) in ["read", "write"].into_iter().zip(by_class) {
                let s = Sample { phase: format!("{phase}-{kind}"), threads, ops: lat_ns.len(), elapsed, lat_ns };
                report.add(name, s);
            }
        }
    }
    Ok(())
}

/// A fixed permutation of `[0, n)` that scatters neighbours: `k -> (a k + b) mod n`, `a` coprime
/// with `n` (near `n / phi`), `b` from the seed.
struct Perm {
    n: u64,
    a: u64,
    b: u64,
}

impl Perm {
    fn new(n: u64, seed: u64) -> Perm {
        let n = n.max(1);
        let mut a = ((n as f64 / 1.618_033_988_749_895) as u64).max(1);
        while gcd(a, n) != 1 {
            a += 1;
        }
        Perm { n, a, b: datasets::splitmix64(seed) % n }
    }

    fn at(&self, k: u64) -> u64 {
        ((u128::from(self.a) * u128::from(k % self.n) + u128::from(self.b)) % u128::from(self.n)) as u64
    }
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Rounds of `update` or `delete` (`kind`), as `put`: 1 thread x `--put-ops`, then each of
/// `--put-threads` x `--put-mt-ops`, as (phase, threads, ops per thread, first op). Every op
/// targets a loaded message of its own, so the rounds are cut short when they run out.
fn mutation_rounds(p: &Params, name: &str, kind: &str, op_bytes: u64) -> Vec<(String, usize, usize, u64)> {
    let mut rounds = Vec::new();
    let mut used = 0u64;
    for (round, &threads) in std::iter::once(&1usize).chain(&p.put_threads).enumerate() {
        let phase = if round == 0 { kind.to_string() } else { format!("{kind}-mt") };
        if !p.has(&phase) {
            continue;
        }
        let per = if round == 0 { p.put_ops } else { p.put_mt_ops };
        let per = budgeted(p, name, &phase, threads, per, op_bytes);
        let left = p.records - used;
        let per = if (threads * per) as u64 <= left {
            per
        } else {
            let n = (left / threads.max(1) as u64) as usize;
            println!("[{name}] {phase:<10} x{threads:<3} {n} ops per thread, not {per} ({left} loaded messages left)");
            n
        };
        if per > 0 {
            rounds.push((phase, threads, per, used));
            used += (threads * per) as u64;
        }
    }
    rounds
}

/// `update` / `update-mt`: overwrite distinct loaded messages with `update_payload`; afterwards
/// every 64th must read back its new payload.
fn update_phases(p: &Params, name: &str, target: &dyn Target, report: &mut Report) -> R<()> {
    let perm = Perm::new(p.records, p.seed);
    for (phase, threads, per, first) in mutation_rounds(p, name, "update", p.value_size as u64) {
        let message = |t: usize, k: usize| perm.at(first + (t * per + k) as u64);
        let mut s = run_phase(
            target,
            threads,
            per,
            |t, k| {
                let i = message(t, k);
                msg_with(p, i, update_payload(p, i))
            },
            |m, sess| sess.update(m),
            |_, ()| Ok(()),
        )?;
        s.phase = phase;
        report.add(name, s);
        let mut sess = target.session()?;
        for t in 0..threads {
            for k in (0..per).step_by(VERIFY_EVERY) {
                let i = message(t, k);
                let (channel, id) = key_of(p, i);
                check(&update_payload(p, i), sess.get(channel, id)?.as_deref(), "update read-back")?;
            }
        }
    }
    Ok(())
}

/// `delete` / `delete-mt`: delete distinct loaded messages (walking the permutation of `update`
/// from its middle); each must report a deletion, and afterwards every 64th must be missing.
fn delete_phases(p: &Params, name: &str, target: &dyn Target, report: &mut Report) -> R<()> {
    let perm = Perm::new(p.records, p.seed);
    let start = p.records / 2;
    for (phase, threads, per, first) in mutation_rounds(p, name, "delete", 16) {
        let message = |t: usize, k: usize| perm.at(start + first + (t * per + k) as u64);
        let mut s = run_phase(
            target,
            threads,
            per,
            |t, k| {
                let i = message(t, k);
                (i, key_of(p, i))
            },
            |&(_, (channel, id)), sess| sess.delete(channel, id),
            |&(i, _), deleted| if deleted { Ok(()) } else { Err(format!("delete: message {i} missing").into()) },
        )?;
        s.phase = phase;
        report.add(name, s);
        let mut sess = target.session()?;
        for t in 0..threads {
            for k in (0..per).step_by(VERIFY_EVERY) {
                let i = message(t, k);
                let (channel, id) = key_of(p, i);
                if sess.get(channel, id)?.is_some() {
                    return Err(format!("delete: message {i} still readable").into());
                }
            }
        }
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
