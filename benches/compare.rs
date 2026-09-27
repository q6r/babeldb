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
//! - `babel-raw`, `babel-adaptive`: `Db<RedbStore>` in-process (no network;
//!   `Config::raw_only()` vs `Config::adaptive()`), writes through a
//!   `GroupCommitter` (one commit per batch of concurrent writes).
//! - `babel-tcp`: the adaptive engine behind the babeldb TCP server
//!   (`cli::server`, localhost, binary protocol): the like-for-like comparison
//!   with the client/server databases.
//! - `postgres`: `messages(channel_id bigint, id bigint, payload bytea,
//!   primary key (channel_id, id))`, prepared statements, one connection per thread.
//! - `mongo`: collection `messages`, `_id = {c, m}` (both int64), `p` = BinData
//!   (exact bytes), one pooled client.
//!
//! Durability, default `durable`: babeldb `Immediate` commits (one
//! FlushFileBuffers per commit), PostgreSQL `synchronous_commit = on` (WAL
//! flush per commit, group commit across connections), MongoDB write concern
//! `{w: 1, j: true}` (journal flush). `--relaxed` runs the non-durable
//! counterparts instead: babeldb `Buffered` group commit (Deferred commits,
//! periodic sync), PostgreSQL `synchronous_commit = off`, MongoDB `{w: 1, j: false}`.
//!
//! Phases: `load` (bulk: `write_batch` / `COPY BINARY` / `insert_many`, 1000 per
//! batch), `space` (bytes on disk after load), `put` (sequential single-message
//! commits), `put-mt` (T writer threads), `get` / `get-mt` (point reads by
//! (channel, id); uniform over loaded messages), `latest` / `latest-mt`
//! (newest 50 messages of a channel; channels weighted by message count).
//! Every 64th read is compared byte for byte with the expected payload.

use std::error::Error as StdError;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use babeldb::cli::server::{self, Client as BabelClient, ServerHandle};
use babeldb::datasets::{self, Scenario, SplitMix64};
use babeldb::scale::chat::message_key;
use babeldb::scale::group_commit::{GroupCommitConfig, GroupCommitter, WriteDurability};
use babeldb::{BatchOp, Config, Db, Expect, ScanOptions};
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, Document, doc};
use mongodb::options::{Acknowledgment, CollectionOptions, WriteConcern};

type R<T> = Result<T, Box<dyn StdError + Send + Sync>>;

const LOAD_BATCH: usize = 1000;
const LATEST_N: usize = 50;
const VERIFY_EVERY: usize = 64;

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
                "babel-tcp".into(),
                "postgres".into(),
                "mongo".into(),
            ],
            put_ops: 300,
            put_threads: vec![1, 4, 16, 64],
            put_mt_ops: 200,
            read_ops: 20_000,
            read_threads: vec![1, 4, 16],
            relaxed: false,
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
            "--dir" => p.dir = PathBuf::from(val()?),
            "--out" => p.out = Some(PathBuf::from(val()?)),
            "--no-out" => p.out = None,
            "--mongo-url" => p.mongo_url = val()?,
            "--tag" => p.tag = val()?,
            "--help" | "-h" => {
                println!(
                    "flags: --records N [100k] --value-size B [512] --systems LIST \
                     [babel-raw,babel-adaptive,babel-tcp,postgres,mongo] --put-ops N [300] \
                     --put-threads LIST [1,4,16,64] --put-mt-ops N [200] --read-ops N [20k] \
                     --read-threads LIST [1,4,16] --relaxed --dir PATH --out PATH --no-out \
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
    space: Vec<(String, u64, String)>,
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

    fn add_space(&mut self, system: &str, bytes: u64, what: String) {
        println!("[{system}] space      {:>10.2} MB  ({what})", bytes as f64 / 1e6);
        if let Some(f) = self.out.as_mut() {
            let _ = writeln!(
                f,
                "{{\"tag\":\"{}\",\"system\":\"{system}\",\"records\":{},\"value_size\":{},\"phase\":\"space\",\"bytes\":{bytes},\"what\":\"{what}\"}}",
                self.params.tag, self.params.records, self.params.value_size
            );
        }
        self.space.push((system.to_string(), bytes, what));
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
}

trait Target: Sync {
    fn session(&self) -> R<Box<dyn Session>>;
    /// Bytes on disk used by the loaded data, and what they include.
    fn space(&self) -> R<(u64, String)>;
    fn close(self: Box<Self>) -> R<()>;
}

// --- babeldb in-process ----------------------------------------------------

struct BabelTarget {
    db: Arc<Db>,
    committer: Arc<GroupCommitter>,
    server: Mutex<Option<(ServerHandle, SocketAddr)>>,
}

fn babel_open(p: &Params, name: &str, cfg: Config, tcp_threads: Option<usize>) -> R<BabelTarget> {
    let dir = p.dir.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let db = Arc::new(Db::open(dir.join("babel.redb"), cfg)?);
    let durability = if p.relaxed {
        WriteDurability::Buffered { flush_interval: Duration::from_millis(100), max_pending_bytes: 64 << 20 }
    } else {
        WriteDurability::Immediate
    };
    let committer = Arc::new(GroupCommitter::new(db.clone(), GroupCommitConfig::from(durability))?);
    let server = match tcp_threads {
        Some(threads) => {
            let listener = server::bind("127.0.0.1:0")?;
            let addr = listener.local_addr()?;
            Some((server::serve_listener(db.clone(), listener, threads)?, addr))
        }
        None => None,
    };
    Ok(BabelTarget { db, committer, server: Mutex::new(server) })
}

struct BabelSession {
    db: Arc<Db>,
    committer: Arc<GroupCommitter>,
}

impl Session for BabelSession {
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
        Ok(items
            .into_iter()
            .map(|it| (u64::from_be_bytes(it.key[8..16].try_into().unwrap()), it.value.unwrap_or_default()))
            .collect())
    }
}

impl Target for BabelTarget {
    fn session(&self) -> R<Box<dyn Session>> {
        if let Some((_, addr)) = self.server.lock().unwrap().as_ref() {
            return Ok(Box::new(BabelTcpSession { client: BabelClient::connect(addr)? }));
        }
        Ok(Box::new(BabelSession { db: self.db.clone(), committer: self.committer.clone() }))
    }

    fn space(&self) -> R<(u64, String)> {
        self.committer.flush()?;
        let s = self.db.stats()?;
        let alloc = s.file_allocated_bytes().unwrap_or_else(|| s.file_apparent_bytes());
        Ok((
            alloc,
            format!(
                "redb file allocated (apparent {:.2} MB, engine payload {:.2} MB)",
                s.file_apparent_bytes() as f64 / 1e6,
                s.payload_bytes() as f64 / 1e6
            ),
        ))
    }

    fn close(self: Box<Self>) -> R<()> {
        if let Some((handle, _)) = self.server.lock().unwrap().take() {
            handle.stop();
        }
        self.committer.shutdown()?;
        Ok(())
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
    let max_threads = p.put_threads.iter().chain(&p.read_threads).copied().max().unwrap_or(1);
    let target: Box<dyn Target> = match name {
        "babel-raw" => Box::new(babel_open(p, name, Config::raw_only(), None)?),
        "babel-adaptive" => Box::new(babel_open(p, name, Config::adaptive(), None)?),
        "babel-tcp" => Box::new(babel_open(p, name, Config::adaptive(), Some(max_threads + 2))?),
        "postgres" => Box::new(pg_open(p)?),
        "mongo" => Box::new(mongo_open(p)?),
        other => return Err(format!("unknown system {other}").into()),
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
    report.add_space(name, bytes, what);

    // single-message commits: 1 thread, then T threads (fresh ids, never loaded)
    let fresh = |round: u64, t: usize, k: usize| p.records + 10_000_000 * (round + 1) + (t * 100_000 + k) as u64;
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

    // point reads, uniform over the loaded messages
    for &threads in &p.read_threads {
        let per = if threads == 1 { p.read_ops } else { (p.read_ops / 2).max(1000) };
        let mut s = run_phase(
            target.as_ref(),
            threads,
            per,
            |t, k| {
                let i = SplitMix64::new(p.seed ^ ((t as u64) << 32) ^ k as u64).next_u64() % p.records;
                ReadIn { i, channel: datasets::channel_of(i, p.seed), id: datasets::s3_snowflake(p.seed, i), k }
            },
            |r, sess| sess.get(r.channel, r.id),
            |r, got| {
                if r.k % VERIFY_EVERY == 0 {
                    check(&msg(p.seed, r.i, p.value_size).payload, got.as_deref(), "get")
                } else if got.is_none() {
                    Err(format!("get: message {} missing", r.i).into())
                } else {
                    Ok(())
                }
            },
        )?;
        s.phase = if threads == 1 { "get".into() } else { "get-mt".into() };
        report.add(name, s);
    }

    // newest 50 messages of a channel (channels weighted by message count)
    for &threads in &p.read_threads {
        let per = if threads == 1 { p.read_ops / 4 } else { (p.read_ops / 8).max(500) };
        let mut s = run_phase(
            target.as_ref(),
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

    target.close()?;
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
    for (sys, bytes, _) in &report.space {
        println!("space      {sys}: {:.2} MB", *bytes as f64 / 1e6);
    }
    Ok(())
}
