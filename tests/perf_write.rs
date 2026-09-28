//! Write-path measurements (release, ignored): bulk load through
//! `Db::write_batch` and concurrent durable puts through a `GroupCommitter`,
//! on `Db::open_wal` with the benchmark's S3 chat messages (512 B).
//!
//! `cargo test --release --test perf_write -- --ignored --nocapture --test-threads 1`
//!
//! Environment: `PERF_RECORDS` (default 100000), `PERF_ROUNDS` (default 3),
//! `PERF_THREADS` (default "1,16,64"), `PERF_PUTS` (puts per thread, default 200).
//! Single machine, OS cache warm: indicative only.

use std::sync::{Arc, Barrier};
use std::time::Instant;

use babeldb::config::{WalConfig, WalSync};
use babeldb::datasets::{self, Scenario};
use babeldb::scale::chat::message_key;
use babeldb::scale::group_commit::{GroupCommitConfig, GroupCommitter, WriteDurability};
use babeldb::{BatchOp, Config, Db, Expect};

const SEED: u64 = datasets::DEFAULT_SEED;
const VALUE_SIZE: usize = 512;
const LOAD_BATCH: usize = 1000;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn message(i: u64) -> ([u8; 16], Vec<u8>) {
    let key = message_key(datasets::channel_of(i, SEED), datasets::s3_snowflake(SEED, i));
    (key, datasets::value(Scenario::ChatJson, SEED, i, VALUE_SIZE))
}

fn configs() -> Vec<(&'static str, Config)> {
    let only = std::env::var("PERF_CONFIGS").unwrap_or_else(|_| "raw,adaptive".into());
    [("raw", Config::raw_only()), ("adaptive", Config::adaptive())]
        .into_iter()
        .filter(|(n, _)| only.split(',').any(|o| o == *n))
        .collect()
}

fn open(dir: &tempfile::TempDir, cfg: Config) -> babeldb::engine::WalDb {
    let wal = WalConfig { sync: WalSync::WriteThrough, ..WalConfig::default() };
    Db::open_wal_with(dir.path().join("perf.redb"), cfg, wal).unwrap()
}

fn load(db: &babeldb::engine::WalDb, msgs: &[([u8; 16], Vec<u8>)]) {
    for chunk in msgs.chunks(LOAD_BATCH) {
        let ops: Vec<BatchOp<'_>> =
            chunk.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any }).collect();
        db.write_batch(&ops).unwrap();
    }
}

/// CPU side of the bulk load (engine + redb, no I/O wait): `Db::open`,
/// `write_batch_each` with `Durability::Deferred` (redb `None`), best of
/// `PERF_ROUNDS` in ms, then `PERF_CPU_WAL=1` adds the WAL (redo logging;
/// its checkpoints still flush).
#[test]
#[ignore]
fn cpu_load() {
    use babeldb::store::Durability;
    let records = env_u64("PERF_RECORDS", 100_000);
    let rounds = env_u64("PERF_ROUNDS", 5);
    let wal = env_u64("PERF_CPU_WAL", 0) == 1;
    let msgs: Vec<([u8; 16], Vec<u8>)> = (0..records).map(message).collect();
    for (name, cfg) in configs() {
        let mut best = f64::MAX;
        for _ in 0..rounds {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("cpu.redb");
            let run = |write: &dyn Fn(&[BatchOp<'_>])| {
                let t = Instant::now();
                for chunk in msgs.chunks(LOAD_BATCH) {
                    let ops: Vec<BatchOp<'_>> =
                        chunk.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any }).collect();
                    write(&ops);
                }
                t.elapsed().as_secs_f64() * 1e3
            };
            let ms = if wal {
                let db = open(&dir, cfg.clone());
                run(&|ops| {
                    db.write_batch_each(ops, Durability::Deferred).unwrap();
                })
            } else {
                let db = Db::open(&path, cfg.clone()).unwrap();
                run(&|ops| {
                    db.write_batch_each(ops, Durability::Deferred).unwrap();
                })
            };
            best = best.min(ms);
        }
        println!(
            "cpu {name:<8} wal={wal}: best {best:.1} ms = {:.0} rec/s",
            records as f64 / best * 1e3
        );
    }
}

#[test]
#[ignore]
fn bulk_load() {
    let records = env_u64("PERF_RECORDS", 100_000);
    let rounds = env_u64("PERF_ROUNDS", 3);
    let msgs: Vec<([u8; 16], Vec<u8>)> = (0..records).map(message).collect();
    for (name, cfg) in configs() {
        for round in 0..rounds {
            let dir = tempfile::tempdir().unwrap();
            let db = open(&dir, cfg.clone());
            let t = Instant::now();
            load(&db, &msgs);
            let took = t.elapsed();
            let w = db.store().stats();
            let ms = |ns: u64| ns as f64 / 1e6;
            println!(
                "load {name:<8} round {round}: {records} in {:>7.1} ms = {:>8.0} rec/s | wal write {:>6.1} ms ({} writes, {:.1} MB), inner commit {:>6.1} ms, checkpoints {} = {:>6.1} ms, rest {:>6.1} ms",
                took.as_secs_f64() * 1e3,
                records as f64 / took.as_secs_f64(),
                ms(w.write_nanos),
                w.wal_writes,
                w.wal_bytes as f64 / 1e6,
                ms(w.inner_commit_nanos),
                w.checkpoints,
                ms(w.checkpoint_nanos),
                took.as_secs_f64() * 1e3 - ms(w.write_nanos + w.inner_commit_nanos + w.checkpoint_nanos),
            );
            // Spot check: exact bytes.
            for i in [0, records / 2, records - 1] {
                let (k, v) = &msgs[i as usize];
                assert_eq!(db.get(k).unwrap().as_deref(), Some(v.as_slice()));
            }
        }
    }
}

#[test]
#[ignore]
fn concurrent_puts() {
    let rounds = env_u64("PERF_ROUNDS", 3);
    let per_thread = env_u64("PERF_PUTS", 200) as usize;
    let threads: Vec<usize> = std::env::var("PERF_THREADS")
        .unwrap_or_else(|_| "1,16,64".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let preload = env_u64("PERF_PRELOAD", 20_000);
    let msgs: Vec<([u8; 16], Vec<u8>)> = (0..preload).map(message).collect();
    for (name, cfg) in configs() {
        for &n in &threads {
            for round in 0..rounds {
                let dir = tempfile::tempdir().unwrap();
                let db = Arc::new(open(&dir, cfg.clone()));
                // Like the comparison: puts into a loaded database.
                load(&db, &msgs);
                let committer = Arc::new(
                    GroupCommitter::new(db.clone(), GroupCommitConfig::from(WriteDurability::Immediate)).unwrap(),
                );
                let barrier = Arc::new(Barrier::new(n + 1));
                let handles: Vec<_> = (0..n)
                    .map(|t| {
                        let committer = committer.clone();
                        let barrier = barrier.clone();
                        std::thread::spawn(move || {
                            let base = preload + (t * per_thread) as u64;
                            let work: Vec<([u8; 16], Vec<u8>)> =
                                (base..base + per_thread as u64).map(message).collect();
                            barrier.wait();
                            for (k, v) in work {
                                committer.put(k.to_vec(), v, Expect::Any).unwrap();
                            }
                        })
                    })
                    .collect();
                barrier.wait();
                let t = Instant::now();
                for h in handles {
                    h.join().unwrap();
                }
                let took = t.elapsed();
                let s = committer.stats();
                committer.shutdown().unwrap();
                let total = n * per_thread;
                println!(
                    "puts {name:<8} x{n:<3} round {round}: {total} in {:>7.1} ms = {:>8.0} ops/s (batches {}, avg {:.1} ops)",
                    took.as_secs_f64() * 1e3,
                    total as f64 / took.as_secs_f64().max(1e-9),
                    s.batches,
                    s.avg_batch_ops(),
                );
                let (k, v) = message(preload + (per_thread as u64 / 2));
                assert_eq!(db.get(&k).unwrap(), Some(v));
            }
        }
    }
}

/// Store-level costs of the bulk-load transaction shape (1000 new keys per
/// commit): get + put per key (what the engine does), put only, put_many.
#[test]
#[ignore]
fn store_costs() {
    use babeldb::store::wal::WalStore;
    use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
    use babeldb::{MemStore, RedbStore};
    let records = env_u64("PERF_RECORDS", 100_000);
    let msgs: Vec<([u8; 16], Vec<u8>)> = (0..records)
        .map(|i| {
            let (k, mut v) = message(i);
            v.resize(610, 7);
            (k, v)
        })
        .collect();
    fn run<S: Store>(name: &str, store: &S, msgs: &[([u8; 16], Vec<u8>)], mode: u8, dur: Durability) {
        let t = Instant::now();
        for chunk in msgs.chunks(LOAD_BATCH) {
            let mut w = store.begin_write().unwrap();
            match mode {
                0 => {
                    for (k, v) in chunk {
                        assert!(w.get(Table::Records, k).unwrap().is_none());
                        w.put(Table::Records, k, v).unwrap();
                    }
                }
                1 => {
                    for (k, v) in chunk {
                        w.put(Table::Records, k, v).unwrap();
                    }
                }
                _ => {
                    let mut it = chunk.iter().map(|(k, v)| (&k[..], &v[..]));
                    w.put_many(Table::Records, &mut it).unwrap();
                }
            }
            w.put(Table::Meta, b"next_revision", &[1; 8]).unwrap();
            w.commit(dur).unwrap();
        }
        let took = t.elapsed();
        let what = ["get+put", "put", "put_many"][mode as usize];
        println!(
            "store {name:<10} {what:<8}: {:>7.1} ms = {:>8.0} rec/s",
            took.as_secs_f64() * 1e3,
            msgs.len() as f64 / took.as_secs_f64()
        );
    }
    for mode in 0..3u8 {
        let mem = MemStore::new();
        run("mem", &mem, &msgs, mode, Durability::Immediate);
        let dir = tempfile::tempdir().unwrap();
        let redb = RedbStore::open(dir.path().join("a.redb"), 64 << 20).unwrap();
        run("redb-none", &redb, &msgs, mode, Durability::Deferred);
        drop(redb);
        let dir = tempfile::tempdir().unwrap();
        let redb = RedbStore::open(dir.path().join("a.redb"), 64 << 20).unwrap();
        let wal = WalStore::open(redb, dir.path().join("a.wal"), WalConfig::default()).unwrap();
        run("redb+wal", &wal, &msgs, mode, Durability::Immediate);
        let s = wal.stats();
        println!(
            "    wal write {:.1} ms, inner commit {:.1} ms, checkpoints {} = {:.1} ms",
            s.write_nanos as f64 / 1e6,
            s.inner_commit_nanos as f64 / 1e6,
            s.checkpoints,
            s.checkpoint_nanos as f64 / 1e6
        );
    }
}

/// Engine CPU cost of the bulk load without redb: `Db<MemStore>`.
#[test]
#[ignore]
fn engine_costs() {
    let records = env_u64("PERF_RECORDS", 100_000);
    let msgs: Vec<([u8; 16], Vec<u8>)> = (0..records).map(message).collect();
    for (name, cfg) in configs() {
        for round in 0..env_u64("PERF_ROUNDS", 3) {
            let db = Db::with_store(babeldb::MemStore::new(), cfg.clone()).unwrap();
            let t = Instant::now();
            for chunk in msgs.chunks(LOAD_BATCH) {
                let ops: Vec<BatchOp<'_>> =
                    chunk.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any }).collect();
                db.write_batch(&ops).unwrap();
            }
            let took = t.elapsed();
            println!(
                "engine(mem) {name:<8} round {round}: {:>7.1} ms = {:>8.0} rec/s",
                took.as_secs_f64() * 1e3,
                records as f64 / took.as_secs_f64()
            );
        }
    }
}
