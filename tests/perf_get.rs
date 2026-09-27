//! Engine overhead of point reads and "latest 50" scans on a cache-resident
//! chat dataset (S3, 20k x 512 B, redb), versus redb alone. Ignored by
//! default; uses only APIs of round 1, so the same file builds against older
//! trees for before/after comparisons (run each build at the same priority).
//!
//! `cargo test --release --test perf_get -- --ignored --nocapture`

use std::hint::black_box;
use std::ops::Bound;
use std::time::Instant;

use babeldb::datasets::{self, Scenario, SplitMix64};
use babeldb::store::redb::RedbStore;
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use babeldb::{BatchOp, Config, Db, Expect, ScanOptions};

const RECORDS: u64 = 20_000;
const ROUNDS: usize = 7;
const GETS: usize = 50_000;
const SCANS: usize = 5_000;

fn records() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..RECORDS).map(|i| datasets::record(Scenario::ChatJson, 42, i, 512)).collect()
}

/// Median and minimum of the per-round means (ns per op).
fn rounds(ops: usize, mut round: impl FnMut(&mut SplitMix64)) -> (f64, f64) {
    let mut rng = SplitMix64::new(7);
    round(&mut rng); // warm-up
    let mut means: Vec<f64> = (0..ROUNDS)
        .map(|_| {
            let t = Instant::now();
            round(&mut rng);
            t.elapsed().as_nanos() as f64 / ops as f64
        })
        .collect();
    means.sort_by(f64::total_cmp);
    (means[ROUNDS / 2], means[0])
}

fn report(name: &str, what: &str, (median, min): (f64, f64)) {
    println!("{name:>10} {what:<9} median {median:>8.0} ns/op   min {min:>8.0} ns/op");
}

fn channels(recs: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    let mut c: Vec<Vec<u8>> = (0..RECORDS).map(|i| datasets::channel_prefix(datasets::channel_of(i, 42))).collect();
    c.sort();
    c.dedup();
    assert!(!recs.is_empty());
    c
}

#[test]
#[ignore]
fn point_reads_and_latest_scans() {
    let recs = records();
    let prefixes = channels(&recs);
    let dir = tempfile::tempdir().unwrap();
    // redb alone: the same records written directly to one table.
    {
        let path = dir.path().join("raw.redb");
        let store = RedbStore::open(&path, 64 << 20).unwrap();
        for chunk in recs.chunks(1000) {
            let mut w = store.begin_write().unwrap();
            for (k, v) in chunk {
                w.put(Table::Records, k, v).unwrap();
            }
            w.commit(Durability::Immediate).unwrap();
        }
        drop(store);
        let store = RedbStore::open(&path, 64 << 20).unwrap();
        report("redb", "get", rounds(GETS, |rng| {
            for _ in 0..GETS {
                let (k, _) = &recs[rng.below(RECORDS) as usize];
                black_box(store.begin_read().unwrap().get(Table::Records, k).unwrap());
            }
        }));
        report("redb", "latest50", rounds(SCANS, |rng| {
            for _ in 0..SCANS {
                let p = &prefixes[rng.below(prefixes.len() as u64) as usize];
                let end = babeldb::engine::prefix_successor(p).unwrap();
                let r = store.begin_read().unwrap();
                let mut n = 0;
                r.scan(Table::Records, Bound::Included(p), Bound::Excluded(&end), true, &mut |k, v| {
                    black_box((k.to_vec(), v.to_vec()));
                    n += 1;
                    Ok(n < 50)
                })
                .unwrap();
            }
        }));
    }
    for (name, cfg) in [("engine-raw", Config::raw_only()), ("adaptive", Config::adaptive())] {
        let path = dir.path().join(format!("{name}.redb"));
        {
            let db = Db::open(&path, cfg.clone()).unwrap();
            for chunk in recs.chunks(1000) {
                let ops: Vec<BatchOp<'_>> =
                    chunk.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any }).collect();
                db.write_batch(&ops).unwrap();
            }
        }
        let db = Db::open(&path, cfg).unwrap();
        report(name, "get", rounds(GETS, |rng| {
            for _ in 0..GETS {
                let (k, v) = &recs[rng.below(RECORDS) as usize];
                let got = db.get(k).unwrap().unwrap();
                assert_eq!(got.len(), v.len());
                black_box(got);
            }
        }));
        report(name, "latest50", rounds(SCANS, |rng| {
            for _ in 0..SCANS {
                let p = &prefixes[rng.below(prefixes.len() as u64) as usize];
                black_box(db.scan(&ScanOptions::prefix(p).reverse(true).limit(50).with_values(true)).unwrap());
            }
        }));
        let s = db.stats().unwrap();
        println!("{name:>10} payload {:.2} MB, params {}", s.payload_bytes() as f64 / 1e6, s.params);
    }
}
