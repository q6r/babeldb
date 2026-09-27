//! The backend conformance suite (`babeldb::store::conformance`) against MemStore and
//! RedbStore, redb specifics, and an ignored quick measurement:
//! `cargo test --release --test store_conformance -- --ignored --nocapture`

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use babeldb::store::conformance;
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use babeldb::{Error, MemStore, RedbStore, Result};

const CACHE: usize = 16 << 20;

#[test]
fn mem_conformance() -> Result<()> {
    conformance::run_all(&mut MemStore::new)
}

#[test]
fn mem_concurrent() -> Result<()> {
    conformance::run_concurrent(Arc::new(MemStore::new()))
}

#[test]
fn redb_conformance() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut n = 0;
    conformance::run_all(&mut || {
        n += 1;
        RedbStore::open(dir.path().join(format!("store-{n}.redb")), CACHE).expect("open redb store")
    })
}

#[test]
fn redb_persistent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    conformance::run_persistent(
        &mut |d| RedbStore::open(d.join("db.redb"), CACHE).expect("open redb store"),
        dir.path(),
    )
}

#[test]
fn redb_concurrent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    conformance::run_concurrent(Arc::new(RedbStore::open(
        dir.path().join("db.redb"),
        CACHE,
    )?))
}

#[test]
fn redb_specifics() -> Result<()> {
    fn send_sync_static<T: Send + Sync + 'static>() {}
    send_sync_static::<RedbStore>();

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.redb");
    let mut store = RedbStore::open(&path, CACHE)?;
    assert_eq!(store.path(), path.as_path());
    assert_eq!(store.files(), vec![path.clone()]);
    assert_eq!(store.backend_name(), "redb");
    assert!(
        matches!(RedbStore::open(&path, CACHE), Err(Error::Backend(_))),
        "second handle on an open file"
    );

    let reader = store.begin_read()?;
    assert!(
        matches!(store.compact(), Err(Error::Backend(_))),
        "compact with a live reader"
    );
    drop(reader);
    assert!(store.compact()?);
    drop(store);
    // reopening an existing file (tables present) and writing still works
    let store = RedbStore::open(&path, CACHE)?;
    let mut w = store.begin_write()?;
    w.put(Table::Records, b"k", b"v")?;
    w.commit(Durability::Immediate)?;
    assert_eq!(
        store.begin_read()?.get(Table::Records, b"k")?,
        Some(b"v".to_vec())
    );

    let junk = dir.path().join("junk.redb");
    std::fs::write(&junk, b"this is not a redb database file")?;
    assert!(
        matches!(RedbStore::open(&junk, CACHE), Err(Error::Backend(_))),
        "not a database"
    );
    Ok(())
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn report(label: &str, lat: &mut [Duration]) {
    lat.sort();
    let mean = lat.iter().sum::<Duration>() / lat.len() as u32;
    println!(
        "{label}: n={} p50={:?} p99={:?} max={:?} mean={:?}",
        lat.len(),
        percentile(lat, 50.0),
        percentile(lat, 99.0),
        lat[lat.len() - 1],
        mean
    );
}

fn per_op(total: Duration, n: usize) -> String {
    let secs = total.as_secs_f64();
    format!(
        "{total:?} total, {:.2} us/op, {:.0} ops/s",
        secs * 1e6 / n as f64,
        n as f64 / secs
    )
}

#[test]
#[ignore = "quick measurement: cargo test --release --test store_conformance -- --ignored --nocapture"]
fn redb_quick_measurement() -> Result<()> {
    const N: usize = 10_000;
    const THREADS: usize = 8;
    let build = if cfg!(debug_assertions) {
        "DEBUG build"
    } else {
        "release build"
    };
    let dir = tempfile::tempdir()?;
    println!(
        "redb quick measurement ({build}), file in {}",
        dir.path().display()
    );
    let store = RedbStore::open(dir.path().join("bench.redb"), 64 << 20)?;
    let value = [0x5a_u8; 100];

    for (label, durability) in [
        ("Immediate", Durability::Immediate),
        ("Deferred", Durability::Deferred),
    ] {
        let mut lat = Vec::with_capacity(200);
        for i in 0..200 {
            let key = format!("commit/{label}/{i:05}");
            let t = Instant::now();
            let mut w = store.begin_write()?;
            w.put(Table::Records, key.as_bytes(), &value)?;
            w.commit(durability)?;
            lat.push(t.elapsed());
        }
        report(
            &format!("commit of 1 put (~20 B key, 100 B value), {label}"),
            &mut lat,
        );
    }
    let t = Instant::now();
    store.begin_write()?.commit(Durability::Immediate)?;
    println!(
        "empty Immediate commit after 200 Deferred ones: {:?}",
        t.elapsed()
    );

    let keys: Vec<[u8; 16]> = (0..N as u64)
        .map(|i| {
            let mut k = [0u8; 16];
            k[..4].copy_from_slice(b"msg/");
            k[8..].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
            k
        })
        .collect();
    let t = Instant::now();
    let mut w = store.begin_write()?;
    for k in &keys {
        w.put(Table::Records, k, &value)?;
    }
    let puts = t.elapsed();
    w.commit(Durability::Immediate)?;
    println!(
        "10k puts (16 B key, 100 B value) in one txn: {}; incl. Immediate commit {:?}",
        per_op(puts, N),
        t.elapsed()
    );

    let order: Vec<usize> = (0..N).map(|i| (i * 7919) % N).collect();
    let t = Instant::now();
    for _ in 0..N {
        black_box(store.begin_read()?);
    }
    println!("begin_read alone: {}", per_op(t.elapsed(), N));
    let t = Instant::now();
    for &i in &order {
        black_box(store.begin_read()?.get(Table::Records, &keys[i])?);
    }
    println!(
        "10k gets, 1 thread, a read txn per get: {}",
        per_op(t.elapsed(), N)
    );
    let t = Instant::now();
    let r = store.begin_read()?;
    for &i in &order {
        black_box(r.get(Table::Records, &keys[i])?);
    }
    drop(r);
    println!(
        "10k gets, 1 thread, one read txn: {}",
        per_op(t.elapsed(), N)
    );

    let t = Instant::now();
    let mut lat = std::thread::scope(|scope| -> Result<Vec<Duration>> {
        let handles: Vec<_> = (0..THREADS)
            .map(|th| {
                let (store, keys, order) = (&store, &keys, &order);
                scope.spawn(move || -> Result<Vec<Duration>> {
                    let mut lat = Vec::with_capacity(N);
                    for j in 0..N {
                        let k = &keys[order[(j + th * 1250) % N]];
                        let t = Instant::now();
                        let v = store.begin_read()?.get(Table::Records, k)?;
                        lat.push(t.elapsed());
                        assert!(v.is_some());
                    }
                    Ok(lat)
                })
            })
            .collect();
        let mut all = Vec::with_capacity(THREADS * N);
        for h in handles {
            all.extend(h.join().expect("reader thread")?);
        }
        Ok(all)
    })?;
    println!(
        "{THREADS} threads x 10k gets, a read txn per get: wall {}",
        per_op(t.elapsed(), THREADS * N)
    );
    report("  latency of one get across the threads", &mut lat);
    drop(store);

    // why RedbRead opens tables lazily: raw redb, begin_read + open 1 or 9 tables + 1 get
    use redb::ReadableDatabase;
    let db = redb::Database::create(dir.path().join("raw.redb")).map_err(Error::backend)?;
    let defs: Vec<redb::TableDefinition<&[u8], &[u8]>> = Table::ALL
        .iter()
        .map(|t| redb::TableDefinition::new(t.name()))
        .collect();
    let w = db.begin_write().map_err(Error::backend)?;
    for d in &defs {
        let mut table = w.open_table(*d).map_err(Error::backend)?;
        table
            .insert(b"k".as_slice(), b"v".as_slice())
            .map_err(Error::backend)?;
    }
    w.commit().map_err(Error::backend)?;
    for open in [1, defs.len()] {
        let t = Instant::now();
        for _ in 0..N {
            let r = db.begin_read().map_err(Error::backend)?;
            let mut tables = Vec::with_capacity(open);
            for d in &defs[..open] {
                tables.push(r.open_table(*d).map_err(Error::backend)?);
            }
            black_box(
                tables[0]
                    .get(b"k".as_slice())
                    .map_err(Error::backend)?
                    .map(|g| g.value().len()),
            );
        }
        println!(
            "raw redb begin_read + open {open} table(s) + 1 get: {}",
            per_op(t.elapsed(), N)
        );
    }

    // what RedbWrite pays for reopening the table on every call
    for reopen in [false, true] {
        let w = db.begin_write().map_err(Error::backend)?;
        let t = Instant::now();
        if reopen {
            for k in &keys {
                let mut table = w.open_table(defs[1]).map_err(Error::backend)?;
                table
                    .insert(k.as_slice(), value.as_slice())
                    .map_err(Error::backend)?;
            }
        } else {
            let mut table = w.open_table(defs[1]).map_err(Error::backend)?;
            for k in &keys {
                table
                    .insert(k.as_slice(), value.as_slice())
                    .map_err(Error::backend)?;
            }
        }
        let puts = t.elapsed();
        w.abort().map_err(Error::backend)?;
        let how = if reopen {
            "table reopened per put"
        } else {
            "table held open"
        };
        println!("raw redb 10k puts in one txn, {how}: {}", per_op(puts, N));
    }
    Ok(())
}
