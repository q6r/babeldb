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

/// `put_many` has exactly the effect of one `put` per entry, in order.
fn put_many_matches_put<S: Store>(s: &S) -> Result<()> {
    let key = |i: u32| format!("k{:04}", (i * 7919) % 1000).into_bytes();
    let value = |i: u32| format!("v{i}").repeat(i as usize % 50).into_bytes();
    let mut w = s.begin_write()?;
    w.put(Table::Records, b"k0000", b"put before")?;
    w.put(Table::Records, b"zz", b"untouched")?;
    // 1500 entries over 1000 keys: 500 keys are given twice, the later value must win
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..1500).map(|i| (key(i % 1000), value(i))).collect();
    w.put_many(
        Table::Records,
        &mut entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
    )?;
    w.put_many(Table::Meta, &mut std::iter::empty())?;
    w.put_many(
        Table::Objects,
        &mut [(b"o".as_slice(), b"object".as_slice())].into_iter(),
    )?;
    let mut model = std::collections::BTreeMap::new();
    model.insert(b"zz".to_vec(), b"untouched".to_vec());
    for (k, v) in &entries {
        model.insert(k.clone(), v.clone());
    }
    assert_eq!(
        w.len(Table::Records)?,
        model.len() as u64,
        "inside the write transaction"
    );
    w.commit(Durability::Immediate)?;
    let r = s.begin_read()?;
    assert_eq!(r.len(Table::Records)?, model.len() as u64);
    for (k, v) in &model {
        assert_eq!(
            r.get(Table::Records, k)?.as_ref(),
            Some(v),
            "key {}",
            String::from_utf8_lossy(k)
        );
    }
    assert_eq!(r.len(Table::Meta)?, 0);
    assert_eq!(r.get(Table::Objects, b"o")?, Some(b"object".to_vec()));
    drop(r);
    // dropped without commit: nothing of it stays
    let mut w = s.begin_write()?;
    w.put_many(
        Table::Records,
        &mut [(b"zz".as_slice(), b"changed".as_slice())].into_iter(),
    )?;
    drop(w);
    assert_eq!(
        s.begin_read()?.get(Table::Records, b"zz")?,
        Some(b"untouched".to_vec())
    );
    Ok(())
}

#[test]
fn put_many_mem_and_redb() -> Result<()> {
    put_many_matches_put(&MemStore::new())?;
    let dir = tempfile::tempdir()?;
    put_many_matches_put(&RedbStore::open(dir.path().join("db.redb"), CACHE)?)
}

fn get_u64<T: ReadTxn>(r: &T, key: &[u8]) -> Result<Option<u64>> {
    Ok(r.get(Table::Records, key)?
        .map(|v| u64::from_be_bytes(v.as_slice().try_into().expect("8-byte counter value"))))
}

fn put_u64(store: &RedbStore, key: &[u8], v: u64, durability: Durability) -> Result<()> {
    let mut w = store.begin_write()?;
    w.put(Table::Records, key, &v.to_be_bytes())?;
    w.commit(durability)
}

/// Snapshot reuse (see `store::redb`): a reused snapshot is never older than the last commit,
/// open read transactions keep their snapshot across commits, aborts keep the cache, and every
/// commit empties it.
#[test]
fn redb_snapshot_reuse_follows_commits() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut store = RedbStore::open(dir.path().join("db.redb"), CACHE)?;
    let k = b"k".as_slice();
    put_u64(&store, k, 1, Durability::Immediate)?;
    assert_eq!(
        store.cached_snapshots(),
        0,
        "a commit leaves nothing cached"
    );

    let r1 = store.begin_read()?;
    assert_eq!(get_u64(&r1, k)?, Some(1));
    assert_eq!(store.cached_snapshots(), 1, "the first read is cached");
    let r2 = store.begin_read()?;
    assert_eq!(get_u64(&r2, k)?, Some(1));
    assert_eq!(store.cached_snapshots(), 1, "the second read reuses it");

    for (v, durability) in [(2, Durability::Deferred), (3, Durability::Immediate)] {
        put_u64(&store, k, v, durability)?;
        assert_eq!(store.cached_snapshots(), 0, "a commit empties the cache");
        assert_eq!(
            get_u64(&store.begin_read()?, k)?,
            Some(v),
            "{durability:?} commit"
        );
        assert_eq!(get_u64(&r1, k)?, Some(1), "an open read keeps its snapshot");
        assert_eq!(get_u64(&r2, k)?, Some(1), "an open read keeps its snapshot");
    }

    // an aborted write changes nothing: the cached snapshot stays valid and in use
    assert_eq!(store.cached_snapshots(), 1);
    {
        let mut w = store.begin_write()?;
        w.put(Table::Records, k, &9u64.to_be_bytes())?;
        w.put(Table::Records, b"other", b"x")?;
        assert_eq!(
            get_u64(&store.begin_read()?, k)?,
            Some(3),
            "uncommitted write"
        );
    }
    assert_eq!(store.cached_snapshots(), 1, "an abort keeps the cache");
    assert_eq!(get_u64(&store.begin_read()?, k)?, Some(3));
    assert_eq!(store.begin_read()?.get(Table::Records, b"other")?, None);

    // a commit that changes nothing still starts a new generation
    store.begin_write()?.commit(Durability::Deferred)?;
    assert_eq!(store.cached_snapshots(), 0);
    let r3 = store.begin_read()?;
    assert_eq!(get_u64(&r3, k)?, Some(3));

    // readers on other threads use their own shards
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                let r = store.begin_read().expect("begin_read");
                assert_eq!(get_u64(&r, k).expect("get"), Some(3));
            });
        }
    });
    assert!(
        store.cached_snapshots() >= 2,
        "one snapshot per reading thread"
    );

    // compaction releases the cached snapshots but not the ones still open
    assert!(
        matches!(store.compact(), Err(Error::Backend(_))),
        "open readers"
    );
    drop((r1, r2, r3));
    assert!(store.compact()?);
    assert_eq!(get_u64(&store.begin_read()?, k)?, Some(3));
    put_u64(&store, k, 4, Durability::Immediate)?;
    assert_eq!(get_u64(&store.begin_read()?, k)?, Some(4));
    Ok(())
}

/// Readers never see a value older than one whose commit returned before they started, nor
/// older than one another reader saw before they started, nor a value not yet committed; each
/// reader's values only grow. Commits alternate `Deferred` and `Immediate`.
#[test]
fn redb_snapshot_reuse_never_serves_stale_reads() -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
    const READERS: usize = 8;
    const COMMITS: u64 = 1500;
    let dir = tempfile::tempdir()?;
    let store = RedbStore::open(dir.path().join("db.redb"), CACHE)?;
    let key = b"counter".as_slice();
    put_u64(&store, key, 0, Durability::Immediate)?;
    // started: highest value a commit was begun with; acked: highest value whose commit
    // returned; seen: highest value a finished read returned.
    let (started, acked, seen) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
    let done = AtomicBool::new(false);
    let reads = std::thread::scope(|s| -> Result<Vec<u64>> {
        let readers: Vec<_> = (0..READERS)
            .map(|t| {
                let (store, started, acked, seen, done) = (&store, &started, &acked, &seen, &done);
                s.spawn(move || -> Result<u64> {
                    let mut last = 0;
                    let mut n = 0u64;
                    let mut held = None;
                    while !done.load(SeqCst) || n < 100 {
                        let floor = acked.load(SeqCst).max(seen.load(SeqCst));
                        let r = store.begin_read()?;
                        let v = get_u64(&r, key)?.expect("counter");
                        let ceil = started.load(SeqCst);
                        assert!(
                            v >= floor,
                            "reader {t}: stale read {v}, {floor} was visible before"
                        );
                        assert!(v <= ceil, "reader {t}: phantom {v}, only {ceil} begun");
                        assert!(v >= last, "reader {t}: went back from {last} to {v}");
                        seen.fetch_max(v, SeqCst);
                        last = v;
                        n += 1;
                        // sometimes keep a read open for a while: its snapshot must not move
                        if n % 97 == t as u64 {
                            held = Some((r, v));
                        } else if let Some((h, hv)) = held.take_if(|_| n.is_multiple_of(13)) {
                            assert_eq!(
                                get_u64(&h, key)?,
                                Some(hv),
                                "reader {t}: a held snapshot moved"
                            );
                        }
                    }
                    Ok(n)
                })
            })
            .collect();
        let written = (1..=COMMITS).try_for_each(|v| {
            started.store(v, SeqCst);
            let durability = if v % 10 == 0 {
                Durability::Immediate
            } else {
                Durability::Deferred
            };
            put_u64(&store, key, v, durability)?;
            acked.store(v, SeqCst);
            Ok(())
        });
        done.store(true, SeqCst);
        let mut reads = Vec::new();
        for r in readers {
            reads.push(r.join().expect("reader thread")?);
        }
        written.map(|()| reads)
    })?;
    assert!(
        reads.iter().all(|&n| n >= 100),
        "reads per thread: {reads:?}"
    );
    assert_eq!(get_u64(&store.begin_read()?, key)?, Some(COMMITS));
    Ok(())
}

/// A snapshot cached by a thread that then stays idle must not keep redb from reusing the pages
/// that later commits free: the file stays small while the same keys are rewritten many times.
#[test]
fn redb_snapshot_cache_does_not_pin_freed_pages() -> Result<()> {
    const KEYS: u32 = 4096;
    const VALUE: usize = 1000;
    const ROUNDS: u8 = 24;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.redb");
    let store = &RedbStore::open(&path, CACHE)?;
    let rewrite = |round: u8| -> Result<()> {
        let mut w = store.begin_write()?;
        let value = vec![round; VALUE];
        for i in 0..KEYS {
            w.put(Table::Records, &i.to_be_bytes(), &value)?;
        }
        w.commit(Durability::Immediate)
    };
    rewrite(0)?;
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|s| -> Result<()> {
        let idle = s.spawn(move || -> Result<()> {
            // one read, cached in this thread's shard, then idle until the end
            assert_eq!(store.begin_read()?.len(Table::Records)?, u64::from(KEYS));
            rx.recv().ok();
            Ok(())
        });
        while store.cached_snapshots() == 0 {
            std::thread::yield_now();
        }
        for round in 1..=ROUNDS {
            rewrite(round)?;
        }
        tx.send(()).ok();
        idle.join().expect("idle reader")
    })?;
    let data = u64::from(KEYS) * VALUE as u64;
    let file = std::fs::metadata(&path)?.len();
    println!("{ROUNDS} rewrites of {data} bytes: file {file} bytes");
    // pinned, every round would need new pages: more than ROUNDS x data
    assert!(
        file < 16 * data,
        "file of {file} bytes for {data} bytes of data"
    );
    let first = store
        .begin_read()?
        .get(Table::Records, &0u32.to_be_bytes())?;
    assert_eq!(first, Some(vec![ROUNDS; VALUE]));
    Ok(())
}

/// `compact` rewrites a table whose leaves were left half empty (appends at the end of many
/// key ranges, like chat messages per channel): same entries, far fewer pages.
#[test]
fn redb_compact_rewrites_fragmented_tables() -> Result<()> {
    const RANGES: u32 = 64;
    const RECORDS: u32 = 12_000;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.redb");
    let mut store = RedbStore::open(&path, CACHE)?;
    let mut model = std::collections::BTreeMap::new();
    let mut payload = 0;
    for batch in 0..RECORDS / 500 {
        let mut w = store.begin_write()?;
        for i in batch * 500..(batch + 1) * 500 {
            let key = format!("ch/{:03}/{i:08}", i % RANGES).into_bytes();
            let value = format!("message {i:08} ")
                .repeat(1 + i as usize % 32)
                .into_bytes();
            w.put(Table::Records, &key, &value)?;
            payload += key.len() + value.len();
            model.insert(key, value);
        }
        w.commit(Durability::Immediate)?;
    }
    // other tables: small, and one of values that each need a leaf of their own
    let mut w = store.begin_write()?;
    w.put(Table::Meta, b"m", b"1")?;
    for i in 0..64u32 {
        w.put(Table::Objects, &i.to_be_bytes(), &vec![i as u8; 20_000])?;
    }
    w.commit(Durability::Immediate)?;
    let loaded = std::fs::metadata(&path)?.len();
    assert!(store.compact()?);
    let compacted = std::fs::metadata(&path)?.len();
    println!(
        "{payload} bytes of records: file {loaded} bytes after the load, {compacted} after compact"
    );
    let objects = 64 * (32 << 10);
    assert!(
        compacted < (payload as u64) * 3 / 2 + objects + (1 << 20),
        "compacted file of {compacted} bytes for {payload} bytes of records"
    );

    let r = store.begin_read()?;
    let mut seen = Vec::new();
    r.scan(
        Table::Records,
        std::ops::Bound::Unbounded,
        std::ops::Bound::Unbounded,
        false,
        &mut |k, v| {
            seen.push((k.to_vec(), v.to_vec()));
            Ok(true)
        },
    )?;
    assert!(
        seen.iter().map(|(k, v)| (k, v)).eq(model.iter()),
        "records changed by compact"
    );
    assert_eq!(r.len(Table::Records)?, u64::from(RECORDS));
    assert_eq!(r.get(Table::Meta, b"m")?, Some(b"1".to_vec()));
    assert_eq!(r.len(Table::Objects)?, 64);
    assert_eq!(
        r.get(Table::Objects, &7u32.to_be_bytes())?,
        Some(vec![7; 20_000])
    );
    drop(r);
    // still writable, and a second compact finds nothing to rewrite
    put_u64(&store, b"after", 1, Durability::Immediate)?;
    assert!(store.compact()?);
    drop(store);
    let store = RedbStore::open(&path, CACHE)?;
    let r = store.begin_read()?;
    assert_eq!(get_u64(&r, b"after")?, Some(1));
    assert_eq!(r.len(Table::Records)?, u64::from(RECORDS) + 1);
    for table in Table::ALL {
        r.len(table)?;
    }
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
