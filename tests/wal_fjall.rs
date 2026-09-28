//! `WalStore<FjallStore>` and `Db::open_fjall_wal` (feature `fjall`).
//!
//! - The backend conformance suite on `WalStore<FjallStore>`: a small WAL (most large
//!   transactions become checkpoints) and a roomy one with the WAL tuning
//!   (`FjallOptions::for_wal`: LZ4 on every level, Deferred commits left in fjall's journal
//!   buffer), a small WAL with fjall's defaults, the persistent and concurrent suites; and the
//!   suite on bare `FjallStore`s with other tunings (block sizes, compression, tiny memtables).
//! - The engine on top: a mixed workload with simulated crashes (the store dropped while its
//!   thread panics, so `WalStore` skips its closing checkpoint), `verify(true)`, `gc` and a final
//!   drain; a group committer.
//! - A child process that exits without running destructors: fjall loses the commits still in
//!   its journal buffer (its journal ends with a torn batch), and recovery replays them from the
//!   WAL. Like every kill test, not a power-loss test.
//! - `FjallStore::flush_memtables` and `DeferredPersist::JournalBuffer` on a bare store; read
//!   snapshot reuse (never stale, emptied by commits and `compact`).
//!
//! Measurement (ignored, release build, idle machine):
//! `cargo test --release --features fjall --test wal_fjall -- --ignored --nocapture measure_reads`

#![cfg(feature = "fjall")]

mod common;

use std::path::Path;
use std::panic::{self, AssertUnwindSafe};
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use babeldb::config::{AutoDictionary, WalConfig};
use babeldb::datasets::{self, Scenario};
use babeldb::engine::FjallWalDb;
use babeldb::maintenance::GcReport;
use babeldb::scale::{GroupCommitConfig, GroupCommitter, WriteDurability, message_key};
use babeldb::store::conformance;
use babeldb::store::fjall::{DeferredPersist, FjallCompression, FjallOptions, FjallStore};
use babeldb::store::wal::WalStore;
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use babeldb::{BatchOp, Config, Db, Error, Expect, Mode, Result, ScanOptions};
use common::{
    Pattern, Rng, State, assert_state_eq, delete_everything_and_check_no_leaks, env_u64, pattern_bytes,
    small_config, snapshot, values_of, verify_ok,
};

const CACHE: usize = 16 << 20;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Small WAL: 256 KiB file, records above 128 KiB become checkpoints.
fn small_wal() -> WalConfig {
    WalConfig {
        segment_bytes: 256 << 10,
        max_pending_bytes: 64 << 10,
        max_record_bytes: 128 << 10,
        ..WalConfig::default()
    }
}

/// Roomy WAL: 8 MiB file, records up to 4 MiB are logged.
fn roomy_wal() -> WalConfig {
    WalConfig {
        segment_bytes: 8 << 20,
        max_pending_bytes: 1 << 20,
        max_record_bytes: 4 << 20,
        ..WalConfig::default()
    }
}

/// fjall in `<dir>/db.fjall`, its WAL next to it.
fn open_store(dir: &Path, wal: &WalConfig, opts: &FjallOptions) -> WalStore<FjallStore> {
    let path = dir.join("db.fjall");
    let inner = FjallStore::open_with(&path, CACHE, opts).unwrap_or_else(|e| panic!("open fjall {}: {e}", path.display()));
    WalStore::open(inner, wal.wal_path(&path), wal.clone()).unwrap_or_else(|e| panic!("open the WAL of {}: {e}", path.display()))
}

/// Drop `value` while the thread panics: `WalStore` skips its closing checkpoint (fjall still
/// hands its journal buffer to the OS when dropped; `a_killed_writer_...` covers losing it).
fn crash<T>(value: T) {
    let outcome = panic::catch_unwind(AssertUnwindSafe(move || {
        let _held = value;
        panic!("simulated crash (expected by this test)");
    }));
    assert!(outcome.is_err(), "the simulated crash did not unwind");
}

// ---------------------------------------------------------------------------
// Conformance
// ---------------------------------------------------------------------------

fn wal_conformance(wal: &WalConfig, opts: &FjallOptions) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut n = 0;
    conformance::run_all(&mut || {
        n += 1;
        let sub = dir.path().join(format!("store-{n}"));
        std::fs::create_dir_all(&sub).expect("create store dir");
        open_store(&sub, wal, opts)
    })
}

#[test]
fn wal_fjall_conformance_small_wal() -> Result<()> {
    wal_conformance(&small_wal(), &FjallOptions::for_wal())
}

#[test]
fn wal_fjall_conformance_roomy_wal() -> Result<()> {
    wal_conformance(&roomy_wal(), &FjallOptions::for_wal())
}

#[test]
fn wal_fjall_conformance_with_fjall_defaults() -> Result<()> {
    wal_conformance(&small_wal(), &FjallOptions::default())
}

#[test]
fn wal_fjall_persistent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    conformance::run_persistent(&mut |d| open_store(d, &small_wal(), &FjallOptions::for_wal()), dir.path())
}

#[test]
fn wal_fjall_concurrent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    conformance::run_concurrent(Arc::new(open_store(dir.path(), &small_wal(), &FjallOptions::for_wal())))
}

/// The suite on bare stores with other tunings: the WAL tuning without a WAL, 16 KiB
/// uncompressed blocks, and 1 KiB blocks with 64 KiB memtables (a flush every few commits) and
/// no last-level filters.
#[test]
fn fjall_tunings_conformance() -> Result<()> {
    let tunings = [
        FjallOptions::for_wal(),
        FjallOptions { compression: FjallCompression::None, data_block_bytes: 16 << 10, ..FjallOptions::for_wal() },
        FjallOptions { data_block_bytes: 1 << 10, memtable_bytes: 64 << 10, expect_point_read_hits: true, ..FjallOptions::default() },
    ];
    for opts in &tunings {
        eprintln!("tuning: {opts:?}");
        let dir = tempfile::tempdir()?;
        let mut n = 0;
        conformance::run_all(&mut || {
            n += 1;
            FjallStore::open_with(dir.path().join(format!("store-{n}")), CACHE, opts).expect("open fjall store")
        })?;
    }
    Ok(())
}

#[test]
fn invalid_options_are_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bad = [
        FjallOptions { data_block_bytes: 512, ..FjallOptions::default() },
        FjallOptions { data_block_bytes: 2 << 20, ..FjallOptions::default() },
        FjallOptions { memtable_bytes: 4 << 10, ..FjallOptions::default() },
    ];
    for opts in &bad {
        match FjallStore::open_with(dir.path().join("refused"), CACHE, opts) {
            Err(Error::InvalidArgument(_)) => {}
            other => panic!("{opts:?}: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Bare store: journal buffer and memtable flush
// ---------------------------------------------------------------------------

fn put_one(s: &FjallStore, table: Table, key: &[u8], value: &[u8], d: Durability) {
    let mut w = s.begin_write().expect("begin_write");
    w.put(table, key, value).expect("put");
    w.commit(d).expect("commit");
}

/// Deferred commits left in the journal buffer are visible at once and durable after the next
/// Immediate commit (an empty one included) and after a clean close.
#[test]
fn journal_buffer_deferred_commits_are_visible_and_become_durable() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let s = FjallStore::open_with(dir.path(), CACHE, &FjallOptions::for_wal()).expect("open");
        assert_eq!(s.deferred_persist(), DeferredPersist::JournalBuffer);
        for i in 0..50u32 {
            put_one(&s, Table::Records, &i.to_be_bytes(), b"deferred", Durability::Deferred);
        }
        assert_eq!(s.begin_read().expect("read").len(Table::Records).expect("len"), 50);
        put_one(&s, Table::Meta, b"synced", b"yes", Durability::Immediate);
        for i in 50..60u32 {
            put_one(&s, Table::Records, &i.to_be_bytes(), b"deferred", Durability::Deferred);
        }
        s.begin_write().expect("begin_write").commit(Durability::Immediate).expect("empty Immediate commit");
        s.begin_write().expect("begin_write").commit(Durability::Deferred).expect("empty Deferred commit");
        for i in 60..70u32 {
            put_one(&s, Table::Records, &i.to_be_bytes(), b"closed", Durability::Deferred);
        }
    }
    let s = FjallStore::open_with(dir.path(), CACHE, &FjallOptions::for_wal()).expect("reopen");
    let r = s.begin_read().expect("read");
    assert_eq!(r.len(Table::Records).expect("len"), 70);
    assert_eq!(r.get(Table::Meta, b"synced").expect("get").as_deref(), Some(&b"yes"[..]));
}

#[test]
fn flush_memtables_moves_the_commits_into_tables() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let table_bytes = |s: &FjallStore| -> u64 {
        let root = s.dir().join("keyspaces");
        s.files().iter().filter(|p| p.starts_with(&root)).map(|p| std::fs::metadata(p).map_or(0, |m| m.len())).sum()
    };
    let opts = FjallOptions { compression: FjallCompression::None, ..FjallOptions::for_wal() };
    let mut rng = Rng::new(0xF1A5);
    let values: Vec<Vec<u8>> = (0..500).map(|_| rng.bytes(1000)).collect();
    {
        let s = FjallStore::open_with(dir.path(), CACHE, &opts)?;
        let mut w = s.begin_write()?;
        for (i, v) in values.iter().enumerate() {
            w.put(Table::Objects, &(i as u64).to_be_bytes(), v)?;
        }
        w.commit(Durability::Deferred)?;
        let before = table_bytes(&s);
        s.flush_memtables()?;
        let after = table_bytes(&s);
        assert!(after >= before + 500 * 1000, "tables grew from {before} to {after} bytes only");
        s.flush_memtables()?;
        put_one(&s, Table::Objects, b"after the flush", b"x", Durability::Deferred);
    }
    let s = FjallStore::open_with(dir.path(), CACHE, &opts)?;
    let r = s.begin_read()?;
    for (i, v) in values.iter().enumerate() {
        assert_eq!(r.get(Table::Objects, &(i as u64).to_be_bytes())?.as_deref(), Some(&v[..]), "value {i}");
    }
    assert_eq!(r.get(Table::Objects, b"after the flush")?.as_deref(), Some(&b"x"[..]));
    Ok(())
}

/// `FjallOptions::for_wal` separates values of 1 KiB and more: behind a small WAL (frequent
/// checkpoints) with 1 MiB memtables (frequent flushes), large values end up in blob files, in
/// tables' pointers and in fjall's journal buffer. A crash (no closing checkpoint) loses no
/// acknowledged commit, and `compact` leaves no blob garbage.
#[test]
fn kv_separated_values_survive_a_crash_behind_the_wal() -> Result<()> {
    const LEN: usize = 16 << 10;
    let dir = tempfile::tempdir()?;
    let opts = FjallOptions { memtable_bytes: 1 << 20, ..FjallOptions::for_wal() };
    let value = |i: u64, round: u64| Rng::new(i * 4 + round + 1).bytes(LEN);
    let mut expected: Vec<Option<Vec<u8>>> = Vec::new();
    let check = |s: &WalStore<FjallStore>, expected: &[Option<Vec<u8>>], what: &str| {
        let r = s.begin_read().expect("begin_read");
        for (i, v) in expected.iter().enumerate() {
            let got = r.get(Table::Objects, &(i as u64).to_be_bytes()).expect("get");
            assert!(got == *v, "{what}: key {i}: {:?} bytes, expected {:?}", got.map(|g| g.len()), v.as_ref().map(Vec::len));
        }
    };
    {
        let store = open_store(dir.path(), &small_wal(), &opts);
        assert!(store.inner().kv_separated());
        for i in 0..240u64 {
            let v = value(i, 0);
            let mut w = store.begin_write()?;
            w.put(Table::Objects, &i.to_be_bytes(), &v)?;
            w.commit(Durability::Immediate)?;
            expected.push(Some(v));
        }
        store.inner().flush_memtables()?;
        assert!(store.inner().blob_file_count() > 0, "no blob file after a flush");
        for i in 0..240u64 {
            let mut w = store.begin_write()?;
            match i % 3 {
                0 => {
                    let v = value(i, 1);
                    w.put(Table::Objects, &i.to_be_bytes(), &v)?;
                    expected[i as usize] = Some(v);
                }
                1 => {
                    assert!(w.remove(Table::Objects, &i.to_be_bytes())?);
                    expected[i as usize] = None;
                }
                _ => continue,
            }
            w.commit(Durability::Immediate)?;
        }
        assert!(store.stats().checkpoints > 0, "no checkpoint");
        crash(store);
    }
    let mut store = open_store(dir.path(), &small_wal(), &opts);
    check(&store, &expected, "after the crash");
    assert!(store.compact()?);
    assert_eq!(store.inner().stale_blob_bytes(), 0, "blob garbage left by compact");
    check(&store, &expected, "after compact");
    drop(store);
    check(&open_store(dir.path(), &small_wal(), &opts), &expected, "after compact and reopen");
    Ok(())
}

// ---------------------------------------------------------------------------
// Read snapshot reuse (see `store::fjall`)
// ---------------------------------------------------------------------------

fn get_u64<R: ReadTxn>(r: &R, key: &[u8]) -> Option<u64> {
    r.get(Table::Records, key).expect("get").map(|v| u64::from_be_bytes(v.as_slice().try_into().expect("8-byte value")))
}

/// A reused snapshot is never older than the last commit, open reads keep their snapshot across
/// commits, aborts and empty commits keep the cache, every commit with changes and `compact`
/// empty it.
#[test]
fn fjall_snapshot_reuse_follows_commits() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut store = FjallStore::open_with(dir.path(), CACHE, &FjallOptions::for_wal())?;
    let k = b"k".as_slice();
    put_one(&store, Table::Records, k, &1u64.to_be_bytes(), Durability::Immediate);
    assert_eq!(store.cached_snapshots(), 0, "a commit leaves nothing cached");
    let r1 = store.begin_read()?;
    assert_eq!(get_u64(&r1, k), Some(1));
    assert_eq!(store.cached_snapshots(), 1, "the first read is cached");
    let r2 = store.begin_read()?;
    assert_eq!(store.cached_snapshots(), 1, "the second read reuses it");
    for (v, d) in [(2u64, Durability::Deferred), (3, Durability::Immediate)] {
        put_one(&store, Table::Records, k, &v.to_be_bytes(), d);
        assert_eq!(store.cached_snapshots(), 0, "a commit empties the cache");
        assert_eq!(get_u64(&store.begin_read()?, k), Some(v), "{d:?} commit");
        assert_eq!((get_u64(&r1, k), get_u64(&r2, k)), (Some(1), Some(1)), "open reads keep their snapshot");
    }
    {
        let mut w = store.begin_write()?;
        w.put(Table::Records, k, &9u64.to_be_bytes())?;
        assert_eq!(get_u64(&store.begin_read()?, k), Some(3), "uncommitted write");
    }
    store.begin_write()?.commit(Durability::Deferred)?;
    assert_eq!(store.cached_snapshots(), 1, "an abort and an empty commit keep the cache");
    assert_eq!(get_u64(&store.begin_read()?, k), Some(3));
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| assert_eq!(get_u64(&store.begin_read().expect("begin_read"), k), Some(3)));
        }
    });
    assert!(store.cached_snapshots() >= 2, "one snapshot per reading thread");
    drop((r1, r2));
    assert!(store.compact()?);
    assert_eq!(store.cached_snapshots(), 0, "compact empties the cache");
    put_one(&store, Table::Records, k, &4u64.to_be_bytes(), Durability::Immediate);
    assert_eq!(get_u64(&store.begin_read()?, k), Some(4));
    Ok(())
}

/// Readers never see a value older than one whose commit returned before they started, nor older
/// than one another reader saw before, nor one not yet committed; held snapshots never move.
#[test]
fn fjall_snapshot_reuse_never_serves_stale_reads() -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
    const READERS: usize = 8;
    const COMMITS: u64 = 1500;
    let dir = tempfile::tempdir()?;
    let store = FjallStore::open_with(dir.path(), CACHE, &FjallOptions::for_wal())?;
    let key = b"counter".as_slice();
    put_one(&store, Table::Records, key, &0u64.to_be_bytes(), Durability::Immediate);
    let (started, acked, seen, done) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicBool::new(false));
    let reads = std::thread::scope(|s| {
        let readers: Vec<_> = (0..READERS)
            .map(|t| {
                let (store, started, acked, seen, done) = (&store, &started, &acked, &seen, &done);
                s.spawn(move || {
                    let (mut last, mut n, mut held) = (0, 0u64, None);
                    while !done.load(SeqCst) || n < 100 {
                        let floor = acked.load(SeqCst).max(seen.load(SeqCst));
                        let r = store.begin_read().expect("begin_read");
                        let v = get_u64(&r, key).expect("counter");
                        assert!(v >= floor, "reader {t}: stale read {v}, {floor} was visible before");
                        assert!(v <= started.load(SeqCst), "reader {t}: phantom {v}");
                        assert!(v >= last, "reader {t}: went back from {last} to {v}");
                        seen.fetch_max(v, SeqCst);
                        (last, n) = (v, n + 1);
                        if n % 97 == t as u64 {
                            held = Some((r, v));
                        } else if let Some((h, hv)) = held.take_if(|_| n.is_multiple_of(13)) {
                            assert_eq!(get_u64(&h, key), Some(hv), "reader {t}: a held snapshot moved");
                        }
                    }
                    n
                })
            })
            .collect();
        for v in 1..=COMMITS {
            started.store(v, SeqCst);
            let d = if v % 10 == 0 { Durability::Immediate } else { Durability::Deferred };
            put_one(&store, Table::Records, key, &v.to_be_bytes(), d);
            acked.store(v, SeqCst);
        }
        done.store(true, SeqCst);
        readers.into_iter().map(|r| r.join().expect("reader thread")).collect::<Vec<u64>>()
    });
    assert!(reads.iter().all(|&n| n >= 100), "reads per thread: {reads:?}");
    assert_eq!(get_u64(&store.begin_read()?, key), Some(COMMITS));
    Ok(())
}

// ---------------------------------------------------------------------------
// The engine on top
// ---------------------------------------------------------------------------

fn engine_wal() -> WalConfig {
    WalConfig {
        segment_bytes: 64 << 10,
        max_pending_bytes: 8 << 10,
        max_record_bytes: 16 << 10,
        ..WalConfig::default()
    }
}

fn open_engine(path: &Path, cfg: &Config, wal: &WalConfig) -> FjallWalDb {
    Db::open_fjall_wal(path, cfg.clone(), wal.clone()).unwrap_or_else(|e| panic!("open_fjall_wal {}: {e}", path.display()))
}

fn value_for(rng: &mut Rng) -> Vec<u8> {
    let len = [0usize, 1, 40, 64, 65, 300, 511, 512, 513, 1500, 4000][rng.below(11) as usize];
    let pattern = Pattern::ALL[rng.below(Pattern::ALL.len() as u64) as usize];
    pattern_bytes(pattern, len, rng.next_u64())
}

#[test]
fn engine_on_fjall_wal_survives_crashes_with_a_mixed_workload() {
    let dir = common::temp_dir("babeldb-fjall-wal-engine-");
    let path = dir.path().join("engine.fjall");
    let cfg = small_config(Mode::Adaptive);
    let wal = engine_wal();
    let mut rng = Rng::new(0x00F1_A11E);
    let mut durable: State = State::new();
    let mut checkpoints = 0;
    for round in 0..5u64 {
        let ctx = format!("round {round}");
        let db = open_engine(&path, &cfg, &wal);
        assert_state_eq(&values_of(&snapshot(&db, &ctx)), &durable, &format!("{ctx}: state after reopen"));
        verify_ok(&db, true, &format!("{ctx}: after recovery"));
        let mut visible = durable.clone();
        // deferred puts since the last durable point, in commit order
        let mut unsynced: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for step in 0..200u64 {
            let key = format!("k{:02}", rng.below(30)).into_bytes();
            match rng.below(10) {
                0..=4 => {
                    let v = value_for(&mut rng);
                    db.put(&key, &v, Expect::Any).expect("put");
                    visible.insert(key, v);
                    durable = visible.clone();
                    unsynced.clear();
                }
                5 => {
                    db.delete(&key, Expect::Any).expect("delete");
                    visible.remove(&key);
                    durable = visible.clone();
                    unsynced.clear();
                }
                6 | 7 => {
                    let keys: Vec<Vec<u8>> = (0..1 + rng.below(4)).map(|i| format!("b{:02}", (step + i) % 25).into_bytes()).collect();
                    let values: Vec<Vec<u8>> = keys.iter().map(|_| value_for(&mut rng)).collect();
                    let ops: Vec<BatchOp<'_>> = keys
                        .iter()
                        .zip(&values)
                        .map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any })
                        .collect();
                    db.write_batch(&ops).expect("write_batch");
                    for (k, v) in keys.into_iter().zip(values) {
                        visible.insert(k, v);
                    }
                    durable = visible.clone();
                    unsynced.clear();
                }
                _ => {
                    let v = value_for(&mut rng);
                    let ops = [BatchOp::Put { key: &key, value: &v, expect: Expect::Any }];
                    for r in db.write_batch_each(&ops, Durability::Deferred).expect("write_batch_each") {
                        r.expect("deferred put");
                    }
                    visible.insert(key.clone(), v.clone());
                    unsynced.push((key, v));
                    if rng.chance(1, 2) {
                        db.sync().expect("sync");
                        durable = visible.clone();
                        unsynced.clear();
                    }
                }
            }
        }
        checkpoints += db.store().stats().checkpoints;
        crash(db);
        let db = open_engine(&path, &cfg, &wal);
        let got = values_of(&snapshot(&db, &ctx));
        // Every acknowledged durable write survives; of the deferred puts after the last
        // durable point, only a prefix.
        let mut candidate = durable.clone();
        let mut matched = got == candidate;
        for (k, v) in &unsynced {
            if matched {
                break;
            }
            candidate.insert(k.clone(), v.clone());
            matched = got == candidate;
        }
        if !matched {
            assert_state_eq(&got, &durable, &format!("{ctx}: not the durable state plus a prefix of the deferred puts"));
        }
        verify_ok(&db, true, &format!("{ctx}: after the crash"));
        let mut db = db;
        let gc = db.gc().expect("gc");
        assert_eq!(gc, GcReport::default(), "{ctx}: a crash must leave nothing to collect");
        durable = got;
        drop(db);
    }
    assert!(checkpoints > 0, "the workload never filled the WAL");
    let mut db = open_engine(&path, &cfg, &wal);
    compact_and_check(&mut db);
    delete_everything_and_check_no_leaks(&mut db, "final drain");
}

/// `Db::compact` (a WAL checkpoint, then fjall's flush and major compaction) keeps every value.
fn compact_and_check(db: &mut FjallWalDb) {
    let before = values_of(&snapshot(db, "before compact"));
    let report = db.compact().expect("compact");
    assert!(report.supported);
    assert_state_eq(&values_of(&snapshot(db, "after compact")), &before, "Db::compact");
    verify_ok(db, true, "after compact");
}

#[test]
fn group_commit_on_a_fjall_wal_database() -> Result<()> {
    let dir = common::temp_dir("babeldb-fjall-wal-group-");
    let path = dir.path().join("group.fjall");
    let cfg = small_config(Mode::Adaptive);
    let wal = WalConfig {
        segment_bytes: 1 << 20,
        ..engine_wal()
    };
    const THREADS: usize = 8;
    const PER_THREAD: usize = 100;
    let db = Arc::new(open_engine(&path, &cfg, &wal));
    let before = db.store().stats();
    let committer = Arc::new(GroupCommitter::new(db.clone(), GroupCommitConfig::from(WriteDurability::Immediate))?);
    let acked: Mutex<State> = Mutex::new(State::new());
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let (committer, acked) = (&committer, &acked);
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    let key = format!("t{t}/m{i:03}").into_bytes();
                    let value = pattern_bytes(Pattern::Text, 100 + (i * 37) % 900, (t * 1000 + i) as u64);
                    committer.put(key.clone(), value.clone(), Expect::Any).expect("group put");
                    acked.lock().expect("acked").insert(key, value);
                }
            });
        }
    });
    let stats = committer.stats();
    committer.shutdown()?;
    drop(committer);
    let wal_stats = db.store().stats();
    assert_eq!(stats.ops, (THREADS * PER_THREAD) as u64);
    // one WAL write per batch
    assert_eq!(wal_stats.wal_writes - before.wal_writes, stats.batches, "{wal_stats:?} {stats:?}");
    let db = Arc::try_unwrap(db).map_err(|_| Error::backend("the committer still holds the database"))?;
    crash(db);
    let db = open_engine(&path, &cfg, &wal);
    let acked = acked.into_inner().expect("acked");
    assert_state_eq(&values_of(&snapshot(&db, "group")), &acked, "every acknowledged group-committed put");
    verify_ok(&db, true, "group commit after a crash");
    Ok(())
}

// ---------------------------------------------------------------------------
// A child process that exits without destructors
// ---------------------------------------------------------------------------

const CHILD_ENV: &str = "BABEL_FJALL_WAL_CHILD_DB";
const CHILD_STEPS: u64 = 400;

/// 256 KiB WAL: the child's workload checkpoints a few times.
fn child_wal() -> WalConfig {
    WalConfig {
        segment_bytes: 256 << 10,
        ..engine_wal()
    }
}

fn child_config() -> Config {
    Config {
        auto_dictionary: AutoDictionary::disabled(),
        ..small_config(Mode::Adaptive)
    }
}

fn child_key(i: u64) -> Vec<u8> {
    format!("c{:03}", i % 150).into_bytes()
}

fn child_value(i: u64) -> Vec<u8> {
    let pattern = Pattern::ALL[(i % Pattern::ALL.len() as u64) as usize];
    pattern_bytes(pattern, [40usize, 300, 700, 3000, 9000][(i % 5) as usize], i)
}

/// Step `i` of the child: a delete, a two-key batch or a put, each acknowledged (Immediate).
fn child_step(i: u64) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    match i % 7 {
        3 => vec![(child_key(i), None)],
        5 => vec![(child_key(i), Some(child_value(i))), (child_key(i + 37), Some(child_value(i + 37)))],
        _ => vec![(child_key(i), Some(child_value(i)))],
    }
}

/// Deferred puts after the steps, made durable by one `sync`.
fn child_deferred() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..8u64).map(|j| (format!("d{j}").into_bytes(), child_value(1000 + j))).collect()
}

fn child_state() -> State {
    let mut s = State::new();
    for i in 0..CHILD_STEPS {
        for (k, v) in child_step(i) {
            match v {
                Some(v) => s.insert(k, v),
                None => s.remove(&k),
            };
        }
    }
    for (k, v) in child_deferred() {
        s.insert(k, v);
    }
    s
}

#[test]
#[ignore = "child process of a_killed_writer_loses_the_journal_buffer_and_the_wal_restores_it (no-op unless BABEL_FJALL_WAL_CHILD_DB is set)"]
fn fjall_wal_child_entry() {
    let Some(path) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let db = open_engine(Path::new(&path), &child_config(), &child_wal());
    for i in 0..CHILD_STEPS {
        let step = child_step(i);
        match step.as_slice() {
            [(k, None)] => {
                db.delete(k, Expect::Any).expect("delete");
            }
            [(k, Some(v))] => {
                db.put(k, v, Expect::Any).expect("put");
            }
            _ => {
                let ops: Vec<BatchOp<'_>> = step
                    .iter()
                    .map(|(k, v)| BatchOp::Put { key: k, value: v.as_deref().unwrap_or_default(), expect: Expect::Any })
                    .collect();
                db.write_batch(&ops).expect("write_batch");
            }
        }
    }
    let deferred = child_deferred();
    let ops: Vec<BatchOp<'_>> = deferred.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any }).collect();
    for r in db.write_batch_each(&ops, Durability::Deferred).expect("deferred batch") {
        r.expect("deferred put");
    }
    db.sync().expect("sync");
    assert!(db.store().stats().checkpoints > 0, "the child never checkpointed");
    // No destructors: no WAL checkpoint, and fjall's journal buffer never reaches the OS.
    std::process::exit(42);
}

#[test]
fn a_killed_writer_loses_the_journal_buffer_and_the_wal_restores_it() {
    let dir = common::temp_dir("babeldb-fjall-wal-exit-");
    let path = dir.path().join("exit.fjall");
    let status = Command::new(std::env::current_exe().expect("test binary"))
        .args(["fjall_wal_child_entry", "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, &path)
        .stdout(Stdio::null())
        .status()
        .expect("run the child");
    assert_eq!(status.code(), Some(42), "the child did not reach its exit");
    let db = open_engine(&path, &child_config(), &child_wal());
    let rec = db.store().recovery().clone();
    assert!(rec.records_replayed > 0, "fjall kept every commit although its journal buffer was dropped: {rec:?}");
    assert_state_eq(&values_of(&snapshot(&db, "exit")), &child_state(), "every acknowledged commit after the exit");
    verify_ok(&db, true, "after the exit");
    let mut db = db;
    assert_eq!(db.gc().expect("gc"), GcReport::default(), "an exit must leave nothing to collect");
    delete_everything_and_check_no_leaks(&mut db, "drain after the exit");
}

// ---------------------------------------------------------------------------
// Measurement (ignored)
// ---------------------------------------------------------------------------

/// The fastest of 3 runs of `timed` (robust against other load on the machine).
fn best_of_3(threads: usize, per_thread: usize, op: &(dyn Fn(usize, usize) + Sync)) -> Duration {
    (0..3).map(|_| timed(threads, per_thread, op)).min().unwrap_or_default()
}

/// `threads` threads started together, `per_thread` calls each: wall time from the start.
fn timed(threads: usize, per_thread: usize, op: &(dyn Fn(usize, usize) + Sync)) -> Duration {
    let barrier = Barrier::new(threads + 1);
    let mut t0 = Instant::now();
    std::thread::scope(|s| {
        for t in 0..threads {
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                for k in 0..per_thread {
                    op(t, k);
                }
            });
        }
        barrier.wait();
        t0 = Instant::now();
    });
    t0.elapsed()
}

fn measure_one<S: Store>(label: &str, db: &Db<S>, keys: &[[u8; 16]]) {
    let n = keys.len();
    let pick = |t: usize, k: usize| &keys[(k * 7919 + t * 104_729) % n];
    // warm the caches: every record once
    let warm = timed(1, n, &|_, k| {
        let r = db.store().begin_read().expect("begin_read");
        assert!(r.get(Table::Records, &keys[k]).expect("get").is_some());
    });
    println!("{label:<12} warm-up pass over {n} records: {:.0} ns per get", warm.as_secs_f64() * 1e9 / n as f64);
    for threads in [1usize, 16] {
        let per = if threads == 1 { 50_000.min(n) } else { 20_000.min(n) };
        let open = best_of_3(threads, per, &|_, _| {
            drop(db.store().begin_read().expect("begin_read"));
        });
        let store = best_of_3(threads, per, &|t, k| {
            let r = db.store().begin_read().expect("begin_read");
            assert!(r.get(Table::Records, pick(t, k)).expect("get").is_some());
        });
        let engine = best_of_3(threads, per, &|t, k| {
            assert!(db.get(pick(t, k)).expect("get").is_some());
        });
        let prefix = |t: usize, k: usize| pick(t, k)[..8].to_vec();
        let latest = best_of_3(threads, per / 10, &|t, k| {
            let items = db.scan(&ScanOptions::prefix(&prefix(t, k)).reverse(true).limit(50).with_values(true)).expect("scan");
            assert!(!items.is_empty());
        });
        let ns = |d: Duration| d.as_secs_f64() * 1e9 / per as f64;
        let rate = |d: Duration| (threads * per) as f64 / d.as_secs_f64();
        println!(
            "{label:<12} x{threads:<2} begin_read+drop {:>5.0} ns | store get {:>5.0} ns ({:>8.0}/s) | Db::get {:>5.0} ns ({:>8.0}/s) | latest-50 {:>6.0} ns ({:>7.0}/s)",
            ns(open),
            ns(store),
            rate(store),
            ns(engine),
            rate(engine),
            ns(latest) * 10.0,
            rate(latest) / 10.0
        );
    }
}

/// Table bytes of a fjall store (everything under `keyspaces/`: tables, filters, indexes,
/// fjall's metadata keyspace; no journal).
fn keyspaces_bytes(s: &FjallStore) -> u64 {
    let root = s.dir().join("keyspaces");
    s.files().iter().filter(|p| p.starts_with(&root)).map(|p| std::fs::metadata(p).map_or(0, |m| m.len())).sum()
}

/// Space of a load larger than the memtable, while fjall's background compactions settle:
/// `BABEL_MEASURE_RECORDS` chat messages (default 100k) with `BABEL_FJALL_MEMTABLE_MB`
/// (default 4) memtables.
#[test]
#[ignore = "measurement: cargo test --release --features fjall --test wal_fjall -- --ignored --nocapture measure_space_settling"]
fn measure_space_settling() {
    let n = env_u64("BABEL_MEASURE_RECORDS", 100_000);
    let memtable_mb = env_u64("BABEL_FJALL_MEMTABLE_MB", 4);
    let seed = datasets::DEFAULT_SEED;
    let dir = common::temp_dir("babeldb-fjall-settle-");
    let opts = FjallOptions { memtable_bytes: memtable_mb << 20, ..FjallOptions::for_wal() };
    let db = Db::open_fjall_wal_with(dir.path().join("s.fjall"), Config::adaptive(), WalConfig::default(), &opts).expect("open");
    let t0 = Instant::now();
    for start in (0..n).step_by(1000) {
        let msgs: Vec<([u8; 16], Vec<u8>)> = (start..(start + 1000).min(n))
            .map(|i| (message_key(datasets::channel_of(i, seed), datasets::s3_snowflake(seed, i)), datasets::value(Scenario::ChatJson, seed, i, 512)))
            .collect();
        let ops: Vec<BatchOp<'_>> = msgs.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any }).collect();
        db.write_batch(&ops).expect("load");
    }
    let load = t0.elapsed();
    let inner = db.store().inner();
    println!("{n} records, {memtable_mb} MiB memtables: load {:.0} rec/s", n as f64 / load.as_secs_f64());
    let t1 = Instant::now();
    inner.flush_memtables().expect("flush");
    println!("  flush_memtables in {:.2}s", t1.elapsed().as_secs_f64());
    for wait in [0u64, 1, 2, 5, 10, 20] {
        let target = t1 + Duration::from_secs(wait);
        if let Some(d) = target.checked_duration_since(Instant::now()) {
            std::thread::sleep(d);
        }
        if wait == 5 {
            // A read (fresh snapshot) and a flush (version GC) let fjall drop replaced tables.
            drop(db.get(b"settle").expect("get"));
            inner.flush_memtables().expect("flush");
        }
        println!("  +{wait:>2}s: keyspaces/ {:.2} MB, {} files", keyspaces_bytes(inner) as f64 / 1e6, inner.files().len());
    }
}

/// fjall's write backpressure while large values go through the engine: `BABEL_MEASURE_RECORDS`
/// (default 2000) incompressible values of `BABEL_MEASURE_VALUE_KIB` KiB (default 1024) loaded,
/// then overwritten twice, in batches of 16 values; a thread samples
/// `FjallStore::write_pressure` every millisecond. `BABEL_FJALL_KV=off` turns key-value
/// separation off, `BABEL_FJALL_MEMTABLE_MB` sets the memtable size, `BABEL_FJALL_JOURNAL_LZ4`
/// (`1` | `0`) the journal compression.
#[test]
#[ignore = "measurement: cargo test --release --features fjall --test wal_fjall -- --ignored --nocapture measure_write_pressure"]
fn measure_write_pressure() {
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    let n = env_u64("BABEL_MEASURE_RECORDS", 2000);
    let len = env_u64("BABEL_MEASURE_VALUE_KIB", 1024) as usize * 1024;
    let mut opts = FjallOptions::for_wal();
    if std::env::var("BABEL_FJALL_KV").as_deref() == Ok("off") {
        opts.kv_separation = None;
    }
    opts.memtable_bytes = env_u64("BABEL_FJALL_MEMTABLE_MB", 64) << 20;
    if let Ok(v) = std::env::var("BABEL_FJALL_JOURNAL_LZ4") {
        opts.journal_lz4 = v == "1";
    }
    println!("{n} values of {len} B, {opts:?}");
    let dir = common::temp_dir("babeldb-fjall-pressure-");
    let db = Db::open_fjall_wal_with(dir.path().join("p.fjall"), Config::adaptive(), WalConfig::default(), &opts).expect("open");
    let done = AtomicBool::new(false);
    std::thread::scope(|s| {
        let sampler = s.spawn(|| {
            let (mut samples, mut max_sealed, mut max_l0, mut sealed4, mut l0_20) = (0u64, 0, 0, 0u64, 0u64);
            while !done.load(SeqCst) {
                let (sealed, l0) = db.store().inner().write_pressure();
                samples += 1;
                max_sealed = max_sealed.max(sealed);
                max_l0 = max_l0.max(l0);
                sealed4 += u64::from(sealed >= 4);
                l0_20 += u64::from(l0 >= 20);
                std::thread::sleep(Duration::from_millis(1));
            }
            println!(
                "  {samples} samples: max {max_sealed} sealed memtables ({sealed4} samples with 4+), max {max_l0} level-0 runs ({l0_20} samples with 20+)"
            );
        });
        for round in 0..3u64 {
            let t0 = Instant::now();
            for start in (0..n).step_by(16) {
                let values: Vec<([u8; 8], Vec<u8>)> =
                    (start..(start + 16).min(n)).map(|i| (i.to_be_bytes(), Rng::new(i * 3 + round + 1).bytes(len))).collect();
                let ops: Vec<BatchOp<'_>> = values.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any }).collect();
                db.write_batch(&ops).expect("write_batch");
            }
            let secs = t0.elapsed().as_secs_f64();
            println!("  round {round}: {:.0} values/s, {:.0} MB/s", n as f64 / secs, (n as usize * len) as f64 / secs / 1e6);
        }
        done.store(true, SeqCst);
        sampler.join().expect("sampler");
    });
}

#[test]
#[ignore = "measurement: cargo test --release --features fjall --test wal_fjall -- --ignored --nocapture measure_reads"]
fn measure_reads() {
    let n = env_u64("BABEL_MEASURE_RECORDS", 100_000);
    let seed = datasets::DEFAULT_SEED;
    let dir = common::temp_dir("babeldb-fjall-measure-");
    let msgs: Vec<([u8; 16], Vec<u8>)> = (0..n)
        .map(|i| {
            let key = message_key(datasets::channel_of(i, seed), datasets::s3_snowflake(seed, i));
            (key, datasets::value(Scenario::ChatJson, seed, i, 512))
        })
        .collect();
    let keys: Vec<[u8; 16]> = msgs.iter().map(|(k, _)| *k).collect();
    let load = |db: &dyn Fn(&[BatchOp<'_>])| {
        for chunk in msgs.chunks(1000) {
            let ops: Vec<BatchOp<'_>> = chunk.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any }).collect();
            db(&ops);
        }
    };
    if std::env::var_os("BABEL_MEASURE_SKIP_REDB").is_none() {
        let mut db = Db::open_wal(dir.path().join("m.redb"), Config::adaptive()).expect("open redb+wal");
        load(&|ops| {
            db.write_batch(ops).expect("load");
        });
        db.compact().expect("compact");
        measure_one("redb+wal", &db, &keys);
    }
    {
        let env = |k: &str| std::env::var(k).ok();
        let mut opts = FjallOptions::for_wal();
        if let Some(v) = env("BABEL_FJALL_BLOCK") {
            opts.data_block_bytes = v.parse().expect("BABEL_FJALL_BLOCK");
        }
        if let Some(v) = env("BABEL_FJALL_PIN") {
            opts.pin_index_and_filters = v == "1";
        }
        if let Some(v) = env("BABEL_FJALL_HASH") {
            opts.data_block_hash_percent = v.parse().expect("BABEL_FJALL_HASH");
        }
        if let Some(v) = env("BABEL_FJALL_RESTART") {
            opts.data_block_restart_interval = v.parse().expect("BABEL_FJALL_RESTART");
        }
        if env("BABEL_FJALL_COMPRESSION").as_deref() == Some("none") {
            opts.compression = FjallCompression::None;
        }
        println!("{opts:?}");
        let path = dir.path().join("m.fjall");
        let mut db = Db::open_fjall_wal_with(&path, Config::adaptive(), WalConfig::default(), &opts).expect("open fjall+wal");
        load(&|ops| {
            db.write_batch(ops).expect("load");
        });
        if env("BABEL_MEASURE_NO_COMPACT").is_some() {
            db.store().inner().flush_memtables().expect("flush");
        } else {
            db.compact().expect("compact");
        }
        let root = db.store().inner().dir().join("keyspaces");
        let tables: u64 = db.store().files().iter().filter(|p| p.starts_with(&root)).map(|p| std::fs::metadata(p).map_or(0, |m| m.len())).sum();
        println!("fjall+wal    keyspaces/ {:.2} MB", tables as f64 / 1e6);
        measure_one("fjall+wal", &db, &keys);
        println!("fjall+wal    cached snapshots: {}", db.store().inner().cached_snapshots());
    }
}
