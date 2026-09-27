//! `WalStore` (write-ahead log in front of a store) and `Db::open_wal`.
//!
//! - The backend conformance suite on `WalStore<RedbStore>` (a small WAL where most large
//!   transactions become checkpoints, a roomy one where they are logged, and the strict sync
//!   modes: unbuffered write-through on Windows, write + flush) and on `WalStore<MemStore>`.
//! - Crash recovery in process: a store dropped while its thread panics behaves like a killed
//!   process. `WalStore` skips its closing checkpoint and redb skips its durable close (both
//!   test `std::thread::panicking()`), so the redb file is left at its last durable commit (the
//!   last checkpoint) and reopening must replay the WAL. `tests/wal_recovery.rs` kills a real
//!   child process instead.
//! - Torn and corrupt tails, stale records after a torn tail, checkpoints and log restarts,
//!   oversized transactions, idempotent replay, a WAL of another database, a replaced database
//!   file, the hidden meta entries, a resized WAL, a WAL in use, a missing WAL, databases
//!   sharing a WAL directory, the engine on top with a mixed workload, and a group committer.
//!
//! Measurement (ignored, release build, idle machine):
//! `cargo test --release --test wal -- --ignored --nocapture measure_wal_vs_redb`

mod common;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use babeldb::config::{WalConfig, WalSync};
use babeldb::datasets::{self, Scenario};
use babeldb::engine::WalDb;
use babeldb::scale::{GroupCommitConfig, GroupCommitter, WriteDurability, message_key};
use babeldb::store::conformance;
use babeldb::store::wal::{
    self, ChainEnd, WAL_CLEAN_KEY, WAL_DATA_START, WAL_FILE_MAGIC, WAL_ID_KEY, WAL_LSN_KEY,
    WalStore,
};
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use babeldb::{BatchOp, Config, Db, Error, Expect, MemStore, Mode, RedbStore, Result};
use common::{
    Pattern, Rng, State, assert_state_eq, delete_everything_and_check_no_leaks, pattern_bytes,
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

fn db_file(dir: &Path) -> PathBuf {
    dir.join("db.redb")
}

fn open_redb_wal(dir: &Path, cfg: &WalConfig) -> WalStore<RedbStore> {
    try_open_redb_wal(dir, cfg).unwrap_or_else(|e| panic!("open WalStore<RedbStore> in {}: {e}", dir.display()))
}

fn try_open_redb_wal(dir: &Path, cfg: &WalConfig) -> Result<WalStore<RedbStore>> {
    let path = db_file(dir);
    let inner = RedbStore::open(&path, CACHE)?;
    WalStore::open(inner, cfg.wal_path(&path), cfg.clone())
}

fn wal_file(dir: &Path, cfg: &WalConfig) -> PathBuf {
    cfg.wal_path(&db_file(dir))
}

/// Drop `value` while the thread panics: no checkpoint, no durable close of redb. The redb
/// file stays at its last durable commit, like after a kill.
fn crash<T>(value: T) {
    let outcome = panic::catch_unwind(AssertUnwindSafe(move || {
        let _held = value;
        panic!("simulated crash (expected by this test)");
    }));
    assert!(outcome.is_err(), "the simulated crash did not unwind");
}

/// Every entry of every table, (table index, key) -> value, through one read transaction.
type Tables = BTreeMap<(usize, Vec<u8>), Vec<u8>>;

fn dump<S: Store>(s: &S) -> Result<Tables> {
    let r = s.begin_read()?;
    let mut out = Tables::new();
    for t in Table::ALL {
        r.scan(t, Bound::Unbounded, Bound::Unbounded, false, &mut |k, v| {
            out.insert((t.index(), k.to_vec()), v.to_vec());
            Ok(true)
        })?;
    }
    Ok(out)
}

/// `dump` of a store read without the WAL wrapper: the WAL's meta entries removed.
fn dump_without_wal_entries<S: Store>(s: &S) -> Result<Tables> {
    let mut t = dump(s)?;
    for key in [WAL_LSN_KEY, WAL_ID_KEY, WAL_CLEAN_KEY] {
        t.remove(&(Table::Meta.index(), key.as_bytes().to_vec()));
    }
    Ok(t)
}

#[derive(Clone, Debug)]
enum Op {
    Put(Table, Vec<u8>, Vec<u8>),
    Del(Table, Vec<u8>),
}

fn apply_to_model(model: &mut Tables, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Put(t, k, v) => {
                model.insert((t.index(), k.clone()), v.clone());
            }
            Op::Del(t, k) => {
                model.remove(&(t.index(), k.clone()));
            }
        }
    }
}

/// One transaction with `ops`, committed with `d`; the model follows once it returned.
fn commit_ops<S: Store>(s: &S, model: &mut Tables, ops: &[Op], d: Durability) -> Result<()> {
    let mut w = s.begin_write()?;
    for op in ops {
        match op {
            Op::Put(t, k, v) => w.put(*t, k, v)?,
            Op::Del(t, k) => {
                w.remove(*t, k)?;
            }
        }
    }
    w.commit(d)?;
    apply_to_model(model, ops);
    Ok(())
}

/// Deterministic transaction `i`: puts in several tables, overwrites and removes.
fn step_ops(i: u64) -> Vec<Op> {
    let mut rng = Rng::new(i ^ 0x5741_4C00);
    let mut ops = Vec::new();
    let n = 1 + rng.below(4);
    for _ in 0..n {
        let table = [Table::Records, Table::Objects, Table::Refcounts, Table::History][rng.below(4) as usize];
        let key = format!("key-{:03}", rng.below(40)).into_bytes();
        if rng.chance(1, 5) {
            ops.push(Op::Del(table, key));
        } else {
            let len = [0usize, 1, 17, 200, 700, 3000][rng.below(6) as usize];
            ops.push(Op::Put(table, key, rng.bytes(len)));
        }
    }
    ops
}

#[track_caller]
fn assert_tables_eq(got: &Tables, want: &Tables, ctx: &str) {
    if got == want {
        return;
    }
    let mut diff = Vec::new();
    for (k, v) in want {
        match got.get(k) {
            None => diff.push(format!("missing {:?}/{}", Table::ALL[k.0], String::from_utf8_lossy(&k.1))),
            Some(g) if g != v => diff.push(format!("differs {:?}/{}", Table::ALL[k.0], String::from_utf8_lossy(&k.1))),
            _ => {}
        }
    }
    for k in got.keys().filter(|k| !want.contains_key(*k)) {
        diff.push(format!("unexpected {:?}/{}", Table::ALL[k.0], String::from_utf8_lossy(&k.1)));
    }
    diff.truncate(20);
    panic!("{ctx}: {} entries, want {}:\n{}", got.len(), want.len(), diff.join("\n"));
}

/// Flip one byte of the file at `offset`.
fn corrupt_byte(path: &Path, offset: u64) {
    let mut bytes = std::fs::read(path).expect("read WAL");
    bytes[offset as usize] ^= 0x5A;
    std::fs::write(path, bytes).expect("write WAL");
}

fn zero_range(path: &Path, start: u64, len: usize) {
    let mut bytes = std::fs::read(path).expect("read WAL");
    bytes[start as usize..start as usize + len].fill(0);
    std::fs::write(path, bytes).expect("write WAL");
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        if entry.file_type().expect("file type").is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy file");
        }
    }
}

// ---------------------------------------------------------------------------
// Conformance
// ---------------------------------------------------------------------------

fn redb_wal_conformance(cfg: &WalConfig) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut n = 0;
    conformance::run_all(&mut || {
        n += 1;
        let sub = dir.path().join(format!("store-{n}"));
        std::fs::create_dir_all(&sub).expect("create store dir");
        open_redb_wal(&sub, cfg)
    })
}

#[test]
fn wal_redb_conformance_small_wal() -> Result<()> {
    redb_wal_conformance(&small_wal())
}

#[test]
fn wal_redb_conformance_roomy_wal() -> Result<()> {
    redb_wal_conformance(&roomy_wal())
}

#[cfg(windows)]
#[test]
fn wal_redb_conformance_unbuffered() -> Result<()> {
    redb_wal_conformance(&WalConfig {
        sync: WalSync::WriteThroughUnbuffered,
        ..roomy_wal()
    })
}

#[test]
fn wal_redb_conformance_flush() -> Result<()> {
    redb_wal_conformance(&WalConfig {
        sync: WalSync::Flush,
        ..small_wal()
    })
}

#[test]
fn wal_redb_persistent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    conformance::run_persistent(&mut |d| open_redb_wal(d, &small_wal()), dir.path())
}

#[test]
fn wal_redb_concurrent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    conformance::run_concurrent(Arc::new(open_redb_wal(dir.path(), &small_wal())))
}

#[test]
fn wal_mem_conformance() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut n = 0;
    conformance::run_all(&mut || {
        n += 1;
        WalStore::open(MemStore::new(), dir.path().join(format!("mem-{n}.wal")), small_wal())
            .expect("open WalStore<MemStore>")
    })
}

#[test]
fn wal_mem_concurrent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = WalStore::open(MemStore::new(), dir.path().join("mem.wal"), small_wal())?;
    conformance::run_concurrent(Arc::new(store))
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

#[test]
fn replay_after_crash_restores_every_acknowledged_commit() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = WalConfig {
        segment_bytes: 4 << 20,
        ..small_wal()
    };
    let mut model = Tables::new();
    let store = open_redb_wal(dir.path(), &cfg);
    assert!(store.recovery().created, "a new database creates its WAL");
    for i in 0..60 {
        commit_ops(&store, &mut model, &step_ops(i), Durability::Immediate)?;
    }
    let stats = store.stats();
    assert!(stats.logged_commits >= 55, "{stats:?}");
    assert_eq!(stats.checkpoints, 0, "no checkpoint expected with a 4 MiB WAL: {stats:?}");
    assert_eq!(stats.pending_bytes, 0);
    assert_tables_eq(&dump(&store)?, &model, "before the crash");
    crash(store);

    let store = open_redb_wal(dir.path(), &cfg);
    let rec = store.recovery().clone();
    assert_eq!(rec.records_replayed, stats.logged_commits, "{rec:?}");
    assert_eq!(rec.records_scanned, stats.logged_commits, "{rec:?}");
    assert_eq!(rec.chain_end, ChainEnd::NoRecord, "the zero-filled rest ends the chain: {rec:?}");
    assert!(!rec.created);
    assert_tables_eq(&dump(&store)?, &model, "after recovery");
    // a second crash right after recovery: recovery made everything durable
    crash(store);
    let store = open_redb_wal(dir.path(), &cfg);
    assert_eq!(store.recovery().records_replayed, 0, "{:?}", store.recovery());
    assert_tables_eq(&dump(&store)?, &model, "after the second recovery");
    drop(store);

    // after a clean close the redb file alone is complete
    let inner = RedbStore::open(db_file(dir.path()), CACHE)?;
    assert_tables_eq(&dump_without_wal_entries(&inner)?, &model, "redb file after a clean close");
    Ok(())
}

#[test]
fn deferred_commits_become_durable_with_the_next_immediate_one() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = WalConfig {
        segment_bytes: 4 << 20,
        max_pending_bytes: 1 << 20,
        ..small_wal()
    };
    let put = |k: &str, v: &str| vec![Op::Put(Table::Records, k.as_bytes().to_vec(), v.as_bytes().to_vec())];
    let mut durable = Tables::new();

    // Immediate a, Deferred b and c, crash: b and c were only in memory.
    let store = open_redb_wal(dir.path(), &cfg);
    commit_ops(&store, &mut durable, &put("a", "1"), Durability::Immediate)?;
    let mut visible = durable.clone();
    commit_ops(&store, &mut visible, &put("b", "2"), Durability::Deferred)?;
    commit_ops(&store, &mut visible, &put("c", "3"), Durability::Deferred)?;
    assert!(store.stats().pending_bytes > 0);
    assert_tables_eq(&dump(&store)?, &visible, "deferred commits are visible at once");
    crash(store);
    let store = open_redb_wal(dir.path(), &cfg);
    assert_tables_eq(&dump(&store)?, &durable, "deferred commits in memory are lost by a crash");

    // Deferred d and e, then Immediate f: all three survive.
    commit_ops(&store, &mut durable, &put("d", "4"), Durability::Deferred)?;
    commit_ops(&store, &mut durable, &put("e", "5"), Durability::Deferred)?;
    commit_ops(&store, &mut durable, &put("f", "6"), Durability::Immediate)?;
    let writes = store.stats().wal_writes;
    assert_eq!(writes, 1, "d, e and f go out in one write");
    crash(store);
    let store = open_redb_wal(dir.path(), &cfg);
    assert_eq!(store.recovery().records_replayed, 3);
    assert_tables_eq(&dump(&store)?, &durable, "deferred commits before an immediate one");

    // Deferred g, then an empty Immediate commit (what Db::sync does).
    commit_ops(&store, &mut durable, &put("g", "7"), Durability::Deferred)?;
    store.begin_write()?.commit(Durability::Immediate)?;
    assert_eq!(store.stats().pending_bytes, 0);
    crash(store);
    let store = open_redb_wal(dir.path(), &cfg);
    assert_tables_eq(&dump(&store)?, &durable, "a deferred commit before an empty immediate one");

    // Deferred h, explicit checkpoint: durable in redb, nothing left to replay.
    commit_ops(&store, &mut durable, &put("h", "8"), Durability::Deferred)?;
    store.checkpoint()?;
    assert_eq!(store.stats().checkpoints, 1);
    crash(store);
    let store = open_redb_wal(dir.path(), &cfg);
    assert_eq!(store.recovery().records_replayed, 0, "{:?}", store.recovery());
    assert_tables_eq(&dump(&store)?, &durable, "a deferred commit made durable by a checkpoint");
    drop(store);

    // Past max_pending_bytes, deferred records are written early: the survivors are a prefix.
    let cfg = WalConfig {
        max_pending_bytes: 1024,
        ..cfg
    };
    let store = open_redb_wal(dir.path(), &cfg);
    let keys: Vec<String> = (0..200).map(|i| format!("p{i:03}")).collect();
    for k in &keys {
        commit_ops(&store, &mut visible, &put(k, &"v".repeat(40)), Durability::Deferred)?;
    }
    let stats = store.stats();
    assert!(stats.wal_writes >= 5, "{stats:?}");
    crash(store);
    let store = open_redb_wal(dir.path(), &cfg);
    let got = dump(&store)?;
    let present: Vec<bool> = keys
        .iter()
        .map(|k| got.contains_key(&(Table::Records.index(), k.as_bytes().to_vec())))
        .collect();
    let n = present.iter().take_while(|p| **p).count();
    assert!(present[n..].iter().all(|p| !p), "survivors are not a prefix: {present:?}");
    assert!(n >= 150, "only {n} of 200 deferred commits written early survived ({stats:?})");
    Ok(())
}

#[test]
fn torn_or_corrupt_tail_ends_the_chain() -> Result<()> {
    // (description, damage applied to record #7 of 10, expected chain end)
    type Damage = fn(&Path, u64, u32);
    let cases: [(&str, Damage, ChainEnd); 4] = [
        ("flipped payload byte", |p, off, len| corrupt_byte(p, off + 32 + u64::from(len) / 2), ChainEnd::BadChecksum),
        ("unwritten second half (torn write)", |p, off, len| {
            let half = (32 + len as usize) / 2;
            zero_range(p, off + half as u64, 32 + len as usize - half);
        }, ChainEnd::BadChecksum),
        ("unwritten header", |p, off, _| zero_range(p, off, 32), ChainEnd::NoRecord),
        ("length past the end of the file", |p, off, _| {
            let mut bytes = std::fs::read(p).expect("read WAL");
            bytes[off as usize + 4..off as usize + 8].copy_from_slice(&u32::MAX.to_le_bytes());
            std::fs::write(p, bytes).expect("write WAL");
        }, ChainEnd::BadLength),
    ];
    for (what, damage, end) in cases {
        let dir = tempfile::tempdir()?;
        let cfg = small_wal();
        let store = open_redb_wal(dir.path(), &cfg);
        let mut models = vec![Tables::new()];
        let mut model = Tables::new();
        for i in 0..10 {
            commit_ops(&store, &mut model, &step_ops(100 + i), Durability::Immediate)?;
            models.push(model.clone());
        }
        assert_eq!(store.stats().logged_commits, 10, "{what}: every transaction changes something");
        crash(store);
        let path = wal_file(dir.path(), &cfg);
        let chain = wal::inspect(&path)?;
        assert_eq!(chain.records.len(), 10, "{what}: {chain:?}");
        assert_eq!(chain.records[0].offset, WAL_DATA_START);
        let victim = &chain.records[7];
        damage(&path, victim.offset, victim.payload_len);
        assert_eq!(wal::inspect(&path)?.records.len(), 7, "{what}");

        let store = open_redb_wal(dir.path(), &cfg);
        let rec = store.recovery().clone();
        assert_eq!(rec.records_replayed, 7, "{what}: {rec:?}");
        assert_eq!(rec.chain_end, end, "{what}: {rec:?}");
        assert_eq!(rec.chain_end_offset, victim.offset, "{what}: {rec:?}");
        assert_tables_eq(&dump(&store)?, &models[7], what);
        // the store keeps working after the truncated tail, and a later crash loses nothing
        let mut model = models[7].clone();
        for i in 0..5 {
            commit_ops(&store, &mut model, &step_ops(200 + i), Durability::Immediate)?;
        }
        // a transaction whose only ops are removes of missing keys changes nothing: no record
        let logged = store.stats().logged_commits;
        assert!(logged >= 1, "{what}");
        crash(store);
        let store = open_redb_wal(dir.path(), &cfg);
        assert_eq!(store.recovery().records_replayed, logged, "{what}: {:?}", store.recovery());
        assert_tables_eq(&dump(&store)?, &model, &format!("{what}: after new commits"));
    }
    Ok(())
}

/// The first record is torn, the second is intact: if the next session reused the LSN of the
/// torn record, a new first record of the same size would be followed by the stale intact
/// one, which would then continue the chain and be replayed. The LSN jump at open prevents it.
#[test]
fn stale_records_after_a_torn_tail_are_never_replayed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = small_wal();
    let one = |k: &str, v: u8| vec![Op::Put(Table::Records, k.as_bytes().to_vec(), vec![v; 32])];
    let store = open_redb_wal(dir.path(), &cfg);
    let mut ignored = Tables::new();
    for (k, v) in [("k1", 1), ("k2", 2), ("k3", 3)] {
        commit_ops(&store, &mut ignored, &one(k, v), Durability::Immediate)?;
    }
    crash(store);
    let path = wal_file(dir.path(), &cfg);
    let before = wal::inspect(&path)?;
    assert_eq!(before.records.len(), 3);
    let sizes: Vec<u32> = before.records.iter().map(|r| r.payload_len).collect();
    assert!(sizes.windows(2).all(|w| w[0] == w[1]), "records must have one size: {sizes:?}");
    corrupt_byte(&path, before.records[0].offset + 40);

    let store = open_redb_wal(dir.path(), &cfg);
    assert_eq!(store.recovery().records_replayed, 0);
    assert_eq!(store.recovery().chain_end, ChainEnd::BadChecksum);
    let next = store.recovery().next_lsn;
    assert!(next > before.records[2].lsn, "next LSN {next} must jump past the stale records");
    let mut model = Tables::new();
    commit_ops(&store, &mut model, &one("k9", 9), Durability::Immediate)?;
    crash(store);

    let after = wal::inspect(&path)?;
    assert_eq!(after.records.len(), 1, "{after:?}");
    assert_eq!(after.records[0].offset, WAL_DATA_START);
    assert_eq!(after.records[0].payload_len, sizes[0]);
    assert_eq!(after.end, ChainEnd::LsnGap, "the stale k2 record is valid but not the successor");
    let store = open_redb_wal(dir.path(), &cfg);
    assert_eq!(store.recovery().records_replayed, 1);
    assert_tables_eq(&dump(&store)?, &model, "only the new commit");
    Ok(())
}

#[test]
fn checkpoints_restart_the_log_and_recovery_spans_them() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = WalConfig {
        segment_bytes: 64 << 10,
        max_pending_bytes: 8 << 10,
        max_record_bytes: 16 << 10,
        ..WalConfig::default()
    };
    let store = open_redb_wal(dir.path(), &cfg);
    let mut model = Tables::new();
    let mut deferred_tail = Tables::new();
    for i in 0..1500u64 {
        let d = if i % 3 == 0 { Durability::Deferred } else { Durability::Immediate };
        commit_ops(&store, &mut deferred_tail, &step_ops(1000 + i), d)?;
        if d == Durability::Immediate {
            model = deferred_tail.clone();
        }
    }
    // end on an immediate commit so the model is the acknowledged state
    commit_ops(&store, &mut model, &step_ops(9999), Durability::Immediate)?;
    let stats = store.stats();
    assert!(stats.checkpoints >= 5, "{stats:?}");
    assert!(stats.wal_writes >= 500, "{stats:?}");
    crash(store);

    let path = wal_file(dir.path(), &cfg);
    let chain = wal::inspect(&path)?;
    assert!(!chain.records.is_empty());
    assert_eq!(chain.records[0].offset, WAL_DATA_START);
    assert!(chain.records.windows(2).all(|w| w[1].lsn == w[0].lsn + 1 && w[1].offset > w[0].offset));
    assert!(chain.end_offset <= chain.capacity);

    let store = open_redb_wal(dir.path(), &cfg);
    let rec = store.recovery().clone();
    let newer = chain.records.iter().filter(|r| r.lsn > rec.durable_lsn).count() as u64;
    assert_eq!(rec.records_replayed, newer, "{rec:?}");
    assert_tables_eq(&dump(&store)?, &model, "after a crash with many checkpoints");

    // crash right after an explicit checkpoint: the old records are all below wal_lsn
    commit_ops(&store, &mut model, &step_ops(12345), Durability::Immediate)?;
    store.checkpoint()?;
    crash(store);
    let store = open_redb_wal(dir.path(), &cfg);
    assert_eq!(store.recovery().records_replayed, 0, "{:?}", store.recovery());
    assert!(store.recovery().records_scanned > 0, "{:?}", store.recovery());
    assert_tables_eq(&dump(&store)?, &model, "after a crash right after a checkpoint");
    Ok(())
}

#[test]
fn oversized_transactions_are_committed_as_checkpoints() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = WalConfig {
        segment_bytes: 256 << 10,
        max_record_bytes: 4 << 10,
        ..WalConfig::default()
    };
    let store = open_redb_wal(dir.path(), &cfg);
    let mut model = Tables::new();
    let small = |k: &[u8]| vec![Op::Put(Table::Records, k.to_vec(), vec![1; 100])];
    commit_ops(&store, &mut model, &small(b"first"), Durability::Immediate)?;
    let big: Vec<Op> = (0..10u8)
        .map(|i| Op::Put(Table::Objects, vec![b'o', i], vec![i; 2000]))
        .collect();
    commit_ops(&store, &mut model, &big, Durability::Deferred)?;
    let stats = store.stats();
    assert_eq!(stats.unlogged_commits, 1, "{stats:?}");
    assert_eq!(stats.checkpoints, 1, "{stats:?}");
    // one 64 KiB value, far above max_record_bytes
    commit_ops(&store, &mut model, &[Op::Put(Table::Records, b"huge".to_vec(), vec![7; 64 << 10])], Durability::Immediate)?;
    assert_eq!(store.stats().unlogged_commits, 2);
    commit_ops(&store, &mut model, &small(b"last"), Durability::Immediate)?;
    crash(store);
    let store = open_redb_wal(dir.path(), &cfg);
    assert_eq!(store.recovery().records_replayed, 1, "{:?}", store.recovery());
    assert_tables_eq(&dump(&store)?, &model, "oversized transactions after a crash");
    Ok(())
}

#[test]
fn replay_is_idempotent() -> Result<()> {
    let root = tempfile::tempdir()?;
    let base = root.path().join("base");
    std::fs::create_dir_all(&base)?;
    let cfg = WalConfig {
        segment_bytes: 4 << 20,
        ..small_wal()
    };
    let store = open_redb_wal(&base, &cfg);
    let mut model = Tables::new();
    for i in 0..40 {
        commit_ops(&store, &mut model, &step_ops(500 + i), Durability::Immediate)?;
    }
    let logged = store.stats().logged_commits;
    assert!(logged >= 30, "{logged}");
    crash(store);
    let wal_path = wal_file(&base, &cfg);

    // Applying the whole log once or twice to an empty store gives the same state.
    let mem = MemStore::new();
    let mut w = mem.begin_write()?;
    assert_eq!(wal::replay_into(&wal_path, &mut w, 0)?, logged);
    w.commit(Durability::Immediate)?;
    let once = dump(&mem)?;
    assert_tables_eq(&once, &model, "log replayed once into an empty store");
    let mut w = mem.begin_write()?;
    wal::replay_into(&wal_path, &mut w, 0)?;
    w.commit(Durability::Immediate)?;
    assert_tables_eq(&dump(&mem)?, &model, "log replayed twice");
    // Replaying from the middle over a state that already holds everything changes nothing.
    let mut w = mem.begin_write()?;
    let middle = wal::inspect(&wal_path)?.records[20].lsn;
    assert_eq!(wal::replay_into(&wal_path, &mut w, middle)?, logged - 21);
    w.commit(Durability::Immediate)?;
    assert_tables_eq(&dump(&mem)?, &model, "log tail replayed over the final state");

    // Two copies of the crashed database: recovery once, or recovery + crash + recovery.
    let a = root.path().join("a");
    let b = root.path().join("b");
    copy_dir(&base, &a);
    copy_dir(&base, &b);
    let sa = {
        let s = open_redb_wal(&a, &cfg);
        assert_eq!(s.recovery().records_replayed, logged);
        dump(&s)?
    };
    let s = open_redb_wal(&b, &cfg);
    assert_eq!(s.recovery().records_replayed, logged);
    crash(s);
    let s = open_redb_wal(&b, &cfg);
    assert_eq!(s.recovery().records_replayed, 0);
    assert_tables_eq(&dump(&s)?, &sa, "recovered twice");
    assert_tables_eq(&sa, &model, "recovered once");
    Ok(())
}

#[test]
fn wal_meta_entries_are_hidden_and_reserved() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = small_wal();
    let store = open_redb_wal(dir.path(), &cfg);
    let mut w = store.begin_write()?;
    assert!(matches!(w.put(Table::Meta, WAL_LSN_KEY.as_bytes(), b"x"), Err(Error::InvalidArgument(_))));
    assert!(matches!(w.put(Table::Meta, WAL_ID_KEY.as_bytes(), b"x"), Err(Error::InvalidArgument(_))));
    assert!(matches!(w.put(Table::Meta, WAL_CLEAN_KEY.as_bytes(), b"x"), Err(Error::InvalidArgument(_))));
    assert!(matches!(w.remove(Table::Meta, WAL_LSN_KEY.as_bytes()), Err(Error::InvalidArgument(_))));
    assert_eq!(w.get(Table::Meta, WAL_LSN_KEY.as_bytes())?, None);
    w.put(Table::Meta, b"m", b"1")?;
    w.put(Table::Meta, b"z", b"2")?;
    assert_eq!(w.len(Table::Meta)?, 2);
    w.commit(Durability::Immediate)?;
    let r = store.begin_read()?;
    assert_eq!(r.get(Table::Meta, WAL_LSN_KEY.as_bytes())?, None);
    assert_eq!(r.get(Table::Meta, WAL_ID_KEY.as_bytes())?, None);
    assert_eq!(r.len(Table::Meta)?, 2);
    let mut seen = Vec::new();
    r.scan(Table::Meta, Bound::Unbounded, Bound::Unbounded, true, &mut |k, _| {
        seen.push(k.to_vec());
        Ok(true)
    })?;
    assert_eq!(seen, vec![b"z".to_vec(), b"m".to_vec()]);
    drop(r);
    let lsn = store.stats().next_lsn - 1;
    drop(store);

    let inner = RedbStore::open(db_file(dir.path()), CACHE)?;
    let r = inner.begin_read()?;
    let raw = r.get(Table::Meta, WAL_LSN_KEY.as_bytes())?.expect("wal_lsn in the redb file");
    assert_eq!(u64::from_le_bytes(raw.try_into().expect("u64")), lsn);
    let id = r.get(Table::Meta, WAL_ID_KEY.as_bytes())?.expect("wal_id in the redb file");
    assert_eq!(id.len(), 16);
    assert_eq!(r.get(Table::Meta, WAL_CLEAN_KEY.as_bytes())?, Some(vec![1]), "clean close recorded");
    drop(r);
    drop(inner);
    let chain = wal::inspect(&wal_file(dir.path(), &cfg))?;
    assert_eq!(chain.id.to_vec(), id);
    Ok(())
}

#[test]
fn wal_file_layout() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = small_wal();
    let store = open_redb_wal(dir.path(), &cfg);
    let mut model = Tables::new();
    for i in 0..5 {
        commit_ops(&store, &mut model, &step_ops(i), Durability::Immediate)?;
    }
    let first = store.recovery().next_lsn;
    crash(store);
    let path = wal_file(dir.path(), &cfg);
    assert_eq!(path, dir.path().join("db.redb.wal"));
    let bytes = std::fs::read(&path)?;
    assert_eq!(bytes.len() as u64, cfg.segment_capacity(), "preallocated to its full size");
    assert_eq!(&bytes[0..8], &WAL_FILE_MAGIC);
    assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().expect("u32")), 1, "version");
    assert_eq!(u32::from_le_bytes(bytes[12..16].try_into().expect("u32")), 4096, "data start");
    assert!(bytes[72..4096].iter().all(|b| *b == 0));
    let chain = wal::inspect(&path)?;
    assert_eq!(chain.records.len(), 5);
    let mut offset = WAL_DATA_START;
    for (i, r) in chain.records.iter().enumerate() {
        assert_eq!(r.offset, offset);
        assert_eq!(r.lsn, first + i as u64);
        let at = r.offset as usize;
        assert_eq!(&bytes[at..at + 4], b"BWR1");
        assert_eq!(u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("u32")), r.payload_len);
        assert_eq!(u64::from_le_bytes(bytes[at + 8..at + 16].try_into().expect("u64")), r.lsn);
        offset += 32 + u64::from(r.payload_len);
    }
    assert_eq!(chain.end_offset, offset);
    assert!(bytes[offset as usize..].iter().all(|b| *b == 0), "nothing written past the chain");
    Ok(())
}

#[test]
fn a_wal_of_another_database_is_refused() -> Result<()> {
    let root = tempfile::tempdir()?;
    let a = root.path().join("a");
    let b = root.path().join("b");
    std::fs::create_dir_all(&a)?;
    std::fs::create_dir_all(&b)?;
    let cfg = small_wal();
    let store = open_redb_wal(&a, &cfg);
    let mut model = Tables::new();
    commit_ops(&store, &mut model, &step_ops(1), Durability::Immediate)?;
    crash(store);
    drop(open_redb_wal(&b, &cfg));
    std::fs::copy(wal_file(&a, &cfg), wal_file(&b, &cfg))?;
    match try_open_redb_wal(&b, &cfg) {
        Err(Error::Integrity { detail, .. }) => assert!(detail.contains("another database"), "{detail}"),
        Err(e) => panic!("unexpected error {e}"),
        Ok(_) => panic!("a foreign WAL with records must be refused"),
    }
    // A foreign WAL without records is replaced by a fresh one (new salt and id).
    let c = root.path().join("c");
    std::fs::create_dir_all(&c)?;
    drop(open_redb_wal(&c, &cfg));
    std::fs::copy(wal_file(&c, &cfg), wal_file(&b, &cfg))?;
    let foreign = wal::inspect(&wal_file(&b, &cfg))?.id;
    let store = open_redb_wal(&b, &cfg);
    assert_eq!(store.recovery().records_replayed, 0);
    assert!(store.recovery().created, "a foreign WAL is replaced, never adopted");
    drop(store);
    assert_ne!(wal::inspect(&wal_file(&b, &cfg))?.id, foreign);
    Ok(())
}

#[test]
fn a_missing_wal_is_an_error_after_a_crash_only() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = small_wal();
    let store = open_redb_wal(dir.path(), &cfg);
    let mut lost = Tables::new();
    commit_ops(&store, &mut lost, &[Op::Put(Table::Records, b"only-in-wal".to_vec(), b"x".to_vec())], Durability::Immediate)?;
    crash(store);
    std::fs::remove_file(wal_file(dir.path(), &cfg))?;
    match try_open_redb_wal(dir.path(), &cfg) {
        Err(Error::Integrity { detail, .. }) => assert!(detail.contains("missing"), "{detail}"),
        Err(e) => panic!("unexpected error {e}"),
        Ok(_) => panic!("a WAL lost after a crash must not be recreated silently"),
    }
    // Accepted explicitly: the state of the last checkpoint (the empty database).
    let lossy = WalConfig { recreate_missing: true, ..cfg.clone() };
    let store = open_redb_wal(dir.path(), &lossy);
    assert!(store.recovery().created);
    assert_tables_eq(&dump(&store)?, &Tables::new(), "without its WAL: the last checkpoint");
    let mut model = Tables::new();
    commit_ops(&store, &mut model, &[Op::Put(Table::Records, b"kept".to_vec(), b"y".to_vec())], Durability::Immediate)?;
    drop(store);
    // After a clean close the database file alone is complete: a missing WAL is no loss.
    std::fs::remove_file(wal_file(dir.path(), &cfg))?;
    let store = open_redb_wal(dir.path(), &cfg);
    assert!(store.recovery().created);
    assert_tables_eq(&dump(&store)?, &model, "after a clean close");
    Ok(())
}

#[test]
fn a_wal_in_use_cannot_be_opened_twice() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("shared.wal");
    let first = WalStore::open(MemStore::new(), path.clone(), small_wal())?;
    match WalStore::open(MemStore::new(), path.clone(), small_wal()) {
        Err(e) => assert!(e.to_string().contains("in use"), "{e}"),
        Ok(_) => panic!("a second store on an open WAL must be refused"),
    }
    drop(first);
    Ok(())
}

#[test]
fn databases_sharing_a_wal_directory_get_distinct_wals() -> Result<()> {
    let root = tempfile::tempdir()?;
    let wal_dir = root.path().join("wals");
    std::fs::create_dir_all(&wal_dir)?;
    let cfg = WalConfig { dir: Some(wal_dir.clone()), ..small_wal() };
    let (a, b) = (root.path().join("a"), root.path().join("b"));
    std::fs::create_dir_all(&a)?;
    std::fs::create_dir_all(&b)?;
    assert_ne!(wal_file(&a, &cfg), wal_file(&b, &cfg), "same file name, different databases");
    assert_eq!(wal_file(&a, &cfg).parent(), Some(wal_dir.as_path()));
    let (sa, sb) = (open_redb_wal(&a, &cfg), open_redb_wal(&b, &cfg));
    let (mut ma, mut mb) = (Tables::new(), Tables::new());
    for i in 0..5 {
        commit_ops(&sa, &mut ma, &step_ops(600 + i), Durability::Immediate)?;
        commit_ops(&sb, &mut mb, &step_ops(700 + i), Durability::Immediate)?;
    }
    crash(sa);
    crash(sb);
    let (sa, sb) = (open_redb_wal(&a, &cfg), open_redb_wal(&b, &cfg));
    assert_tables_eq(&dump(&sa)?, &ma, "database a");
    assert_tables_eq(&dump(&sb)?, &mb, "database b");
    Ok(())
}

#[test]
fn a_replaced_database_file_is_refused() -> Result<()> {
    let root = tempfile::tempdir()?;
    let live = root.path().join("live");
    let old = root.path().join("old");
    std::fs::create_dir_all(&live)?;
    let cfg = small_wal();
    let mut model = Tables::new();
    let store = open_redb_wal(&live, &cfg);
    commit_ops(&store, &mut model, &step_ops(1), Durability::Immediate)?;
    drop(store);
    copy_dir(&live, &old);
    // the live database moves on: a new session (LSN jump), commits, crash
    let store = open_redb_wal(&live, &cfg);
    commit_ops(&store, &mut model, &step_ops(2), Durability::Immediate)?;
    crash(store);
    // put the old redb file back next to the live WAL
    std::fs::copy(db_file(&old), db_file(&live))?;
    match try_open_redb_wal(&live, &cfg) {
        Err(Error::Integrity { detail, .. }) => assert!(detail.contains("records are missing"), "{detail}"),
        Err(e) => panic!("unexpected error {e}"),
        Ok(_) => panic!("a WAL that does not continue the database must be refused"),
    }
    Ok(())
}

#[test]
fn a_wal_of_another_size_is_replaced_after_recovery() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let small = small_wal();
    let store = open_redb_wal(dir.path(), &small);
    let mut model = Tables::new();
    for i in 0..10 {
        commit_ops(&store, &mut model, &step_ops(300 + i), Durability::Immediate)?;
    }
    crash(store);
    let bigger = WalConfig {
        segment_bytes: 512 << 10,
        ..small
    };
    let store = open_redb_wal(dir.path(), &bigger);
    assert_eq!(store.recovery().records_replayed, 10);
    assert!(store.recovery().created);
    assert_eq!(std::fs::metadata(wal_file(dir.path(), &bigger))?.len(), 512 << 10);
    commit_ops(&store, &mut model, &step_ops(400), Durability::Immediate)?;
    crash(store);
    let store = open_redb_wal(dir.path(), &bigger);
    assert_eq!(store.recovery().records_replayed, 1);
    assert_tables_eq(&dump(&store)?, &model, "after the resize");
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

fn open_engine(path: &Path, cfg: &Config, wal: &WalConfig) -> WalDb {
    Db::open_wal_with(path, cfg.clone(), wal.clone()).unwrap_or_else(|e| panic!("open_wal {}: {e}", path.display()))
}

fn value_for(rng: &mut Rng) -> Vec<u8> {
    let len = [0usize, 1, 40, 64, 65, 300, 511, 512, 513, 1500, 4000][rng.below(11) as usize];
    let pattern = Pattern::ALL[rng.below(Pattern::ALL.len() as u64) as usize];
    pattern_bytes(pattern, len, rng.next_u64())
}

#[test]
fn engine_on_wal_survives_crashes_with_a_mixed_workload() {
    let dir = common::temp_dir("babeldb-wal-engine-");
    let path = dir.path().join("engine.redb");
    let cfg = small_config(Mode::Adaptive);
    let wal = engine_wal();
    let mut rng = Rng::new(0x000E_17A1);
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
        // durable point, only a prefix (those written early with max_pending_bytes).
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
        assert_eq!(gc, babeldb::maintenance::GcReport::default(), "{ctx}: a crash must leave nothing to collect");
        durable = got;
        drop(db);
    }
    assert!(checkpoints > 0, "the workload never filled the WAL");
    let mut db = open_engine(&path, &cfg, &wal);
    delete_everything_and_check_no_leaks(&mut db, "final drain");
}

#[test]
fn group_commit_on_a_wal_database() -> Result<()> {
    let dir = common::temp_dir("babeldb-wal-group-");
    let path = dir.path().join("group.redb");
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
    // one WAL write per batch (the engine opened the database with one more)
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
// Measurement (ignored)
// ---------------------------------------------------------------------------

struct Msg {
    key: [u8; 16],
    payload: Vec<u8>,
}

fn msg(seed: u64, i: u64, value_size: usize) -> Msg {
    Msg {
        key: message_key(datasets::channel_of(i, seed), datasets::s3_snowflake(seed, i)),
        payload: datasets::value(Scenario::ChatJson, seed, i, value_size),
    }
}

/// `threads` threads started together, `per_thread` timed calls each (the method of
/// benches/compare.rs): wall time from the start barrier, and every latency.
fn run_phase(threads: usize, per_thread: usize, op: &(dyn Fn(usize, usize) + Sync)) -> (Duration, Vec<u64>) {
    let barrier = Barrier::new(threads + 1);
    let lat: Mutex<Vec<u64>> = Mutex::new(Vec::with_capacity(threads * per_thread));
    let started = std::thread::scope(|s| {
        for t in 0..threads {
            let (barrier, lat) = (&barrier, &lat);
            s.spawn(move || {
                let mut mine = Vec::with_capacity(per_thread);
                barrier.wait();
                for k in 0..per_thread {
                    let t0 = Instant::now();
                    op(t, k);
                    mine.push(t0.elapsed().as_nanos() as u64);
                }
                lat.lock().expect("lat").extend(mine);
            });
        }
        barrier.wait();
        Instant::now()
    });
    (started.elapsed(), lat.into_inner().expect("lat"))
}

type PutFn = Box<dyn Fn(&[u8], &[u8]) + Sync>;
type StatsFn = Box<dyn Fn() -> String + Sync>;

/// One database under measurement, type-erased so different stores run side by side.
struct Bench {
    name: String,
    put: PutFn,
    committer: GroupCommitter,
    stats: StatsFn,
}

fn bench<S: Store>(
    name: String,
    db: Db<S>,
    records: u64,
    value_size: usize,
    seed: u64,
    describe: fn(&Db<S>) -> String,
) -> Bench {
    let db = Arc::new(db);
    let t = Instant::now();
    let mut i = 0;
    while i < records {
        let end = (i + 1000).min(records);
        let batch: Vec<Msg> = (i..end).map(|j| msg(seed, j, value_size)).collect();
        let ops: Vec<BatchOp<'_>> = batch
            .iter()
            .map(|m| BatchOp::Put { key: &m.key, value: &m.payload, expect: Expect::Any })
            .collect();
        db.write_batch(&ops).expect("load");
        i = end;
    }
    println!(
        "[{name}] loaded {records} records in {:.2}s (backend {})",
        t.elapsed().as_secs_f64(),
        db.store().backend_name()
    );
    let committer = GroupCommitter::new(db.clone(), GroupCommitConfig::from(WriteDurability::Immediate)).expect("committer");
    let for_put = db.clone();
    let for_stats = db;
    Bench {
        name,
        put: Box::new(move |k, v| {
            for_put.put(k, v, Expect::Any).expect("put");
        }),
        committer,
        stats: Box::new(move || match for_stats.stats() {
            Ok(s) => format!(
                "{}, files {:.1} MB {}",
                s.backend,
                s.file_apparent_bytes() as f64 / 1e6,
                describe(&for_stats)
            ),
            Err(e) => format!("stats failed: {e}"),
        }),
    }
}

fn no_wal(_: &Db<RedbStore>) -> String {
    String::new()
}

fn wal_internals(db: &WalDb) -> String {
    let s = db.store().stats();
    let per = |ns: u64, n: u64| if n == 0 { 0.0 } else { ns as f64 / n as f64 / 1000.0 };
    format!(
        "| WAL {:?}: {} logged commits, {} writes ({:.1} MB), avg write {:.0}us, avg inner commit {:.0}us, {} checkpoints (avg {:.2}ms)",
        db.store().config().sync,
        s.logged_commits,
        s.wal_writes,
        s.wal_bytes as f64 / 1e6,
        per(s.write_nanos, s.wal_writes),
        per(s.inner_commit_nanos, s.logged_commits),
        s.checkpoints,
        per(s.checkpoint_nanos, s.checkpoints) / 1000.0
    )
}

fn pct(sorted: &[u64], q: f64) -> f64 {
    let i = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[i] as f64 / 1000.0
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn fmt_us(us: f64) -> String {
    if us >= 1000.0 {
        format!("{:.2}ms", us / 1000.0)
    } else {
        format!("{us:.0}us")
    }
}

/// `BABEL_WAL_BENCH_SYNC`: write-through (default), unbuffered or flush.
fn sync_from_env() -> WalSync {
    match std::env::var("BABEL_WAL_BENCH_SYNC").as_deref() {
        Ok("unbuffered") => WalSync::WriteThroughUnbuffered,
        Ok("flush") => WalSync::Flush,
        _ => WalSync::WriteThrough,
    }
}

#[test]
#[ignore = "measurement: cargo test --release --test wal -- --ignored --nocapture measure_wal_vs_redb"]
fn measure_wal_vs_redb() {
    let records = common::env_u64("BABEL_WAL_BENCH_RECORDS", 100_000);
    let reps = common::env_u64("BABEL_WAL_BENCH_REPS", 3) as usize;
    let only = std::env::var("BABEL_WAL_BENCH_SYSTEMS").unwrap_or_default();
    let value_size = 512;
    let seed = datasets::DEFAULT_SEED;
    let root = match std::env::var_os("BABEL_WAL_BENCH_DIR") {
        Some(d) => tempfile::Builder::new().prefix("wal-bench-").tempdir_in(d),
        None => tempfile::Builder::new().prefix("wal-bench-").tempdir(),
    }
    .expect("bench dir");
    let build = if cfg!(debug_assertions) { "DEBUG build (numbers meaningless)" } else { "release build" };
    println!(
        "WAL measurement ({build}): {records} records x {value_size} B loaded per system, {reps} interleaved repetitions, files in {}",
        root.path().display()
    );
    let wanted = |name: &str| only.is_empty() || only.split(',').any(|s| s == name);
    // "": redb alone (Immediate = FlushFileBuffers per commit); the others: WalConfig::default()
    // with the given sync mode.
    let modes: [(&str, Option<WalSync>); 4] = [
        ("", None),
        ("+wal", Some(WalSync::WriteThrough)),
        ("+wal-unbuf", Some(WalSync::WriteThroughUnbuffered)),
        ("+wal-flush", Some(WalSync::Flush)),
    ];
    let mut systems: Vec<Bench> = Vec::new();
    for (base, cfg) in [("babel-raw", Config::raw_only()), ("babel-adaptive", Config::adaptive())] {
        for (suffix, sync) in modes {
            let name = format!("{base}{suffix}");
            if !wanted(&name) || (!cfg!(windows) && sync == Some(WalSync::WriteThroughUnbuffered)) {
                continue;
            }
            let path = root.path().join(format!("{name}.redb"));
            systems.push(match sync {
                None => bench(name, Db::open(&path, cfg.clone()).expect("open"), records, value_size, seed, no_wal),
                Some(sync) => {
                    let wal = WalConfig { sync, ..WalConfig::default() };
                    let db = Db::open_wal_with(&path, cfg.clone(), wal).expect("open_wal");
                    bench(name, db, records, value_size, seed, wal_internals)
                }
            });
        }
    }
    // (label, threads, ops per thread, through the group committer)
    let phases: [(&str, usize, usize, bool); 6] = [
        ("db.put", 1, 300, false),
        ("put", 1, 300, true),
        ("put-mt", 1, 200, true),
        ("put-mt", 4, 200, true),
        ("put-mt", 16, 200, true),
        ("put-mt", 64, 200, true),
    ];
    // results[system][phase] = (ops/s of each repetition, every latency)
    let mut results: Vec<Vec<(Vec<f64>, Vec<u64>)>> = systems
        .iter()
        .map(|_| phases.iter().map(|_| (Vec::new(), Vec::new())).collect())
        .collect();
    let mut counter = 0u64;
    for rep in 0..reps {
        for (pi, &(label, threads, per, grouped)) in phases.iter().enumerate() {
            // rotate the order of the systems so none always runs first
            for j in 0..systems.len() {
                let si = (j + rep + pi) % systems.len();
                let sys = &systems[si];
                counter += 1;
                let base = records + 10_000_000 * counter;
                let inputs: Vec<Vec<Msg>> = (0..threads)
                    .map(|t| (0..per).map(|k| msg(seed, base + (t * 100_000 + k) as u64, value_size)).collect())
                    .collect();
                let op = |t: usize, k: usize| {
                    let m = &inputs[t][k];
                    if grouped {
                        sys.committer.put(m.key.to_vec(), m.payload.clone(), Expect::Any).expect("group put");
                    } else {
                        (sys.put)(&m.key, &m.payload);
                    }
                };
                let (elapsed, mut lat) = run_phase(threads, per, &op);
                lat.sort_unstable();
                let ops = lat.len() as f64 / elapsed.as_secs_f64();
                println!(
                    "rep {rep} [{}] {label:<6} x{threads:<2} {ops:>8.0} ops/s  p50 {:>7} p99 {:>7}",
                    sys.name,
                    fmt_us(pct(&lat, 0.5)),
                    fmt_us(pct(&lat, 0.99))
                );
                results[si][pi].0.push(ops);
                results[si][pi].1.extend(lat);
            }
        }
    }
    println!("\n== summary: median ops/s over {reps} repetitions; p50 / p99 over every latency of the phase");
    for (pi, &(label, threads, _, _)) in phases.iter().enumerate() {
        println!("{label:<6} x{threads:<2}");
        for (si, sys) in systems.iter().enumerate() {
            let (ops, lat) = &mut results[si][pi];
            lat.sort_unstable();
            println!(
                "    {:<22} {:>7.0}/s  p50 {:>7}  p99 {:>7}",
                sys.name,
                median(ops.clone()),
                fmt_us(pct(lat, 0.5)),
                fmt_us(pct(lat, 0.99))
            );
        }
    }
    for sys in &systems {
        let s = sys.committer.stats();
        println!(
            "[{}] committer: {} batches, avg {:.1} ops/batch; {}",
            sys.name,
            s.batches,
            s.avg_batch_ops(),
            (sys.stats)()
        );
    }
    for sys in systems {
        sys.committer.shutdown().expect("shutdown");
    }
}

/// What a checkpoint costs: one `Immediate` redb commit after N `Deferred` ones, with and
/// without flushing the file (through a second handle, outside any transaction) first.
#[test]
#[ignore = "measurement: cargo test --release --test wal -- --ignored --nocapture measure_checkpoint_cost"]
fn measure_checkpoint_cost() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("ckpt.redb");
    let store = RedbStore::open(&path, 64 << 20)?;
    let value = vec![0x5Au8; 600];
    let mut next = 0u64;
    let mut put_batch = |store: &RedbStore, n: usize, d: Durability| -> Result<()> {
        let mut w = store.begin_write()?;
        for _ in 0..n {
            next += 1;
            let key = next.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes();
            w.put(Table::Records, &key, &value)?;
        }
        w.commit(d)
    };
    for _ in 0..100 {
        put_batch(&store, 1000, Durability::Immediate)?;
    }
    println!("loaded 100k x 600 B; file {:.1} MB", std::fs::metadata(&path)?.len() as f64 / 1e6);
    for n in [100usize, 1000, 5000, 5000] {
        for preflush in [false, true] {
            let t = Instant::now();
            for _ in 0..n {
                put_batch(&store, 12, Durability::Deferred)?;
            }
            let deferred = t.elapsed();
            let t = Instant::now();
            if preflush {
                std::fs::OpenOptions::new().write(true).open(&path)?.sync_data()?;
            }
            let flush = t.elapsed();
            let t = Instant::now();
            store.begin_write()?.commit(Durability::Immediate)?;
            let commit = t.elapsed();
            println!(
                "{n:>5} deferred commits of 12 puts ({:.1} us each), pre-flush {preflush}: flush {:?}, then Immediate commit {:?}",
                deferred.as_secs_f64() * 1e6 / n as f64,
                flush,
                commit
            );
        }
    }
    Ok(())
}

/// Sustained group-committed writes (64 threads for a fixed time, so the WAL fills and
/// checkpoints happen) with several WAL sizes: throughput, tail latency, checkpoint cost.
/// `BABEL_WAL_BENCH_SEGMENTS_MIB` (default 16,64,256), `BABEL_WAL_BENCH_SYNC`,
/// `BABEL_WAL_BENCH_SECS` (default 8).
#[test]
#[ignore = "measurement: cargo test --release --test wal -- --ignored --nocapture measure_sustained_checkpoints"]
fn measure_sustained_checkpoints() {
    let secs = common::env_u64("BABEL_WAL_BENCH_SECS", 8);
    let threads = 64usize;
    let value_size = 512;
    let seed = datasets::DEFAULT_SEED;
    let sync = sync_from_env();
    let root = tempfile::Builder::new().prefix("wal-sustained-").tempdir().expect("dir");
    println!("sustained: {threads} writers x {secs}s through a GroupCommitter, 512 B chat values, raw_only config, {sync:?}");
    let segments: Vec<u64> = std::env::var("BABEL_WAL_BENCH_SEGMENTS_MIB")
        .unwrap_or_else(|_| "16,64,256".into())
        .split(',')
        .map(|x| x.trim().parse().expect("BABEL_WAL_BENCH_SEGMENTS_MIB: comma-separated MiB"))
        .collect();
    for segment_mib in segments {
        let wal = WalConfig { segment_bytes: segment_mib << 20, sync, ..WalConfig::default() };
        let db = Arc::new(Db::open_wal_with(root.path().join(format!("s{segment_mib}.redb")), Config::raw_only(), wal).expect("open"));
        let committer = GroupCommitter::new(db.clone(), GroupCommitConfig::from(WriteDurability::Immediate)).expect("committer");
        let stop = std::sync::atomic::AtomicBool::new(false);
        let lat: Mutex<Vec<u64>> = Mutex::new(Vec::new());
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for t in 0..threads {
                let (committer, stop, lat) = (&committer, &stop, &lat);
                s.spawn(move || {
                    let mut mine = Vec::new();
                    let mut k = 0u64;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let m = msg(seed, (t as u64) << 40 | k, value_size);
                        let t1 = Instant::now();
                        committer.put(m.key.to_vec(), m.payload, Expect::Any).expect("put");
                        mine.push(t1.elapsed().as_nanos() as u64);
                        k += 1;
                    }
                    lat.lock().expect("lat").extend(mine);
                });
            }
            std::thread::sleep(Duration::from_secs(secs));
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        });
        let elapsed = t0.elapsed();
        committer.shutdown().expect("shutdown");
        let mut lat = lat.into_inner().expect("lat");
        lat.sort_unstable();
        let s = db.store().stats();
        let per = |ns: u64, n: u64| if n == 0 { 0.0 } else { ns as f64 / n as f64 / 1e6 };
        println!(
            "segment {segment_mib:>3} MiB: {:>6.0} ops/s  p50 {} p99 {} p99.9 {} max {} | {} checkpoints, avg {:.1} ms, total {:.2}s; avg write {:.0}us; avg inner commit {:.0}us; {:.1} MB logged",
            lat.len() as f64 / elapsed.as_secs_f64(),
            fmt_us(pct(&lat, 0.5)),
            fmt_us(pct(&lat, 0.99)),
            fmt_us(pct(&lat, 0.999)),
            fmt_us(pct(&lat, 1.0)),
            s.checkpoints,
            per(s.checkpoint_nanos, s.checkpoints),
            s.checkpoint_nanos as f64 / 1e9,
            per(s.write_nanos, s.wal_writes) * 1000.0,
            per(s.inner_commit_nanos, s.logged_commits) * 1000.0,
            s.wal_bytes as f64 / 1e6
        );
    }
}
