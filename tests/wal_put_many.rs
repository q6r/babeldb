//! `WalStore`'s `put_many` (one `put_many` of the inner store, every entry
//! logged): the redo records it writes replay to the same tables after a
//! crash, reserved `meta` entries stay refused, and an oversized transaction
//! still becomes a checkpoint.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;

use babeldb::config::WalConfig;
use babeldb::store::redb::RedbStore;
use babeldb::store::wal::{WAL_LSN_KEY, WalStore};
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use babeldb::{Error, Result};

type Tables = BTreeMap<(usize, Vec<u8>), Vec<u8>>;

fn open(dir: &Path, cfg: &WalConfig) -> WalStore<RedbStore> {
    let path = dir.join("db.redb");
    let inner = RedbStore::open(&path, 16 << 20).unwrap();
    WalStore::open(inner, cfg.wal_path(&path), cfg.clone()).unwrap()
}

/// Drop `value` while the thread unwinds: no checkpoint, no clean close.
fn crash<T>(value: T) {
    let outcome = panic::catch_unwind(AssertUnwindSafe(move || {
        let _held = value;
        panic!("simulated crash (expected by this test)");
    }));
    assert!(outcome.is_err());
}

fn dump<S: Store>(s: &S) -> Tables {
    let r = s.begin_read().unwrap();
    let mut out = Tables::new();
    for table in [Table::Meta, Table::Records, Table::Objects, Table::History] {
        r.scan(table, Bound::Unbounded, Bound::Unbounded, false, &mut |k: &[u8], v: &[u8]| {
            out.insert((table.index(), k.to_vec()), v.to_vec());
            Ok(true)
        })
        .unwrap();
    }
    out
}

fn entries(round: u64, n: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let k = (round * 7919 + i * 104_729) % 100_000;
            (format!("key-{k:06}").into_bytes(), format!("value {round} {i} ").repeat(1 + (i % 5) as usize).into_bytes())
        })
        .collect()
}

#[test]
fn put_many_records_replay_after_a_crash() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = WalConfig { segment_bytes: 4 << 20, ..WalConfig::default() };
    let store = open(dir.path(), &cfg);
    let mut model = Tables::new();
    for round in 0..20u64 {
        let mut w = store.begin_write()?;
        let batch = entries(round, 50);
        let mut it = batch.iter().map(|(k, v)| (k.as_slice(), v.as_slice()));
        w.put_many(Table::Records, &mut it)?;
        // Mixed with single puts, removes and a meta put_many (not reserved).
        w.put(Table::Objects, &round.to_be_bytes(), b"object")?;
        let gone = format!("key-{:06}", (round * 104_729) % 100_000).into_bytes();
        let removed = w.remove(Table::Records, &gone)?;
        let meta = [(b"counter".as_slice(), round.to_le_bytes())];
        let mut meta_it = meta.iter().map(|(k, v)| (*k, v.as_slice()));
        w.put_many(Table::Meta, &mut meta_it)?;
        w.commit(Durability::Immediate)?;
        for (k, v) in batch {
            model.insert((Table::Records.index(), k), v);
        }
        model.insert((Table::Objects.index(), round.to_be_bytes().to_vec()), b"object".to_vec());
        assert_eq!(removed, model.remove(&(Table::Records.index(), gone)).is_some());
        model.insert((Table::Meta.index(), b"counter".to_vec()), round.to_le_bytes().to_vec());
    }
    let visible = |t: &Tables| -> Tables {
        t.iter()
            .filter(|((table, k), _)| !(*table == Table::Meta.index() && k.starts_with(b"wal_")))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    let before = visible(&dump(&store));
    for (k, v) in &model {
        assert_eq!(before.get(k), Some(v), "{k:?} before the crash");
    }
    let logged = store.stats().logged_commits;
    assert!(logged >= 20, "{logged}");
    crash(store);
    let store = open(dir.path(), &cfg);
    assert_eq!(store.recovery().records_replayed, logged, "{:?}", store.recovery());
    assert_eq!(visible(&dump(&store)), before, "after recovery");
    Ok(())
}

#[test]
fn put_many_refuses_reserved_meta_entries() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = open(dir.path(), &WalConfig::default());
    let mut w = store.begin_write()?;
    let entries = [(b"fine".as_slice(), b"1".as_slice()), (WAL_LSN_KEY.as_bytes(), b"2".as_slice())];
    let mut it = entries.iter().copied();
    assert!(matches!(w.put_many(Table::Meta, &mut it), Err(Error::InvalidArgument(_))));
    drop(w);
    // Nothing of the failed transaction is visible or logged.
    let r = store.begin_read()?;
    assert_eq!(r.get(Table::Meta, b"fine")?, None);
    Ok(())
}

#[test]
fn an_oversized_put_many_becomes_a_checkpoint() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cfg = WalConfig { segment_bytes: 256 << 10, max_pending_bytes: 64 << 10, max_record_bytes: 16 << 10, ..WalConfig::default() };
    let store = open(dir.path(), &cfg);
    let big: Vec<(Vec<u8>, Vec<u8>)> = (0..40u32).map(|i| (i.to_be_bytes().to_vec(), vec![i as u8; 1000])).collect();
    let mut w = store.begin_write()?;
    let mut it = big.iter().map(|(k, v)| (k.as_slice(), v.as_slice()));
    w.put_many(Table::Records, &mut it)?;
    w.commit(Durability::Immediate)?;
    let s = store.stats();
    assert_eq!((s.unlogged_commits, s.checkpoints >= 1), (1, true), "{s:?}");
    // A small one after it is logged again.
    let mut w = store.begin_write()?;
    let small = [(b"x".as_slice(), b"y".as_slice())];
    let mut it = small.iter().copied();
    w.put_many(Table::Records, &mut it)?;
    w.commit(Durability::Immediate)?;
    assert_eq!(store.stats().logged_commits, 1);
    crash(store);
    let store = open(dir.path(), &cfg);
    let r = store.begin_read()?;
    for (k, v) in &big {
        assert_eq!(r.get(Table::Records, k)?.as_ref(), Some(v));
    }
    assert_eq!(r.get(Table::Records, b"x")?.as_deref(), Some(&b"y"[..]));
    Ok(())
}
