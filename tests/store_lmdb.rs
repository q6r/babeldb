#![cfg(feature = "lmdb")]
//! LMDB (heed) backend tests. Run with `cargo test --features lmdb --test store_lmdb`.

use std::collections::BTreeMap;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use babeldb::Error;
use babeldb::store::heed::{DEFAULT_MAP_SIZE, HeedRead, HeedStore};
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use proptest::prelude::*;
use tempfile::TempDir;

/// Small map for tests: ~3 MiB of process commit per open store on Windows.
const TEST_MAP_SIZE: usize = 64 << 20;

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

fn temp_store() -> (TempDir, HeedStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = HeedStore::open(dir.path(), TEST_MAP_SIZE).expect("open store");
    (dir, store)
}

fn put_all<K: AsRef<[u8]>, V: AsRef<[u8]>>(store: &HeedStore, table: Table, entries: &[(K, V)]) {
    let mut w = store.begin_write().expect("begin_write");
    for (k, v) in entries {
        w.put(table, k.as_ref(), v.as_ref()).expect("put");
    }
    w.commit(Durability::Immediate).expect("commit");
}

fn scan_all(
    txn: &impl ReadTxn,
    table: Table,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
) -> Entries {
    let mut out = Vec::new();
    txn.scan(table, start, end, reverse, &mut |k, v| {
        out.push((k.to_vec(), v.to_vec()));
        Ok(true)
    })
    .expect("scan");
    out
}

fn inc(key: &[u8]) -> Bound<&[u8]> {
    Included(key)
}

fn exc(key: &[u8]) -> Bound<&[u8]> {
    Excluded(key)
}

fn in_range(key: &[u8], start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let above = match start {
        Included(s) => key >= s,
        Excluded(s) => key > s,
        Unbounded => true,
    };
    let below = match end {
        Included(e) => key <= e,
        Excluded(e) => key < e,
        Unbounded => true,
    };
    above && below
}

/// Reference scan over a model (never panics on inverted ranges, unlike
/// `BTreeMap::range`).
fn model_scan(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
) -> Entries {
    let mut out: Entries = model
        .iter()
        .filter(|(k, _)| in_range(k, start, end))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if reverse {
        out.reverse();
    }
    out
}

fn keys(entries: &Entries) -> Vec<Vec<u8>> {
    entries.iter().map(|(k, _)| k.clone()).collect()
}

/// Deterministic pseudo-random bytes (xorshift64*).
fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8
        })
        .collect()
}

#[test]
fn put_get_remove_overwrite() {
    let (_dir, store) = temp_store();
    let mut w = store.begin_write().unwrap();
    assert_eq!(w.get(Table::Records, b"k").unwrap(), None);
    w.put(Table::Records, b"k", b"v1").unwrap();
    assert_eq!(
        w.get(Table::Records, b"k").unwrap().as_deref(),
        Some(&b"v1"[..])
    );
    w.put(Table::Records, b"k", b"value two").unwrap();
    assert_eq!(
        w.get(Table::Records, b"k").unwrap().as_deref(),
        Some(&b"value two"[..])
    );
    w.put(Table::Records, b"other", b"x").unwrap();
    w.commit(Durability::Immediate).unwrap();

    let r = store.begin_read().unwrap();
    assert_eq!(
        r.get(Table::Records, b"k").unwrap().as_deref(),
        Some(&b"value two"[..])
    );
    assert_eq!(r.get(Table::Records, b"missing").unwrap(), None);
    drop(r);

    let mut w = store.begin_write().unwrap();
    assert!(w.remove(Table::Records, b"k").unwrap());
    assert!(!w.remove(Table::Records, b"k").unwrap());
    assert!(!w.remove(Table::Records, b"never-existed").unwrap());
    assert_eq!(w.get(Table::Records, b"k").unwrap(), None);
    w.commit(Durability::Immediate).unwrap();

    let r = store.begin_read().unwrap();
    assert_eq!(r.get(Table::Records, b"k").unwrap(), None);
    assert_eq!(
        r.get(Table::Records, b"other").unwrap().as_deref(),
        Some(&b"x"[..])
    );
    assert_eq!(r.len(Table::Records).unwrap(), 1);
}

#[test]
fn tables_are_independent() {
    let (_dir, store) = temp_store();
    let mut w = store.begin_write().unwrap();
    for (i, table) in Table::ALL.iter().enumerate() {
        w.put(*table, b"shared-key", format!("value-{i}").as_bytes())
            .unwrap();
    }
    w.put(Table::Objects, b"only-objects", b"o").unwrap();
    w.commit(Durability::Immediate).unwrap();

    let r = store.begin_read().unwrap();
    for (i, table) in Table::ALL.iter().enumerate() {
        let expected = format!("value-{i}").into_bytes();
        assert_eq!(
            r.get(*table, b"shared-key").unwrap(),
            Some(expected),
            "{table:?}"
        );
        let expected_len = if *table == Table::Objects { 2 } else { 1 };
        assert_eq!(r.len(*table).unwrap(), expected_len, "{table:?}");
    }
    assert_eq!(r.get(Table::Records, b"only-objects").unwrap(), None);
    drop(r);

    let mut w = store.begin_write().unwrap();
    assert!(w.remove(Table::History, b"shared-key").unwrap());
    w.commit(Durability::Immediate).unwrap();
    let r = store.begin_read().unwrap();
    assert_eq!(r.get(Table::History, b"shared-key").unwrap(), None);
    assert_eq!(r.len(Table::History).unwrap(), 0);
    for table in Table::ALL.iter().filter(|t| **t != Table::History) {
        assert!(r.get(*table, b"shared-key").unwrap().is_some(), "{table:?}");
    }
    // Scans only visit their own table.
    assert_eq!(
        scan_all(&r, Table::Refcounts, Unbounded, Unbounded, false).len(),
        1
    );
}

#[test]
fn ordered_scans_with_every_bound_kind() {
    let (_dir, store) = temp_store();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = [b"b", b"d", b"f", b"h", b"j"]
        .iter()
        .map(|k| (k.to_vec(), [k.as_slice(), b"-v"].concat()))
        .collect();
    put_all(&store, Table::Records, &entries);
    let model: BTreeMap<Vec<u8>, Vec<u8>> = entries.into_iter().collect();

    let probes: [&[u8]; 8] = [b"a", b"b", b"c", b"d", b"h", b"i", b"j", b"z"];
    let mut bounds: Vec<Bound<&[u8]>> = vec![Unbounded];
    for p in probes {
        bounds.push(Included(p));
        bounds.push(Excluded(p));
    }
    let r = store.begin_read().unwrap();
    let mut checked = 0;
    for start in &bounds {
        for end in &bounds {
            for reverse in [false, true] {
                let got = scan_all(&r, Table::Records, *start, *end, reverse);
                let want = model_scan(&model, *start, *end, reverse);
                assert_eq!(got, want, "start={start:?} end={end:?} reverse={reverse}");
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 17 * 17 * 2);

    // Spot checks written out by hand.
    let fwd = scan_all(&r, Table::Records, inc(b"c"), exc(b"h"), false);
    assert_eq!(keys(&fwd), vec![b"d".to_vec(), b"f".to_vec()]);
    let rev = scan_all(&r, Table::Records, exc(b"b"), inc(b"h"), true);
    assert_eq!(
        keys(&rev),
        vec![b"h".to_vec(), b"f".to_vec(), b"d".to_vec()]
    );
    assert_eq!(fwd[0].1, b"d-v");
}

#[test]
fn scans_stop_early_and_propagate_callback_errors() {
    let (_dir, store) = temp_store();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0u8..10).map(|i| (vec![b'k', i], vec![i])).collect();
    put_all(&store, Table::Objects, &entries);

    let r = store.begin_read().unwrap();
    for reverse in [false, true] {
        let mut seen = Vec::new();
        r.scan(
            Table::Objects,
            Unbounded,
            Unbounded,
            reverse,
            &mut |k, _| {
                seen.push(k.to_vec());
                Ok(seen.len() < 3)
            },
        )
        .unwrap();
        let expected: Vec<Vec<u8>> = if reverse {
            vec![vec![b'k', 9], vec![b'k', 8], vec![b'k', 7]]
        } else {
            vec![vec![b'k', 0], vec![b'k', 1], vec![b'k', 2]]
        };
        assert_eq!(seen, expected, "reverse={reverse}");

        let mut calls = 0;
        r.scan(
            Table::Objects,
            Unbounded,
            Unbounded,
            reverse,
            &mut |_, _| {
                calls += 1;
                Ok(false)
            },
        )
        .unwrap();
        assert_eq!(calls, 1);

        let mut calls = 0;
        let err = r
            .scan(
                Table::Objects,
                Unbounded,
                Unbounded,
                reverse,
                &mut |_, _| {
                    calls += 1;
                    if calls == 2 {
                        Err(Error::Format("stop here".into()))
                    } else {
                        Ok(true)
                    }
                },
            )
            .expect_err("callback error must propagate");
        assert!(
            matches!(err, Error::Format(ref m) if m == "stop here"),
            "{err}"
        );
        assert_eq!(calls, 2);
    }
}

#[test]
fn inverted_empty_and_odd_ranges_do_not_error() {
    let (_dir, store) = temp_store();
    // Empty table first.
    {
        let r = store.begin_read().unwrap();
        for reverse in [false, true] {
            assert!(scan_all(&r, Table::Params, Unbounded, Unbounded, reverse).is_empty());
            assert!(scan_all(&r, Table::Params, inc(b"a"), inc(b"z"), reverse).is_empty());
        }
    }
    put_all(
        &store,
        Table::Params,
        &[(b"b", b"1"), (b"c", b"2"), (b"d", b"3")],
    );
    let r = store.begin_read().unwrap();
    let long = vec![b'c'; 2000]; // longer than LMDB's maximum key size
    let all = vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()];
    // (start, end, keys expected in forward order)
    type Case<'a> = (Bound<&'a [u8]>, Bound<&'a [u8]>, Vec<Vec<u8>>);
    let cases: Vec<Case<'_>> = vec![
        // Inverted.
        (inc(b"d"), inc(b"b"), vec![]),
        (exc(b"d"), exc(b"b"), vec![]),
        // Degenerate single points.
        (exc(b"c"), exc(b"c"), vec![]),
        (inc(b"c"), exc(b"c"), vec![]),
        (exc(b"c"), inc(b"c"), vec![]),
        (inc(b"c"), inc(b"c"), vec![b"c".to_vec()]),
        // Outside the key range.
        (inc(b"e"), Unbounded, vec![]),
        (Unbounded, exc(b"b"), vec![]),
        (exc(b"d"), Unbounded, vec![]),
        (Unbounded, inc(b"a"), vec![]),
        (inc(b"a"), inc(b"z"), all.clone()),
        // Empty-key bounds: no key is empty, every key sorts after it.
        (inc(b""), Unbounded, all.clone()),
        (exc(b""), Unbounded, all.clone()),
        (Unbounded, inc(b""), vec![]),
        (Unbounded, exc(b""), vec![]),
        (inc(b""), exc(b""), vec![]),
        (inc(b""), inc(b"c"), vec![b"b".to_vec(), b"c".to_vec()]),
        // Bounds longer than the maximum key size.
        (inc(&long), Unbounded, vec![b"d".to_vec()]),
        (Unbounded, exc(&long), vec![b"b".to_vec(), b"c".to_vec()]),
        (exc(&long), inc(&long), vec![]),
    ];
    for (start, end, want) in cases {
        for reverse in [false, true] {
            let mut want = want.clone();
            if reverse {
                want.reverse();
            }
            let got = keys(&scan_all(&r, Table::Params, start, end, reverse));
            assert_eq!(got, want, "start={start:?} end={end:?} reverse={reverse}");
        }
    }
}

#[test]
fn len_counts_committed_and_pending_entries() {
    let (_dir, store) = temp_store();
    assert_eq!(
        store.begin_read().unwrap().len(Table::Refcounts).unwrap(),
        0
    );
    let mut w = store.begin_write().unwrap();
    for i in 0u64..100 {
        w.put(Table::Refcounts, &i.to_be_bytes(), &i.to_le_bytes())
            .unwrap();
    }
    w.put(
        Table::Refcounts,
        &5u64.to_be_bytes(),
        b"overwrite keeps the count",
    )
    .unwrap();
    assert_eq!(w.len(Table::Refcounts).unwrap(), 100);
    assert!(w.remove(Table::Refcounts, &7u64.to_be_bytes()).unwrap());
    assert_eq!(w.len(Table::Refcounts).unwrap(), 99);
    assert_eq!(
        store.begin_read().unwrap().len(Table::Refcounts).unwrap(),
        0
    );
    w.commit(Durability::Immediate).unwrap();
    assert_eq!(
        store.begin_read().unwrap().len(Table::Refcounts).unwrap(),
        99
    );
    assert_eq!(store.begin_read().unwrap().len(Table::Objects).unwrap(), 0);
}

#[test]
fn write_transaction_reads_its_own_writes() {
    let (_dir, store) = temp_store();
    put_all(
        &store,
        Table::Records,
        &[(b"a", b"old-a"), (b"b", b"old-b"), (b"c", b"old-c")],
    );
    let before = store.begin_read().unwrap();

    let mut w = store.begin_write().unwrap();
    w.put(Table::Records, b"a", b"new-a").unwrap();
    assert!(w.remove(Table::Records, b"b").unwrap());
    w.put(Table::Records, b"d", b"new-d").unwrap();

    assert_eq!(
        w.get(Table::Records, b"a").unwrap().as_deref(),
        Some(&b"new-a"[..])
    );
    assert_eq!(w.get(Table::Records, b"b").unwrap(), None);
    let seen = scan_all(&w, Table::Records, Unbounded, Unbounded, false);
    assert_eq!(
        seen,
        vec![
            (b"a".to_vec(), b"new-a".to_vec()),
            (b"c".to_vec(), b"old-c".to_vec()),
            (b"d".to_vec(), b"new-d".to_vec()),
        ]
    );
    let seen_rev = scan_all(&w, Table::Records, inc(b"b"), Unbounded, true);
    assert_eq!(keys(&seen_rev), vec![b"d".to_vec(), b"c".to_vec()]);
    assert_eq!(w.len(Table::Records).unwrap(), 3);

    // A read transaction on the same thread, begun while the writer is active,
    // sees the last committed state only.
    let during = store.begin_read().unwrap();
    assert_eq!(
        during.get(Table::Records, b"a").unwrap().as_deref(),
        Some(&b"old-a"[..])
    );
    assert_eq!(during.get(Table::Records, b"d").unwrap(), None);
    w.commit(Durability::Immediate).unwrap();

    assert_eq!(
        before.get(Table::Records, b"b").unwrap().as_deref(),
        Some(&b"old-b"[..])
    );
    assert_eq!(
        during.get(Table::Records, b"a").unwrap().as_deref(),
        Some(&b"old-a"[..])
    );
    let after = store.begin_read().unwrap();
    assert_eq!(
        after.get(Table::Records, b"a").unwrap().as_deref(),
        Some(&b"new-a"[..])
    );
    assert_eq!(after.get(Table::Records, b"b").unwrap(), None);
}

#[test]
fn dropping_a_write_transaction_aborts_it() {
    let (_dir, store) = temp_store();
    put_all(&store, Table::Meta, &[(b"kept", b"1")]);
    {
        let mut w = store.begin_write().unwrap();
        w.put(Table::Meta, b"discarded", b"2").unwrap();
        assert!(w.remove(Table::Meta, b"kept").unwrap());
        // Dropped without commit.
    }
    let r = store.begin_read().unwrap();
    assert_eq!(r.get(Table::Meta, b"discarded").unwrap(), None);
    assert_eq!(
        r.get(Table::Meta, b"kept").unwrap().as_deref(),
        Some(&b"1"[..])
    );
    drop(r);
    // The writer lock was released: a new write transaction can begin and commit.
    put_all(&store, Table::Meta, &[(b"after", b"3")]);
    assert_eq!(store.begin_read().unwrap().len(Table::Meta).unwrap(), 2);
}

#[test]
fn read_transactions_are_isolated_snapshots() {
    let (_dir, store) = temp_store();
    put_all(&store, Table::Objects, &[(b"x", b"v1"), (b"y", b"v1")]);
    let r1 = store.begin_read().unwrap();

    let mut w = store.begin_write().unwrap();
    w.put(Table::Objects, b"x", b"v2").unwrap();
    w.put(Table::Objects, b"z", b"v2").unwrap();
    assert!(w.remove(Table::Objects, b"y").unwrap());
    w.commit(Durability::Immediate).unwrap();

    let r2 = store.begin_read().unwrap();
    assert_eq!(
        scan_all(&r1, Table::Objects, Unbounded, Unbounded, false),
        vec![
            (b"x".to_vec(), b"v1".to_vec()),
            (b"y".to_vec(), b"v1".to_vec())
        ]
    );
    assert_eq!(r1.len(Table::Objects).unwrap(), 2);
    assert_eq!(
        scan_all(&r2, Table::Objects, Unbounded, Unbounded, true),
        vec![
            (b"z".to_vec(), b"v2".to_vec()),
            (b"x".to_vec(), b"v2".to_vec())
        ]
    );
    assert_eq!(r2.len(Table::Objects).unwrap(), 2);
}

#[test]
fn several_read_transactions_on_one_thread() {
    // With LMDB's default TLS mode the second one would fail (MDB_BAD_RSLOT).
    let (_dir, store) = temp_store();
    put_all(&store, Table::Sources, &[(b"s", b"1")]);
    let readers: Vec<HeedRead<'_>> = (0..8).map(|_| store.begin_read().unwrap()).collect();
    put_all(&store, Table::Sources, &[(b"s", b"2")]);
    let late = store.begin_read().unwrap();
    for r in &readers {
        assert_eq!(
            r.get(Table::Sources, b"s").unwrap().as_deref(),
            Some(&b"1"[..])
        );
    }
    assert_eq!(
        late.get(Table::Sources, b"s").unwrap().as_deref(),
        Some(&b"2"[..])
    );
}

#[test]
fn read_transactions_can_move_between_threads() {
    let (_dir, store) = temp_store();
    put_all(&store, Table::Sources, &[(b"s", b"1")]);
    let r = store.begin_read().unwrap();
    let value = std::thread::scope(|s| s.spawn(move || r.get(Table::Sources, b"s")).join())
        .unwrap()
        .unwrap();
    assert_eq!(value.as_deref(), Some(&b"1"[..]));
}

#[test]
fn empty_values_are_stored_and_distinct_from_missing() {
    let (_dir, store) = temp_store();
    let mut w = store.begin_write().unwrap();
    w.put(Table::PendingImports, b"empty", b"").unwrap();
    assert_eq!(
        w.get(Table::PendingImports, b"empty").unwrap(),
        Some(Vec::new())
    );
    w.commit(Durability::Immediate).unwrap();

    let r = store.begin_read().unwrap();
    assert_eq!(
        r.get(Table::PendingImports, b"empty").unwrap(),
        Some(Vec::new())
    );
    assert_eq!(r.get(Table::PendingImports, b"absent").unwrap(), None);
    assert_eq!(r.len(Table::PendingImports).unwrap(), 1);
    assert_eq!(
        scan_all(&r, Table::PendingImports, Unbounded, Unbounded, false),
        vec![(b"empty".to_vec(), Vec::new())]
    );
    drop(r);

    put_all(&store, Table::PendingImports, &[(b"empty", b"now-full")]);
    put_all(&store, Table::PendingImports, &[(b"empty", b"")]);
    let r = store.begin_read().unwrap();
    assert_eq!(
        r.get(Table::PendingImports, b"empty").unwrap(),
        Some(Vec::new())
    );
}

#[test]
fn large_values_round_trip_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let one_mib = pseudo_random_bytes(1 << 20, 1);
    let odd = pseudo_random_bytes((3 << 20) + 1, 2);
    {
        let store = HeedStore::open(dir.path(), TEST_MAP_SIZE).unwrap();
        let mut w = store.begin_write().unwrap();
        w.put(Table::Objects, b"one-mib", &one_mib).unwrap();
        w.put(Table::Objects, b"odd", &odd).unwrap();
        assert_eq!(
            w.get(Table::Objects, b"one-mib").unwrap().as_deref(),
            Some(&one_mib[..])
        );
        w.commit(Durability::Immediate).unwrap();

        let r = store.begin_read().unwrap();
        assert_eq!(
            r.get(Table::Objects, b"one-mib").unwrap().as_deref(),
            Some(&one_mib[..])
        );
        assert_eq!(
            r.get(Table::Objects, b"odd").unwrap().as_deref(),
            Some(&odd[..])
        );
        let mut sizes = Vec::new();
        r.scan(Table::Objects, Unbounded, Unbounded, false, &mut |k, v| {
            sizes.push((k.to_vec(), v.len()));
            Ok(true)
        })
        .unwrap();
        assert_eq!(
            sizes,
            vec![
                (b"odd".to_vec(), (3 << 20) + 1),
                (b"one-mib".to_vec(), 1 << 20)
            ]
        );
        drop(r);
        // Shrink one value in place.
        put_all(&store, Table::Objects, &[(b"odd", b"small")]);
    }
    let store = HeedStore::open(dir.path(), TEST_MAP_SIZE).unwrap();
    let r = store.begin_read().unwrap();
    assert_eq!(
        r.get(Table::Objects, b"one-mib").unwrap().as_deref(),
        Some(&one_mib[..])
    );
    assert_eq!(
        r.get(Table::Objects, b"odd").unwrap().as_deref(),
        Some(&b"small"[..])
    );
}

#[test]
fn binary_keys_sort_bytewise() {
    let (_dir, store) = temp_store();
    let keys_sorted: Vec<Vec<u8>> = vec![
        vec![0x00],
        vec![0x00, 0x00],
        vec![0x00, 0xFF],
        vec![0x01],
        vec![0x7F, 0x00],
        vec![0x80],
        vec![0xFF],
        vec![0xFF, 0x00],
        vec![0xFF, 0xFF],
        vec![0xFF, 0xFF, 0xFF],
    ];
    let mut shuffled = keys_sorted.clone();
    shuffled.reverse();
    shuffled.swap(1, 7);
    let mut w = store.begin_write().unwrap();
    for (i, k) in shuffled.iter().enumerate() {
        w.put(Table::HashCandidates, k, &[i as u8]).unwrap();
    }
    w.commit(Durability::Immediate).unwrap();

    let r = store.begin_read().unwrap();
    let forward = keys(&scan_all(
        &r,
        Table::HashCandidates,
        Unbounded,
        Unbounded,
        false,
    ));
    assert_eq!(forward, keys_sorted);
    let mut backward = keys(&scan_all(
        &r,
        Table::HashCandidates,
        Unbounded,
        Unbounded,
        true,
    ));
    backward.reverse();
    assert_eq!(backward, keys_sorted);
    for (i, k) in shuffled.iter().enumerate() {
        assert_eq!(
            r.get(Table::HashCandidates, k).unwrap(),
            Some(vec![i as u8]),
            "{k:?}"
        );
    }
    let tail = keys(&scan_all(
        &r,
        Table::HashCandidates,
        exc(&[0xFF]),
        Unbounded,
        false,
    ));
    assert_eq!(tail, keys_sorted[7..].to_vec());
    let head = keys(&scan_all(
        &r,
        Table::HashCandidates,
        Unbounded,
        inc(&[0x00, 0xFF]),
        true,
    ));
    assert_eq!(head, vec![vec![0x00, 0xFF], vec![0x00, 0x00], vec![0x00]]);
}

#[test]
fn data_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = HeedStore::open(dir.path(), TEST_MAP_SIZE).unwrap();
        put_all(&store, Table::Meta, &[(b"format", b"1")]);
        let mut w = store.begin_write().unwrap();
        w.put(Table::Records, b"user-key", b"manifest").unwrap();
        w.put(Table::History, b"user-key\0\0rev", b"old").unwrap();
        w.commit(Durability::Deferred).unwrap();
    }
    let store = HeedStore::open(dir.path(), TEST_MAP_SIZE).unwrap();
    let r = store.begin_read().unwrap();
    assert_eq!(
        r.get(Table::Meta, b"format").unwrap().as_deref(),
        Some(&b"1"[..])
    );
    assert_eq!(
        r.get(Table::Records, b"user-key").unwrap().as_deref(),
        Some(&b"manifest"[..])
    );
    assert_eq!(
        r.get(Table::History, b"user-key\0\0rev")
            .unwrap()
            .as_deref(),
        Some(&b"old"[..])
    );
    // Every table exists after reopen, including the untouched ones.
    for table in Table::ALL {
        let expected = u64::from(matches!(
            table,
            Table::Meta | Table::Records | Table::History
        ));
        assert_eq!(r.len(table).unwrap(), expected, "{table:?}");
    }
}

#[test]
fn concurrent_readers_see_consistent_snapshots_while_a_writer_commits() {
    const KEYS: u64 = 64;
    const GENERATIONS: u64 = 60;
    const READERS: usize = 8;
    let (_dir, store) = temp_store();
    let write_generation = |g: u64| {
        let mut w = store.begin_write().unwrap();
        for k in 0..KEYS {
            w.put(Table::Objects, &k.to_be_bytes(), &g.to_be_bytes())
                .unwrap();
        }
        w.put(Table::Meta, b"generation", &g.to_be_bytes()).unwrap();
        w.commit(Durability::Immediate).unwrap();
    };
    write_generation(0);

    let done = AtomicBool::new(false);
    std::thread::scope(|s| {
        let readers: Vec<_> = (0..READERS)
            .map(|_| {
                s.spawn(|| {
                    let mut last = 0;
                    let mut snapshots = 0u64;
                    loop {
                        // Read the flag first so one more snapshot is always
                        // taken after the last commit.
                        let finished = done.load(Ordering::Acquire);
                        let r = store.begin_read().unwrap();
                        let mut generations = Vec::with_capacity(KEYS as usize);
                        r.scan(Table::Objects, Unbounded, Unbounded, false, &mut |_, v| {
                            generations.push(u64::from_be_bytes(v.try_into().unwrap()));
                            Ok(true)
                        })
                        .unwrap();
                        assert_eq!(generations.len() as u64, KEYS);
                        let g = generations[0];
                        assert!(generations.iter().all(|&x| x == g), "torn snapshot");
                        assert!(g >= last, "generation went backwards: {g} < {last}");
                        let meta = r.get(Table::Meta, b"generation").unwrap().unwrap();
                        assert_eq!(meta, g.to_be_bytes().to_vec(), "tables disagree");
                        last = g;
                        snapshots += 1;
                        if finished {
                            return (last, snapshots);
                        }
                    }
                })
            })
            .collect();
        for g in 1..=GENERATIONS {
            write_generation(g);
        }
        done.store(true, Ordering::Release);
        for h in readers {
            let (last, snapshots) = h.join().unwrap();
            assert_eq!(
                last, GENERATIONS,
                "the final snapshot must see the last commit"
            );
            assert!(snapshots >= 1);
        }
    });
}

#[test]
fn begin_write_waits_for_the_active_writer_on_another_thread() {
    let (_dir, store) = temp_store();
    let store = &store;
    let (ready_tx, ready_rx) = mpsc::channel();
    std::thread::scope(|s| {
        let mut first = store.begin_write().unwrap();
        first.put(Table::Meta, b"owner", b"first").unwrap();
        let second = s.spawn(move || {
            // Timestamp before signalling: the main thread sleeps 150 ms after
            // the signal and only then commits, so the wait is at least that.
            let started = Instant::now();
            ready_tx.send(()).unwrap();
            let mut w = store.begin_write().unwrap(); // blocks until `first` commits
            let waited = started.elapsed();
            let seen = w.get(Table::Meta, b"owner").unwrap();
            w.put(Table::Meta, b"owner", b"second").unwrap();
            w.commit(Durability::Immediate).unwrap();
            (seen, waited)
        });
        ready_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(150));
        first.commit(Durability::Immediate).unwrap();
        let (seen, waited) = second.join().unwrap();
        assert_eq!(seen.as_deref(), Some(&b"first"[..]));
        assert!(
            waited >= Duration::from_millis(150),
            "second writer did not wait: {waited:?}"
        );
    });
    let r = store.begin_read().unwrap();
    assert_eq!(
        r.get(Table::Meta, b"owner").unwrap().as_deref(),
        Some(&b"second"[..])
    );
}

#[test]
fn a_second_write_transaction_on_the_same_thread_is_rejected() {
    // LMDB's writer mutex is recursive on Windows: without the store's own lock
    // this would silently alias the active write transaction.
    let (_dir, store) = temp_store();
    let mut w = store.begin_write().unwrap();
    w.put(Table::Meta, b"k", b"v").unwrap();
    assert!(matches!(store.begin_write(), Err(Error::Backend(_))));
    // The active transaction is unaffected.
    assert_eq!(
        w.get(Table::Meta, b"k").unwrap().as_deref(),
        Some(&b"v"[..])
    );
    w.commit(Durability::Immediate).unwrap();
    let w2 = store.begin_write().unwrap();
    assert_eq!(
        w2.get(Table::Meta, b"k").unwrap().as_deref(),
        Some(&b"v"[..])
    );
}

#[test]
fn opening_the_same_environment_twice_in_one_process_fails() {
    let dir = tempfile::tempdir().unwrap();
    let first = HeedStore::open(dir.path(), TEST_MAP_SIZE).unwrap();
    assert!(matches!(
        HeedStore::open(dir.path(), TEST_MAP_SIZE),
        Err(Error::Backend(_))
    ));
    // Other spellings of the same directory are caught too (canonical path).
    assert!(matches!(
        HeedStore::open(dir.path().join("."), TEST_MAP_SIZE),
        Err(Error::Backend(_))
    ));
    #[cfg(windows)]
    {
        let upper = std::path::PathBuf::from(dir.path().to_str().unwrap().to_uppercase());
        assert!(matches!(
            HeedStore::open(&upper, TEST_MAP_SIZE),
            Err(Error::Backend(_))
        ));
    }
    put_all(&first, Table::Meta, &[(b"k", b"v")]);
    drop(first);
    // Dropping the store closes the environment: it can be reopened at once.
    let again = HeedStore::open(dir.path(), TEST_MAP_SIZE).unwrap();
    assert_eq!(
        again
            .begin_read()
            .unwrap()
            .get(Table::Meta, b"k")
            .unwrap()
            .as_deref(),
        Some(&b"v"[..])
    );
}

#[test]
fn empty_keys_are_invalid_arguments() {
    let (_dir, store) = temp_store();
    let mut w = store.begin_write().unwrap();
    assert!(matches!(
        w.put(Table::Records, b"", b"v"),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        w.get(Table::Records, b""),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        w.remove(Table::Records, b""),
        Err(Error::InvalidArgument(_))
    ));
    // The transaction is still usable.
    w.put(Table::Records, b"k", b"v").unwrap();
    w.commit(Durability::Immediate).unwrap();
    let r = store.begin_read().unwrap();
    assert!(matches!(
        r.get(Table::Records, b""),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(r.len(Table::Records).unwrap(), 1);
}

#[test]
fn keys_longer_than_the_lmdb_limit_are_rejected_by_put() {
    let (_dir, store) = temp_store();
    let max = store.max_key_size();
    assert_eq!(max, 511, "heed default MDB_MAXKEYSIZE");
    let longest = vec![0xAB; max];
    let too_long = vec![0xAB; max + 1];
    let mut w = store.begin_write().unwrap();
    w.put(Table::Records, &longest, b"fits").unwrap();
    let err = w
        .put(Table::Records, &too_long, b"v")
        .expect_err("key over the limit");
    assert!(matches!(err, Error::LimitExceeded(_)), "{err}");
    // Lookups of impossible keys report absence instead of failing.
    assert_eq!(w.get(Table::Records, &too_long).unwrap(), None);
    assert!(!w.remove(Table::Records, &too_long).unwrap());
    w.commit(Durability::Immediate).unwrap();
    let r = store.begin_read().unwrap();
    assert_eq!(
        r.get(Table::Records, &longest).unwrap().as_deref(),
        Some(&b"fits"[..])
    );
}

#[test]
fn a_full_map_is_limit_exceeded_and_leaves_the_store_usable() {
    let dir = tempfile::tempdir().unwrap();
    let store = HeedStore::open(dir.path(), 1 << 20).unwrap();
    put_all(&store, Table::Meta, &[(b"committed", b"yes")]);

    let value = vec![0xCD; 64 * 1024];
    let mut w = store.begin_write().unwrap();
    let mut failure = None;
    for i in 0u32..64 {
        if let Err(e) = w.put(Table::Objects, &i.to_be_bytes(), &value) {
            failure = Some(e);
            break;
        }
    }
    let err = match failure {
        Some(e) => {
            drop(w);
            e
        }
        None => w
            .commit(Durability::Immediate)
            .expect_err("4 MiB cannot fit in a 1 MiB map"),
    };
    assert!(
        matches!(err, Error::LimitExceeded(ref m) if m.contains("map_size")),
        "{err}"
    );

    let r = store.begin_read().unwrap();
    assert_eq!(r.len(Table::Objects).unwrap(), 0);
    assert_eq!(
        r.get(Table::Meta, b"committed").unwrap().as_deref(),
        Some(&b"yes"[..])
    );
    drop(r);
    put_all(&store, Table::Meta, &[(b"after", b"ok")]);
    assert_eq!(store.begin_read().unwrap().len(Table::Meta).unwrap(), 2);
}

#[test]
fn map_size_is_validated_and_rounded() {
    let dir = tempfile::tempdir().unwrap();
    assert!(matches!(
        HeedStore::open(dir.path(), 0),
        Err(Error::InvalidArgument(_))
    ));
    let store = HeedStore::open(dir.path(), 10_000_001).unwrap();
    assert_eq!(store.map_size(), 10_027_008); // next multiple of 64 KiB
    assert!(store.map_size().is_multiple_of(64 * 1024));
    put_all(&store, Table::Meta, &[(b"k", b"v")]);
    const { assert!(DEFAULT_MAP_SIZE.is_multiple_of(64 * 1024)) };
}

/// Puts `count` values of 64 KiB into `Objects`, committing at the end;
/// returns the first error (from a put or from the commit).
fn put_64k_values(store: &HeedStore, first: u32, count: u32) -> Result<(), Error> {
    let value = vec![0x5A; 64 * 1024];
    let mut w = store.begin_write()?;
    for i in first..first + count {
        w.put(Table::Objects, &i.to_be_bytes(), &value)?;
    }
    w.commit(Durability::Immediate)
}

#[test]
fn reopening_with_a_smaller_map_keeps_the_used_size_and_a_larger_one_grows_it() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = HeedStore::open(dir.path(), 4 << 20).unwrap();
        put_64k_values(&store, 0, 16).unwrap(); // ~1 MiB
    }
    {
        let store = HeedStore::open(dir.path(), 64 * 1024).unwrap();
        assert!(store.map_size() > 1 << 20, "map_size {}", store.map_size());
        assert_eq!(store.begin_read().unwrap().len(Table::Objects).unwrap(), 16);
        // The map is exactly the used size: another MiB does not fit.
        let err = put_64k_values(&store, 16, 16).expect_err("map is full");
        assert!(matches!(err, Error::LimitExceeded(_)), "{err}");
    }
    let store = HeedStore::open(dir.path(), 8 << 20).unwrap();
    assert_eq!(store.map_size(), 8 << 20);
    put_64k_values(&store, 16, 16).unwrap();
    assert_eq!(store.begin_read().unwrap().len(Table::Objects).unwrap(), 32);
}

#[test]
fn the_reader_table_limit_is_limit_exceeded() {
    let (_dir, store) = temp_store();
    let mut readers = Vec::new();
    let err = loop {
        match store.begin_read() {
            Ok(r) => readers.push(r),
            Err(e) => break e,
        }
        assert!(readers.len() <= 10_000, "no reader limit hit");
    };
    assert!(matches!(err, Error::LimitExceeded(_)), "{err}");
    assert_eq!(readers.len(), 126, "LMDB default reader slots");
    // Dropping a read transaction frees its slot.
    readers.pop();
    let r = store.begin_read().unwrap();
    assert_eq!(r.len(Table::Meta).unwrap(), 0);
    // Writers do not use reader slots.
    put_all(&store, Table::Meta, &[(b"k", b"v")]);
}

#[test]
fn files_backend_name_and_compact() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("a").join("b");
    let mut store = HeedStore::open(&nested, TEST_MAP_SIZE).unwrap();
    assert_eq!(store.dir(), nested.as_path());
    assert_eq!(store.backend_name(), "lmdb");
    let files = store.files();
    let names: Vec<_> = files
        .iter()
        .map(|p| p.file_name().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["data.mdb", "lock.mdb"]);
    for f in &files {
        assert!(f.is_file(), "{}", f.display());
    }
    put_all(&store, Table::Meta, &[(b"k", b"v")]);
    assert!(!store.compact().unwrap());
    assert_eq!(
        store
            .begin_read()
            .unwrap()
            .get(Table::Meta, b"k")
            .unwrap()
            .as_deref(),
        Some(&b"v"[..])
    );
}

/// (apparent, allocated) sizes; allocated is `None` where not measured.
fn file_sizes(path: &Path) -> (u64, Option<u64>) {
    let apparent = std::fs::metadata(path).unwrap().len();
    (apparent, allocated_size(path))
}

#[cfg(windows)]
fn allocated_size(path: &Path) -> Option<u64> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_STANDARD_INFO, FileStandardInfo, GetFileInformationByHandleEx,
    };
    let file = std::fs::File::open(path).ok()?;
    let mut info = FILE_STANDARD_INFO::default();
    // SAFETY: `file` is an open handle for the duration of the call and `info`
    // is a properly aligned FILE_STANDARD_INFO whose size is passed.
    let ok = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileStandardInfo,
            (&raw mut info).cast(),
            size_of::<FILE_STANDARD_INFO>() as u32,
        )
    };
    (ok != 0).then(|| u64::try_from(info.AllocationSize).unwrap())
}

#[cfg(not(windows))]
fn allocated_size(_path: &Path) -> Option<u64> {
    None
}

#[test]
fn data_file_is_not_preallocated_to_the_map_size() {
    // A 1 GiB map with a few KB of data: data.mdb grows with use (measured on
    // Windows 11: 24 KiB apparent and allocated), which is what makes a
    // generous DEFAULT_MAP_SIZE affordable on disk.
    let dir = tempfile::tempdir().unwrap();
    let store = HeedStore::open(dir.path(), 1 << 30).unwrap();
    let values: Vec<Vec<u8>> = (0u8..4).map(|i| vec![i; 1000]).collect();
    let mut w = store.begin_write().unwrap();
    for (i, v) in values.iter().enumerate() {
        w.put(Table::Objects, &(i as u64).to_be_bytes(), v).unwrap();
    }
    w.commit(Durability::Immediate).unwrap();
    for f in store.files() {
        let (apparent, allocated) = file_sizes(&f);
        println!(
            "{}: apparent={apparent} allocated={allocated:?}",
            f.display()
        );
        assert!(apparent < 1 << 20, "{} apparent {apparent}", f.display());
        if let Some(allocated) = allocated {
            assert!(allocated < 1 << 20, "{} allocated {allocated}", f.display());
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
    Commit,
    Abort,
}

fn key_strategy(min_len: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        prop::sample::select(vec![0x00u8, 0x01, 0x7F, 0x80, 0xFE, 0xFF]),
        min_len..4,
    )
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (key_strategy(1), prop::collection::vec(any::<u8>(), 0..6)).prop_map(|(k, v)| Op::Put(k, v)),
        3 => key_strategy(1).prop_map(Op::Remove),
        1 => Just(Op::Commit),
        1 => Just(Op::Abort),
    ]
}

fn bound_strategy() -> impl Strategy<Value = Bound<Vec<u8>>> {
    prop_oneof![
        1 => Just(Unbounded),
        3 => key_strategy(0).prop_map(Included),
        3 => key_strategy(0).prop_map(Excluded),
    ]
}

fn as_ref_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Included(k) => Included(k.as_slice()),
        Excluded(k) => Excluded(k.as_slice()),
        Unbounded => Unbounded,
    }
}

/// Scan through `txn` stopping after `limit` entries (0 = no limit).
fn scan_limited(
    txn: &impl ReadTxn,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
    limit: usize,
) -> Entries {
    let mut out = Vec::new();
    txn.scan(Table::Records, start, end, reverse, &mut |k, v| {
        out.push((k.to_vec(), v.to_vec()));
        Ok(limit == 0 || out.len() < limit)
    })
    .unwrap();
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Random writes, commits and aborts, then random scans (every bound kind,
    /// both directions, early stop) must match a BTreeMap model, both inside
    /// the open write transaction and in a read snapshot.
    #[test]
    fn scans_match_a_model(
        ops in prop::collection::vec(op_strategy(), 1..60),
        scans in prop::collection::vec((bound_strategy(), bound_strategy(), any::<bool>(), 0usize..4), 1..24),
    ) {
        let (_dir, store) = temp_store();
        let mut committed: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut pending = committed.clone();
        let mut w = store.begin_write().unwrap();
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    w.put(Table::Records, &k, &v).unwrap();
                    pending.insert(k, v);
                }
                Op::Remove(k) => {
                    let existed = w.remove(Table::Records, &k).unwrap();
                    prop_assert_eq!(existed, pending.remove(&k).is_some());
                }
                Op::Commit => {
                    w.commit(Durability::Immediate).unwrap();
                    committed = pending.clone();
                    w = store.begin_write().unwrap();
                }
                Op::Abort => {
                    drop(w);
                    pending = committed.clone();
                    w = store.begin_write().unwrap();
                }
            }
        }
        let r = store.begin_read().unwrap();
        prop_assert_eq!(w.len(Table::Records).unwrap(), pending.len() as u64);
        prop_assert_eq!(r.len(Table::Records).unwrap(), committed.len() as u64);
        for (start, end, reverse, limit) in &scans {
            let (s, e) = (as_ref_bound(start), as_ref_bound(end));
            for (txn_entries, model) in [
                (scan_limited(&w, s, e, *reverse, *limit), &pending),
                (scan_limited(&r, s, e, *reverse, *limit), &committed),
            ] {
                let mut want = model_scan(model, s, e, *reverse);
                if *limit > 0 {
                    want.truncate(*limit);
                }
                prop_assert_eq!(txn_entries, want, "start={:?} end={:?} reverse={} limit={}", s, e, reverse, limit);
            }
        }
    }
}

fn percentile(sorted: &[Duration], p: usize) -> Duration {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// Keys in a scrambled (non-sequential) order: an odd multiplier is a bijection on u64.
fn bench_key(i: u64) -> [u8; 8] {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes()
}

/// `n` point gets of the `bench_key(0..n)` entries in a scattered order.
fn get_many(txn: &impl ReadTxn, n: u64, offset: u64) {
    let mut bytes = 0usize;
    for i in 0..n {
        let k = bench_key((i * 7919 + offset) % n);
        bytes += txn.get(Table::Objects, &k).unwrap().expect("present").len();
    }
    assert_eq!(bytes, 128 * n as usize);
}

#[test]
#[ignore = "measurement: cargo test --release --features lmdb --test store_lmdb -- --ignored --nocapture quick_measurement"]
fn quick_measurement() {
    const N: u64 = 10_000;
    const THREADS: u64 = 8;
    let profile = if cfg!(debug_assertions) {
        "DEBUG"
    } else {
        "release"
    };
    println!(
        "\n[{profile}] LMDB (heed 0.22.1) quick measurement; noisy, other builds may be running"
    );
    let dir = tempfile::tempdir().unwrap();
    let store = HeedStore::open(dir.path(), 1 << 30).unwrap();
    let value = pseudo_random_bytes(128, 7);

    for durability in [Durability::Immediate, Durability::Deferred] {
        let mut lat = Vec::with_capacity(200);
        for i in 0..200u64 {
            let t = Instant::now();
            let mut w = store.begin_write().unwrap();
            w.put(Table::Meta, &bench_key(1_000_000 + i), &value)
                .unwrap();
            w.commit(durability).unwrap();
            lat.push(t.elapsed());
        }
        lat.sort();
        println!(
            "200 single-put commits ({durability:?}): p50={:?} p99={:?} max={:?}",
            percentile(&lat, 50),
            percentile(&lat, 99),
            lat[lat.len() - 1]
        );
    }

    let t = Instant::now();
    let mut w = store.begin_write().unwrap();
    for i in 0..N {
        w.put(Table::Objects, &bench_key(i), &value).unwrap();
    }
    let puts = t.elapsed();
    w.commit(Durability::Immediate).unwrap();
    let total = t.elapsed();
    println!(
        "{N} puts (8 B keys, 128 B values) in one txn: puts {:?} ({:.0} ns/put), incl. commit {:?}",
        puts,
        puts.as_nanos() as f64 / N as f64,
        total
    );

    let t = Instant::now();
    let r = store.begin_read().unwrap();
    get_many(&r, N, 0);
    drop(r);
    let one_txn = t.elapsed();
    let t = Instant::now();
    for i in 0..N {
        let r = store.begin_read().unwrap();
        assert!(
            r.get(Table::Objects, &bench_key((i * 7919) % N))
                .unwrap()
                .is_some()
        );
    }
    let txn_per_get = t.elapsed();
    println!(
        "{N} gets, 1 thread: one read txn {:?} ({:.0} ns/get); one read txn per get {:?} ({:.0} ns/get)",
        one_txn,
        one_txn.as_nanos() as f64 / N as f64,
        txn_per_get,
        txn_per_get.as_nanos() as f64 / N as f64
    );

    for per_get_txn in [false, true] {
        let t = Instant::now();
        std::thread::scope(|s| {
            for th in 0..THREADS {
                let store = &store;
                s.spawn(move || {
                    if per_get_txn {
                        for i in 0..N {
                            let r = store.begin_read().unwrap();
                            let k = bench_key((i * 7919 + th * 13) % N);
                            assert!(r.get(Table::Objects, &k).unwrap().is_some());
                        }
                    } else {
                        let r = store.begin_read().unwrap();
                        get_many(&r, N, th * 13);
                    }
                });
            }
        });
        let el = t.elapsed();
        let ops = N * THREADS;
        println!(
            "{THREADS} threads x {N} gets ({}): {:?} total, {:.2} M gets/s aggregate",
            if per_get_txn {
                "one read txn per get"
            } else {
                "one read txn per thread"
            },
            el,
            ops as f64 / el.as_secs_f64() / 1e6
        );
    }
}

#[test]
fn conformance_suite() {
    let mut dirs = Vec::new();
    babeldb::store::conformance::run_all(&mut || {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = HeedStore::open(dir.path(), 256 << 20).expect("open store");
        dirs.push(dir);
        store
    })
    .expect("conformance suite");
}
