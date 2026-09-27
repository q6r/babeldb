//! Engine core on the in-memory store (Adaptive mode, small blocks), plus
//! `#[ignore]`d versions against redb, BabelPure and real codecs that run
//! once those parts are merged.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use babeldb::engine::{HistoryEntry, ScanItem};
use babeldb::format::{self, ChunkRef, Manifest, ManifestBody, SourceDescriptor};
use babeldb::generator::Generator;
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use babeldb::{BatchOp, Config, Db, Error, Expect, MemStore, Mode, RedbStore, ScanOptions};
use proptest::prelude::*;

const BS: usize = 512;
const INLINE: usize = 64;

fn small_cfg() -> Config {
    Config { block_size: BS as u32, inline_max: INLINE as u32, ..Config::adaptive() }
}

fn mem_db(cfg: Config) -> Db<MemStore> {
    Db::with_store(MemStore::new(), cfg).unwrap()
}

fn db() -> Db<MemStore> {
    mem_db(small_cfg())
}

fn history_db() -> Db<MemStore> {
    mem_db(Config { keep_history: true, ..small_cfg() })
}

/// Deterministic pseudo-random bytes (xorshift64).
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

fn put_any<S: Store>(db: &Db<S>, key: &[u8], value: &[u8]) -> u64 {
    db.put(key, value, Expect::Any).unwrap()
}

fn conflict_actual(e: Error) -> Option<u64> {
    match e {
        Error::RevisionConflict { actual, .. } => actual,
        other => panic!("expected a revision conflict, got {other:?}"),
    }
}

fn object_ids<S: Store>(db: &Db<S>, key: &[u8]) -> Vec<u64> {
    db.inspect(key).unwrap().unwrap().units.iter().map(|u| u.object_id.unwrap()).collect()
}

/// Recount every reference held by `records` + `history` and compare it with
/// `refcounts`, `objects` and `hash_candidates`.
fn check_invariants<S: Store>(db: &Db<S>) {
    let r = db.store().begin_read().unwrap();
    let all = (Bound::Unbounded, Bound::Unbounded);
    let mut refs: BTreeMap<u64, u64> = BTreeMap::new();
    for table in [Table::Records, Table::History] {
        r.scan(table, all.0, all.1, false, &mut |_: &[u8], v: &[u8]| {
            for id in Manifest::decode(v)?.object_ids() {
                *refs.entry(id).or_default() += 1;
            }
            Ok(true)
        })
        .unwrap();
    }
    let mut counts = BTreeMap::new();
    r.scan(Table::Refcounts, all.0, all.1, false, &mut |k: &[u8], v: &[u8]| {
        counts.insert(format::parse_id_key(k)?, format::decode_u64(v)?);
        Ok(true)
    })
    .unwrap();
    assert_eq!(counts, refs, "refcounts differ from the references of records + history");
    let mut objects = BTreeMap::new();
    r.scan(Table::Objects, all.0, all.1, false, &mut |k: &[u8], v: &[u8]| {
        let (h, _) = format::read_envelope(v)?;
        objects.insert(format::parse_id_key(k)?, (h.digest, h.raw_len));
        Ok(true)
    })
    .unwrap();
    assert!(objects.keys().eq(refs.keys()), "stored objects differ from referenced objects");
    let mut listed = 0usize;
    r.scan(Table::HashCandidates, all.0, all.1, false, &mut |k: &[u8], v: &[u8]| {
        for id in format::decode_id_list(v)? {
            let (digest, raw_len) = objects.get(&id).expect("candidate points to a missing object");
            assert_eq!(k, format::candidate_key(digest, *raw_len).as_slice());
            listed += 1;
        }
        Ok(true)
    })
    .unwrap();
    if db.config().effective_dedupe(db.mode()) {
        assert_eq!(listed, objects.len(), "every object must be a dedupe candidate");
    } else {
        assert_eq!(listed, 0, "no candidates without dedupe");
    }
}

/// Replace a stored value through the store API (simulated corruption).
fn tamper<S: Store>(db: &Db<S>, table: Table, key: &[u8], f: impl FnOnce(&mut Vec<u8>)) {
    let mut w = db.store().begin_write().unwrap();
    let mut v = w.get(table, key).unwrap().expect("value to tamper with");
    f(&mut v);
    w.put(table, key, &v).unwrap();
    w.commit(Durability::Immediate).unwrap();
    db.clear_cache();
}

fn raw_record<S: Store>(db: &Db<S>, key: &[u8]) -> Vec<u8> {
    db.store().begin_read().unwrap().get(Table::Records, key).unwrap().unwrap()
}

fn write_raw<S: Store>(db: &Db<S>, table: Table, key: &[u8], value: &[u8]) {
    let mut w = db.store().begin_write().unwrap();
    w.put(table, key, value).unwrap();
    w.commit(Durability::Immediate).unwrap();
    db.clear_cache();
}

// ---------------------------------------------------------------------------
// Test generator: byte i = mix(i) ^ seed; params = len (u64 LE) | seed (u8)
// ---------------------------------------------------------------------------

const MIX_ID: u16 = 900;

struct MixGen {
    broken: Arc<AtomicBool>,
}

fn mix(i: u64) -> u8 {
    (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as u8
}

fn mix_params(len: u64, seed: u8) -> Vec<u8> {
    let mut p = len.to_le_bytes().to_vec();
    p.push(seed);
    p
}

fn mix_value(len: usize, seed: u8) -> Vec<u8> {
    (0..len as u64).map(|i| mix(i) ^ seed).collect()
}

impl Generator for MixGen {
    fn id(&self) -> u16 {
        MIX_ID
    }
    fn version(&self) -> u16 {
        1
    }
    fn name(&self) -> &'static str {
        "test-mix"
    }
    fn output_len(&self, params: &[u8]) -> babeldb::Result<u64> {
        if params.len() != 9 {
            return Err(Error::InvalidArgument("mix params are 9 bytes".into()));
        }
        Ok(u64::from_le_bytes(params[..8].try_into().unwrap()))
    }
    fn generate(&self, params: &[u8], offset: u64, out: &mut [u8]) -> babeldb::Result<()> {
        let len = self.output_len(params)?;
        if offset.checked_add(out.len() as u64).is_none_or(|end| end > len) {
            return Err(Error::InvalidArgument("range outside the output".into()));
        }
        let seed = params[8] ^ u8::from(self.broken.load(Ordering::Relaxed));
        for (i, b) in out.iter_mut().enumerate() {
            *b = mix(offset + i as u64) ^ seed;
        }
        Ok(())
    }
}

fn db_with_mix(cfg: Config) -> (Db<MemStore>, Arc<AtomicBool>) {
    let broken = Arc::new(AtomicBool::new(false));
    let mut db = mem_db(cfg);
    db.register_generator(Box::new(MixGen { broken: broken.clone() }));
    (db, broken)
}

// ---------------------------------------------------------------------------
// Basic semantics
// ---------------------------------------------------------------------------

#[test]
fn db_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Db<MemStore>>();
    assert_send_sync::<Db<RedbStore>>();
}

#[test]
fn put_get_overwrite_delete() {
    let db = db();
    assert_eq!(db.get(b"k").unwrap(), None);
    let r1 = put_any(&db, b"k", b"one");
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(db.head(b"k").unwrap(), Some((r1, 3)));
    let big = bytes(1, 3 * BS + 17);
    let r2 = put_any(&db, b"k", &big);
    assert!(r2 > r1);
    assert_eq!(db.get_with_revision(b"k").unwrap(), Some((r2, big.clone())));
    assert_eq!(db.head(b"k").unwrap(), Some((r2, big.len() as u64)));
    assert!(db.delete(b"k", Expect::Any).unwrap());
    assert_eq!(db.get(b"k").unwrap(), None);
    assert_eq!(db.head(b"k").unwrap(), None);
    assert!(!db.delete(b"k", Expect::Any).unwrap());
    let r3 = put_any(&db, b"k", b"again");
    assert!(r3 > r2);
    check_invariants(&db);
    let s = db.stats().unwrap();
    assert_eq!((s.records, s.tombstones, s.objects), (1, 0, 0));
}

#[test]
fn validation_errors() {
    let db = mem_db(Config { max_value_len: 2 * BS as u64, ..small_cfg() });
    assert!(matches!(db.put(b"", b"v", Expect::Any), Err(Error::InvalidArgument(_))));
    let long_key = vec![b'k'; db.config().max_key_len + 1];
    assert!(matches!(db.put(&long_key, b"v", Expect::Any), Err(Error::LimitExceeded(_))));
    put_any(&db, &long_key[1..], b"v");
    assert!(matches!(db.put(b"k", &vec![0; 2 * BS + 1], Expect::Any), Err(Error::LimitExceeded(_))));
    put_any(&db, b"k", &vec![0; 2 * BS]);
    assert!(matches!(db.delete(b"", Expect::Any), Err(Error::InvalidArgument(_))));
    assert!(!db.delete(&long_key, Expect::Any).unwrap(), "long keys stay deletable");
    assert!(matches!(db.put_generated(b"", MIX_ID, 1, &mix_params(1, 0), Expect::Any), Err(Error::InvalidArgument(_))));
    assert_eq!(db.stats().unwrap().records, 2);
}

#[test]
fn expect_conflicts() {
    let db = db();
    let r1 = db.put(b"a", b"1", Expect::Absent).unwrap();
    assert_eq!(conflict_actual(db.put(b"a", b"2", Expect::Absent).unwrap_err()), Some(r1));
    assert_eq!(conflict_actual(db.put(b"a", b"2", Expect::Revision(r1 + 100)).unwrap_err()), Some(r1));
    let r2 = db.put(b"a", b"2", Expect::Revision(r1)).unwrap();
    assert_eq!(conflict_actual(db.put(b"a", b"3", Expect::Revision(r1)).unwrap_err()), Some(r2));
    assert_eq!(conflict_actual(db.put(b"missing", b"x", Expect::Revision(1)).unwrap_err()), None);
    match db.put(b"a", b"x", Expect::Revision(7)).unwrap_err() {
        Error::RevisionConflict { key, expected, actual } => {
            assert_eq!(key, b"a");
            assert_eq!(expected, "revision 7");
            assert_eq!(actual, Some(r2));
        }
        other => panic!("{other:?}"),
    }
    // Deletes.
    assert_eq!(conflict_actual(db.delete(b"missing", Expect::Revision(1)).unwrap_err()), None);
    assert!(!db.delete(b"missing", Expect::Absent).unwrap());
    assert_eq!(conflict_actual(db.delete(b"a", Expect::Absent).unwrap_err()), Some(r2));
    assert_eq!(conflict_actual(db.delete(b"a", Expect::Revision(r1)).unwrap_err()), Some(r2));
    assert!(db.delete(b"a", Expect::Revision(r2)).unwrap());
    db.put(b"a", b"new", Expect::Absent).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn inline_and_chunked_boundaries() {
    let db = db();
    let lens = [0, 1, INLINE - 1, INLINE, INLINE + 1, BS - 1, BS, BS + 1, 2 * BS - 1, 2 * BS, 2 * BS + 1, 3 * BS, 5 * BS + 3];
    for (i, &len) in lens.iter().enumerate() {
        let key = format!("len/{len}");
        let value = bytes(i as u64 + 10, len);
        let rev = put_any(&db, key.as_bytes(), &value);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(value.clone()), "len {len}");
        assert_eq!(db.head(key.as_bytes()).unwrap(), Some((rev, len as u64)));
        let ins = db.inspect(key.as_bytes()).unwrap().unwrap();
        assert_eq!(ins.logical_len, len as u64);
        if len <= INLINE {
            assert_eq!(ins.kind, "inline");
            assert_eq!(ins.units.len(), 1);
            assert_eq!(ins.units[0].raw_len as usize, len);
            assert_eq!(ins.encoded_bytes as usize, 64 + len);
        } else {
            assert_eq!(ins.kind, "chunks");
            assert_eq!(ins.units.len(), len.div_ceil(BS));
            for (j, u) in ins.units.iter().enumerate() {
                let expected = if j + 1 < ins.units.len() { BS } else { len - j * BS };
                assert_eq!(u.raw_len as usize, expected);
                assert_eq!(u.refcount, Some(1));
            }
        }
    }
    check_invariants(&db);
}

#[test]
fn get_range_edges() {
    let db = db();
    let v = bytes(7, 3 * BS + 100);
    let n = v.len() as u64;
    put_any(&db, b"k", &v);
    let cases = [
        (0, 0),
        (0, 1),
        (BS as u64 - 1, 2),
        (BS as u64, BS as u64),
        (100, 1000),
        (n - 1, 1),
        (n - 1, 100),
        (n, 0),
        (n, 5),
        (0, u64::MAX),
        (1000, u64::MAX),
        (0, n),
    ];
    for (off, len) in cases {
        let end = off.saturating_add(len).min(n);
        let got = db.get_range(b"k", off, len).unwrap().unwrap();
        assert_eq!(got, v[off as usize..end as usize], "range ({off}, {len})");
    }
    assert!(matches!(db.get_range(b"k", n + 1, 1), Err(Error::InvalidArgument(_))));
    assert!(matches!(db.get_range(b"k", u64::MAX, 1), Err(Error::InvalidArgument(_))));
    assert_eq!(db.get_range(b"missing", 0, 1).unwrap(), None);

    // Only the blocks that intersect the range are decoded.
    db.clear_cache();
    let before = db.stats().unwrap().counters;
    assert_eq!(db.get_range(b"k", BS as u64 + 10, 20).unwrap().unwrap(), v[BS + 10..BS + 30]);
    let after = db.stats().unwrap().counters;
    assert_eq!(after.units_decoded - before.units_decoded, 1);
    assert_eq!(after.bytes_reconstructed - before.bytes_reconstructed, BS as u64);
    assert_eq!(after.bytes_requested - before.bytes_requested, 20);

    let small = bytes(8, 50);
    put_any(&db, b"s", &small);
    assert_eq!(db.get_range(b"s", 10, 5).unwrap().unwrap(), small[10..15]);
    assert_eq!(db.get_range(b"s", 50, 5).unwrap().unwrap(), Vec::<u8>::new());
    assert!(matches!(db.get_range(b"s", 51, 0), Err(Error::InvalidArgument(_))));
    put_any(&db, b"e", b"");
    assert_eq!(db.get(b"e").unwrap(), Some(Vec::new()));
    assert_eq!(db.get_range(b"e", 0, 10).unwrap(), Some(Vec::new()));
    assert!(matches!(db.get_range(b"e", 1, 0), Err(Error::InvalidArgument(_))));
}

#[test]
fn dedupe_within_one_value() {
    let db = db();
    let (a, b) = (bytes(1, BS), bytes(2, BS));
    let v = [a.clone(), a.clone(), b, a].concat();
    put_any(&db, b"k", &v);
    assert_eq!(db.get(b"k").unwrap(), Some(v));
    let ins = db.inspect(b"k").unwrap().unwrap();
    let ids: Vec<_> = ins.units.iter().map(|u| u.object_id.unwrap()).collect();
    assert_eq!(ids[0], ids[1]);
    assert_eq!(ids[0], ids[3]);
    assert_ne!(ids[0], ids[2]);
    assert_eq!(ins.units[0].refcount, Some(3));
    assert_eq!(ins.units[2].refcount, Some(1));
    assert_eq!(ins.encoded_bytes, 2 * (64 + BS as u64), "distinct objects counted once");
    let s = db.stats().unwrap();
    assert_eq!((s.objects, s.hash_candidates), (2, 2));
    assert_eq!((s.counters.dedupe_hits, s.counters.objects_written), (2, 2));
    check_invariants(&db);
    assert!(db.delete(b"k", Expect::Any).unwrap());
    let s = db.stats().unwrap();
    assert_eq!((s.objects, s.hash_candidates, s.refcount_bytes), (0, 0, 0));
    check_invariants(&db);
}

#[test]
fn dedupe_across_keys_and_release() {
    let db = db();
    let (a, b, c) = (bytes(1, BS), bytes(2, BS), bytes(3, BS));
    put_any(&db, b"k1", &[a.clone(), b.clone()].concat());
    put_any(&db, b"k2", &[a.clone(), c.clone()].concat());
    assert_eq!(object_ids(&db, b"k1")[0], object_ids(&db, b"k2")[0]);
    assert_eq!(db.stats().unwrap().objects, 3);
    check_invariants(&db);

    // Overwrite reusing an object of the replaced value: it must survive.
    let b_id = object_ids(&db, b"k1")[1];
    put_any(&db, b"k1", &[b.clone(), b.clone()].concat());
    assert_eq!(object_ids(&db, b"k1"), vec![b_id, b_id]);
    assert_eq!(db.inspect(b"k1").unwrap().unwrap().units[0].refcount, Some(2));
    assert_eq!(db.stats().unwrap().objects, 3);
    check_invariants(&db);

    assert!(db.delete(b"k2", Expect::Any).unwrap());
    assert_eq!(db.stats().unwrap().objects, 1);
    check_invariants(&db);

    put_any(&db, b"k1", b"small");
    let s = db.stats().unwrap();
    assert_eq!((s.objects, s.hash_candidates, s.refcount_bytes, s.candidate_bytes), (0, 0, 0, 0));
    check_invariants(&db);
    assert_eq!(db.get(b"k1").unwrap().as_deref(), Some(&b"small"[..]));
}

#[test]
fn raw_only_has_no_dedupe() {
    let db = mem_db(Config { block_size: BS as u32, inline_max: INLINE as u32, ..Config::raw_only() });
    let a = bytes(1, BS);
    put_any(&db, b"k", &[a.clone(), a.clone()].concat());
    let s = db.stats().unwrap();
    assert_eq!((s.objects, s.hash_candidates), (2, 0));
    check_invariants(&db);
    assert!(db.delete(b"k", Expect::Any).unwrap());
    assert_eq!(db.stats().unwrap().objects, 0);
}

// ---------------------------------------------------------------------------
// Batches, group commit, durability
// ---------------------------------------------------------------------------

#[test]
fn write_batch_is_atomic_and_sequential() {
    let db = db();
    let rb = put_any(&db, b"b", b"old");
    let commits = db.stats().unwrap().counters.commits;

    let err = db
        .write_batch(&[
            BatchOp::Put { key: b"a", value: b"1", expect: Expect::Absent },
            BatchOp::Put { key: b"b", value: b"2", expect: Expect::Absent },
        ])
        .unwrap_err();
    assert_eq!(conflict_actual(err), Some(rb));
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"old"[..]));

    // Same key twice: the second op sees the first one.
    let err = db
        .write_batch(&[
            BatchOp::Put { key: b"x", value: b"1", expect: Expect::Absent },
            BatchOp::Put { key: b"x", value: b"2", expect: Expect::Absent },
        ])
        .unwrap_err();
    assert!(matches!(err, Error::RevisionConflict { .. }));
    assert_eq!(db.get(b"x").unwrap(), None);

    // Invalid op: nothing applied.
    let err = db
        .write_batch(&[
            BatchOp::Put { key: b"y", value: b"1", expect: Expect::Any },
            BatchOp::Put { key: b"", value: b"1", expect: Expect::Any },
        ])
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert_eq!(db.get(b"y").unwrap(), None);
    assert_eq!(db.stats().unwrap().counters.commits, commits);

    let big = bytes(3, 2 * BS + 5);
    let res = db
        .write_batch(&[
            BatchOp::Put { key: b"k", value: b"v1", expect: Expect::Absent },
            BatchOp::Put { key: b"k", value: &big, expect: Expect::Any },
            BatchOp::Delete { key: b"b", expect: Expect::Revision(rb) },
            BatchOp::Delete { key: b"missing", expect: Expect::Any },
            BatchOp::Put { key: b"z", value: b"z", expect: Expect::Any },
        ])
        .unwrap();
    let (r1, r2) = (res[0].unwrap(), res[1].unwrap());
    assert!(r2 > r1);
    assert_eq!(res[2], Some(rb));
    assert_eq!(res[3], None);
    // Revisions inside one batch are consecutive, so a later op can expect one.
    let rz = res[4].unwrap();
    let res2 = db
        .write_batch(&[
            BatchOp::Put { key: b"w", value: b"1", expect: Expect::Absent },
            BatchOp::Put { key: b"w", value: b"2", expect: Expect::Revision(rz + 1) },
        ])
        .unwrap();
    assert_eq!(res2, vec![Some(rz + 1), Some(rz + 2)]);
    assert_eq!(db.get(b"k").unwrap(), Some(big));
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"w").unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(db.stats().unwrap().counters.commits, commits + 2);
    assert_eq!(db.write_batch(&[]).unwrap(), Vec::<Option<u64>>::new());
    assert_eq!(db.stats().unwrap().counters.commits, commits + 2);
    check_invariants(&db);
}

#[test]
fn write_batch_each_applies_the_valid_ops() {
    let db = db();
    let re = put_any(&db, b"e", b"exists");
    let commits = db.stats().unwrap().counters.commits;
    let big = bytes(4, 3 * BS);
    let long_key = vec![b'k'; db.config().max_key_len + 1];
    let res = db
        .write_batch_each(
            &[
                BatchOp::Put { key: b"a", value: b"v", expect: Expect::Absent },
                BatchOp::Put { key: b"e", value: b"v", expect: Expect::Absent },
                BatchOp::Put { key: b"", value: b"v", expect: Expect::Any },
                BatchOp::Delete { key: b"missing", expect: Expect::Any },
                BatchOp::Delete { key: b"e", expect: Expect::Revision(re + 50) },
                BatchOp::Put { key: b"a", value: b"v2", expect: Expect::Absent },
                BatchOp::Put { key: b"big", value: &big, expect: Expect::Any },
                BatchOp::Delete { key: b"e", expect: Expect::Any },
                BatchOp::Put { key: &long_key, value: b"v", expect: Expect::Any },
            ],
            Durability::Deferred,
        )
        .unwrap();
    assert_eq!(res.len(), 9);
    assert!(matches!(res[0], Ok(Some(_))));
    match &res[1] {
        Err(Error::RevisionConflict { actual, .. }) => assert_eq!(*actual, Some(re)),
        other => panic!("{other:?}"),
    }
    assert!(matches!(res[2], Err(Error::InvalidArgument(_))));
    assert!(matches!(res[3], Ok(None)));
    assert!(matches!(res[4], Err(Error::RevisionConflict { .. })));
    assert!(matches!(res[5], Err(Error::RevisionConflict { .. })), "sees the first put of a");
    assert!(matches!(res[6], Ok(Some(_))));
    assert_eq!(res[7].as_ref().ok(), Some(&Some(re)));
    assert!(matches!(res[8], Err(Error::LimitExceeded(_))));
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(db.get(b"big").unwrap(), Some(big));
    assert_eq!(db.get(b"e").unwrap(), None);
    assert_eq!(db.stats().unwrap().counters.commits, commits + 1);

    // Nothing to apply: no commit.
    let res = db
        .write_batch_each(
            &[
                BatchOp::Put { key: b"", value: b"v", expect: Expect::Any },
                BatchOp::Put { key: b"a", value: b"v", expect: Expect::Absent },
                BatchOp::Delete { key: b"missing", expect: Expect::Any },
            ],
            Durability::Immediate,
        )
        .unwrap();
    assert!(res[0].is_err() && res[1].is_err() && matches!(res[2], Ok(None)));
    assert_eq!(db.stats().unwrap().counters.commits, commits + 1);
    assert!(db.write_batch_each(&[], Durability::Immediate).unwrap().is_empty());

    db.sync().unwrap();
    assert_eq!(db.stats().unwrap().counters.commits, commits + 2);
    check_invariants(&db);
}

#[test]
fn many_small_values_in_one_batch() {
    // Enough units for the parallel-preparation probe to run.
    let db = mem_db(Config::adaptive());
    let values: Vec<(Vec<u8>, Vec<u8>)> = (0..300u64)
        .map(|i| (format!("m/{i:05}").into_bytes(), bytes(i, 40 + (i as usize * 37) % 900)))
        .collect();
    let ops: Vec<BatchOp> = values
        .iter()
        .map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Absent })
        .collect();
    let res = db.write_batch_each(&ops, Durability::Immediate).unwrap();
    let revs: Vec<u64> = res.into_iter().map(|r| r.unwrap().unwrap()).collect();
    assert!(revs.windows(2).all(|w| w[1] == w[0] + 1));
    for (k, v) in &values {
        assert_eq!(db.get(k).unwrap().as_ref(), Some(v));
    }
    let big = bytes(99, 40 * 16 * 1024 + 3);
    put_any(&db, b"big", &big);
    assert_eq!(db.get(b"big").unwrap(), Some(big));
    check_invariants(&db);
}

// ---------------------------------------------------------------------------
// Scans
// ---------------------------------------------------------------------------

fn msg_key(channel: u32, seq: u32) -> Vec<u8> {
    format!("c{channel}/m/{seq:04}").into_bytes()
}

fn msg_value(channel: u32, seq: u32) -> Vec<u8> {
    let len = if seq.is_multiple_of(10) { 700 } else { 30 + (seq as usize % 20) };
    bytes(u64::from(channel) * 10_000 + u64::from(seq), len)
}

fn keys_of(items: &[ScanItem]) -> Vec<Vec<u8>> {
    items.iter().map(|i| i.key.clone()).collect()
}

#[test]
fn scan_prefix_reverse_limit_values() {
    let db = db();
    for channel in 1..=2 {
        let values: Vec<_> = (0..100).map(|s| (msg_key(channel, s), msg_value(channel, s))).collect();
        let ops: Vec<_> = values
            .iter()
            .map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Absent })
            .collect();
        db.write_batch(&ops).unwrap();
    }
    put_any(&db, b"c10/other", b"x");
    put_any(&db, b"c1", b"channel record");

    let latest = db.scan(&ScanOptions::prefix(b"c1/").reverse(true).limit(50).with_values(true)).unwrap();
    assert_eq!(latest.len(), 50);
    for (item, seq) in latest.iter().zip((50..100).rev()) {
        assert_eq!(item.key, msg_key(1, seq));
        assert_eq!(item.value.as_deref(), Some(msg_value(1, seq).as_slice()));
        assert_eq!(item.logical_len, msg_value(1, seq).len() as u64);
        assert_eq!(db.head(&item.key).unwrap(), Some((item.revision, item.logical_len)));
    }
    let first = db.scan(&ScanOptions::prefix(b"c1/").limit(5)).unwrap();
    assert_eq!(keys_of(&first), (0..5).map(|s| msg_key(1, s)).collect::<Vec<_>>());
    assert!(first.iter().all(|i| i.value.is_none()));
    assert_eq!(db.scan(&ScanOptions::prefix(b"c1/")).unwrap().len(), 100);
    assert_eq!(db.scan(&ScanOptions::prefix(b"c2/")).unwrap().len(), 100);
    assert_eq!(db.scan(&ScanOptions::prefix(b"c1")).unwrap().len(), 102);
    assert_eq!(db.scan(&ScanOptions::all()).unwrap().len(), 202);
    let all_rev = db.scan(&ScanOptions::all().reverse(true)).unwrap();
    assert_eq!(all_rev.first().unwrap().key, msg_key(2, 99));
    assert_eq!(all_rev.last().unwrap().key, b"c1");
}

#[test]
fn scan_bounds_and_tombstones() {
    let db = history_db();
    for s in 0..20 {
        put_any(&db, &msg_key(1, s), &msg_value(1, s));
    }
    let opts = |start, end| ScanOptions { start, end, ..ScanOptions::all() };
    let k = |s| msg_key(1, s);
    let got = db.scan(&opts(Bound::Excluded(k(10)), Bound::Included(k(12)))).unwrap();
    assert_eq!(keys_of(&got), vec![k(11), k(12)]);
    let got = db.scan(&opts(Bound::Included(k(10)), Bound::Excluded(k(12)))).unwrap();
    assert_eq!(keys_of(&got), vec![k(10), k(11)]);
    for (start, end) in [
        (Bound::Included(k(5)), Bound::Excluded(k(5))),
        (Bound::Excluded(k(5)), Bound::Excluded(k(5))),
        (Bound::Excluded(k(5)), Bound::Included(k(5))),
        (Bound::Included(k(9)), Bound::Included(k(3))),
    ] {
        assert!(db.scan(&opts(start, end)).unwrap().is_empty());
    }
    assert_eq!(keys_of(&db.scan(&opts(Bound::Included(k(5)), Bound::Included(k(5)))).unwrap()), vec![k(5)]);

    // Tombstones are skipped and do not count toward the limit.
    for s in 15..20 {
        assert!(db.delete(&k(s), Expect::Any).unwrap());
    }
    let latest = db.scan(&ScanOptions::prefix(b"c1/").reverse(true).limit(3).with_values(true)).unwrap();
    assert_eq!(keys_of(&latest), vec![k(14), k(13), k(12)]);
    assert_eq!(latest[0].value.as_deref(), Some(msg_value(1, 14).as_slice()));
    assert_eq!(db.scan(&ScanOptions::prefix(b"c1/")).unwrap().len(), 15);

    put_any(&db, &[0xFF, 0xFF, 1], b"x");
    put_any(&db, &[0xFF, 0xFE], b"y");
    let ff = db.scan(&ScanOptions::prefix(&[0xFF, 0xFF])).unwrap();
    assert_eq!(keys_of(&ff), vec![vec![0xFF, 0xFF, 1]]);
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

fn revs(entries: &[HistoryEntry]) -> Vec<(u64, bool, bool)> {
    entries.iter().map(|e| (e.revision, e.tombstone, e.current)).collect()
}

#[test]
fn history_tombstones_and_get_at() {
    let db = history_db();
    let (v1, v2, v3) = (bytes(1, 3 * BS), bytes(2, 3 * BS), bytes(3, 10));
    let r1 = put_any(&db, b"k", &v1);
    let r2 = put_any(&db, b"k", &v2);
    let r3 = put_any(&db, b"k", &v3);
    assert!(db.delete(b"k", Expect::Revision(r3)).unwrap());
    let h = db.history(b"k").unwrap();
    assert_eq!(revs(&h[..3]), vec![(r1, false, false), (r2, false, false), (r3, false, false)]);
    let r4 = h[3].revision;
    assert_eq!(revs(&h[3..]), vec![(r4, true, true)]);
    assert!(r4 > r3);
    assert_eq!(h[0].logical_len, v1.len() as u64);
    assert_eq!(db.get(b"k").unwrap(), None);
    assert_eq!(db.head(b"k").unwrap(), None);
    assert!(!db.delete(b"k", Expect::Any).unwrap(), "a tombstone is not live");
    assert_eq!(db.get_at(b"k", r1).unwrap(), Some(v1.clone()));
    assert_eq!(db.get_at(b"k", r2).unwrap(), Some(v2.clone()));
    assert_eq!(db.get_at(b"k", r3).unwrap(), Some(v3.clone()));
    assert_eq!(db.get_at(b"k", r4).unwrap(), None);
    assert_eq!(db.get_at(b"k", 999_999).unwrap(), None);
    assert_eq!(db.get_at(b"other", r1).unwrap(), None);
    let s = db.stats().unwrap();
    assert_eq!((s.records, s.tombstones, s.history_entries, s.objects), (0, 1, 3, 6));
    check_invariants(&db);

    // A tombstone counts as absent; the new value reuses v1's objects.
    let r5 = db.put(b"k", &v1, Expect::Absent).unwrap();
    assert_eq!(db.stats().unwrap().objects, 6);
    assert_eq!(db.history(b"k").unwrap().len(), 5);
    check_invariants(&db);

    // Keep the newest history entry (the tombstone): r1..r3 go, v2's objects too.
    assert_eq!(db.prune_history(Some(b"k"), 1).unwrap(), 3);
    assert_eq!(revs(&db.history(b"k").unwrap()), vec![(r4, true, false), (r5, false, true)]);
    assert_eq!(db.get_at(b"k", r1).unwrap(), None);
    assert_eq!(db.get_at(b"k", r5).unwrap(), Some(v1));
    assert_eq!(db.stats().unwrap().objects, 3);
    check_invariants(&db);
    assert_eq!(db.prune_history(Some(b"k"), 1).unwrap(), 0);
    assert_eq!(db.prune_history(None, 0).unwrap(), 1);
    assert_eq!(revs(&db.history(b"k").unwrap()), vec![(r5, false, true)]);
    assert_eq!(db.history(b"missing").unwrap(), Vec::new());
    check_invariants(&db);
}

#[test]
fn prune_history_every_key() {
    let db = history_db();
    for key in [&b"a"[..], b"a\0", b"b\0\0c", b"c"] {
        for i in 0..4u64 {
            put_any(&db, key, &bytes(i + key.len() as u64 * 7, BS + 1 + i as usize));
        }
    }
    assert_eq!(db.stats().unwrap().history_entries, 12);
    assert_eq!(db.prune_history(None, 1).unwrap(), 8);
    for key in [&b"a"[..], b"a\0", b"b\0\0c", b"c"] {
        let h = db.history(key).unwrap();
        assert_eq!(h.len(), 2, "{key:?}");
        assert!(!h[0].current && h[1].current);
        assert!(h[0].revision < h[1].revision);
    }
    check_invariants(&db);
    assert_eq!(db.prune_history(None, 0).unwrap(), 4);
    let s = db.stats().unwrap();
    assert_eq!((s.history_entries, s.history_bytes), (0, 0));
    check_invariants(&db);
}

#[test]
fn history_survives_turning_history_off_for_deletes() {
    let db = mem_db(small_cfg());
    // Entries written by an earlier history-enabled session stay readable.
    let v = bytes(5, 2 * BS);
    let r1 = put_any(&db, b"k", &v);
    let old = raw_record(&db, b"k");
    write_raw(&db, Table::History, &format::history_key(b"k", r1), &old);
    let mut w = db.store().begin_write().unwrap();
    for id in Manifest::decode(&old).unwrap().object_ids() {
        babeldb::engine::ops::incref(&mut w, id).unwrap();
    }
    w.commit(Durability::Immediate).unwrap();
    put_any(&db, b"k", b"new");
    assert_eq!(db.get_at(b"k", r1).unwrap(), Some(v));
    check_invariants(&db);
    assert!(db.delete(b"k", Expect::Any).unwrap());
    assert_eq!(revs(&db.history(b"k").unwrap()), vec![(r1, false, false)]);
    assert_eq!(db.prune_history(Some(b"k"), 0).unwrap(), 1);
    assert_eq!(db.stats().unwrap().objects, 0);
    check_invariants(&db);
}

// ---------------------------------------------------------------------------
// Generated values, sources
// ---------------------------------------------------------------------------

#[test]
fn put_generated_roundtrip_and_ranges() {
    let (db, _) = db_with_mix(small_cfg());
    let len = 200_000usize;
    let expected = mix_value(len, 7);
    let rev = db.put_generated(b"g", MIX_ID, 1, &mix_params(len as u64, 7), Expect::Absent).unwrap();
    assert_eq!(db.get(b"g").unwrap(), Some(expected.clone()));
    assert_eq!(db.head(b"g").unwrap(), Some((rev, len as u64)));
    for (off, n) in [(0u64, 10u64), (65_530, 20), (199_990, 100), (len as u64, 3)] {
        let end = (off + n).min(len as u64) as usize;
        assert_eq!(db.get_range(b"g", off, n).unwrap().unwrap(), expected[off as usize..end]);
    }
    let ins = db.inspect(b"g").unwrap().unwrap();
    assert_eq!(ins.kind, "generated");
    assert_eq!(ins.generator, Some((MIX_ID, 1, "test-mix".to_string(), 9)));
    assert!(ins.units.is_empty());
    assert_eq!(ins.encoded_bytes, 0);
    let s = db.stats().unwrap();
    assert_eq!((s.records, s.objects, s.logical_bytes), (1, 0, len as u64));

    // Overwrite a chunked value with a generated one: its objects are released.
    put_any(&db, b"c", &bytes(1, 3 * BS));
    let r = db.put_generated(b"c", MIX_ID, 1, &mix_params(5, 1), Expect::Any).unwrap();
    assert_eq!(db.get_with_revision(b"c").unwrap(), Some((r, mix_value(5, 1))));
    assert_eq!(db.stats().unwrap().objects, 0);
    db.put_generated(b"empty", MIX_ID, 1, &mix_params(0, 1), Expect::Any).unwrap();
    assert_eq!(db.get(b"empty").unwrap(), Some(Vec::new()));
    check_invariants(&db);
}

#[test]
fn generated_values_are_verified_on_whole_reads() {
    let (db, broken) = db_with_mix(small_cfg());
    db.put_generated(b"g", MIX_ID, 1, &mix_params(1000, 3), Expect::Any).unwrap();
    broken.store(true, Ordering::Relaxed);
    assert!(matches!(db.get(b"g"), Err(Error::Integrity { .. })));
    // Documented: range reads of generated values are not digest-verified.
    assert_eq!(db.get_range(b"g", 0, 4).unwrap().unwrap(), mix_value(4, 3 ^ 1));
    broken.store(false, Ordering::Relaxed);
    assert_eq!(db.get(b"g").unwrap(), Some(mix_value(1000, 3)));
}

#[test]
fn generated_errors() {
    let (db, _) = db_with_mix(Config { max_value_len: 1000, ..small_cfg() });
    assert!(matches!(
        db.put_generated(b"g", 4242, 1, &mix_params(1, 0), Expect::Any),
        Err(Error::UnknownGenerator { id: 4242, version: 1 })
    ));
    assert!(matches!(
        db.put_generated(b"g", MIX_ID, 2, &mix_params(1, 0), Expect::Any),
        Err(Error::UnknownGenerator { .. })
    ));
    let huge_params = vec![0u8; format::MAX_PARAMS_LEN + 1];
    assert!(matches!(db.put_generated(b"g", MIX_ID, 1, &huge_params, Expect::Any), Err(Error::LimitExceeded(_))));
    assert!(matches!(db.put_generated(b"g", MIX_ID, 1, &[1, 2], Expect::Any), Err(Error::InvalidArgument(_))));
    assert!(matches!(
        db.put_generated(b"g", MIX_ID, 1, &mix_params(1001, 0), Expect::Any),
        Err(Error::LimitExceeded(_))
    ));
    let r = db.put_generated(b"g", MIX_ID, 1, &mix_params(10, 0), Expect::Absent).unwrap();
    assert_eq!(conflict_actual(db.put_generated(b"g", MIX_ID, 1, &mix_params(10, 0), Expect::Absent).unwrap_err()), Some(r));

    // A record naming a generator this binary does not have: an error, never bytes.
    let m = Manifest {
        revision: 77,
        logical_len: 4,
        source_id: None,
        body: ManifestBody::Generated { generator_id: 4242, generator_version: 1, params: vec![], digest: [0; 32] },
    };
    write_raw(&db, Table::Records, b"alien", &m.encode());
    assert!(matches!(db.get(b"alien"), Err(Error::UnknownGenerator { id: 4242, .. })));
    assert_eq!(db.inspect(b"alien").unwrap().unwrap().generator, Some((4242, 1, "unknown".to_string(), 0)));
    // A generator whose output length disagrees with the manifest.
    let m = Manifest {
        revision: 78,
        logical_len: 11,
        source_id: None,
        body: ManifestBody::Generated { generator_id: MIX_ID, generator_version: 1, params: mix_params(10, 0), digest: [0; 32] },
    };
    write_raw(&db, Table::Records, b"liar", &m.encode());
    assert!(matches!(db.get(b"liar"), Err(Error::Integrity { .. })));
}

#[test]
fn sources_register_and_list() {
    let db = db();
    assert!(db.sources().unwrap().is_empty());
    let a = SourceDescriptor::local_file("C:/data/a.bin");
    let b = SourceDescriptor::local_file("/tmp/b");
    let ia = db.register_source(a.clone()).unwrap();
    let ib = db.register_source(b.clone()).unwrap();
    assert!(ib > ia);
    assert_eq!(db.sources().unwrap(), vec![(ia, a.clone()), (ib, b)]);
    assert_eq!(db.stats().unwrap().sources, 2);

    // A record attached to a source reports it; the content does not depend on it.
    put_any(&db, b"k", b"payload");
    let mut m = Manifest::decode(&raw_record(&db, b"k")).unwrap();
    m.source_id = Some(ia);
    write_raw(&db, Table::Records, b"k", &m.encode());
    let ins = db.inspect(b"k").unwrap().unwrap();
    assert_eq!(ins.source, Some((ia, a)));
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"payload"[..]));
}

// ---------------------------------------------------------------------------
// Inspection, stats, counters
// ---------------------------------------------------------------------------

#[test]
fn inspect_output() {
    let db = history_db();
    assert_eq!(db.inspect(b"missing").unwrap(), None);
    let r = put_any(&db, b"i", &bytes(1, 10));
    let ins = db.inspect(b"i").unwrap().unwrap();
    assert_eq!((ins.key.as_slice(), ins.revision, ins.logical_len, ins.kind), (&b"i"[..], r, 10, "inline"));
    let u = &ins.units[0];
    assert_eq!((u.object_id, u.codec.as_str(), u.raw_len, u.body_len, u.aux_id, u.refcount), (None, "RawV1", 10, 10, 0, None));
    assert_eq!(ins.encoded_bytes, 74);
    assert_eq!(ins.manifest_bytes, raw_record(&db, b"i").len() as u64);
    assert_eq!(ins.manifest_bytes, 1 + 8 + 8 + 1 + 1 + 1 + 74);
    assert_eq!((ins.source.clone(), ins.generator.clone()), (None, None));

    let v = bytes(2, 2 * BS + 7);
    put_any(&db, b"c", &v);
    let ins = db.inspect(b"c").unwrap().unwrap();
    assert_eq!(ins.kind, "chunks");
    assert_eq!(ins.units.iter().map(|u| u.raw_len).collect::<Vec<_>>(), vec![BS as u32, BS as u32, 7]);
    assert!(ins.units.iter().all(|u| u.codec == "RawV1" && u.refcount == Some(1) && u.object_id.is_some()));
    assert_eq!(ins.encoded_bytes, (3 * 64 + v.len()) as u64);
    assert_eq!(ins.manifest_bytes, 1 + 8 + 8 + 1 + 1 + 1 + 3 * 16);

    assert!(db.delete(b"c", Expect::Any).unwrap());
    let ins = db.inspect(b"c").unwrap().unwrap();
    assert_eq!((ins.kind, ins.logical_len, ins.units.len()), ("tombstone", 0, 0));
}

#[test]
fn stats_are_consistent() {
    let db = db();
    let (a, b) = (bytes(1, BS), bytes(2, BS));
    put_any(&db, b"inline", &bytes(3, 10));
    put_any(&db, b"empty", b"");
    put_any(&db, b"two", &[a.clone(), b.clone()].concat());
    put_any(&db, b"shared", &[a.clone(), bytes(4, 100)].concat());
    put_any(&db, b"gone", &bytes(5, 3 * BS));
    assert!(db.delete(b"gone", Expect::Any).unwrap());
    db.get(b"two").unwrap();
    db.get_range(b"shared", 0, 1).unwrap();
    let s = db.stats().unwrap();
    assert_eq!((s.backend, s.mode, s.block_size, s.inline_max), ("mem", Mode::Adaptive, BS as u32, INLINE as u32));
    assert_eq!((s.records, s.tombstones), (4, 0));
    assert_eq!(s.logical_bytes, 10 + 2 * BS as u64 + BS as u64 + 100);
    assert_eq!(s.key_bytes, (6 + 5 + 3 + 6) as u64);
    let mut manifest_bytes = 0;
    for k in [&b"inline"[..], b"empty", b"two", b"shared"] {
        manifest_bytes += raw_record(&db, k).len() as u64;
    }
    assert_eq!(s.manifest_bytes, manifest_bytes);
    assert_eq!(s.inline_envelope_bytes, 74 + 64);
    assert_eq!(s.objects, 3);
    assert_eq!(s.object_bytes, 3 * (8 + 64) + 2 * BS as u64 + 100);
    assert_eq!((s.hash_candidates, s.candidate_bytes), (3, 3 * (36 + 8)));
    assert_eq!(s.refcount_bytes, 3 * 16);
    assert_eq!((s.params, s.param_bytes, s.history_entries, s.history_bytes), (0, 0, 0, 0));
    assert_eq!((s.sources, s.source_bytes, s.pending_imports, s.pending_import_bytes), (0, 0, 0, 0));
    assert!(s.meta_bytes > 0);
    assert_eq!(s.per_codec.len(), 1);
    let raw = &s.per_codec[0];
    assert_eq!(raw.codec, "RawV1");
    assert_eq!(raw.units, 3 + 2);
    assert_eq!(raw.raw_bytes, 2 * BS as u64 + 100 + 10);
    assert_eq!(raw.body_bytes, raw.raw_bytes);
    assert_eq!(raw.envelope_bytes, raw.body_bytes + 64 * raw.units);
    assert_eq!(raw.envelope_bytes, s.object_bytes - 8 * s.objects + s.inline_envelope_bytes);
    assert_eq!(
        s.payload_bytes(),
        s.key_bytes
            + s.manifest_bytes
            + s.object_bytes
            + s.candidate_bytes
            + s.refcount_bytes
            + s.meta_bytes
    );
    assert!(s.files.is_empty());
    let c = &s.counters;
    assert_eq!((c.puts, c.deletes, c.commits, c.gets), (5, 1, 6, 2));
    assert_eq!((c.objects_written, c.dedupe_hits), (3 + 3, 1));
    assert_eq!(c.bytes_requested, 2 * BS as u64 + 1);
    assert_eq!(s.planner.chosen.iter().find(|(n, _)| n == "RawV1").unwrap().1, 2 + 2 + 2 + 3);
    assert_eq!(s.cache.capacity_bytes, db.config().cache_bytes);
}

// ---------------------------------------------------------------------------
// Corruption: errors, never panics or wrong bytes
// ---------------------------------------------------------------------------

#[test]
fn corrupted_objects_are_detected() {
    let db = db();
    let v = bytes(1, 3 * BS);
    put_any(&db, b"k", &v);
    let ids = object_ids(&db, b"k");
    tamper(&db, Table::Objects, &format::id_key(ids[1]), |env| env[64 + 5] ^= 1);
    match db.get(b"k") {
        Err(Error::Integrity { object_id, .. }) => assert_eq!(object_id, Some(ids[1])),
        other => panic!("{other:?}"),
    }
    // Blocks outside the range are not decoded.
    assert_eq!(db.get_range(b"k", 0, BS as u64).unwrap().unwrap(), v[..BS]);
    assert_eq!(db.get_range(b"k", 2 * BS as u64, 10).unwrap().unwrap(), v[2 * BS..2 * BS + 10]);
    assert!(db.scan(&ScanOptions::all().with_values(true)).is_err());
    assert_eq!(db.scan(&ScanOptions::all()).unwrap().len(), 1);

    tamper(&db, Table::Objects, &format::id_key(ids[2]), |env| env[0] = b'X');
    assert!(matches!(db.get_range(b"k", 2 * BS as u64, 1), Err(Error::Format(_))));
    tamper(&db, Table::Objects, &format::id_key(ids[2]), |env| {
        env[0] = b'B';
        env.pop();
    });
    assert!(db.get_range(b"k", 2 * BS as u64, 1).is_err());

    let mut w = db.store().begin_write().unwrap();
    w.remove(Table::Objects, &format::id_key(ids[0])).unwrap();
    w.commit(Durability::Immediate).unwrap();
    db.clear_cache();
    match db.get_range(b"k", 0, 1) {
        Err(Error::Integrity { object_id, .. }) => assert_eq!(object_id, Some(ids[0])),
        other => panic!("{other:?}"),
    }
    assert!(db.inspect(b"k").is_err());
}

#[test]
fn corrupted_manifests_are_detected() {
    let db = db();
    put_any(&db, b"inline", &bytes(1, 20));
    tamper(&db, Table::Records, b"inline", |m| {
        let n = m.len();
        m[n - 1] ^= 0x40;
    });
    assert!(matches!(db.get(b"inline"), Err(Error::Integrity { object_id: None, .. })));
    tamper(&db, Table::Records, b"inline", |m| m.truncate(m.len() - 1));
    assert!(matches!(db.get(b"inline"), Err(Error::Format(_))));
    assert!(matches!(db.head(b"inline"), Err(Error::Format(_))));
    assert!(db.scan(&ScanOptions::all()).is_err());
    assert!(db.stats().is_err());
    assert!(db.inspect(b"inline").is_err());
    assert!(db.put(b"inline", b"fresh", Expect::Any).is_err(), "the replaced manifest cannot be released");
    let res = db
        .write_batch_each(
            &[
                BatchOp::Put { key: b"inline", value: b"x", expect: Expect::Any },
                BatchOp::Put { key: b"ok", value: b"y", expect: Expect::Any },
            ],
            Durability::Immediate,
        )
        .unwrap();
    assert!(matches!(res[0], Err(Error::Format(_))));
    assert!(res[1].is_ok());
    assert_eq!(db.get(b"ok").unwrap().as_deref(), Some(&b"y"[..]));

    // Chunk lengths that cannot be real are rejected before allocating.
    put_any(&db, b"c", &bytes(2, 2 * BS));
    let id = object_ids(&db, b"c")[0];
    for (logical_len, chunks) in [
        (1u64 << 40, vec![ChunkRef { logical_end: 1 << 40, object_id: id }]),
        (u64::MAX, vec![ChunkRef { logical_end: 10, object_id: id }, ChunkRef { logical_end: u64::MAX, object_id: id }]),
        (BS as u64 + 1, vec![ChunkRef { logical_end: BS as u64 + 1, object_id: id }]),
    ] {
        let m = Manifest { revision: 1, logical_len, source_id: None, body: ManifestBody::Chunks(chunks) };
        write_raw(&db, Table::Records, b"bogus", &m.encode());
        assert!(matches!(db.get(b"bogus"), Err(Error::Integrity { .. })), "len {logical_len}");
        assert!(db.get_range(b"bogus", logical_len - 1, 1).is_err());
    }
}

#[test]
fn verify_on_read_can_be_disabled() {
    for verify in [true, false] {
        let db = mem_db(Config { verify_on_read: verify, ..small_cfg() });
        put_any(&db, b"k", &bytes(1, 2 * BS));
        let id = object_ids(&db, b"k")[0];
        tamper(&db, Table::Objects, &format::id_key(id), |env| env[64] ^= 0xFF);
        let got = db.get(b"k");
        if verify {
            assert!(matches!(got, Err(Error::Integrity { .. })));
        } else {
            let mut expected = bytes(1, 2 * BS);
            expected[0] ^= 0xFF;
            assert_eq!(got.unwrap(), Some(expected), "no digest check: the stored bytes are returned");
        }
    }
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

#[test]
fn concurrent_readers_see_whole_values() {
    let db = Arc::new(db());
    let value = |g: u8| vec![g; 700 + usize::from(g) * 13];
    put_any(&*db, b"k", &value(0));
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (db, stop) = (db.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut reads = 0u64;
                loop {
                    let v = db.get(b"k").unwrap().unwrap();
                    assert_eq!(v, value(v[0]));
                    let items = db.scan(&ScanOptions::prefix(b"k").with_values(true)).unwrap();
                    assert_eq!(items[0].value.as_deref(), Some(value(items[0].value.as_ref().unwrap()[0]).as_slice()));
                    reads += 1;
                    if stop.load(Ordering::Relaxed) {
                        return reads;
                    }
                }
            })
        })
        .collect();
    for g in 1..=60u8 {
        put_any(&*db, b"k", &value(g));
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
    assert_eq!(db.get(b"k").unwrap(), Some(value(60)));
    check_invariants(&*db);
}

// ---------------------------------------------------------------------------
// Chat workload: batched deferred commits, "latest N in a channel"
// ---------------------------------------------------------------------------

fn chat_key(channel: u32, seq: u64) -> Vec<u8> {
    format!("ch/{channel:04}/{seq:016}").into_bytes()
}

fn chat_value(channel: u32, seq: u64) -> Vec<u8> {
    let text_len = 20 + (seq as usize * 31 + channel as usize) % 400;
    let text: String = bytes(seq ^ u64::from(channel), text_len).iter().map(|b| char::from(b'a' + b % 26)).collect();
    format!("{{\"id\":{seq},\"channel\":{channel},\"author\":{},\"text\":\"{text}\"}}", seq % 97).into_bytes()
}

fn chat_workload<S: Store>(db: &Db<S>, channels: u32, per_channel: u64) {
    let records_before = db.stats().unwrap().records;
    let mut seq = 0u64;
    for round in 0..per_channel / 10 {
        let mut msgs = Vec::new();
        for channel in 0..channels {
            for i in 0..10 {
                let s = round * 10 + i;
                msgs.push((chat_key(channel, s), chat_value(channel, s)));
                seq += 1;
            }
        }
        let ops: Vec<_> = msgs.iter().map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Absent }).collect();
        for r in db.write_batch_each(&ops, Durability::Deferred).unwrap() {
            r.unwrap();
        }
    }
    db.sync().unwrap();
    assert_eq!(db.stats().unwrap().records, records_before + seq);
    for channel in 0..channels {
        let prefix = format!("ch/{channel:04}/");
        let latest = db.scan(&ScanOptions::prefix(prefix.as_bytes()).reverse(true).limit(50).with_values(true)).unwrap();
        assert_eq!(latest.len(), 50.min(per_channel as usize));
        for (item, s) in latest.iter().zip((0..per_channel).rev()) {
            assert_eq!(item.key, chat_key(channel, s));
            assert_eq!(item.value.as_deref(), Some(chat_value(channel, s).as_slice()));
        }
    }
}

#[test]
fn chat_workload_on_memory() {
    let db = mem_db(Config::adaptive());
    chat_workload(&db, 8, 120);
    check_invariants(&db);
}

// ---------------------------------------------------------------------------
// Model test: the engine against a BTreeMap
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum ExpectKind {
    Any,
    Absent,
    Current,
    Stale,
}

/// (length, seed, blocky)
type ValueSpec = (usize, u8, bool);

#[derive(Clone, Debug)]
enum ModelOp {
    Put { key: u8, value: ValueSpec, expect: ExpectKind },
    Delete { key: u8, expect: ExpectKind },
    /// Puts (`Some`) and deletes (`None`) through `write_batch_each`.
    Batch(Vec<(u8, Option<ValueSpec>)>),
    Range { key: u8, offset: u64, len: u64 },
}

/// `blocky` values are built from two fixed blocks so dedupe kicks in.
fn model_value((len, seed, blocky): ValueSpec) -> Vec<u8> {
    if !blocky {
        return bytes(u64::from(seed) + 1000, len);
    }
    let blocks = [bytes(1, BS), bytes(2, BS)];
    (0..len).map(|i| blocks[(usize::from(seed) >> ((i / BS) % 8)) & 1][i % BS]).collect()
}

fn model_key(k: u8) -> Vec<u8> {
    vec![b'k', k]
}

fn value_strategy() -> impl Strategy<Value = ValueSpec> {
    let lens = vec![0usize, 1, 63, 64, 65, 511, 512, 513, 1024, 1100, 1536, 2600];
    (prop::sample::select(lens), any::<u8>(), any::<bool>())
}

fn expect_strategy() -> impl Strategy<Value = ExpectKind> {
    prop_oneof![Just(ExpectKind::Any), Just(ExpectKind::Absent), Just(ExpectKind::Current), Just(ExpectKind::Stale)]
}

fn op_strategy() -> impl Strategy<Value = ModelOp> {
    prop_oneof![
        4 => (0u8..6, value_strategy(), expect_strategy()).prop_map(|(key, value, expect)| ModelOp::Put { key, value, expect }),
        2 => (0u8..6, expect_strategy()).prop_map(|(key, expect)| ModelOp::Delete { key, expect }),
        1 => prop::collection::vec((0u8..6, prop::option::of(value_strategy())), 1..6).prop_map(ModelOp::Batch),
        2 => (0u8..6, 0u64..3000, 0u64..3000).prop_map(|(key, offset, len)| ModelOp::Range { key, offset, len }),
    ]
}

fn to_expect(kind: ExpectKind, current: Option<u64>) -> (Expect, bool) {
    match (kind, current) {
        (ExpectKind::Any, _) => (Expect::Any, true),
        (ExpectKind::Absent, c) => (Expect::Absent, c.is_none()),
        (ExpectKind::Current, Some(r)) => (Expect::Revision(r), true),
        (ExpectKind::Current, None) => (Expect::Revision(1), false),
        (ExpectKind::Stale, c) => (Expect::Revision(c.unwrap_or(0) + 1_000_000), false),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 40, ..ProptestConfig::default() })]

    #[test]
    fn engine_matches_model(ops in prop::collection::vec(op_strategy(), 1..30), keep_history in any::<bool>()) {
        let db = mem_db(Config { keep_history, ..small_cfg() });
        let mut model: BTreeMap<Vec<u8>, (u64, Vec<u8>)> = BTreeMap::new();
        let mut last_rev = 0u64;
        for op in ops {
            match op {
                ModelOp::Put { key, value, expect } => {
                    let (k, v) = (model_key(key), model_value(value));
                    let (expect, holds) = to_expect(expect, model.get(&k).map(|e| e.0));
                    match db.put(&k, &v, expect) {
                        Ok(rev) => {
                            prop_assert!(holds);
                            prop_assert!(rev > last_rev);
                            last_rev = rev;
                            model.insert(k, (rev, v));
                        }
                        Err(Error::RevisionConflict { .. }) => prop_assert!(!holds),
                        Err(e) => panic!("{e:?}"),
                    }
                }
                ModelOp::Delete { key, expect } => {
                    let k = model_key(key);
                    let (expect, holds) = to_expect(expect, model.get(&k).map(|e| e.0));
                    match db.delete(&k, expect) {
                        Ok(deleted) => {
                            prop_assert!(holds);
                            prop_assert_eq!(deleted, model.remove(&k).is_some());
                        }
                        Err(Error::RevisionConflict { .. }) => prop_assert!(!holds),
                        Err(e) => panic!("{e:?}"),
                    }
                }
                ModelOp::Batch(items) => {
                    let data: Vec<(Vec<u8>, Option<Vec<u8>>)> =
                        items.iter().map(|(k, v)| (model_key(*k), v.map(model_value))).collect();
                    let ops: Vec<BatchOp> = data
                        .iter()
                        .map(|(k, v)| match v {
                            Some(v) => BatchOp::Put { key: k, value: v, expect: Expect::Any },
                            None => BatchOp::Delete { key: k, expect: Expect::Any },
                        })
                        .collect();
                    let res = db.write_batch_each(&ops, Durability::Deferred).unwrap();
                    for ((k, v), r) in data.into_iter().zip(res) {
                        let r = r.unwrap();
                        match v {
                            Some(v) => {
                                let rev = r.unwrap();
                                prop_assert!(rev > last_rev);
                                last_rev = rev;
                                model.insert(k, (rev, v));
                            }
                            None => prop_assert_eq!(r, model.remove(&k).map(|e| e.0)),
                        }
                    }
                }
                ModelOp::Range { key, offset, len } => {
                    let k = model_key(key);
                    let got = db.get_range(&k, offset, len);
                    match model.get(&k) {
                        None => prop_assert_eq!(got.unwrap(), None),
                        Some((_, v)) if offset > v.len() as u64 => {
                            prop_assert!(matches!(got, Err(Error::InvalidArgument(_))))
                        }
                        Some((_, v)) => {
                            let end = offset.saturating_add(len).min(v.len() as u64) as usize;
                            prop_assert_eq!(got.unwrap(), Some(v[offset as usize..end].to_vec()));
                        }
                    }
                }
            }
            for key in 0..6u8 {
                let k = model_key(key);
                prop_assert_eq!(db.get_with_revision(&k).unwrap(), model.get(&k).cloned());
            }
        }
        let scanned = db.scan(&ScanOptions::all().with_values(true)).unwrap();
        let expected: Vec<_> = model.iter().map(|(k, (r, v))| (k.clone(), *r, v.clone())).collect();
        let got: Vec<_> = scanned.into_iter().map(|i| (i.key, i.revision, i.value.unwrap())).collect();
        prop_assert_eq!(got, expected);
        check_invariants(&db);
        let s = db.stats().unwrap();
        prop_assert_eq!(s.records, model.len() as u64);
        prop_assert_eq!(s.logical_bytes, model.values().map(|(_, v)| v.len() as u64).sum::<u64>());
        if keep_history {
            db.prune_history(None, 0).unwrap();
            check_invariants(&db);
            prop_assert_eq!(db.stats().unwrap().history_entries, 0);
        }
    }
}

// ---------------------------------------------------------------------------
// After merge: redb backend, BabelPure, real codecs, verify
// ---------------------------------------------------------------------------

/// Mode-agnostic subset of the semantics above, for any store.
fn exercise<S: Store>(db: &Db<S>) {
    let v = bytes(1, 4 * BS + 3);
    let r1 = put_any(db, b"k", &v);
    assert_eq!(db.get(b"k").unwrap(), Some(v.clone()));
    assert_eq!(db.get_range(b"k", BS as u64 - 3, 10).unwrap().unwrap(), v[BS - 3..BS + 7]);
    let r2 = db.put(b"k", b"inline", Expect::Revision(r1)).unwrap();
    assert_eq!(db.get_with_revision(b"k").unwrap(), Some((r2, b"inline".to_vec())));
    let res = db
        .write_batch(&[
            BatchOp::Put { key: b"a", value: &v, expect: Expect::Absent },
            BatchOp::Delete { key: b"k", expect: Expect::Revision(r2) },
        ])
        .unwrap();
    assert_eq!(res[1], Some(r2));
    assert_eq!(db.get(b"k").unwrap(), None);
    for s in 0..30 {
        put_any(db, &msg_key(3, s), &msg_value(3, s));
    }
    let latest = db.scan(&ScanOptions::prefix(b"c3/").reverse(true).limit(5).with_values(true)).unwrap();
    assert_eq!(keys_of(&latest), (25..30).rev().map(|s| msg_key(3, s)).collect::<Vec<_>>());
    assert_eq!(latest[0].value.as_deref(), Some(msg_value(3, 29).as_slice()));
    check_invariants(db);
}

#[test]
fn redb_roundtrip_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("engine.redb");
    let big = bytes(5, 7 * BS + 1);
    {
        let db = Db::open(&path, small_cfg()).unwrap();
        exercise(&db);
        put_any(&db, b"persist", &big);
        let res = db
            .write_batch_each(&[BatchOp::Put { key: b"deferred", value: b"d", expect: Expect::Any }], Durability::Deferred)
            .unwrap();
        assert!(res[0].is_ok());
        db.sync().unwrap();
        chat_workload(&db, 4, 60);
    }
    let db = Db::open(&path, Config::adaptive()).unwrap();
    assert_eq!((db.block_size(), db.inline_max()), (BS as u32, INLINE as u32), "persisted creation parameters win");
    assert_eq!(db.get(b"persist").unwrap(), Some(big));
    assert_eq!(db.get(b"deferred").unwrap().as_deref(), Some(&b"d"[..]));
    check_invariants(&db);
    let s = db.stats().unwrap();
    assert_eq!(s.backend, "redb");
    assert!(s.file_apparent_bytes() > 0);
}

#[test]
fn redb_concurrent_readers() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path().join("c.redb"), small_cfg()).unwrap());
    let value = |g: u8| vec![g; 700 + usize::from(g) * 13];
    put_any(&*db, b"k", &value(0));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            std::thread::spawn(move || {
                for _ in 0..200 {
                    let v = db.get(b"k").unwrap().unwrap();
                    assert_eq!(v, value(v[0]));
                }
            })
        })
        .collect();
    for g in 1..=40u8 {
        put_any(&*db, b"k", &value(g));
    }
    for r in readers {
        r.join().unwrap();
    }
    check_invariants(&*db);
}

#[test]
fn babel_pure_roundtrip() {
    let db = mem_db(Config { block_size: BS as u32, inline_max: INLINE as u32, ..Config::babel_pure() });
    exercise(&db);
    let a = bytes(1, BS);
    put_any(&db, b"dup", &[a.clone(), a].concat());
    let ins = db.inspect(b"dup").unwrap().unwrap();
    assert!(ins.units.iter().all(|u| u.codec == "BabelAffineV1" && u.body_len == u.raw_len));
    assert_ne!(ins.units[0].object_id, ins.units[1].object_id, "no dedupe in BabelPure");
    let s = db.stats().unwrap();
    assert_eq!(s.hash_candidates, 0);
    assert!(s.per_codec.iter().all(|c| c.codec == "BabelAffineV1"));
}

#[test]
fn adaptive_codecs_shrink_compressible_values() {
    let db = mem_db(Config { block_size: 4096, inline_max: 256, ..Config::adaptive() });
    let text = "{\"author\":42,\"text\":\"hello there, this is a chat message\"}".repeat(300).into_bytes();
    let zeros = vec![0u8; 3000];
    put_any(&db, b"text", &text);
    put_any(&db, b"zeros", &zeros);
    assert_eq!(db.get(b"text").unwrap(), Some(text.clone()));
    assert_eq!(db.get(b"zeros").unwrap(), Some(zeros));
    assert_eq!(db.get_range(b"text", 5000, 100).unwrap().unwrap(), text[5000..5100]);
    let ins = db.inspect(b"text").unwrap().unwrap();
    assert!(ins.units.iter().all(|u| u.codec != "RawV1" && u.body_len < u.raw_len));
    let s = db.stats().unwrap();
    let body: u64 = s.per_codec.iter().map(|c| c.body_bytes).sum();
    let raw: u64 = s.per_codec.iter().map(|c| c.raw_bytes).sum();
    assert!(body < raw);
    check_invariants(&db);
}

#[test]
fn verify_after_mixed_workload() {
    let db = history_db();
    exercise(&db);
    put_any(&db, b"x", &bytes(9, 3 * BS));
    assert!(db.delete(b"x", Expect::Any).unwrap());
    let report = db.verify(true).unwrap();
    assert!(report.ok(), "{report:?}");
}
