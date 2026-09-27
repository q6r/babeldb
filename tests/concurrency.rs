//! Concurrent readers and one writer sharing a `Db` through `Arc`.
//!
//! Values are self-describing: word `j` (u64 LE) of version `v` of the key with
//! id `id` is `(v << 24) | (id << 16) | f(j)`, where `f(j) = j` for even
//! versions (an arithmetic sequence: ArithmeticU64V1) and `j * 40503 mod 2^16`
//! for odd ones (no recipe applies), and the length (3 to 7 blocks of 512 B)
//! depends on the version. A torn, mixed, shifted, truncated or foreign read
//! therefore cannot validate.
//!
//! Checks while 8 readers run `get_with_revision`, `get_range`, `scan` and
//! `get` (plus `clear_cache`) against a writer doing `put`, `write_batch`,
//! `write_batch_each` (Immediate, or Deferred + `sync`) and delete/re-create:
//! - every value or range validates (no torn or mixed versions);
//! - no stale read: the version seen is >= the last one whose write returned
//!   before the read started, and <= the last one started (no phantoms);
//! - per reader, versions and revisions of a key never go backwards;
//! - group writes are atomic for readers: one `scan` snapshot shows the same
//!   version for every key of the group;
//! - after the run: exact final contents, `verify(true).ok()`, `gc()` finds
//!   nothing to do.
//!
//! Plus optimistic concurrency: read-modify-write loops with
//! `Expect::Revision` never lose an update, and among racing `Expect::Absent`
//! creators exactly one wins.
//!
//! Runtime: about 2 s of writing per backend (`BABEL_CONCURRENCY_MS` changes it).
//! Run: `cargo test --test concurrency`.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use babeldb::maintenance::GcReport;
use babeldb::store::{Durability, Store};
use babeldb::{BatchOp, Config, Db, Expect, Mode, ScanOptions};
use common::{
    Rng, assert_bytes_eq, env_u64, is_conflict, mem_db, mix, open_redb, small_config, temp_dir,
    verify_ok,
};

const SINGLE: usize = 4;
const GROUP: usize = 4;
const READERS: usize = 8;
/// Minimum value length in words (3 blocks of 512 bytes).
const MIN_WORDS: u64 = 3 * 512 / 8;
const SPREAD_WORDS: u64 = 200;
const CHURN_ID: u8 = 0x2F;
const CHURN_KEY: &[u8] = b"churn";

fn config() -> Config {
    Config {
        cache_bytes: 16 * 1024,
        ..small_config(Mode::Adaptive)
    }
}

fn single_id(i: usize) -> u8 {
    i as u8
}

fn group_id(i: usize) -> u8 {
    0x10 + i as u8
}

fn single_key(i: usize) -> Vec<u8> {
    format!("single/{i}").into_bytes()
}

fn group_key(i: usize) -> Vec<u8> {
    format!("group/{i}").into_bytes()
}

fn words_for(id: u8, version: u64) -> u64 {
    MIN_WORDS + mix(version, u64::from(id)) % SPREAD_WORDS
}

fn word(id: u8, version: u64, j: u64) -> u64 {
    let low = if version.is_multiple_of(2) {
        j
    } else {
        u64::from((j as u16).wrapping_mul(40503))
    };
    (version << 24) | (u64::from(id) << 16) | low
}

fn make_value(id: u8, version: u64) -> Vec<u8> {
    (0..words_for(id, version))
        .flat_map(|j| word(id, version, j).to_le_bytes())
        .collect()
}

fn check_words(id: u8, version: u64, first_word: u64, bytes: &[u8]) -> Result<(), String> {
    if !bytes.len().is_multiple_of(8) {
        return Err(format!("length {} is not a multiple of 8", bytes.len()));
    }
    for (i, chunk) in bytes.chunks_exact(8).enumerate() {
        let j = first_word + i as u64;
        let got = u64::from_le_bytes(chunk.try_into().expect("8 bytes"));
        let want = word(id, version, j);
        if got != want {
            return Err(format!(
                "word {j}: got {got:#018x} (version {}), expected {want:#018x} (version {version}): \
                 torn, mixed or corrupted read",
                got >> 24
            ));
        }
    }
    Ok(())
}

/// Validate a whole value of key `id`; returns its version.
fn check_value(id: u8, bytes: &[u8]) -> Result<u64, String> {
    if bytes.len() < 8 {
        return Err(format!("value of {} bytes is too short", bytes.len()));
    }
    let version = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes")) >> 24;
    let words = bytes.len() as u64 / 8;
    if !bytes.len().is_multiple_of(8) || words != words_for(id, version) {
        return Err(format!(
            "version {version} of key id {id:#x} has {} words, got {} bytes (truncated or mixed)",
            words_for(id, version),
            bytes.len()
        ));
    }
    check_words(id, version, 0, bytes)?;
    Ok(version)
}

#[derive(Default)]
struct Shared {
    stop: AtomicBool,
    single_committed: [AtomicU64; SINGLE],
    single_started: [AtomicU64; SINGLE],
    group_committed: AtomicU64,
    group_started: AtomicU64,
}

/// Sets `stop` when the owning thread ends, including by panic, so that no
/// thread waits forever for a dead peer.
struct StopOnDrop<'a>(&'a AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
    }
}

fn join<T>(handle: JoinHandle<T>) -> T {
    handle
        .join()
        .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
}

struct WriterReport {
    writes: u64,
    churn: Option<u64>,
}

fn group_ops<'a>(keys: &'a [Vec<u8>], values: &'a [Vec<u8>]) -> Vec<BatchOp<'a>> {
    keys.iter()
        .zip(values)
        .map(|(key, value)| BatchOp::Put {
            key,
            value,
            expect: Expect::Any,
        })
        .collect()
}

fn writer<S: Store>(db: &Db<S>, sh: &Shared, budget: Duration) -> WriterReport {
    let _stop = StopOnDrop(&sh.stop);
    let start = Instant::now();
    let keys: Vec<Vec<u8>> = (0..GROUP).map(group_key).collect();
    let mut version = 1u64;
    let mut churn = Some(1u64);
    let mut writes = 0u64;
    while !sh.stop.load(SeqCst) && (start.elapsed() < budget || writes < 20) {
        version += 1;
        match version % 5 {
            0..=2 => {
                let k = (mix(version, 11) % SINGLE as u64) as usize;
                sh.single_started[k].store(version, SeqCst);
                db.put(
                    &single_key(k),
                    &make_value(single_id(k), version),
                    Expect::Any,
                )
                .unwrap_or_else(|e| panic!("writer: put failed: {e}"));
                sh.single_committed[k].store(version, SeqCst);
            }
            3 => {
                let values: Vec<Vec<u8>> = (0..GROUP)
                    .map(|i| make_value(group_id(i), version))
                    .collect();
                sh.group_started.store(version, SeqCst);
                let revs = db
                    .write_batch(&group_ops(&keys, &values))
                    .unwrap_or_else(|e| panic!("writer: write_batch failed: {e}"));
                assert_eq!(revs.len(), GROUP, "writer: one result per op");
                sh.group_committed.store(version, SeqCst);
            }
            _ => {
                let values: Vec<Vec<u8>> = (0..GROUP)
                    .map(|i| make_value(group_id(i), version))
                    .collect();
                let deferred = (version / 5) % 2 == 1;
                let durability = if deferred {
                    Durability::Deferred
                } else {
                    Durability::Immediate
                };
                sh.group_started.store(version, SeqCst);
                let results = db
                    .write_batch_each(&group_ops(&keys, &values), durability)
                    .unwrap_or_else(|e| panic!("writer: write_batch_each failed: {e}"));
                assert!(
                    results.iter().all(|r| matches!(r, Ok(Some(_)))),
                    "writer: every group op must apply: {results:?}"
                );
                // Deferred commits are visible to later transactions right away.
                sh.group_committed.store(version, SeqCst);
                if deferred {
                    db.sync()
                        .unwrap_or_else(|e| panic!("writer: sync failed: {e}"));
                }
            }
        }
        if version.is_multiple_of(3) {
            churn = match churn {
                Some(_) => {
                    let deleted = db
                        .delete(CHURN_KEY, Expect::Any)
                        .unwrap_or_else(|e| panic!("writer: delete failed: {e}"));
                    assert!(deleted, "writer: churn key was live");
                    None
                }
                None => {
                    db.put(CHURN_KEY, &make_value(CHURN_ID, version), Expect::Absent)
                        .unwrap_or_else(|e| panic!("writer: re-create failed: {e}"));
                    Some(version)
                }
            };
        }
        writes += 1;
    }
    WriterReport { writes, churn }
}

#[track_caller]
fn check_window(what: &str, seen: u64, floor: u64, ceil: u64, last: u64) {
    assert!(
        seen >= floor,
        "{what}: stale read: version {seen}, but version {floor} was committed before the read began"
    );
    assert!(
        seen <= ceil,
        "{what}: phantom version {seen} (latest started: {ceil})"
    );
    assert!(
        seen >= last,
        "{what}: went back in time: version {seen} after {last}"
    );
}

fn reader<S: Store>(db: &Db<S>, sh: &Shared, seed: u64, deadline: Instant) -> u64 {
    let _stop = StopOnDrop(&sh.stop);
    let mut rng = Rng::new(seed);
    // Highest (version, revision) observed per single key, and the last
    // (version, revision) pair read together by get_with_revision.
    let mut seen = [(0u64, 0u64); SINGLE];
    let mut pair = [(0u64, 0u64); SINGLE];
    let mut seen_group = 0u64;
    let mut reads = 0u64;
    while !sh.stop.load(SeqCst) {
        assert!(
            Instant::now() < deadline,
            "reader {seed}: past its deadline (writer stuck?)"
        );
        match rng.below(7) {
            0 | 1 => {
                let k = rng.below(SINGLE as u64) as usize;
                let floor = sh.single_committed[k].load(SeqCst);
                let (rev, bytes) = db
                    .get_with_revision(&single_key(k))
                    .unwrap_or_else(|e| panic!("single/{k}: get_with_revision failed: {e}"))
                    .unwrap_or_else(|| panic!("single/{k} vanished"));
                let ceil = sh.single_started[k].load(SeqCst);
                let v =
                    check_value(single_id(k), &bytes).unwrap_or_else(|e| panic!("single/{k}: {e}"));
                let what = format!("single/{k}");
                check_window(&what, v, floor, ceil, seen[k].0);
                assert!(
                    rev >= seen[k].1,
                    "{what}: revision {rev} after {}",
                    seen[k].1
                );
                if v == pair[k].0 {
                    assert_eq!(rev, pair[k].1, "{what}: same version, different revision");
                }
                seen[k] = (v, rev);
                pair[k] = (v, rev);
            }
            2 => {
                let k = rng.below(SINGLE as u64) as usize;
                let first = rng.below(MIN_WORDS);
                let count = 1 + rng.below(MIN_WORDS + SPREAD_WORDS);
                let floor = sh.single_committed[k].load(SeqCst);
                let bytes = db
                    .get_range(&single_key(k), first * 8, count * 8)
                    .unwrap_or_else(|e| panic!("single/{k}: get_range failed: {e}"))
                    .unwrap_or_else(|| panic!("single/{k} vanished"));
                let ceil = sh.single_started[k].load(SeqCst);
                let what = format!("single/{k} range [{first}, +{count}) words");
                assert!(bytes.len() >= 8, "{what}: {} bytes", bytes.len());
                let v = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes")) >> 24;
                let total = words_for(single_id(k), v);
                let want_words = count.min(total.saturating_sub(first));
                assert_eq!(
                    bytes.len() as u64,
                    want_words * 8,
                    "{what}: clamp inconsistent with version {v}"
                );
                check_words(single_id(k), v, first, &bytes)
                    .unwrap_or_else(|e| panic!("{what}: {e}"));
                check_window(&what, v, floor, ceil, seen[k].0);
                seen[k].0 = v;
            }
            3 => {
                let floor = sh.group_committed.load(SeqCst);
                let items = db
                    .scan(&ScanOptions::prefix(b"group/").with_values(true))
                    .unwrap_or_else(|e| panic!("group scan failed: {e}"));
                let ceil = sh.group_started.load(SeqCst);
                assert_eq!(items.len(), GROUP, "group scan item count");
                let mut versions = Vec::with_capacity(GROUP);
                for (i, item) in items.iter().enumerate() {
                    assert_eq!(item.key, group_key(i), "group scan order");
                    let bytes = item
                        .value
                        .as_deref()
                        .expect("scan with_values returns values");
                    assert_eq!(
                        bytes.len() as u64,
                        item.logical_len,
                        "group/{i}: logical_len"
                    );
                    versions.push(
                        check_value(group_id(i), bytes)
                            .unwrap_or_else(|e| panic!("group/{i}: {e}")),
                    );
                }
                assert!(
                    versions.iter().all(|&v| v == versions[0]),
                    "group writes are not atomic for readers: one scan saw versions {versions:?}"
                );
                check_window("group scan", versions[0], floor, ceil, seen_group);
                seen_group = versions[0];
            }
            4 => {
                let items = db
                    .scan(&ScanOptions::prefix(b"single/").reverse(true).limit(2))
                    .unwrap_or_else(|e| panic!("reverse scan failed: {e}"));
                let keys: Vec<Vec<u8>> = items.iter().map(|i| i.key.clone()).collect();
                assert_eq!(
                    keys,
                    vec![single_key(SINGLE - 1), single_key(SINGLE - 2)],
                    "reverse scan"
                );
                for item in &items {
                    assert!(item.value.is_none(), "scan without values returned a value");
                    assert!(
                        item.logical_len % 8 == 0
                            && (MIN_WORDS * 8..(MIN_WORDS + SPREAD_WORDS) * 8)
                                .contains(&item.logical_len),
                        "implausible logical_len {}",
                        item.logical_len
                    );
                }
            }
            5 => {
                let got = db
                    .get(CHURN_KEY)
                    .unwrap_or_else(|e| panic!("churn: get failed: {e}"));
                if let Some(bytes) = got {
                    check_value(CHURN_ID, &bytes).unwrap_or_else(|e| panic!("churn: {e}"));
                }
            }
            _ => {
                let i = rng.below(GROUP as u64) as usize;
                let floor = sh.group_committed.load(SeqCst);
                let bytes = db
                    .get(&group_key(i))
                    .unwrap_or_else(|e| panic!("group/{i}: get failed: {e}"))
                    .unwrap_or_else(|| panic!("group/{i} vanished"));
                let ceil = sh.group_started.load(SeqCst);
                let v =
                    check_value(group_id(i), &bytes).unwrap_or_else(|e| panic!("group/{i}: {e}"));
                check_window(&format!("group/{i}"), v, floor, ceil, seen_group);
                seen_group = v;
                if rng.below(16) == 0 {
                    db.clear_cache();
                }
            }
        }
        reads += 1;
    }
    reads
}

fn readers_and_writer<S: Store>(db: Db<S>, label: &str) {
    let sh = Arc::new(Shared::default());
    let keys: Vec<Vec<u8>> = (0..GROUP).map(group_key).collect();
    for k in 0..SINGLE {
        db.put(&single_key(k), &make_value(single_id(k), 1), Expect::Absent)
            .unwrap_or_else(|e| panic!("{label}: setup put failed: {e}"));
        sh.single_committed[k].store(1, SeqCst);
        sh.single_started[k].store(1, SeqCst);
    }
    let values: Vec<Vec<u8>> = (0..GROUP).map(|i| make_value(group_id(i), 1)).collect();
    db.write_batch(&group_ops(&keys, &values))
        .unwrap_or_else(|e| panic!("{label}: setup write_batch failed: {e}"));
    sh.group_committed.store(1, SeqCst);
    sh.group_started.store(1, SeqCst);
    db.put(CHURN_KEY, &make_value(CHURN_ID, 1), Expect::Absent)
        .unwrap_or_else(|e| panic!("{label}: setup churn put failed: {e}"));

    let db = Arc::new(db);
    let budget = Duration::from_millis(env_u64("BABEL_CONCURRENCY_MS", 2000));
    let deadline = Instant::now() + budget + Duration::from_secs(120);
    let readers: Vec<JoinHandle<u64>> = (0..READERS)
        .map(|r| {
            let (db, sh) = (Arc::clone(&db), Arc::clone(&sh));
            thread::spawn(move || reader(&db, &sh, 0xC0DE + r as u64, deadline))
        })
        .collect();
    let writer_handle = {
        let (db, sh) = (Arc::clone(&db), Arc::clone(&sh));
        thread::spawn(move || writer(&db, &sh, budget))
    };
    let report = join(writer_handle);
    let reads: Vec<u64> = readers.into_iter().map(join).collect();
    assert!(
        report.writes >= 20,
        "{label}: writer only completed {} writes",
        report.writes
    );
    for (i, n) in reads.iter().enumerate() {
        assert!(*n >= 5, "{label}: reader {i} only completed {n} reads");
    }

    for k in 0..SINGLE {
        let v = sh.single_committed[k].load(SeqCst);
        let got = db.get(&single_key(k)).unwrap().expect("single key");
        assert_bytes_eq(
            &got,
            &make_value(single_id(k), v),
            &format!("{label}: final single/{k}"),
        );
    }
    let v = sh.group_committed.load(SeqCst);
    for i in 0..GROUP {
        let got = db.get(&group_key(i)).unwrap().expect("group key");
        assert_bytes_eq(
            &got,
            &make_value(group_id(i), v),
            &format!("{label}: final group/{i}"),
        );
    }
    let churn = db.get(CHURN_KEY).unwrap();
    assert_eq!(
        churn,
        report.churn.map(|v| make_value(CHURN_ID, v)),
        "{label}: final churn key"
    );
    verify_ok(&*db, true, label);
    let Ok(mut db) = Arc::try_unwrap(db) else {
        panic!("{label}: a thread still holds the database");
    };
    let gc = db
        .gc()
        .unwrap_or_else(|e| panic!("{label}: gc failed: {e}"));
    assert_eq!(
        gc,
        GcReport::default(),
        "{label}: gc found work after concurrent writes"
    );
    verify_ok(&db, true, label);
}

#[test]
fn readers_never_see_torn_stale_or_mixed_values_redb() {
    let dir = temp_dir("babeldb-concurrency-");
    let db = open_redb(&dir.path().join("concurrency.redb"), config());
    readers_and_writer(db, "redb");
}

#[test]
fn readers_never_see_torn_stale_or_mixed_values_mem() {
    readers_and_writer(mem_db(config()), "mem");
}

fn counter_value(n: u64) -> Vec<u8> {
    (0..150u64)
        .flat_map(|j| ((n << 16) | j).to_le_bytes())
        .collect()
}

fn decode_counter(bytes: &[u8]) -> u64 {
    let n = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes")) >> 16;
    assert_bytes_eq(bytes, &counter_value(n), "counter value");
    n
}

fn optimistic_increments<S: Store>(db: Db<S>, label: &str) {
    const THREADS: u64 = 4;
    const PER_THREAD: u64 = 12;
    db.put(b"counter", &counter_value(0), Expect::Absent)
        .unwrap_or_else(|e| panic!("{label}: setup failed: {e}"));
    let db = Arc::new(db);
    let conflicts = Arc::new(AtomicU64::new(0));
    let handles: Vec<JoinHandle<()>> = (0..THREADS)
        .map(|_| {
            let (db, conflicts) = (Arc::clone(&db), Arc::clone(&conflicts));
            thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    loop {
                        let (rev, bytes) = db
                            .get_with_revision(b"counter")
                            .unwrap_or_else(|e| panic!("get failed: {e}"))
                            .expect("counter exists");
                        let n = decode_counter(&bytes);
                        match db.put(b"counter", &counter_value(n + 1), Expect::Revision(rev)) {
                            Ok(new_rev) => {
                                assert!(new_rev > rev, "revision must grow");
                                break;
                            }
                            Err(e) if is_conflict(&e) => {
                                conflicts.fetch_add(1, SeqCst);
                            }
                            Err(e) => panic!("put with Expect::Revision failed: {e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        join(h);
    }
    let (_, bytes) = db.get_with_revision(b"counter").unwrap().expect("counter");
    assert_eq!(
        decode_counter(&bytes),
        THREADS * PER_THREAD,
        "{label}: lost update ({} conflicts were retried)",
        conflicts.load(SeqCst)
    );
    verify_ok(&*db, true, label);
}

#[test]
fn optimistic_increments_are_never_lost_redb() {
    let dir = temp_dir("babeldb-optimistic-");
    optimistic_increments(
        open_redb(&dir.path().join("counter.redb"), config()),
        "redb",
    );
}

#[test]
fn optimistic_increments_are_never_lost_mem() {
    optimistic_increments(mem_db(config()), "mem");
}

fn exclusive_create<S: Store>(db: Db<S>, label: &str) {
    const THREADS: usize = 8;
    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<JoinHandle<Option<(usize, u64)>>> = (0..THREADS)
        .map(|t| {
            let (db, barrier) = (Arc::clone(&db), Arc::clone(&barrier));
            thread::spawn(move || {
                let value = make_value(0x40 + t as u8, t as u64 + 1);
                barrier.wait();
                match db.put(b"once", &value, Expect::Absent) {
                    Ok(rev) => Some((t, rev)),
                    Err(e) if is_conflict(&e) => None,
                    Err(e) => panic!("put with Expect::Absent failed: {e}"),
                }
            })
        })
        .collect();
    let winners: Vec<(usize, u64)> = handles.into_iter().filter_map(join).collect();
    assert_eq!(
        winners.len(),
        1,
        "{label}: exactly one Expect::Absent creator must win: {winners:?}"
    );
    let (t, rev) = winners[0];
    assert_eq!(
        db.get_with_revision(b"once").unwrap(),
        Some((rev, make_value(0x40 + t as u8, t as u64 + 1))),
        "{label}: the winner's value is stored"
    );
    verify_ok(&*db, true, label);
}

#[test]
fn exactly_one_absent_creator_wins_redb() {
    let dir = temp_dir("babeldb-once-");
    exclusive_create(open_redb(&dir.path().join("once.redb"), config()), "redb");
}

#[test]
fn exactly_one_absent_creator_wins_mem() {
    exclusive_create(mem_db(config()), "mem");
}
