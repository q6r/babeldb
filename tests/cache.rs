//! Block cache: byte budget, eviction policy, admission, stats, concurrency.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;

use babeldb::cache::{BlockCache, CacheStats, ENTRY_OVERHEAD_BYTES};
use babeldb::config::{Config, MAX_BLOCK_SIZE};
use proptest::prelude::*;

const OVERHEAD: usize = ENTRY_OVERHEAD_BYTES;

fn block(len: usize, seed: u8) -> Arc<[u8]> {
    (0..len)
        .map(|i| seed.wrapping_add(i as u8))
        .collect::<Vec<u8>>()
        .into()
}

fn charge(len: usize) -> usize {
    len + OVERHEAD
}

/// xorshift64: tiny deterministic per-thread RNG (no allocation, no locks).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

#[test]
fn cache_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BlockCache>();
}

#[test]
fn stats_accounting() {
    let cap = 1 << 20;
    let cache = BlockCache::new(cap);
    assert_eq!(cache.shard_count(), 1);
    assert_eq!(
        cache.stats(),
        CacheStats {
            capacity_bytes: cap,
            ..CacheStats::default()
        }
    );

    for id in 1..=10u64 {
        cache.insert(id, block(id as usize * 100, id as u8));
    }
    let payload: usize = (1..=10).map(|i| i * 100).sum();
    let s = cache.stats();
    assert_eq!((s.entries, s.used_bytes), (10, payload + 10 * OVERHEAD));
    assert_eq!((s.hits, s.misses, s.insertions, s.evictions), (0, 0, 10, 0));

    for id in [1u64, 5, 10] {
        assert_eq!(
            cache.get(id).as_deref(),
            Some(&block(id as usize * 100, id as u8)[..])
        );
    }
    assert!(cache.get(11).is_none());
    assert!(cache.get(0).is_none());
    let s = cache.stats();
    assert_eq!((s.hits, s.misses), (3, 2));

    // Removal frees exactly the entry's charge; removing twice or an unknown id is a no-op.
    cache.remove(5);
    cache.remove(5);
    cache.remove(999);
    assert!(cache.get(5).is_none());
    let s = cache.stats();
    assert_eq!((s.entries, s.used_bytes), (9, payload - 500 + 9 * OVERHEAD));
    assert_eq!((s.misses, s.evictions), (3, 0), "remove is not an eviction");

    // Replacement: the latest block wins and only its length is charged.
    cache.insert(1, block(5000, 42));
    assert_eq!(cache.get(1).as_deref(), Some(&block(5000, 42)[..]));
    let s = cache.stats();
    assert_eq!(
        (s.entries, s.used_bytes),
        (9, payload - 500 - 100 + 5000 + 9 * OVERHEAD)
    );
    assert_eq!(s.insertions, 11);

    // clear() empties the cache but keeps the cumulative activity counters.
    cache.clear();
    let s = cache.stats();
    assert_eq!(
        s,
        CacheStats {
            capacity_bytes: cap,
            used_bytes: 0,
            entries: 0,
            hits: 4,
            misses: 3,
            insertions: 11,
            evictions: 0
        }
    );
    assert!(cache.get(1).is_none());
    cache.insert(2, block(10, 0));
    assert!(cache.get(2).is_some(), "usable after clear");
}

#[test]
fn unreferenced_blocks_are_evicted_oldest_first() {
    let cap = 4 * charge(1000);
    let cache = BlockCache::new(cap);
    for id in 1..=4 {
        cache.insert(id, block(1000, id as u8));
    }
    assert_eq!(cache.stats().used_bytes, cap, "exactly full");
    cache.insert(5, block(1000, 5));
    cache.insert(6, block(1000, 6));
    // Misses do not touch reference bits, so check the evicted ids first.
    assert!(cache.get(1).is_none());
    assert!(cache.get(2).is_none());
    for id in 3..=6 {
        assert!(cache.get(id).is_some(), "id {id}");
    }
    let s = cache.stats();
    assert_eq!((s.entries, s.used_bytes, s.evictions), (4, cap, 2));
}

#[test]
fn second_chance_protects_recently_read_blocks() {
    let cap = 4 * charge(1000);
    let cache = BlockCache::new(cap);
    for id in 1..=4 {
        cache.insert(id, block(1000, id as u8));
    }
    assert!(cache.get(1).is_some());
    cache.insert(5, block(1000, 5));
    // 1 is the oldest entry but was read since insertion: it gets a second chance,
    // so 2 (the oldest unreferenced block) is the victim.
    assert!(cache.get(2).is_none());
    for id in [1, 3, 4, 5] {
        assert!(cache.get(id).is_some(), "id {id}");
    }
    assert_eq!(cache.stats().evictions, 1);
}

#[test]
fn hot_set_survives_a_stream_of_one_shot_blocks() {
    // 16 slots: 8 hot blocks read after every insertion of a one-shot (cold) block.
    let cache = BlockCache::new(16 * charge(512));
    let mut hot_misses = 0;
    for i in 0..1000u64 {
        cache.insert(1000 + i, block(512, i as u8));
        for h in 0..8u64 {
            if cache.get(h).is_none() {
                hot_misses += 1;
                cache.insert(h, block(512, h as u8));
            }
        }
    }
    // Only the 8 first-touch misses: the reference bits keep the hot set resident.
    // Plain FIFO would evict each hot block about every 16 insertions
    // (hundreds of misses).
    assert_eq!(hot_misses, 8);
    assert_eq!(cache.stats().entries, 16);
}

#[test]
fn blocks_larger_than_a_shard_budget_are_not_cached() {
    // A single shard: one block may take the whole capacity.
    let cap = 1 << 20;
    let cache = BlockCache::new(cap);
    let max = cache.max_block_len().unwrap();
    assert_eq!(max, cap - OVERHEAD);
    cache.insert(1, block(max, 1));
    assert_eq!(cache.stats().used_bytes, cap);
    assert!(cache.get(1).is_some());
    cache.insert(2, block(max + 1, 2));
    assert!(cache.get(2).is_none());
    assert!(cache.get(1).is_some(), "a rejected block evicts nothing");
    let s = cache.stats();
    assert_eq!(
        (s.entries, s.insertions, s.evictions, s.used_bytes),
        (1, 1, 0, cap)
    );

    // With 4 shards, a 300 KiB block is smaller than the capacity but larger than a shard.
    let cache = BlockCache::with_shards(cap, 4);
    assert_eq!(cache.max_block_len(), Some(cap / 4 - OVERHEAD));
    cache.insert(7, block(300 << 10, 7));
    assert!(cache.get(7).is_none());
    assert_eq!(cache.stats().entries, 0);
    cache.insert(8, block(cap / 4 - OVERHEAD, 8));
    assert!(cache.get(8).is_some());

    // An oversized block for an id already cached drops the stale block.
    let cache = BlockCache::new(cap);
    cache.insert(9, block(10, 1));
    cache.insert(9, block(2 << 20, 2));
    assert!(cache.get(9).is_none(), "never serve a superseded block");
    assert_eq!((cache.stats().entries, cache.stats().used_bytes), (0, 0));

    // The default configuration caches the largest block the engine produces.
    let cache = BlockCache::new(Config::default().cache_bytes);
    assert!(cache.max_block_len().unwrap() >= MAX_BLOCK_SIZE as usize);
}

#[test]
fn zero_or_tiny_capacity_disables_caching() {
    for cap in [0, 1, OVERHEAD - 1] {
        let cache = BlockCache::new(cap);
        assert_eq!(cache.max_block_len(), None, "cap {cap}");
        cache.insert(1, block(0, 0));
        cache.insert(2, block(10, 0));
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_none());
        cache.remove(1);
        cache.clear();
        assert_eq!(
            cache.stats(),
            CacheStats {
                capacity_bytes: cap,
                misses: 2,
                ..CacheStats::default()
            }
        );
    }
    // Room for exactly one per-entry overhead: only an empty block fits.
    let cache = BlockCache::new(OVERHEAD);
    assert_eq!(cache.max_block_len(), Some(0));
    cache.insert(1, block(0, 0));
    cache.insert(2, block(1, 0));
    assert!(cache.get(2).is_none());
    assert_eq!(cache.get(1).as_deref(), Some(&[][..]));
    assert_eq!(cache.stats().used_bytes, OVERHEAD);
}

/// Deterministic content per id, so readers can verify every hit.
fn content(id: u64) -> Vec<u8> {
    let len = (id % 97) as usize * 37 + 1;
    (0..len).map(|i| (id as u8) ^ (i as u8)).collect()
}

#[test]
fn concurrent_readers_and_writers() {
    const THREADS: usize = 16;
    const OPS: usize = 20_000;
    const IDS: u64 = 2_000;
    // 8 shards of 32 KiB: constant eviction pressure in every shard.
    let cache = Arc::new(BlockCache::with_shards(256 << 10, 8));
    let stop = Arc::new(AtomicBool::new(false));
    let monitor = {
        let (cache, stop) = (Arc::clone(&cache), Arc::clone(&stop));
        thread::spawn(move || {
            let mut snapshots = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let s = cache.stats();
                assert!(s.used_bytes <= s.capacity_bytes, "{s:?}");
                assert!(s.entries * OVERHEAD <= s.used_bytes, "{s:?}");
                snapshots += 1;
            }
            snapshots
        })
    };
    let barrier = Arc::new(Barrier::new(THREADS));
    let workers: Vec<_> = (0..THREADS)
        .map(|t| {
            let (cache, barrier) = (Arc::clone(&cache), Arc::clone(&barrier));
            thread::spawn(move || {
                let mut rng = Rng::new(t as u64 + 1);
                let (mut hits, mut misses) = (0u64, 0u64);
                barrier.wait();
                for _ in 0..OPS {
                    let id = rng.next() % IDS;
                    match rng.next() % 100 {
                        0..70 => match cache.get(id) {
                            Some(v) => {
                                assert_eq!(&v[..], &content(id)[..], "wrong block for id {id}");
                                hits += 1;
                            }
                            None => {
                                misses += 1;
                                cache.insert(id, content(id).into());
                            }
                        },
                        70..95 => cache.insert(id, content(id).into()),
                        _ => cache.remove(id),
                    }
                }
                (hits, misses)
            })
        })
        .collect();
    let (mut hits, mut misses) = (0, 0);
    for w in workers {
        let (h, m) = w.join().expect("worker panicked");
        hits += h;
        misses += m;
    }
    stop.store(true, Ordering::Relaxed);
    let snapshots = monitor.join().expect("monitor panicked");

    let s = cache.stats();
    assert_eq!(
        (s.hits, s.misses),
        (hits, misses),
        "every get counted exactly once"
    );
    assert!(s.used_bytes <= s.capacity_bytes);
    assert!(s.hits > 0 && s.evictions > 0, "{s:?}");
    assert!(snapshots > 0);
    // Quiescent state: whatever is cached is intact.
    for id in 0..IDS {
        if let Some(v) = cache.get(id) {
            assert_eq!(&v[..], &content(id)[..]);
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Insert { id: u64, len: usize },
    Get(u64),
    Remove(u64),
    Clear,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0..48u64, prop_oneof![0..=256usize, 0..=12_000usize]).prop_map(|(id, len)| Op::Insert { id, len }),
        6 => (0..48u64).prop_map(Op::Get),
        2 => (0..48u64).prop_map(Op::Remove),
        1 => Just(Op::Clear),
    ]
}

// Case count follows proptest's default (256), overridable with PROPTEST_CASES.
proptest! {
    /// Random workloads against a model of "last block inserted per id".
    /// The budget is never exceeded, a hit is always the latest block for its
    /// id, a block that fits is always admitted, and the byte and activity
    /// accounting is exact.
    #[test]
    fn budget_never_exceeded_and_hits_never_stale(
        capacity in 0usize..=64 * 1024,
        shards in 1usize..=8,
        ops in prop::collection::vec(op(), 1..200),
    ) {
        let cache = BlockCache::with_shards(capacity, shards);
        let mut model: HashMap<u64, Arc<[u8]>> = HashMap::new();
        let (mut hits, mut misses, mut seq) = (0u64, 0u64, 0u8);
        for op in ops {
            match op {
                Op::Insert { id, len } => {
                    seq = seq.wrapping_add(1);
                    let v = block(len, seq);
                    cache.insert(id, Arc::clone(&v));
                    if cache.max_block_len().is_some_and(|max| len <= max) {
                        let got = cache.get(id);
                        hits += 1;
                        prop_assert!(got.as_deref() == Some(&v[..]), "admitted block must be served");
                    }
                    model.insert(id, v);
                }
                Op::Get(id) => match cache.get(id) {
                    Some(got) => {
                        hits += 1;
                        let want = model.get(&id);
                        prop_assert!(want.is_some_and(|w| w[..] == got[..]), "stale or foreign block for {id}");
                    }
                    None => misses += 1,
                },
                Op::Remove(id) => {
                    cache.remove(id);
                    model.remove(&id);
                    prop_assert!(cache.get(id).is_none());
                    misses += 1;
                }
                Op::Clear => {
                    cache.clear();
                    model.clear();
                }
            }
            let s = cache.stats();
            prop_assert!(s.used_bytes <= capacity, "{:?}", s);
            prop_assert!(s.entries * OVERHEAD <= s.used_bytes, "{:?}", s);
            prop_assert!(s.entries <= model.len(), "{:?}", s);
        }
        // Exact byte accounting: used == sum of charges of the blocks still cached.
        let s = cache.stats();
        let (mut used, mut present) = (0usize, 0usize);
        for (id, v) in &model {
            match cache.get(*id) {
                Some(got) => {
                    hits += 1;
                    prop_assert!(got[..] == v[..]);
                    used += charge(v.len());
                    present += 1;
                }
                None => misses += 1,
            }
        }
        prop_assert_eq!((s.used_bytes, s.entries), (used, present));
        let s = cache.stats();
        prop_assert_eq!((s.hits, s.misses), (hits, misses));
    }
}

/// Measure total get() throughput (millions of ops per second) of `threads`
/// threads doing `ops` lookups each over ids drawn by `pick`.
fn measure(
    threads: usize,
    ops: u64,
    pick: impl Fn(&mut Rng) -> u64 + Sync,
    get: impl Fn(u64) -> usize + Sync,
) -> f64 {
    let barrier = Barrier::new(threads + 1);
    let start = thread::scope(|s| {
        for t in 0..threads {
            let (barrier, pick, get) = (&barrier, &pick, &get);
            s.spawn(move || {
                let mut rng = Rng::new(0xC0FFEE + t as u64);
                barrier.wait();
                let mut sum = 0usize;
                for _ in 0..ops {
                    sum = sum.wrapping_add(get(pick(&mut rng)));
                }
                black_box(sum);
            });
        }
        barrier.wait();
        Instant::now()
    });
    (threads as u64 * ops) as f64 / start.elapsed().as_secs_f64() / 1e6
}

#[test]
#[ignore = "throughput measurement: cargo test --release --test cache -- --ignored --nocapture"]
fn read_throughput() {
    const BLOCKS: u64 = 4096;
    const BLOCK_LEN: usize = 4096;
    const OPS: u64 = 2_000_000;
    let cache = BlockCache::new(64 << 20);
    let baseline: Mutex<HashMap<u64, Arc<[u8]>>> = Mutex::new(HashMap::new());
    for id in 0..BLOCKS {
        let b: Arc<[u8]> = vec![id as u8; BLOCK_LEN].into();
        cache.insert(id, Arc::clone(&b));
        baseline.lock().unwrap().insert(id, b);
    }
    assert_eq!(cache.stats().entries, BLOCKS as usize);
    let uniform = |r: &mut Rng| r.next() % BLOCKS;
    let single = |_: &mut Rng| 7u64;
    println!(
        "get() hits, {BLOCKS} x {BLOCK_LEN} B blocks, {} shards, {OPS} ops/thread (Mops/s; noisy shared machine)",
        cache.shard_count()
    );
    println!("threads | BlockCache uniform | Mutex<HashMap> uniform | BlockCache one hot block");
    for threads in [1, 2, 4, 8, 16] {
        let sharded = measure(threads, OPS, uniform, |id| {
            cache.get(id).map_or(0, |b| b.len())
        });
        let global = measure(threads, OPS, uniform, |id| {
            baseline
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .map_or(0, |b| b.len())
        });
        let hot = measure(threads, OPS, single, |id| {
            cache.get(id).map_or(0, |b| b.len())
        });
        println!("{threads:>7} | {sharded:>18.1} | {global:>22.1} | {hot:>24.1}");
    }
    let s = cache.stats();
    assert_eq!(s.misses, 0);
    assert_eq!(s.evictions, 0);
}
