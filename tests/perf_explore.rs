//! Exploration measurements (release, ignored): component costs of the
//! small-value path on the benchmark's S3 chat messages (512 B).
//!
//! `cargo test --release --test perf_explore -- --ignored --nocapture --test-threads 1`
//!
//! Timings are means of tight loops on one thread: indicative only (the
//! reference numbers of the report come from `benches/engine.rs` and
//! `benches/codec.rs`).

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use babeldb::codec::{self, Deps, ZstdDict, lz4, zstd};
use babeldb::config::{CodecPolicy, Mode};
use babeldb::datasets::{self, Scenario};
use babeldb::format::{self, CodecTag, Manifest, ManifestBody};
use babeldb::planner::Planner;

fn msgs(from: u64, n: u64, size: usize) -> Vec<Vec<u8>> {
    (from..from + n).map(|i| datasets::value(Scenario::ChatJson, 42, i, size)).collect()
}

fn ns_per<F: FnMut(usize)>(iters: usize, mut f: F) -> f64 {
    for i in 0..iters.min(2000) {
        f(i);
    }
    let t = Instant::now();
    for i in 0..iters {
        f(i);
    }
    t.elapsed().as_nanos() as f64 / iters as f64
}

fn avg(b: &[Vec<u8>]) -> f64 {
    b.iter().map(Vec::len).sum::<usize>() as f64 / b.len() as f64
}

#[test]
#[ignore]
fn components() {
    let m = msgs(0, 4000, 512);
    let n = m.len();
    println!("blake3 512: {:.0} ns", ns_per(200_000, |i| {
        black_box(blake3::hash(black_box(&m[i % n])));
    }));
    let env: Vec<Vec<u8>> = m
        .iter()
        .map(|v| format::write_envelope(CodecTag::RAW_V1, 0, v.len() as u32, blake3::hash(v).as_bytes(), v))
        .collect();
    println!("read_envelope: {:.1} ns", ns_per(1_000_000, |i| {
        black_box(format::read_envelope(black_box(&env[i % n])).unwrap());
    }));
    let man: Vec<Vec<u8>> = env
        .iter()
        .map(|e| Manifest { revision: 7, logical_len: 512, source_id: None, body: ManifestBody::Inline(e.clone()) }.encode())
        .collect();
    println!("Manifest::decode inline: {:.1} ns", ns_per(1_000_000, |i| {
        black_box(Manifest::decode(black_box(&man[i % n])).unwrap());
    }));
    println!("to_vec 600: {:.1} ns", ns_per(1_000_000, |i| {
        black_box(black_box(&man[i % n]).to_vec());
    }));
    let eval = &m[2000..];
    let ne = eval.len();
    let lz: Vec<Vec<u8>> = eval.iter().map(|v| lz4::encode(v)).collect();
    let zs: Vec<Vec<u8>> = eval.iter().map(|v| zstd::encode(v, 3, None).unwrap()).collect();
    println!("sizes: raw 512, lz4 {:.1}, zstd3 {:.1}", avg(&lz), avg(&zs));
    println!("repeat recognize: {:.0} ns", ns_per(100_000, |i| {
        black_box(codec::repeat::recognize(&eval[i % ne], 4096));
    }));
    println!("lz4 encode: {:.0} ns", ns_per(50_000, |i| {
        black_box(lz4::encode(&eval[i % ne]));
    }));
    println!("zstd3 encode: {:.0} ns", ns_per(50_000, |i| {
        black_box(zstd::encode(&eval[i % ne], 3, None).unwrap());
    }));
    let mut out = Vec::with_capacity(1024);
    println!("lz4 decode: {:.0} ns", ns_per(200_000, |i| {
        out.clear();
        lz4::decode(&lz[i % ne], 512, &mut out).unwrap();
    }));
    println!("zstd decode: {:.0} ns", ns_per(200_000, |i| {
        out.clear();
        zstd::decode(&zs[i % ne], 512, None, &mut out).unwrap();
    }));
    println!("codec::decode raw: {:.0} ns", ns_per(500_000, |i| {
        codec::decode(CodecTag::RAW_V1, 0, &eval[i % ne], 512, Deps::default(), &mut out).unwrap();
    }));
}

/// Dictionary size and compression level: mean body, encode and decode time.
#[test]
#[ignore]
fn dictionary_levels() {
    let train = msgs(0, 2000, 512);
    let eval = msgs(10_000, 2000, 512);
    let ne = eval.len();
    let mut out = Vec::with_capacity(1024);
    for dict_len in [16usize << 10, 32 << 10, 64 << 10] {
        let t = Instant::now();
        let bytes = zstd::train_dictionary(&train, dict_len).unwrap();
        let train_ms = t.elapsed().as_secs_f64() * 1e3;
        for level in [1, 3, 6, 9, 12, 15, 19] {
            let d = ZstdDict::new(1, bytes.clone(), level).unwrap();
            let zd: Vec<Vec<u8>> = eval.iter().map(|v| zstd::encode(v, level, Some(&d)).unwrap()).collect();
            let enc = ns_per(20_000, |i| {
                black_box(zstd::encode(&eval[i % ne], level, Some(&d)).unwrap());
            });
            let dec = ns_per(100_000, |i| {
                out.clear();
                zstd::decode(&zd[i % ne], 512, Some(&d), &mut out).unwrap();
            });
            println!(
                "dict {:>6} (train {train_ms:>4.0} ms) level {level:>2}: body {:.1}, enc {enc:>6.0} ns, dec {dec:>5.0} ns",
                d.bytes().len(),
                avg(&zd)
            );
        }
    }
}

/// Whole planner per 512 B message, without and with a dictionary.
#[test]
#[ignore]
fn planner_costs() {
    let train = msgs(0, 2000, 512);
    let eval = msgs(10_000, 2000, 512);
    let ne = eval.len();
    let dict = Arc::new(ZstdDict::new(9, zstd::train_dictionary(&train, 64 << 10).unwrap(), 3).unwrap());
    for (name, p) in [
        ("raw only", Planner::new(Mode::Adaptive, CodecPolicy::raw_only())),
        ("adaptive", Planner::new(Mode::Adaptive, CodecPolicy::default())),
        ("adaptive+dict", Planner::new(Mode::Adaptive, CodecPolicy::default()).with_params(Some(dict.clone()), None)),
    ] {
        let t = ns_per(20_000, |i| {
            black_box(p.encode_unit(&eval[i % ne]));
        });
        let body: f64 = eval.iter().map(|v| p.encode_unit(v).body.len() as f64).sum::<f64>() / ne as f64;
        println!("{name:>14}: {t:>6.0} ns per message, mean body {body:.1}");
    }
}
