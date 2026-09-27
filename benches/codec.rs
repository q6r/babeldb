//! Criterion microbenchmarks of isolated codecs and of the Adaptive planner.
//!
//! Inputs (built inline, deterministic): zeros, repeated text, a u64
//! arithmetic sequence, chat-like JSON records and pseudo-random bytes, at
//! 512 B (chat-message size), 4 KiB and 64 KiB.
//!
//! Groups:
//! - `encode/<codec>/<input>/<size>`: body production (recognizers are
//!   measured on every input, since the planner runs them on every unit), and
//!   `encode/planner/...`: the whole Adaptive selection with its roundtrip check;
//! - `decode/<codec>/<input>/<size>`: `codec::decode` of an existing body, the
//!   numbers behind `planner::measured_cost_model`;
//! - `zstd_ctx/...`: thread-local context reuse versus a fresh context per
//!   call (`zstd::bulk`).
//!
//! Quick pass: `cargo bench --bench codec -- --warm-up-time 0.2
//! --measurement-time 0.5 --output-format bencher`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use babeldb::codec::{
    self, Deps, Template, ZstdDict, arithmetic, babel_affine, lz4, raw, repeat, template_patch,
    zstd,
};
use babeldb::config::{CodecPolicy, Mode};
use babeldb::format::CodecTag;
use babeldb::planner::Planner;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const SIZES: [usize; 3] = [512, 4096, 65536];
const LEVEL: i32 = 3;
const DICT_ID: u64 = 1;
const TEMPLATE_ID: u64 = 2;

fn chat_json(i: u64) -> Vec<u8> {
    let words = [
        "hello", "anyone", "around", "the", "deploy", "worked", "ok", "thanks", "lol", "see",
        "you", "tomorrow", "ship", "it", "why", "is", "prod", "slow",
    ];
    let text: Vec<&str> = (0..(4 + i % 23))
        .map(|k| words[((i * 7 + k * 5 + k * k) % words.len() as u64) as usize])
        .collect();
    format!(
        r#"{{"id":"{}","channel_id":"1180000000000000123","author":{{"id":"{}","username":"user{}","global_name":null,"avatar":null}},"content":"{}","timestamp":"2026-09-27T12:{:02}:{:02}.{:03}000+00:00","edited_timestamp":null,"attachments":[],"embeds":[],"mentions":[],"pinned":false,"type":0}}"#,
        1_190_000_000_000_000_000u64 + i * 4_194_304 + i % 4096,
        1_000_000_000_000_000_000u64 + (i % 29) * 1_234_567,
        i % 29,
        text.join(" "),
        (i / 60) % 60,
        i % 60,
        (i * 37) % 1000
    )
    .into_bytes()
}

fn pseudo_random(n: usize) -> Vec<u8> {
    let mut s = 0x2545_F491_4F6C_DD1Du64;
    (0..n)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 24) as u8
        })
        .collect()
}

/// (name, bytes) of every input at `size` bytes.
fn inputs(size: usize) -> Vec<(&'static str, Vec<u8>)> {
    let text = b"The Library of Babel contains every book; finding one is the hard part. ";
    let chat = (1_000u64..).flat_map(|i| {
        let mut m = chat_json(i);
        m.push(b'\n');
        m
    });
    vec![
        ("zeros", vec![0u8; size]),
        ("text", text.iter().cycle().take(size).copied().collect()),
        (
            "u64seq",
            (0..(size / 8) as u64)
                .flat_map(|i| (1_000_000 + 3 * i).to_le_bytes())
                .collect(),
        ),
        ("chat", chat.take(size).collect()),
        ("random", pseudo_random(size)),
    ]
}

fn dict() -> Arc<ZstdDict> {
    let samples: Vec<Vec<u8>> = (0..2000).map(chat_json).collect();
    let bytes = zstd::train_dictionary(&samples, 16 * 1024).expect("dictionary training");
    Arc::new(ZstdDict::new(DICT_ID, bytes, LEVEL).expect("dictionary"))
}

fn template() -> Arc<Template> {
    Arc::new(Template::new(TEMPLATE_ID, chat_json(0)))
}

/// Encoder under test; `None` = the codec does not apply to the input.
type Encoder = Box<dyn Fn(&[u8]) -> Option<Vec<u8>>>;

fn enc(f: impl Fn(&[u8]) -> Option<Vec<u8>> + 'static) -> Encoder {
    Box::new(f)
}

fn encoders(
    dict: &Arc<ZstdDict>,
    template: &Arc<Template>,
) -> Vec<(&'static str, CodecTag, u64, Encoder)> {
    let (d, t) = (dict.clone(), template.clone());
    vec![
        ("raw", CodecTag::RAW_V1, 0, enc(|x| Some(raw::encode(x)))),
        (
            "babel_affine",
            CodecTag::BABEL_AFFINE_V1,
            0,
            enc(|x| Some(babel_affine::encode(x))),
        ),
        (
            "repeat",
            CodecTag::REPEAT_V1,
            0,
            enc(|x| repeat::recognize(x, 4096)),
        ),
        (
            "arith",
            CodecTag::ARITH_U64_V1,
            0,
            enc(arithmetic::recognize),
        ),
        ("lz4", CodecTag::LZ4_V1, 0, enc(|x| Some(lz4::encode(x)))),
        (
            "zstd",
            CodecTag::ZSTD_V1,
            0,
            enc(|x| zstd::encode(x, LEVEL, None).ok()),
        ),
        (
            "zstd_dict",
            CodecTag::ZSTD_V1,
            DICT_ID,
            enc(move |x| zstd::encode(x, LEVEL, Some(&d)).ok()),
        ),
        (
            "template",
            CodecTag::TEMPLATE_PATCH_V1,
            TEMPLATE_ID,
            enc(move |x| template_patch::encode(x, &t)),
        ),
    ]
}

fn bench_encode(c: &mut Criterion) {
    let (dict, template) = (dict(), template());
    let encoders = encoders(&dict, &template);
    let planner = Planner::new(Mode::Adaptive, CodecPolicy::default())
        .with_params(Some(dict.clone()), Some(template.clone()));
    let mut group = c.benchmark_group("encode");
    for size in SIZES {
        group.throughput(Throughput::Bytes(size as u64));
        for (input, data) in inputs(size) {
            let id = format!("{input}/{size}");
            for (name, _, _, f) in &encoders {
                group.bench_with_input(BenchmarkId::new(*name, &id), &data, |b, d| {
                    b.iter(|| f(black_box(d)))
                });
            }
            let chosen = planner.encode_unit(&data);
            println!(
                "planner {id}: {} body {} bytes",
                chosen.codec.name(),
                chosen.body.len()
            );
            group.bench_with_input(BenchmarkId::new("planner", &id), &data, |b, d| {
                b.iter(|| planner.encode_unit(black_box(d)))
            });
        }
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let (dict, template) = (dict(), template());
    let deps = Deps {
        zstd_dict: Some(&dict),
        template: Some(&template),
    };
    let encoders = encoders(&dict, &template);
    let mut group = c.benchmark_group("decode");
    for size in SIZES {
        group.throughput(Throughput::Bytes(size as u64));
        for (input, data) in inputs(size) {
            let id = format!("{input}/{size}");
            for (name, tag, aux, f) in &encoders {
                let Some(body) = f(&data) else { continue };
                let mut out = Vec::with_capacity(size);
                codec::decode(*tag, *aux, &body, size as u32, deps, &mut out).expect("roundtrip");
                assert!(out == data, "{name}/{id}");
                println!("body {name}/{id}: {} bytes", body.len());
                group.bench_with_input(BenchmarkId::new(*name, &id), &body, |b, body| {
                    b.iter(|| {
                        codec::decode(*tag, *aux, black_box(body), size as u32, deps, &mut out)
                            .unwrap();
                        out.len()
                    })
                });
            }
        }
    }
    group.finish();
}

/// Thread-local context reuse (`codec::zstd`) versus `zstd::bulk`, which
/// creates a context per call.
fn bench_zstd_contexts(c: &mut Criterion) {
    let mut group = c.benchmark_group("zstd_ctx");
    for size in [512usize, 4096] {
        let data: Vec<u8> = inputs(size).swap_remove(3).1;
        let id = format!("chat/{size}");
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("encode_reused", &id), &data, |b, d| {
            b.iter(|| zstd::encode(black_box(d), LEVEL, None).unwrap())
        });
        group.bench_with_input(BenchmarkId::new("encode_fresh", &id), &data, |b, d| {
            b.iter(|| ::zstd::bulk::compress(black_box(d), LEVEL).unwrap())
        });
        let body = zstd::encode(&data, LEVEL, None).unwrap();
        let mut out = Vec::with_capacity(size);
        group.bench_with_input(BenchmarkId::new("decode_reused", &id), &body, |b, body| {
            b.iter(|| {
                out.clear();
                zstd::decode(black_box(body), size as u32, None, &mut out).unwrap();
                out.len()
            })
        });
        group.bench_with_input(BenchmarkId::new("decode_fresh", &id), &body, |b, body| {
            b.iter(|| ::zstd::bulk::decompress(black_box(body), size).unwrap())
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(30);
    targets = bench_encode, bench_decode, bench_zstd_contexts
}
criterion_main!(benches);
