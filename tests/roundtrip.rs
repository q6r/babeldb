//! Exact roundtrip of every codec and of the planner, and rejection (never a
//! panic) of corrupted bodies.

use std::sync::{Arc, OnceLock};

use babeldb::codec::{
    self, Deps, Template, ZstdDict, arithmetic, babel_affine, lz4, raw, repeat, template_patch,
    zstd,
};
use babeldb::config::{CodecPolicy, DecodeCostModel, Mode};
use babeldb::format::{self, CodecTag, codec_id};
use babeldb::planner::{self, Planner, TrainOptions};
use proptest::prelude::*;

const DICT_ID: u64 = 7;
const TEMPLATE_ID: u64 = 8;

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

fn pseudo_random(seed: u64, n: usize) -> Vec<u8> {
    let mut s = seed ^ 0x9E37_79B9_7F4A_7C15;
    (0..n)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 24) as u8
        })
        .collect()
}

fn u64_seq(start: u64, step: u64, count: usize) -> Vec<u8> {
    (0..count as u64)
        .flat_map(|i| start.wrapping_add(i.wrapping_mul(step)).to_le_bytes())
        .collect()
}

/// A chat message record in JSON (snowflake-like ids, author, text).
fn chat_json(i: u64) -> Vec<u8> {
    let i = i % 1_000_000_000;
    let words = [
        "hello", "anyone", "around", "the", "deploy", "worked", "ok", "thanks", "lol", "see",
        "you", "tomorrow",
    ];
    let text: Vec<&str> = (0..(3 + i % 17))
        .map(|k| words[((i * 7 + k * 3) % 12) as usize])
        .collect();
    format!(
        r#"{{"id":"{}","channel_id":"1180000000000000123","author":{{"id":"{}","username":"user{}","global_name":null,"avatar":null}},"content":"{}","timestamp":"2026-09-27T12:{:02}:{:02}.{:03}000+00:00","edited_timestamp":null,"attachments":[],"embeds":[],"mentions":[],"pinned":false,"type":0}}"#,
        1_190_000_000_000_000_000u64 + i * 4_194_304 + i % 4096,
        1_000_000_000_000_000_000u64 + (i % 13) * 1_234_567,
        i % 13,
        text.join(" "),
        (i / 60) % 60,
        i % 60,
        (i * 37) % 1000
    )
    .into_bytes()
}

fn edge_inputs() -> Vec<(String, Vec<u8>)> {
    let mut v: Vec<(String, Vec<u8>)> = vec![
        ("empty".into(), vec![]),
        ("one zero".into(), vec![0]),
        ("one 0x7f".into(), vec![0x7f]),
        ("one 0xff".into(), vec![0xff]),
        ("all bytes".into(), (0..=255u8).collect()),
        (
            "all bytes x4".into(),
            (0..=255u8).cycle().take(1024).collect(),
        ),
        ("all bytes reversed".into(), (0..=255u8).rev().collect()),
        (
            "utf8 accents".into(),
            "ação, coração, pão de açúcar — ñ ü ß Ελληνικά 東京 🚀 "
                .repeat(30)
                .into_bytes(),
        ),
        ("utf8 short".into(), "é".as_bytes().to_vec()),
        ("zeros 4096".into(), vec![0; 4096]),
        ("zeros 65536".into(), vec![0; 65536]),
        ("ff 4096".into(), vec![0xff; 4096]),
        ("ff run in random".into(), {
            let mut d = pseudo_random(3, 3000);
            d[1000..2000].fill(0xff);
            d
        }),
        ("u64 seq".into(), u64_seq(1_000_000, 3, 512)),
        ("u64 const".into(), u64_seq(u64::MAX, 0, 64)),
        ("u64 top".into(), u64_seq(u64::MAX - 63, 1, 64)),
        (
            "period 3 partial".into(),
            b"abc".iter().cycle().take(1000).copied().collect(),
        ),
        ("period 4096".into(), pseudo_random(9, 4096).repeat(3)),
        ("period 4097".into(), pseudo_random(10, 4097).repeat(3)),
        ("chat json".into(), chat_json(42)),
        ("chat json x8".into(), (0..8).flat_map(chat_json).collect()),
    ];
    for n in [
        2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 511, 512, 513, 4095, 4096, 4097, 16384,
        65535, 65536,
    ] {
        v.push((format!("random {n}"), pseudo_random(n as u64, n)));
    }
    v
}

fn dict() -> &'static Arc<ZstdDict> {
    static D: OnceLock<Arc<ZstdDict>> = OnceLock::new();
    D.get_or_init(|| {
        let samples: Vec<Vec<u8>> = (1000..1400).map(chat_json).collect();
        let bytes = zstd::train_dictionary(&samples, 8 * 1024).unwrap();
        Arc::new(ZstdDict::new(DICT_ID, bytes, 3).unwrap())
    })
}

fn template() -> &'static Arc<Template> {
    static T: OnceLock<Arc<Template>> = OnceLock::new();
    T.get_or_init(|| Arc::new(Template::new(TEMPLATE_ID, chat_json(999))))
}

fn deps() -> Deps<'static> {
    Deps {
        zstd_dict: Some(dict().as_ref()),
        template: Some(template().as_ref()),
    }
}

fn decode_with(
    codec: CodecTag,
    aux_id: u64,
    body: &[u8],
    raw_len: usize,
    deps: Deps<'_>,
) -> babeldb::Result<Vec<u8>> {
    let mut out = vec![0xEE; 3];
    codec::decode(codec, aux_id, body, raw_len as u32, deps, &mut out)?;
    Ok(out)
}

fn assert_roundtrip(name: &str, codec: CodecTag, aux_id: u64, body: &[u8], data: &[u8]) {
    let out = decode_with(codec, aux_id, body, data.len(), deps())
        .unwrap_or_else(|e| panic!("{name}: {e}"));
    assert!(out == data, "{name}: {} roundtrip differs", codec.name());
}

// ---------------------------------------------------------------------------
// Per-codec roundtrips
// ---------------------------------------------------------------------------

#[test]
fn raw_and_babel_roundtrip_every_input() {
    for (name, data) in edge_inputs() {
        assert_roundtrip(&name, CodecTag::RAW_V1, 0, &raw::encode(&data), &data);
        let seed = babel_affine::encode(&data);
        assert_eq!(seed.len(), data.len());
        assert_roundtrip(&name, CodecTag::BABEL_AFFINE_V1, 0, &seed, &data);
    }
}

#[test]
fn lz4_and_zstd_roundtrip_every_input() {
    for (name, data) in edge_inputs() {
        assert_roundtrip(&name, CodecTag::LZ4_V1, 0, &lz4::encode(&data), &data);
        for level in [-5, 1, 3, 19] {
            let body = zstd::encode(&data, level, None).unwrap();
            assert_roundtrip(&name, CodecTag::ZSTD_V1, 0, &body, &data);
        }
        let body = zstd::encode(&data, 3, Some(dict())).unwrap();
        assert_roundtrip(&name, CodecTag::ZSTD_V1, DICT_ID, &body, &data);
    }
}

#[test]
fn recipes_roundtrip_when_recognized() {
    let mut recognized = 0;
    for (name, data) in edge_inputs() {
        if let Some(body) = repeat::recognize(&data, 4096) {
            assert!(body.len() < data.len());
            assert_roundtrip(&name, CodecTag::REPEAT_V1, 0, &body, &data);
            recognized += 1;
        }
        if let Some(body) = arithmetic::recognize(&data) {
            assert_eq!(body.len(), 24);
            assert_roundtrip(&name, CodecTag::ARITH_U64_V1, 0, &body, &data);
            recognized += 1;
        }
    }
    assert!(recognized >= 8, "{recognized}");
    assert!(repeat::recognize(&pseudo_random(10, 4097).repeat(3), 4096).is_none());
    assert!(repeat::recognize(&pseudo_random(9, 4096).repeat(3), 4096).is_some());
}

#[test]
fn repeat_every_period_and_partial_tail() {
    for period in 1..=40usize {
        let motif = pseudo_random(period as u64, period);
        for n in [period + 5, 2 * period + 5, 3 * period + 7, 257] {
            if n <= period + 4 {
                continue;
            }
            let data: Vec<u8> = motif.iter().cycle().take(n).copied().collect();
            let body = repeat::recognize(&data, 4096).expect("periodic input");
            let found = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
            assert!(
                found <= period && period % found == 0,
                "period {period} found {found}"
            );
            assert_roundtrip("periodic", CodecTag::REPEAT_V1, 0, &body, &data);
        }
    }
}

#[test]
fn arithmetic_edges() {
    for (start, step, count) in [
        (0u64, 0u64, 4usize),
        (0, 1, 4),
        (u64::MAX - 3, 1, 4),
        (5, u64::MAX / 4, 4),
        (1 << 63, 1 << 60, 8),
        (123, 456, 2048),
    ] {
        let data = u64_seq(start, step, count);
        let body = arithmetic::recognize(&data).expect("arithmetic sequence");
        assert_roundtrip("arith", CodecTag::ARITH_U64_V1, 0, &body, &data);
    }
    assert!(
        arithmetic::recognize(&u64_seq(0, 1, 3)).is_none(),
        "too short"
    );
    assert!(
        arithmetic::recognize(&u64_seq(u64::MAX - 1, 1, 4)).is_none(),
        "wraps"
    );
    assert!(
        arithmetic::recognize(&u64_seq(10, u64::MAX, 4)).is_none(),
        "decreasing"
    );
    let mut bumpy = u64_seq(0, 5, 10);
    bumpy[40] ^= 1;
    assert!(arithmetic::recognize(&bumpy).is_none());
    assert!(
        arithmetic::recognize(&[0u8; 33]).is_none(),
        "not a multiple of 8"
    );
}

#[test]
fn template_patch_roundtrip() {
    let t = template();
    let mut patched = 0;
    for i in 0..300 {
        let data = chat_json(i);
        if let Some(body) = template_patch::encode(&data, t) {
            assert!(body.len() < data.len());
            assert_roundtrip(
                "chat",
                CodecTag::TEMPLATE_PATCH_V1,
                TEMPLATE_ID,
                &body,
                &data,
            );
            patched += 1;
        }
    }
    assert!(patched >= 290, "{patched}");
    for (name, data) in edge_inputs() {
        if let Some(body) = template_patch::encode(&data, t) {
            assert_roundtrip(
                &name,
                CodecTag::TEMPLATE_PATCH_V1,
                TEMPLATE_ID,
                &body,
                &data,
            );
        }
    }
    // Degenerate templates.
    for bytes in [vec![], vec![1, 2, 3], vec![0; 100_000]] {
        let t = Template::new(TEMPLATE_ID, bytes);
        for (name, data) in edge_inputs() {
            if let Some(body) = template_patch::encode(&data, &t) {
                let mut out = Vec::new();
                template_patch::decode(&body, data.len() as u32, &t, &mut out).unwrap();
                assert!(out == data, "{name}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Corrupted bodies
// ---------------------------------------------------------------------------

fn expect_err(what: &str, codec: CodecTag, aux_id: u64, body: &[u8], raw_len: usize) {
    let res = decode_with(codec, aux_id, body, raw_len, deps());
    assert!(res.is_err(), "{what}: accepted {:?}", res.map(|v| v.len()));
}

#[test]
fn corrupted_recipes_are_rejected() {
    let rep = CodecTag::REPEAT_V1;
    expect_err("empty body", rep, 0, &[], 10);
    expect_err("truncated header", rep, 0, &[1, 0, 0], 10);
    expect_err("period 0", rep, 0, &[0, 0, 0, 0], 10);
    expect_err(
        "motif shorter than period",
        rep,
        0,
        &[3, 0, 0, 0, b'a', b'b'],
        10,
    );
    expect_err(
        "motif longer than period",
        rep,
        0,
        &[1, 0, 0, 0, b'a', b'b'],
        10,
    );
    expect_err("raw_len 0", rep, 0, &[1, 0, 0, 0, b'a'], 0);
    expect_err(
        "period > raw_len",
        rep,
        0,
        &[3, 0, 0, 0, b'a', b'b', b'c'],
        2,
    );
    expect_err("huge period", rep, 0, &[0xff, 0xff, 0xff, 0xff, b'a'], 10);

    let ari = CodecTag::ARITH_U64_V1;
    let body = |start: u64, step: u64, count: u64| -> Vec<u8> {
        [start, step, count]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect()
    };
    expect_err("short body", ari, 0, &body(0, 1, 4)[..23], 32);
    expect_err(
        "long body",
        ari,
        0,
        &[&body(0, 1, 4)[..], &[0][..]].concat(),
        32,
    );
    expect_err("count mismatch", ari, 0, &body(0, 1, 4), 40);
    expect_err("raw_len not multiple of 8", ari, 0, &body(0, 1, 4), 31);
    expect_err("count * 8 overflows", ari, 0, &body(0, 1, u64::MAX / 4), 8);
    expect_err("sequence overflows", ari, 0, &body(u64::MAX - 2, 1, 4), 32);
    expect_err("step overflows", ari, 0, &body(0, u64::MAX / 2, 4), 32);
    assert!(
        decode_with(ari, 0, &body(9, 9, 0), 0, deps())
            .unwrap()
            .is_empty()
    );

    let raw_tag = CodecTag::RAW_V1;
    expect_err("raw too short", raw_tag, 0, b"abc", 4);
    expect_err("raw too long", raw_tag, 0, b"abc", 2);

    let babel = CodecTag::BABEL_AFFINE_V1;
    expect_err("babel too short", babel, 0, b"abc", 4);
    expect_err("babel too long", babel, 0, b"abc", 2);
}

#[test]
fn corrupted_compressed_bodies_are_rejected() {
    let data = chat_json(5);
    let n = data.len();

    let lz = lz4::encode(&data);
    expect_err("lz4 truncated", CodecTag::LZ4_V1, 0, &lz[..lz.len() - 3], n);
    expect_err("lz4 wrong raw_len (short)", CodecTag::LZ4_V1, 0, &lz, n - 1);
    expect_err("lz4 wrong raw_len (long)", CodecTag::LZ4_V1, 0, &lz, n + 1);
    expect_err("lz4 garbage", CodecTag::LZ4_V1, 0, &[0xff; 40], n);
    expect_err("lz4 empty", CodecTag::LZ4_V1, 0, &[], 1);
    expect_err("lz4 bomb", CodecTag::LZ4_V1, 0, &[0x1f, 0, 1, 0], 1 << 20);

    let zs = zstd::encode(&data, 3, None).unwrap();
    expect_err(
        "zstd truncated",
        CodecTag::ZSTD_V1,
        0,
        &zs[..zs.len() - 1],
        n,
    );
    expect_err(
        "zstd trailing byte",
        CodecTag::ZSTD_V1,
        0,
        &[&zs[..], &[0][..]].concat(),
        n,
    );
    expect_err(
        "zstd two frames",
        CodecTag::ZSTD_V1,
        0,
        &[&zs[..], &zs[..]].concat(),
        2 * n,
    );
    expect_err("zstd wrong raw_len", CodecTag::ZSTD_V1, 0, &zs, n + 1);
    expect_err(
        "zstd garbage",
        CodecTag::ZSTD_V1,
        0,
        &pseudo_random(1, 64),
        n,
    );
    expect_err("zstd empty", CodecTag::ZSTD_V1, 0, &[], 0);
    let mut flipped = zs.clone();
    let last = flipped.len() - 2;
    flipped[last] ^= 0x55;
    if let Ok(out) = decode_with(CodecTag::ZSTD_V1, 0, &flipped, n, deps()) {
        assert_eq!(out.len(), n);
    }

    let zd = zstd::encode(&data, 3, Some(dict())).unwrap();
    let no_dict = Deps {
        zstd_dict: None,
        template: None,
    };
    assert!(matches!(
        decode_with(CodecTag::ZSTD_V1, DICT_ID, &zd, n, no_dict),
        Err(babeldb::Error::MissingDependency { param_id: DICT_ID })
    ));
    assert!(matches!(
        decode_with(CodecTag::ZSTD_V1, DICT_ID + 1, &zd, n, deps()),
        Err(babeldb::Error::MissingDependency { .. })
    ));
    expect_err(
        "dict frame decoded without dict",
        CodecTag::ZSTD_V1,
        0,
        &zd,
        n,
    );
    let other = ZstdDict::new(
        DICT_ID,
        (0..2000u32).flat_map(|i| i.to_le_bytes()).collect(),
        3,
    )
    .unwrap();
    let wrong = Deps {
        zstd_dict: Some(&other),
        template: None,
    };
    assert!(
        decode_with(CodecTag::ZSTD_V1, DICT_ID, &zd, n, wrong)
            .is_err_and(|e| !matches!(e, babeldb::Error::MissingDependency { .. }))
    );

    let tp = template_patch::encode(&data, template()).unwrap();
    expect_err(
        "template truncated",
        CodecTag::TEMPLATE_PATCH_V1,
        TEMPLATE_ID,
        &tp[..tp.len() - 1],
        n,
    );
    expect_err(
        "template wrong raw_len",
        CodecTag::TEMPLATE_PATCH_V1,
        TEMPLATE_ID,
        &tp,
        n + 1,
    );
    expect_err(
        "template bad varint",
        CodecTag::TEMPLATE_PATCH_V1,
        TEMPLATE_ID,
        &[0x81, 0x80, 0x00],
        n,
    );
    expect_err(
        "template copy past end",
        CodecTag::TEMPLATE_PATCH_V1,
        TEMPLATE_ID,
        &[0x03, 0xfe, 0xff, 0x03],
        5,
    );
    expect_err(
        "template huge copy",
        CodecTag::TEMPLATE_PATCH_V1,
        TEMPLATE_ID,
        &[0xff; 10],
        n,
    );
    assert!(matches!(
        decode_with(CodecTag::TEMPLATE_PATCH_V1, TEMPLATE_ID + 1, &tp, n, deps()),
        Err(babeldb::Error::MissingDependency { .. })
    ));

    assert!(matches!(
        decode_with(
            CodecTag {
                id: codec_id::RAW,
                version: 2
            },
            0,
            b"x",
            1,
            deps()
        ),
        Err(babeldb::Error::UnknownCodec { .. })
    ));
    expect_err(
        "raw_len above MAX_UNIT_LEN",
        CodecTag::RAW_V1,
        0,
        b"",
        format::MAX_UNIT_LEN as usize + 1,
    );
}

#[test]
fn zstd_dictionary_validation() {
    assert!(ZstdDict::new(1, vec![], 3).is_err());
    // zstd dictionary magic followed by garbage: rejected, not a panic.
    let mut fake = vec![0x37, 0xA4, 0x30, 0xEC];
    fake.extend_from_slice(&pseudo_random(4, 300));
    let _ = ZstdDict::new(1, fake, 3);
    assert!(matches!(
        zstd::train_dictionary(&[b"tiny".to_vec()], 4096),
        Err(babeldb::Error::InvalidArgument(_))
    ));
    assert!(matches!(
        zstd::train_dictionary::<Vec<u8>>(&[], 4096),
        Err(babeldb::Error::InvalidArgument(_))
    ));
    assert!(matches!(
        zstd::train_dictionary(&[vec![1u8; 100]], 10),
        Err(babeldb::Error::InvalidArgument(_))
    ));
}

// ---------------------------------------------------------------------------
// Random bodies never panic
// ---------------------------------------------------------------------------

const ALL_CODECS: [(CodecTag, u64); 8] = [
    (CodecTag::RAW_V1, 0),
    (CodecTag::REPEAT_V1, 0),
    (CodecTag::ARITH_U64_V1, 0),
    (CodecTag::LZ4_V1, 0),
    (CodecTag::ZSTD_V1, 0),
    (CodecTag::ZSTD_V1, DICT_ID),
    (CodecTag::BABEL_AFFINE_V1, 0),
    (CodecTag::TEMPLATE_PATCH_V1, TEMPLATE_ID),
];

fn encode_with(codec: CodecTag, aux_id: u64, data: &[u8]) -> Option<Vec<u8>> {
    match codec.id {
        codec_id::RAW => Some(raw::encode(data)),
        codec_id::REPEAT => repeat::recognize(data, 4096),
        codec_id::ARITH_U64 => arithmetic::recognize(data),
        codec_id::LZ4 => Some(lz4::encode(data)),
        codec_id::ZSTD => zstd::encode(data, 3, (aux_id != 0).then(|| dict().as_ref())).ok(),
        codec_id::BABEL_AFFINE => Some(babel_affine::encode(data)),
        codec_id::TEMPLATE_PATCH => template_patch::encode(data, template()),
        _ => None,
    }
}

fn mutate(mut body: Vec<u8>, how: u8, at: prop::sample::Index, byte: u8) -> Vec<u8> {
    match how {
        0 if !body.is_empty() => {
            let i = at.index(body.len());
            body[i] ^= byte | 1;
        }
        1 => body.truncate(at.index(body.len() + 1)),
        2 => body.push(byte),
        3 if !body.is_empty() => {
            let i = at.index(body.len());
            body.remove(i);
        }
        _ => {}
    }
    body
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn random_bodies_never_panic(
        which in 0..ALL_CODECS.len(),
        body in prop::collection::vec(any::<u8>(), 0..300),
        raw_len in prop_oneof![0u32..64, 0u32..70_000],
    ) {
        let (codec, aux) = ALL_CODECS[which];
        if let Ok(out) = decode_with(codec, aux, &body, raw_len as usize, deps()) {
            prop_assert_eq!(out.len(), raw_len as usize);
        }
    }

    #[test]
    fn mutated_bodies_never_panic(
        which in 0..ALL_CODECS.len(),
        seed in any::<u64>(),
        len in 0usize..3000,
        how in 0u8..4,
        at in any::<prop::sample::Index>(),
        byte in any::<u8>(),
        len_delta in prop_oneof![Just(0i64), -2i64..=2],
    ) {
        let (codec, aux) = ALL_CODECS[which];
        let data: Vec<u8> = if seed % 3 == 0 {
            chat_json(seed).into_iter().take(len).collect()
        } else if seed % 3 == 1 {
            b"abcabcabd".iter().cycle().take(len).copied().collect()
        } else {
            u64_seq(seed, seed >> 40, len / 8)
        };
        if let Some(body) = encode_with(codec, aux, &data) {
            let body = mutate(body, how, at, byte);
            let raw_len = (data.len() as i64 + len_delta).max(0) as usize;
            if let Ok(out) = decode_with(codec, aux, &body, raw_len, deps()) {
                prop_assert_eq!(out.len(), raw_len);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Planner exactness
// ---------------------------------------------------------------------------

fn planners() -> Vec<(&'static str, Planner)> {
    let budget = CodecPolicy {
        decode_budget_ns: Some(2_000),
        cost_model: DecodeCostModel {
            entries: vec![(codec_id::ZSTD, 1_000.0, 1.0), (codec_id::LZ4, 100.0, 0.05)],
        },
        ..CodecPolicy::default()
    };
    vec![
        (
            "babel-pure",
            Planner::new(Mode::BabelPure, CodecPolicy::default()),
        ),
        (
            "adaptive",
            Planner::new(Mode::Adaptive, CodecPolicy::default()),
        ),
        (
            "adaptive+deps",
            Planner::new(Mode::Adaptive, CodecPolicy::default())
                .with_params(Some(dict().clone()), Some(template().clone())),
        ),
        (
            "adaptive+dict level 1",
            Planner::new(
                Mode::Adaptive,
                CodecPolicy {
                    zstd_level: 1,
                    ..CodecPolicy::default()
                },
            )
            .with_params(Some(dict().clone()), None),
        ),
        (
            "adaptive+template",
            Planner::new(Mode::Adaptive, CodecPolicy::default())
                .with_params(None, Some(template().clone())),
        ),
        (
            "adaptive budget",
            Planner::new(Mode::Adaptive, budget)
                .with_params(Some(dict().clone()), Some(template().clone())),
        ),
        (
            "raw only",
            Planner::new(Mode::Adaptive, CodecPolicy::raw_only())
                .with_params(Some(dict().clone()), None),
        ),
    ]
}

fn check_planner(name: &str, p: &Planner, data: &[u8]) -> CodecTag {
    let enc = p.encode_unit(data);
    let digest = [0u8; 32];
    let env = format::write_envelope(enc.codec, enc.aux_id, data.len() as u32, &digest, &enc.body);
    let (h, body) = format::read_envelope(&env).unwrap();
    let mut out = Vec::new();
    codec::decode(h.codec, h.aux_id, body, h.raw_len, p.deps(), &mut out)
        .unwrap_or_else(|e| panic!("{name}: {e}"));
    assert!(
        out == data,
        "{name}: planner output differs ({})",
        enc.codec.name()
    );
    match p.mode() {
        Mode::BabelPure => assert_eq!(enc.codec, CodecTag::BABEL_AFFINE_V1),
        Mode::Adaptive => {
            assert!(enc.body.len() <= data.len(), "{name}: body larger than raw");
            if enc.codec != CodecTag::RAW_V1 {
                assert!(
                    enc.body.len() < data.len(),
                    "{name}: non-raw body not smaller"
                );
            }
        }
    }
    assert_eq!(
        codec::required_param(enc.codec, enc.aux_id).is_some(),
        enc.aux_id != 0,
        "{name}"
    );
    enc.codec
}

#[test]
fn planner_edge_inputs_all_modes() {
    let planners = planners();
    for (name, data) in edge_inputs() {
        for (pname, p) in &planners {
            check_planner(&format!("{pname}/{name}"), p, &data);
        }
    }
    for (pname, p) in &planners {
        assert_eq!(p.snapshot().roundtrip_failures, 0, "{pname}");
    }
}

#[test]
fn planner_picks_expected_codecs() {
    let p = Planner::new(Mode::Adaptive, CodecPolicy::default())
        .with_params(Some(dict().clone()), Some(template().clone()));
    assert_eq!(p.encode_unit(&[0u8; 4096]).codec, CodecTag::REPEAT_V1);
    assert_eq!(
        p.encode_unit(&u64_seq(7, 9, 512)).codec,
        CodecTag::ARITH_U64_V1
    );
    assert_eq!(
        p.encode_unit(&pseudo_random(1, 4096)).codec,
        CodecTag::RAW_V1
    );
    assert_eq!(p.encode_unit(b"tiny").codec, CodecTag::RAW_V1);
    let chat = p.encode_unit(&chat_json(123_456));
    assert!(
        chat.codec == CodecTag::TEMPLATE_PATCH_V1 || chat.aux_id == DICT_ID,
        "{:?}",
        chat.codec
    );
    let pure = Planner::new(Mode::BabelPure, CodecPolicy::default());
    assert_eq!(pure.encode_unit(&[0u8; 4096]).body.len(), 4096);
    let snap = p.snapshot();
    assert_eq!(snap.chosen.len(), 7);
    assert_eq!(snap.chosen.iter().map(|(_, n)| n).sum::<u64>(), 5);
}

fn structured_input() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        prop::collection::vec(any::<u8>(), 0..2048),
        (prop::collection::vec(any::<u8>(), 1..24), 0usize..5000).prop_map(|(m, n)| m
            .iter()
            .cycle()
            .take(n)
            .copied()
            .collect()),
        (any::<u64>(), any::<u64>(), 0usize..400).prop_map(|(s, st, c)| u64_seq(
            s,
            st >> (st % 64),
            c
        )),
        (any::<u64>(), 0usize..6).prop_map(|(i, k)| (0..k as u64)
            .flat_map(|j| chat_json(i.wrapping_add(j) % 1_000_000))
            .collect()),
        (any::<u64>(), any::<prop::sample::Index>(), any::<u8>()).prop_map(|(i, at, b)| {
            let mut d = chat_json(i % 1_000_000);
            let k = at.index(d.len());
            d[k] = b;
            d.truncate(at.index(d.len() + 1).max(k));
            d
        }),
        prop::collection::vec(prop_oneof![Just(0u8), Just(0xffu8), any::<u8>()], 0..700),
        (0usize..20_000).prop_map(|n| vec![0u8; n]),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 400, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn planner_is_exact(data in structured_input()) {
        for (name, p) in planners() {
            check_planner(name, &p, &data);
            prop_assert_eq!(p.snapshot().roundtrip_failures, 0);
        }
    }
}

// ---------------------------------------------------------------------------
// Training helpers
// ---------------------------------------------------------------------------

#[test]
fn train_zstd_dictionary_reports_gain() {
    let samples: Vec<Vec<u8>> = (0..600).map(chat_json).collect();
    let opts = TrainOptions {
        max_dict_bytes: 16 * 1024,
        ..TrainOptions::default()
    };
    let (dict_bytes, report) = planner::train_zstd_dictionary(&samples, 3, &opts).unwrap();
    assert_eq!(report.kind, "zstd_dict");
    assert_eq!(
        (report.train_samples, report.validation_samples),
        (480, 120)
    );
    assert!(
        report.validation_bytes_with < report.validation_bytes_without,
        "{report:?}"
    );
    assert!(report.projected_net_gain > 0, "{report:?}");
    assert!(!report.installed && report.param_id.is_none());
    let bytes = dict_bytes.expect("gain > 0");
    assert_eq!(bytes.len(), report.param_bytes);
    assert!(bytes.len() <= 16 * 1024);

    let few = TrainOptions {
        expected_uses: 1,
        ..opts.clone()
    };
    let (none, report) = planner::train_zstd_dictionary(&samples, 3, &few).unwrap();
    assert!(
        none.is_none() && report.projected_net_gain <= 0,
        "{report:?}"
    );
    let forced = TrainOptions {
        require_gain: false,
        ..few
    };
    assert!(
        planner::train_zstd_dictionary(&samples, 3, &forced)
            .unwrap()
            .0
            .is_some()
    );

    assert!(matches!(
        planner::train_zstd_dictionary(&samples[..3], 3, &opts),
        Err(babeldb::Error::InvalidArgument(_))
    ));
}

#[test]
fn train_template_reports_gain() {
    let samples: Vec<Vec<u8>> = (0..300).map(chat_json).collect();
    let (t, report) = planner::train_template(&samples, &TrainOptions::default()).unwrap();
    assert_eq!(report.kind, "template");
    assert_eq!((report.train_samples, report.validation_samples), (240, 60));
    assert!(
        report.validation_bytes_with < report.validation_bytes_without,
        "{report:?}"
    );
    assert!(report.projected_net_gain > 0, "{report:?}");
    let t = t.expect("gain > 0");
    assert_eq!(t.len(), report.param_bytes);
    assert!(samples.contains(&t));

    let random: Vec<Vec<u8>> = (0..50).map(|i| pseudo_random(i, 300)).collect();
    let (none, report) = planner::train_template(&random, &TrainOptions::default()).unwrap();
    assert!(none.is_none(), "{report:?}");
    assert!(report.projected_net_gain <= 0);

    let tiny: Vec<Vec<u8>> = vec![vec![1], vec![2], vec![3]];
    let (none, report) = planner::train_template(&tiny, &TrainOptions::default()).unwrap();
    assert!(none.is_none() && report.param_bytes == 0);
    assert!(planner::train_template(&tiny[..1], &TrainOptions::default()).is_err());
}
