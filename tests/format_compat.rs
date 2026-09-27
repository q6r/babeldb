//! Persistent format v1 compatibility tests. The normative text is `docs/format.md`.
//!
//! * Golden byte vectors. Every literal below was written by hand, field by field,
//!   from the documented layout (and cross-checked against an independent Python
//!   implementation of the layouts). Each golden is compared with the encoder
//!   output *and* decoded back into the expected value, so a change in either
//!   direction is caught.
//! * Rejections: malformed input always yields an `Err`, never a panic.
//! * Property tests: roundtrips, canonical (unique) encodings, no panic on
//!   arbitrary or mutated bytes, history key ordering.
//! * Codec dispatch: unknown codec and missing dependency errors.
//! * An independent table-level validator (every table decodes with `format`,
//!   refcounts follow the object lifetime rule, ...), exercised on `MemStore`.
//! * Frozen databases in `tests/data`: `generate_v1_fixtures` (ignored; run it
//!   only to regenerate them) writes them; `open_v1_fixtures` checks them forever.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use babeldb::codec::{self, Deps};
use babeldb::config::{MAX_BLOCK_SIZE, MIN_BLOCK_SIZE};
use babeldb::error::{Error, Result};
use babeldb::format::{
    self, ChunkRef, CodecTag, EnvelopeHeader, ImportInfo, MAX_PARAMS_LEN, MAX_UNIT_LEN, Manifest,
    ManifestBody, Param, SourceDescriptor, candidate_key, codec_id, decode_id_list, encode_id_list,
    get_varint, history_key, history_prefix, id_key, meta_key, param_kind, parse_history_key,
    parse_id_key, put_varint, read_envelope, source_kind, write_envelope,
};
use babeldb::generator::{self, ids as gen_ids};
use babeldb::ingest::ByteSource;
use babeldb::planner::TrainOptions;
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};
use babeldb::{Config, Db, Expect, ImportOptions, MemStore, Mode};
use proptest::prelude::*;

// ===========================================================================
// Helpers
// ===========================================================================

/// Bytes of a hex literal; ASCII whitespace is ignored.
fn hx(s: &str) -> Vec<u8> {
    let digits: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    assert!(
        digits.len().is_multiple_of(2),
        "odd number of hex digits in {s:?}"
    );
    digits
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

/// Concatenation of annotated hex fields.
fn hxs(fields: &[&str]) -> Vec<u8> {
    hx(&fields.concat())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").unwrap();
    }
    s
}

fn digest_of(hex: &str) -> [u8; 32] {
    hx(hex).try_into().expect("a digest is 32 bytes")
}

fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Deterministic high-entropy bytes: the unkeyed BLAKE3 XOF of `label`.
fn xof(label: &[u8], len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let mut h = blake3::Hasher::new();
    h.update(label);
    h.finalize_xof().fill(&mut out);
    out
}

fn show_key(key: &[u8]) -> String {
    format!("{:?} (hex {})", String::from_utf8_lossy(key), to_hex(key))
}

#[track_caller]
fn assert_format_err<T: std::fmt::Debug>(r: Result<T>, needle: &str) {
    match r {
        Err(Error::Format(msg)) => {
            assert!(
                msg.contains(needle),
                "expected a format error mentioning {needle:?}, got {msg:?}"
            )
        }
        other => panic!("expected Err(Error::Format(..{needle}..)), got {other:?}"),
    }
}

#[track_caller]
fn assert_any_format_err<T: std::fmt::Debug>(r: Result<T>) {
    assert!(
        matches!(r, Err(Error::Format(_))),
        "expected Err(Error::Format(_)), got {r:?}"
    );
}

// ===========================================================================
// Golden vectors
// ===========================================================================

/// BLAKE3("abc"), published BLAKE3 test vector.
const BLAKE3_ABC: &str = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";
/// BLAKE3(""), published BLAKE3 test vector.
const BLAKE3_EMPTY: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
/// Synthetic digests (byte i = start + i): any reordering of the field shows.
const DIGEST_00_1F: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const DIGEST_20_3F: &str = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f";
const DIGEST_40_5F: &str = "404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f";

/// RawV1 envelope of "abc" (67 bytes).
fn golden_envelope_abc() -> Vec<u8> {
    hxs(&[
        "42424f31",         // 0  magic "BBO1"
        "01",               // 4  codec_id 0x01 (RawV1)
        "01",               // 5  codec_version 1
        "0000",             // 6  flags = 0 (u16 LE)
        "03000000",         // 8  raw_len = 3 (u32 LE)
        "03000000",         // 12 body_len = 3 (u32 LE)
        "0000000000000000", // 16 aux_id = 0 (u64 LE)
        BLAKE3_ABC,         // 24 digest = BLAKE3(original bytes)
        "0000000000000000", // 56 reserved = 0
        "616263",           // 64 body = "abc"
    ])
}

/// Envelope with every field non-trivial (69 bytes).
fn golden_envelope_fields() -> Vec<u8> {
    hxs(&[
        "42424f31",         // 0  magic
        "11",               // 4  codec_id 0x11 (ZstdV1)
        "01",               // 5  codec_version 1
        "0000",             // 6  flags
        "45230100",         // 8  raw_len = 0x0001_2345 = 74565
        "05000000",         // 12 body_len = 5
        "0807060504030201", // 16 aux_id = 0x0102_0304_0506_0708
        DIGEST_00_1F,       // 24 digest
        "0000000000000000", // 56 reserved
        "deadbeef00",       // 64 body
    ])
}

/// RawV1 envelope of the empty value (64 bytes, no body).
fn golden_envelope_empty() -> Vec<u8> {
    hxs(&[
        "42424f31",         // magic
        "0101",             // RawV1
        "0000",             // flags
        "00000000",         // raw_len = 0
        "00000000",         // body_len = 0
        "0000000000000000", // aux_id
        BLAKE3_EMPTY,       // digest = BLAKE3("")
        "0000000000000000", // reserved
    ])
}

/// Inline manifest holding `golden_envelope_abc` (87 bytes).
fn golden_manifest_inline() -> Vec<u8> {
    let mut b = hxs(&[
        "01",               // 0  manifest_version = 1
        "0100000000000000", // 1  revision = 1 (u64 LE)
        "0300000000000000", // 9  logical_len = 3 (u64 LE)
        "00",               // 17 kind = 0 (Inline)
        "00",               // 18 flags = 0 (no source)
        "43",               // 19 varint envelope length = 67
    ]);
    b.extend_from_slice(&golden_envelope_abc()); // 20 envelope
    b
}

fn manifest_inline() -> Manifest {
    Manifest {
        revision: 1,
        logical_len: 3,
        source_id: None,
        body: ManifestBody::Inline(golden_envelope_abc()),
    }
}

/// Chunks manifest without source (52 bytes).
fn golden_manifest_chunks() -> Vec<u8> {
    hxs(&[
        "01",               // 0  manifest_version
        "0201000000000000", // 1  revision = 258
        "e803000000000000", // 9  logical_len = 1000
        "01",               // 17 kind = 1 (Chunks)
        "00",               // 18 flags
        "02",               // 19 varint count = 2
        "0002000000000000", // 20 chunk 0: logical_end = 512
        "0100000000000000", // 28          object_id = 1
        "e803000000000000", // 36 chunk 1: logical_end = 1000
        "0200000000000000", // 44          object_id = 2
    ])
}

fn manifest_chunks() -> Manifest {
    Manifest {
        revision: 258,
        logical_len: 1000,
        source_id: None,
        body: ManifestBody::Chunks(vec![
            ChunkRef {
                logical_end: 512,
                object_id: 1,
            },
            ChunkRef {
                logical_end: 1000,
                object_id: 2,
            },
        ]),
    }
}

/// Chunks manifest with a source; the same object twice (dedupe) (60 bytes).
fn golden_manifest_chunks_source() -> Vec<u8> {
    hxs(&[
        "01",               // 0  manifest_version
        "0700000000000000", // 1  revision = 7
        "0004000000000000", // 9  logical_len = 1024
        "01",               // 17 kind = 1 (Chunks)
        "01",               // 18 flags = bit0 has_source
        "0300000000000000", // 19 source_id = 3 (u64 LE)
        "02",               // 27 varint count = 2
        "0002000000000000", // 28 chunk 0: logical_end = 512
        "0500000000000000", // 36          object_id = 5
        "0004000000000000", // 44 chunk 1: logical_end = 1024
        "0500000000000000", // 52          object_id = 5
    ])
}

fn manifest_chunks_source() -> Manifest {
    Manifest {
        revision: 7,
        logical_len: 1024,
        source_id: Some(3),
        body: ManifestBody::Chunks(vec![
            ChunkRef {
                logical_end: 512,
                object_id: 5,
            },
            ChunkRef {
                logical_end: 1024,
                object_id: 5,
            },
        ]),
    }
}

/// Generated manifest: generator 1 (ARITH_U64) v1, params (1, 2, 3) (80 bytes).
fn golden_manifest_generated() -> Vec<u8> {
    hxs(&[
        "01",               // 0  manifest_version
        "0900000000000000", // 1  revision = 9
        "1800000000000000", // 9  logical_len = 24
        "02",               // 17 kind = 2 (Generated)
        "00",               // 18 flags
        "0100",             // 19 generator_id = 1 (u16 LE)
        "0100",             // 21 generator_version = 1 (u16 LE)
        "18",               // 23 varint params_len = 24
        "0100000000000000", // 24 params: start = 1
        "0200000000000000", // 32         step = 2
        "0300000000000000", // 40         count = 3
        DIGEST_20_3F,       // 48 digest (synthetic)
    ])
}

fn manifest_generated() -> Manifest {
    Manifest {
        revision: 9,
        logical_len: 24,
        source_id: None,
        body: ManifestBody::Generated {
            generator_id: 1,
            generator_version: 1,
            params: generator::arith_params(1, 2, 3),
            digest: digest_of(DIGEST_20_3F),
        },
    }
}

/// Tombstone manifest (19 bytes).
fn golden_manifest_tombstone() -> Vec<u8> {
    hxs(&[
        "01",               // 0  manifest_version
        "0c00000000000000", // 1  revision = 12
        "0000000000000000", // 9  logical_len = 0
        "03",               // 17 kind = 3 (Tombstone)
        "00",               // 18 flags
    ])
}

fn manifest_tombstone() -> Manifest {
    Manifest {
        revision: 12,
        logical_len: 0,
        source_id: None,
        body: ManifestBody::Tombstone,
    }
}

/// Every golden manifest with its decoded value.
fn golden_manifests() -> Vec<(&'static str, Vec<u8>, Manifest)> {
    vec![
        ("inline", golden_manifest_inline(), manifest_inline()),
        ("chunks", golden_manifest_chunks(), manifest_chunks()),
        (
            "chunks+source",
            golden_manifest_chunks_source(),
            manifest_chunks_source(),
        ),
        (
            "generated",
            golden_manifest_generated(),
            manifest_generated(),
        ),
        (
            "tombstone",
            golden_manifest_tombstone(),
            manifest_tombstone(),
        ),
    ]
}

/// Source descriptor without last import (16 bytes).
fn golden_source_plain() -> Vec<u8> {
    hxs(&[
        "01",                   // version = 1
        "01",                   // kind = 1 (LOCAL_FILE)
        "0100",                 // adapter_version = 1 (u16 LE)
        "0a",                   // varint location length = 10
        "646174612f612e62696e", // "data/a.bin"
        "00",                   // has_last = 0
    ])
}

fn source_plain() -> SourceDescriptor {
    SourceDescriptor {
        kind: source_kind::LOCAL_FILE,
        location: "data/a.bin".to_string(),
        adapter_version: 1,
        last_import: None,
    }
}

/// Source descriptor with a last import and a non-ASCII location (74 bytes).
fn golden_source_last_import() -> Vec<u8> {
    hxs(&[
        "01",                       // version = 1
        "01",                       // kind = 1 (LOCAL_FILE)
        "0201",                     // adapter_version = 0x0102
        "0c",                       // varint location length = 12 (bytes, not chars)
        "6461646f732fc3a72e747874", // "dados/ç.txt" (utf-8)
        "01",                       // has_last = 1
        "0500000000000000",         // revision = 5
        "e803000000000000",         // bytes = 1000
        DIGEST_40_5F,               // digest
        "0068e5cf8b010000",         // unix_ms = 1_700_000_000_000
    ])
}

fn source_last_import() -> SourceDescriptor {
    SourceDescriptor {
        kind: source_kind::LOCAL_FILE,
        location: "dados/\u{e7}.txt".to_string(),
        adapter_version: 0x0102,
        last_import: Some(ImportInfo {
            revision: 5,
            bytes: 1000,
            digest: digest_of(DIGEST_40_5F),
            unix_ms: 1_700_000_000_000,
        }),
    }
}

#[test]
fn golden_digest_is_unkeyed_blake3_256() {
    assert_eq!(blake3_hex(b"abc"), BLAKE3_ABC);
    assert_eq!(blake3_hex(b""), BLAKE3_EMPTY);
}

#[test]
fn golden_format_constants() {
    assert_eq!(format::FORMAT_VERSION, 1);
    assert_eq!(&format::OBJECT_MAGIC, b"BBO1");
    assert_eq!(format::ENVELOPE_HEADER_LEN, 64);
    assert_eq!(MAX_UNIT_LEN, 16 * 1024 * 1024);
    assert_eq!(MAX_PARAMS_LEN, 64 * 1024);
    assert_eq!(format::MANIFEST_VERSION, 1);
}

#[test]
fn golden_codec_ids_are_permanent() {
    let table = [
        (CodecTag::RAW_V1, codec_id::RAW, 0x01, "RawV1"),
        (CodecTag::REPEAT_V1, codec_id::REPEAT, 0x02, "RepeatV1"),
        (
            CodecTag::ARITH_U64_V1,
            codec_id::ARITH_U64,
            0x03,
            "ArithmeticU64V1",
        ),
        (CodecTag::LZ4_V1, codec_id::LZ4, 0x10, "Lz4V1"),
        (CodecTag::ZSTD_V1, codec_id::ZSTD, 0x11, "ZstdV1"),
        (
            CodecTag::BABEL_AFFINE_V1,
            codec_id::BABEL_AFFINE,
            0x20,
            "BabelAffineV1",
        ),
        (
            CodecTag::TEMPLATE_PATCH_V1,
            codec_id::TEMPLATE_PATCH,
            0x30,
            "TemplatePatchV1",
        ),
    ];
    for (tag, named_id, id, name) in table {
        assert_eq!(named_id, id, "{name}");
        assert_eq!(tag, CodecTag { id, version: 1 }, "{name}");
        assert_eq!(tag.name(), name);
    }
    let all: BTreeSet<CodecTag> = CodecTag::ALL_V1.into_iter().collect();
    let expected: BTreeSet<CodecTag> = table.iter().map(|t| t.0).collect();
    assert_eq!(all, expected);
    assert_eq!(
        CodecTag {
            id: 0x40,
            version: 1
        }
        .name(),
        "Unknown",
        "0x40 is not a codec (Generated is a manifest kind)"
    );
}

#[test]
fn golden_kinds_modes_and_table_names() {
    assert_eq!((param_kind::ZSTD_DICT, param_kind::TEMPLATE), (1, 2));
    assert_eq!(
        (
            source_kind::LOCAL_FILE,
            source_kind::GENERATOR,
            source_kind::EXTERNAL
        ),
        (1, 2, 3)
    );
    assert_eq!((Mode::BabelPure as u8, Mode::Adaptive as u8), (1, 2));
    assert_eq!(Mode::from_u8(1), Some(Mode::BabelPure));
    assert_eq!(Mode::from_u8(2), Some(Mode::Adaptive));
    assert_eq!(Mode::from_u8(0), None);
    assert_eq!(Mode::from_u8(3), None);
    let names: Vec<&str> = Table::ALL.iter().map(|t| t.name()).collect();
    assert_eq!(
        names,
        [
            "meta",
            "records",
            "objects",
            "hash_candidates",
            "refcounts",
            "params",
            "history",
            "sources",
            "pending_imports"
        ]
    );
}

#[test]
fn golden_meta_entry_names() {
    let names = [
        (meta_key::FORMAT_VERSION, "format_version"),
        (meta_key::MODE, "mode"),
        (meta_key::BLOCK_SIZE, "block_size"),
        (meta_key::INLINE_MAX, "inline_max"),
        (meta_key::NEXT_OBJECT_ID, "next_object_id"),
        (meta_key::NEXT_REVISION, "next_revision"),
        (meta_key::NEXT_PARAM_ID, "next_param_id"),
        (meta_key::NEXT_SOURCE_ID, "next_source_id"),
        (meta_key::NEXT_IMPORT_ID, "next_import_id"),
        (meta_key::ACTIVE_ZSTD_DICT, "active_zstd_dict"),
        (meta_key::ACTIVE_TEMPLATE, "active_template"),
        (meta_key::CREATED_BY, "created_by"),
    ];
    for (got, want) in names {
        assert_eq!(got, want);
    }
}

#[test]
fn golden_fixed_width_values() {
    // meta format_version (u32 LE), counters (u64 LE), refcounts (u64 LE)
    assert_eq!(
        format::FORMAT_VERSION.to_le_bytes().to_vec(),
        hx("01000000")
    );
    assert_eq!(format::encode_u64(1).to_vec(), hx("0100000000000000"));
    assert_eq!(
        format::decode_u64(&hx("0807060504030201")).unwrap(),
        0x0102_0304_0506_0708
    );
    assert_eq!(format::decode_u32(&hx("00020000")).unwrap(), 512);
    for n in [0usize, 7, 9, 16] {
        assert_format_err(format::decode_u64(&vec![0u8; n]), "8-byte");
    }
    for n in [0usize, 3, 5, 8] {
        assert_format_err(format::decode_u32(&vec![0u8; n]), "4-byte");
    }
}

#[test]
fn golden_envelope_raw_abc() {
    let golden = golden_envelope_abc();
    assert_eq!(golden.len(), 67);
    let digest = digest_of(BLAKE3_ABC);
    assert_eq!(
        write_envelope(CodecTag::RAW_V1, 0, 3, &digest, b"abc"),
        golden
    );
    let (h, body) = read_envelope(&golden).unwrap();
    assert_eq!(
        h,
        EnvelopeHeader {
            codec: CodecTag::RAW_V1,
            flags: 0,
            raw_len: 3,
            body_len: 3,
            aux_id: 0,
            digest
        }
    );
    assert_eq!(body, b"abc");
    // Full read path of a RawV1 unit: decode, then length + BLAKE3 verification.
    let mut out = b"junk".to_vec();
    codec::decode(
        h.codec,
        h.aux_id,
        body,
        h.raw_len,
        Deps::default(),
        &mut out,
    )
    .unwrap();
    assert_eq!(out, b"abc");
    assert_eq!(*blake3::hash(&out).as_bytes(), h.digest);
}

#[test]
fn golden_envelope_every_field() {
    let golden = golden_envelope_fields();
    assert_eq!(golden.len(), 69);
    let digest = digest_of(DIGEST_00_1F);
    let body = hx("deadbeef00");
    assert_eq!(
        write_envelope(
            CodecTag::ZSTD_V1,
            0x0102_0304_0506_0708,
            0x0001_2345,
            &digest,
            &body
        ),
        golden
    );
    let (h, b) = read_envelope(&golden).unwrap();
    let expected = EnvelopeHeader {
        codec: CodecTag::ZSTD_V1,
        flags: 0,
        raw_len: 74565,
        body_len: 5,
        aux_id: 0x0102_0304_0506_0708,
        digest,
    };
    assert_eq!(h, expected);
    assert_eq!(b, &body[..]);
}

#[test]
fn golden_envelope_empty_value() {
    let golden = golden_envelope_empty();
    assert_eq!(golden.len(), format::ENVELOPE_HEADER_LEN);
    assert_eq!(
        write_envelope(CodecTag::RAW_V1, 0, 0, &digest_of(BLAKE3_EMPTY), b""),
        golden
    );
    let (h, body) = read_envelope(&golden).unwrap();
    assert_eq!((h.raw_len, h.body_len, body.len()), (0, 0, 0));
}

#[test]
fn golden_manifest_vectors() {
    for (what, golden, value) in golden_manifests() {
        assert_eq!(value.encode(), golden, "{what}: encode");
        assert_eq!(Manifest::decode(&golden).unwrap(), value, "{what}: decode");
    }
    assert_eq!(golden_manifest_inline().len(), 87);
    assert_eq!(golden_manifest_chunks().len(), 52);
    assert_eq!(golden_manifest_chunks_source().len(), 60);
    assert_eq!(golden_manifest_generated().len(), 80);
    assert_eq!(golden_manifest_tombstone().len(), 19);
    assert_eq!(manifest_chunks_source().object_ids(), vec![5, 5]);
    assert!(manifest_tombstone().is_tombstone());
    assert!(manifest_inline().object_ids().is_empty());
}

#[test]
fn golden_manifest_inline_empty_value() {
    // An empty value is always Inline (Chunks needs at least one chunk).
    let mut golden = hxs(&[
        "01",
        "0200000000000000",
        "0000000000000000",
        "00",
        "00",
        "40",
    ]);
    golden.extend_from_slice(&golden_envelope_empty());
    let m = Manifest {
        revision: 2,
        logical_len: 0,
        source_id: None,
        body: ManifestBody::Inline(golden_envelope_empty()),
    };
    assert_eq!(m.encode(), golden);
    assert_eq!(Manifest::decode(&golden).unwrap(), m);
}

#[test]
fn golden_manifest_chunk_count_is_a_varint() {
    let refs: Vec<ChunkRef> = (1..=128u64)
        .map(|i| ChunkRef {
            logical_end: i * 512,
            object_id: i,
        })
        .collect();
    let m = Manifest {
        revision: 1,
        logical_len: 128 * 512,
        source_id: None,
        body: ManifestBody::Chunks(refs),
    };
    let b = m.encode();
    assert_eq!(b.len(), 19 + 2 + 128 * 16);
    assert_eq!(b[19..21], [0x80, 0x01], "varint 128");
    assert_eq!(b[21..37], hx("0002000000000000 0100000000000000")[..]);
    assert_eq!(Manifest::decode(&b).unwrap(), m);
}

#[test]
fn golden_params() {
    let cases = [
        (
            Param {
                kind: param_kind::ZSTD_DICT,
                version: 1,
                bytes: hx("37a430ec"),
            },
            "01 0100 37a430ec",
        ),
        (
            Param {
                kind: param_kind::TEMPLATE,
                version: 1,
                bytes: b"tpl".to_vec(),
            },
            "02 0100 74706c",
        ),
        (
            Param {
                kind: param_kind::TEMPLATE,
                version: 1,
                bytes: Vec::new(),
            },
            "02 0100",
        ),
    ];
    for (param, golden) in cases {
        let golden = hx(golden);
        assert_eq!(param.encode(), golden);
        assert_eq!(Param::decode(&golden).unwrap(), param);
    }
}

#[test]
fn golden_source_descriptors() {
    let cases = [
        (golden_source_plain(), source_plain()),
        (golden_source_last_import(), source_last_import()),
    ];
    for (golden, value) in cases {
        assert_eq!(value.encode(), golden);
        assert_eq!(SourceDescriptor::decode(&golden).unwrap(), value);
    }
    assert_eq!(golden_source_plain().len(), 16);
    assert_eq!(golden_source_last_import().len(), 74);
}

#[test]
fn golden_history_keys() {
    let cases: [(&[u8], u64, &str); 6] = [
        (b"a", 1, "61 0000 0000000000000001"),
        (b"", 0x0102_0304_0506_0708, "0000 0102030405060708"),
        (b"a\x00b", 5, "61 00ff 62 0000 0000000000000005"),
        (b"\x00", 7, "00ff 0000 0000000000000007"),
        (b"\xff\x00\xff", 2, "ff 00ff ff 0000 0000000000000002"),
        (b"\x00\x00", u64::MAX, "00ff 00ff 0000 ffffffffffffffff"),
    ];
    for (key, rev, golden) in cases {
        let golden = hx(golden);
        assert_eq!(history_key(key, rev), golden, "history_key({key:?}, {rev})");
        assert_eq!(parse_history_key(&golden).unwrap(), (key.to_vec(), rev));
        assert_eq!(history_prefix(key), golden[..golden.len() - 8]);
    }
    assert_eq!(history_prefix(b"a\x00"), hx("61 00ff 0000"));
}

#[test]
fn golden_candidate_keys() {
    assert_eq!(
        candidate_key(&digest_of(BLAKE3_ABC), 3).to_vec(),
        hxs(&[BLAKE3_ABC, "03000000"])
    );
    // raw_len is little-endian in this key (exact-match lookups only).
    assert_eq!(
        candidate_key(&digest_of(DIGEST_00_1F), 0x0102_0304).to_vec(),
        hxs(&[DIGEST_00_1F, "04030201"])
    );
}

#[test]
fn golden_id_lists_and_id_keys() {
    let golden = hx("0100000000000000 0807060504030201");
    assert_eq!(encode_id_list(&[1, 0x0102_0304_0506_0708]), golden);
    assert_eq!(
        decode_id_list(&golden).unwrap(),
        vec![1, 0x0102_0304_0506_0708]
    );
    assert_eq!(decode_id_list(&[]).unwrap(), Vec::<u64>::new());
    // Integer keys are big-endian: byte order == numeric order.
    assert_eq!(id_key(1).to_vec(), hx("0000000000000001"));
    assert_eq!(
        id_key(0x0102_0304_0506_0708).to_vec(),
        hx("0102030405060708")
    );
    assert_eq!(id_key(u64::MAX).to_vec(), hx("ffffffffffffffff"));
    assert_eq!(
        parse_id_key(&hx("0102030405060708")).unwrap(),
        0x0102_0304_0506_0708
    );
}

#[test]
fn golden_varints() {
    let cases: [(u64, &str); 10] = [
        (0, "00"),
        (1, "01"),
        (127, "7f"),
        (128, "8001"),
        (300, "ac02"),
        (16383, "ff7f"),
        (16384, "808001"),
        (u32::MAX as u64, "ffffffff0f"),
        (1 << 63, "80808080808080808001"),
        (u64::MAX, "ffffffffffffffffff01"),
    ];
    for (v, golden) in cases {
        let golden = hx(golden);
        let mut enc = Vec::new();
        put_varint(&mut enc, v);
        assert_eq!(enc, golden, "put_varint({v})");
        // Decoding in the middle of a buffer consumes exactly the varint.
        let mut buf = vec![0xaa];
        buf.extend_from_slice(&golden);
        buf.push(0x7f);
        let mut pos = 1;
        assert_eq!(get_varint(&buf, &mut pos).unwrap(), v);
        assert_eq!(pos, 1 + golden.len());
    }
}

#[test]
fn golden_generator_ids_and_params() {
    assert_eq!(
        (gen_ids::ARITH_U64, gen_ids::BLAKE3_XOF, gen_ids::REPEAT),
        (1, 2, 3)
    );
    assert_eq!(
        generator::arith_params(1, 2, 3),
        hx("0100000000000000 0200000000000000 0300000000000000")
    );
    let key_hex = "07".repeat(32);
    assert_eq!(
        generator::blake3_xof_params(&[7; 32], 5000),
        hxs(&[key_hex.as_str(), "8813000000000000"])
    );
    assert_eq!(
        generator::repeat_params(5, b"ab"),
        hx("0500000000000000 6162")
    );
}

#[test]
fn unknown_generator_is_an_error() {
    let unknown = [
        (0u16, 1u16),
        (0x7fff, 1),
        (gen_ids::ARITH_U64, 0),
        (gen_ids::ARITH_U64, 2),
        (u16::MAX, u16::MAX),
    ];
    for (id, version) in unknown {
        match generator::Registry::builtin().get(id, version) {
            Err(Error::UnknownGenerator { id: i, version: v }) => assert_eq!((i, v), (id, version)),
            Err(e) => panic!("generator {id} v{version}: unexpected error {e}"),
            Ok(g) => panic!(
                "generator {id} v{version} unexpectedly registered as {}",
                g.name()
            ),
        }
    }
    assert!(matches!(
        generator::Registry::empty().get(1, 1),
        Err(Error::UnknownGenerator { id: 1, version: 1 })
    ));
}

// ===========================================================================
// Rejections (always Err, never a panic)
// ===========================================================================

#[test]
fn reject_envelope_bad_magic() {
    let golden = golden_envelope_abc();
    for i in 0..4 {
        let mut b = golden.clone();
        b[i] ^= 0x20;
        assert_format_err(read_envelope(&b), "magic");
    }
    let mut b = golden.clone();
    b[3] = b'2'; // "BBO2"
    assert_format_err(read_envelope(&b), "magic");
}

#[test]
fn reject_envelope_unknown_flags() {
    for bit in 0..16 {
        let mut b = golden_envelope_abc();
        b[6..8].copy_from_slice(&(1u16 << bit).to_le_bytes());
        assert_format_err(read_envelope(&b), "flags");
    }
}

#[test]
fn reject_envelope_non_zero_reserved() {
    for i in 56..64 {
        let mut b = golden_envelope_abc();
        b[i] = 1;
        assert_format_err(read_envelope(&b), "reserved");
    }
}

#[test]
fn reject_envelope_body_len_mismatch() {
    for body_len in [0u32, 2, 4, u32::MAX] {
        let mut b = golden_envelope_abc();
        b[12..16].copy_from_slice(&body_len.to_le_bytes());
        assert_format_err(read_envelope(&b), "body_len");
    }
    let mut longer = golden_envelope_abc();
    longer.push(0);
    assert_format_err(read_envelope(&longer), "body_len");
    let mut shorter = golden_envelope_abc();
    shorter.pop();
    assert_format_err(read_envelope(&shorter), "body_len");
}

#[test]
fn reject_envelope_raw_len_above_max_unit_len() {
    let at_limit = write_envelope(CodecTag::RAW_V1, 0, MAX_UNIT_LEN, &[0; 32], b"");
    assert!(
        read_envelope(&at_limit).is_ok(),
        "raw_len == MAX_UNIT_LEN is valid for the envelope layer"
    );
    for raw_len in [MAX_UNIT_LEN + 1, u32::MAX] {
        let b = write_envelope(CodecTag::RAW_V1, 0, raw_len, &[0; 32], b"");
        assert_format_err(read_envelope(&b), "MAX_UNIT_LEN");
    }
}

#[test]
fn reject_envelope_truncated() {
    let golden = golden_envelope_abc();
    for n in 0..format::ENVELOPE_HEADER_LEN {
        assert_format_err(read_envelope(&golden[..n]), "too short");
    }
    for n in format::ENVELOPE_HEADER_LEN..golden.len() {
        assert_format_err(read_envelope(&golden[..n]), "body_len");
    }
}

#[test]
fn envelope_layer_accepts_unknown_codecs() {
    // Codec support is checked when decoding, so inspection/verify can report it.
    let tag = CodecTag {
        id: 0x7f,
        version: 9,
    };
    let env = write_envelope(tag, 0, 3, &[0; 32], b"abc");
    let (h, _) = read_envelope(&env).unwrap();
    assert_eq!((h.codec, h.codec.name()), (tag, "Unknown"));
}

/// Manifest bytes with the given fixed header (no source) followed by `body`.
fn manifest_bytes(revision: u64, logical_len: u64, kind: u8, body: &[u8]) -> Vec<u8> {
    let mut b = vec![format::MANIFEST_VERSION];
    b.extend_from_slice(&revision.to_le_bytes());
    b.extend_from_slice(&logical_len.to_le_bytes());
    b.push(kind);
    b.push(0);
    b.extend_from_slice(body);
    b
}

/// Chunks body: varint count, then (logical_end, object_id) u64 LE pairs.
fn chunk_refs(refs: &[(u64, u64)]) -> Vec<u8> {
    let mut b = Vec::new();
    put_varint(&mut b, refs.len() as u64);
    for &(end, id) in refs {
        b.extend_from_slice(&end.to_le_bytes());
        b.extend_from_slice(&id.to_le_bytes());
    }
    b
}

#[test]
fn reject_manifest_unknown_version() {
    for v in [0u8, 2, 0xff] {
        let mut b = golden_manifest_chunks();
        b[0] = v;
        assert_format_err(Manifest::decode(&b), "unknown manifest version");
    }
}

#[test]
fn reject_manifest_unknown_kind() {
    for kind in [4u8, 5, 0x40, 0xff] {
        let mut b = golden_manifest_chunks();
        b[17] = kind;
        assert_format_err(Manifest::decode(&b), "unknown manifest kind");
    }
}

#[test]
fn reject_manifest_unknown_flags() {
    for flags in [0x02u8, 0x03, 0x80, 0xfe, 0xff] {
        let mut b = golden_manifest_chunks();
        b[18] = flags;
        assert_format_err(Manifest::decode(&b), "unknown manifest flags");
    }
}

#[test]
fn reject_manifest_bad_chunks() {
    let one_ref = chunk_refs(&[(512, 1)]);
    let two_refs = chunk_refs(&[(512, 1), (1000, 2)]);
    let cases: [(&str, Vec<u8>, &str); 9] = [
        (
            "zero chunk count",
            manifest_bytes(1, 0, 1, &chunk_refs(&[])),
            "invalid chunk count",
        ),
        (
            "equal ends",
            manifest_bytes(1, 512, 1, &chunk_refs(&[(512, 1), (512, 2)])),
            "strictly increasing",
        ),
        (
            "decreasing ends",
            manifest_bytes(1, 100, 1, &chunk_refs(&[(512, 1), (100, 2)])),
            "strictly increasing",
        ),
        (
            "first end 0",
            manifest_bytes(1, 5, 1, &chunk_refs(&[(0, 1), (5, 2)])),
            "strictly increasing",
        ),
        (
            "last end < logical_len",
            manifest_bytes(1, 1001, 1, &two_refs),
            "logical_len",
        ),
        (
            "last end > logical_len",
            manifest_bytes(1, 999, 1, &two_refs),
            "logical_len",
        ),
        (
            "object id 0",
            manifest_bytes(1, 1000, 1, &chunk_refs(&[(512, 1), (1000, 0)])),
            "object id 0",
        ),
        (
            "count larger than the remaining bytes",
            manifest_bytes(1, 512, 1, &[&[3u8][..], &one_ref[1..]].concat()),
            "invalid chunk count",
        ),
        (
            "non-canonical count",
            manifest_bytes(1, 1000, 1, &[&[0x82u8, 0x00][..], &two_refs[1..]].concat()),
            "non-canonical",
        ),
    ];
    for (what, bytes, needle) in cases {
        let r = Manifest::decode(&bytes);
        assert!(
            matches!(&r, Err(Error::Format(m)) if m.contains(needle)),
            "{what}: got {r:?}"
        );
    }
    // Absurd counts are rejected before allocating.
    for count in [1u64 << 59, u64::MAX] {
        let mut body = Vec::new();
        put_varint(&mut body, count);
        assert_format_err(
            Manifest::decode(&manifest_bytes(1, 512, 1, &body)),
            "invalid chunk count",
        );
    }
}

#[test]
fn reject_manifest_trailing_bytes() {
    for (what, mut golden, _) in golden_manifests() {
        golden.push(0);
        let r = Manifest::decode(&golden);
        assert!(
            matches!(&r, Err(Error::Format(m)) if m.contains("trailing")),
            "{what}: got {r:?}"
        );
    }
}

#[test]
fn reject_manifest_truncated() {
    for (what, golden, _) in golden_manifests() {
        for n in 0..golden.len() {
            let r = Manifest::decode(&golden[..n]);
            assert!(
                matches!(r, Err(Error::Format(_))),
                "{what} truncated to {n} bytes: got {r:?}"
            );
        }
    }
}

#[test]
fn reject_manifest_bad_inline_envelope() {
    // raw_len of the embedded envelope must equal logical_len.
    for logical_len in [2u64, 4, 0] {
        let mut b = golden_manifest_inline();
        b[9..17].copy_from_slice(&logical_len.to_le_bytes());
        assert_format_err(Manifest::decode(&b), "raw_len != logical_len");
    }
    // The embedded envelope is fully validated.
    let mut bad_magic = golden_manifest_inline();
    bad_magic[20] = b'X';
    assert_format_err(Manifest::decode(&bad_magic), "magic");
    let mut flags = golden_manifest_inline();
    flags[20 + 6] = 1;
    assert_format_err(Manifest::decode(&flags), "flags");
    // Declared envelope length larger / smaller than the stored envelope.
    let mut longer = golden_manifest_inline();
    longer[19] = 0x44;
    assert_format_err(Manifest::decode(&longer), "truncated");
    let mut shorter = golden_manifest_inline();
    shorter[19] = 0x42;
    assert_format_err(Manifest::decode(&shorter), "body_len");
    // Non-canonical length varint (67 encoded in two bytes).
    let mut b = golden_manifest_inline();
    b.splice(19..20, [0xc3, 0x00]);
    assert_format_err(Manifest::decode(&b), "non-canonical");
}

#[test]
fn reject_manifest_generated_params_above_limit() {
    let generated = |params_len: usize, with_bytes: bool| {
        let mut body = hx("0100 0100");
        put_varint(&mut body, params_len as u64);
        if with_bytes {
            body.extend(std::iter::repeat_n(0x5a, params_len));
            body.extend_from_slice(&digest_of(DIGEST_20_3F));
        }
        manifest_bytes(1, 24, 2, &body)
    };
    let at_limit = generated(MAX_PARAMS_LEN, true);
    let m = Manifest::decode(&at_limit).expect("params of exactly MAX_PARAMS_LEN bytes are valid");
    assert_eq!(m.encode(), at_limit);
    assert_format_err(
        Manifest::decode(&generated(MAX_PARAMS_LEN + 1, true)),
        "too long",
    );
    assert_format_err(
        Manifest::decode(&generated(MAX_PARAMS_LEN + 1, false)),
        "too long",
    );
    assert_format_err(
        Manifest::decode(&generated(usize::MAX >> 1, false)),
        "too long",
    );
}

#[test]
fn reject_manifest_tombstone_with_length() {
    for len in [1u64, u64::MAX] {
        let mut b = golden_manifest_tombstone();
        b[9..17].copy_from_slice(&len.to_le_bytes());
        assert_format_err(Manifest::decode(&b), "tombstone with non-zero length");
    }
}

#[test]
fn reject_manifest_source_flag_without_source_id() {
    let mut b = golden_manifest_tombstone();
    b[18] = 1; // has_source, but the 8-byte source_id is missing
    assert_format_err(Manifest::decode(&b), "truncated");
}

#[test]
fn reject_non_canonical_and_overlong_varints() {
    let cases = [
        ("0 in two bytes", "8000", "non-canonical"),
        ("127 in two bytes", "ff00", "non-canonical"),
        ("1 in three bytes", "818000", "non-canonical"),
        (
            "zero high byte at position 10",
            "80808080808080808000",
            "non-canonical",
        ),
        ("10th byte payload 2", "ffffffffffffffffff02", "overflow"),
        ("10th byte payload 0x7f", "ffffffffffffffffff7f", "overflow"),
        (
            "10th byte with continuation",
            "ffffffffffffffffff81",
            "too long",
        ),
        ("11 bytes", "8080808080808080808001", "too long"),
        ("empty", "", "truncated"),
        ("continuation without next byte", "80", "truncated"),
        ("two continuation bytes", "ffff", "truncated"),
    ];
    for (what, bytes, needle) in cases {
        let bytes = hx(bytes);
        let mut pos = 0;
        let r = get_varint(&bytes, &mut pos);
        assert!(
            matches!(&r, Err(Error::Format(m)) if m.contains(needle)),
            "{what}: got {r:?}"
        );
    }
    // A position past the end is an error too.
    let mut pos = 5;
    assert_format_err(get_varint(&[0x01], &mut pos), "truncated");
}

#[test]
fn reject_bad_params() {
    for kind in [0u8, 3, 0xff] {
        assert_format_err(Param::decode(&[kind, 1, 0, 0xaa]), "unknown param kind");
    }
    for version in [0u16, 2, u16::MAX] {
        let mut b = vec![param_kind::ZSTD_DICT];
        b.extend_from_slice(&version.to_le_bytes());
        assert_format_err(Param::decode(&b), "unknown param version");
    }
    for n in 0..3 {
        assert_format_err(Param::decode(&hx("010100")[..n]), "truncated");
    }
}

#[test]
fn reject_bad_source_descriptors() {
    for version in [0u8, 2, 0xff] {
        let mut b = golden_source_plain();
        b[0] = version;
        assert_format_err(
            SourceDescriptor::decode(&b),
            "unknown source descriptor version",
        );
    }
    for bad in ["fffe", "c080", "eda080", "c3"] {
        let bad = hx(bad);
        let mut b = hx("01 01 0100");
        put_varint(&mut b, bad.len() as u64);
        b.extend_from_slice(&bad);
        b.push(0);
        assert_format_err(SourceDescriptor::decode(&b), "utf-8");
    }
    for flag in [2u8, 0xff] {
        let mut b = golden_source_plain();
        *b.last_mut().unwrap() = flag;
        assert_format_err(SourceDescriptor::decode(&b), "has_last");
    }
    for mut golden in [golden_source_plain(), golden_source_last_import()] {
        golden.push(0);
        assert_format_err(SourceDescriptor::decode(&golden), "trailing");
    }
    for golden in [golden_source_plain(), golden_source_last_import()] {
        for n in 0..golden.len() {
            assert_any_format_err(SourceDescriptor::decode(&golden[..n]));
        }
    }
    // Location length beyond the available bytes.
    assert_format_err(
        SourceDescriptor::decode(&hx("01 01 0100 7f 6161 00")),
        "truncated",
    );
}

#[test]
fn reject_bad_history_keys() {
    let cases = [
        ("empty", "", "without terminator"),
        ("no terminator", "616263", "without terminator"),
        ("escaped zero at the end", "6100ff", "without terminator"),
        ("lone zero at the end", "6100", "bad escape"),
        (
            "bad escape 00 01",
            "61 0001 0000 0000000000000001",
            "bad escape",
        ),
        (
            "bad escape 00 fe",
            "61 00fe 0000 0000000000000001",
            "bad escape",
        ),
        ("revision of 7 bytes", "61 0000 00000000000001", "revision"),
        (
            "revision of 9 bytes",
            "61 0000 000000000000000001",
            "revision",
        ),
        ("terminator without revision", "61 0000", "revision"),
    ];
    for (what, bytes, needle) in cases {
        let r = parse_history_key(&hx(bytes));
        assert!(
            matches!(&r, Err(Error::Format(m)) if m.contains(needle)),
            "{what}: got {r:?}"
        );
    }
}

#[test]
fn reject_bad_id_lists_and_id_keys() {
    for n in [1usize, 7, 9, 15, 17] {
        assert_format_err(decode_id_list(&vec![1u8; n]), "multiple of 8");
    }
    for n in [0usize, 7, 9, 36] {
        assert_format_err(parse_id_key(&vec![0u8; n]), "8 bytes");
    }
}

// ===========================================================================
// Codec dispatch (runnable now: only paths that need no codec body)
// ===========================================================================

#[test]
fn codec_unknown_tag_is_unknown_codec_for_every_unassigned_pair() {
    let known: BTreeSet<CodecTag> = CodecTag::ALL_V1.into_iter().collect();
    let mut out = Vec::new();
    let mut checked = 0u32;
    for id in 0..=u8::MAX {
        for version in 0..=u8::MAX {
            let tag = CodecTag { id, version };
            if known.contains(&tag) {
                continue;
            }
            for aux_id in [0u64, 7] {
                match codec::decode(tag, aux_id, b"abc", 3, Deps::default(), &mut out) {
                    Err(Error::UnknownCodec { id: i, version: v }) => {
                        assert_eq!((i, v), (id, version))
                    }
                    other => panic!("{tag:?} aux {aux_id}: expected UnknownCodec, got {other:?}"),
                }
            }
            assert_eq!(tag.name(), "Unknown");
            checked += 1;
        }
    }
    assert_eq!(checked, 65536 - 7);
}

#[test]
fn codec_missing_dependency() {
    let mut out = Vec::new();
    // ZstdV1 with a dictionary id but no prepared dictionary.
    let frame = hx("28b52ffd2003190000616263");
    for aux_id in [1u64, 42, u64::MAX] {
        match codec::decode(
            CodecTag::ZSTD_V1,
            aux_id,
            &frame,
            3,
            Deps::default(),
            &mut out,
        ) {
            Err(Error::MissingDependency { param_id }) => assert_eq!(param_id, aux_id),
            other => panic!("ZstdV1 aux {aux_id}: expected MissingDependency, got {other:?}"),
        }
    }
    // TemplatePatchV1 always needs its template (aux_id 0 included).
    for aux_id in [0u64, 9] {
        match codec::decode(
            CodecTag::TEMPLATE_PATCH_V1,
            aux_id,
            b"patch",
            3,
            Deps::default(),
            &mut out,
        ) {
            Err(Error::MissingDependency { param_id }) => assert_eq!(param_id, aux_id),
            other => {
                panic!("TemplatePatchV1 aux {aux_id}: expected MissingDependency, got {other:?}")
            }
        }
    }
}

#[test]
fn codec_required_params() {
    assert_eq!(
        codec::required_param(CodecTag::ZSTD_V1, 0),
        None,
        "ZstdV1 without dictionary"
    );
    assert_eq!(
        codec::required_param(CodecTag::ZSTD_V1, 5),
        Some(param_kind::ZSTD_DICT)
    );
    assert_eq!(
        codec::required_param(CodecTag::TEMPLATE_PATCH_V1, 0),
        Some(param_kind::TEMPLATE)
    );
    assert_eq!(
        codec::required_param(CodecTag::TEMPLATE_PATCH_V1, 5),
        Some(param_kind::TEMPLATE)
    );
    let independent = [
        CodecTag::RAW_V1,
        CodecTag::REPEAT_V1,
        CodecTag::ARITH_U64_V1,
        CodecTag::LZ4_V1,
        CodecTag::BABEL_AFFINE_V1,
    ];
    for tag in independent {
        for aux_id in [0u64, 5] {
            assert_eq!(codec::required_param(tag, aux_id), None, "{}", tag.name());
        }
    }
}

#[test]
fn codec_raw_len_limit_is_checked_first() {
    let mut out = Vec::new();
    let tags = CodecTag::ALL_V1.into_iter().chain([CodecTag {
        id: 0x7f,
        version: 1,
    }]);
    for tag in tags {
        assert_format_err(
            codec::decode(tag, 3, b"", MAX_UNIT_LEN + 1, Deps::default(), &mut out),
            "MAX_UNIT_LEN",
        );
    }
}

#[test]
fn codec_raw_v1_body_is_the_bytes() {
    let mut out = vec![0xee; 10];
    codec::decode(CodecTag::RAW_V1, 0, b"abc", 3, Deps::default(), &mut out).unwrap();
    assert_eq!(out, b"abc", "output is cleared first");
    codec::decode(CodecTag::RAW_V1, 0, b"", 0, Deps::default(), &mut out).unwrap();
    assert!(out.is_empty());
    assert_eq!(codec::raw::encode(b"abc"), b"abc");
    for raw_len in [0u32, 2, 4] {
        assert_format_err(
            codec::decode(
                CodecTag::RAW_V1,
                0,
                b"abc",
                raw_len,
                Deps::default(),
                &mut out,
            ),
            "RawV1",
        );
    }
}

// ===========================================================================
// Codec bodies and generators
// ===========================================================================

#[test]
fn after_merge_codec_body_goldens() {
    let arith_body = hx("0100000000000000 0200000000000000 0300000000000000");
    let arith_edge = hx("feffffffffffffff 0100000000000000 0200000000000000");
    let cases: Vec<(CodecTag, Vec<u8>, u32, Vec<u8>)> = vec![
        // BabelAffineV1: x = (5 * seed + 1) mod 2^(8n), n-byte big-endian seed.
        (CodecTag::BABEL_AFFINE_V1, vec![], 0, vec![]),
        (CodecTag::BABEL_AFFINE_V1, hx("db48"), 2, b"Hi".to_vec()),
        (CodecTag::BABEL_AFFINE_V1, hx("79e07a"), 3, b"abc".to_vec()),
        (CodecTag::BABEL_AFFINE_V1, hx("33"), 1, hx("00")),
        (CodecTag::BABEL_AFFINE_V1, hx("00"), 1, hx("01")),
        (CodecTag::BABEL_AFFINE_V1, hx("66"), 1, hx("ff")),
        (
            CodecTag::BABEL_AFFINE_V1,
            vec![0x33; 16],
            16,
            vec![0x00; 16],
        ),
        (
            CodecTag::BABEL_AFFINE_V1,
            vec![0x66; 16],
            16,
            vec![0xff; 16],
        ),
        (
            CodecTag::BABEL_AFFINE_V1,
            vec![0xff; 16],
            16,
            [vec![0xff; 15], vec![0xfc]].concat(),
        ),
        // RepeatV1: period u32 LE ++ motif; the last repetition may be partial.
        (
            CodecTag::REPEAT_V1,
            hx("02000000 6162"),
            5,
            b"ababa".to_vec(),
        ),
        (CodecTag::REPEAT_V1, hx("01000000 7a"), 3, b"zzz".to_vec()),
        (
            CodecTag::REPEAT_V1,
            hx("03000000 616263"),
            3,
            b"abc".to_vec(),
        ),
        // ArithmeticU64V1: start, step, count (u64 LE each) -> count u64 LE values.
        (
            CodecTag::ARITH_U64_V1,
            arith_body,
            24,
            hx("0100000000000000 0300000000000000 0500000000000000"),
        ),
        (
            CodecTag::ARITH_U64_V1,
            arith_edge,
            16,
            hx("feffffffffffffff ffffffffffffffff"),
        ),
        // Lz4V1: one raw LZ4 block, no size prefix.
        (CodecTag::LZ4_V1, hx("30 616263"), 3, b"abc".to_vec()),
        (
            CodecTag::LZ4_V1,
            hx("1a 61 0100 50 6161616161"),
            20,
            vec![b'a'; 20],
        ),
        // ZstdV1 (aux_id 0): one frame (RFC 8878). Raw block, then RLE block.
        (
            CodecTag::ZSTD_V1,
            hx("28b52ffd 20 03 190000 616263"),
            3,
            b"abc".to_vec(),
        ),
        (
            CodecTag::ZSTD_V1,
            hx("28b52ffd 20 04 230000 61"),
            4,
            b"aaaa".to_vec(),
        ),
    ];
    let mut out = Vec::new();
    for (tag, body, raw_len, expected) in cases {
        codec::decode(tag, 0, &body, raw_len, Deps::default(), &mut out)
            .unwrap_or_else(|e| panic!("{} body {}: {e}", tag.name(), to_hex(&body)));
        assert_eq!(out, expected, "{} body {}", tag.name(), to_hex(&body));
    }
}

#[test]
fn after_merge_codec_body_rejections() {
    let cases: Vec<(CodecTag, Vec<u8>, u32, &str)> = vec![
        (
            CodecTag::BABEL_AFFINE_V1,
            hx("db48"),
            3,
            "seed length != raw_len",
        ),
        (
            CodecTag::BABEL_AFFINE_V1,
            hx("db48"),
            1,
            "seed length != raw_len",
        ),
        (CodecTag::REPEAT_V1, hx("020000"), 5, "truncated period"),
        (CodecTag::REPEAT_V1, hx("02000000 61"), 5, "truncated motif"),
        (
            CodecTag::REPEAT_V1,
            hx("00000000"),
            5,
            "period 0 cannot produce bytes",
        ),
        (
            CodecTag::ARITH_U64_V1,
            hx("0100000000000000 0200000000000000"),
            16,
            "truncated body",
        ),
        (
            CodecTag::ARITH_U64_V1,
            generator::arith_params(1, 2, 3),
            16,
            "raw_len != count * 8",
        ),
        (
            CodecTag::ARITH_U64_V1,
            generator::arith_params(u64::MAX - 1, 1, 3),
            24,
            "overflow",
        ),
        (CodecTag::LZ4_V1, hx("ff"), 3, "garbage"),
        (
            CodecTag::LZ4_V1,
            hx("30 616263"),
            4,
            "block shorter than raw_len",
        ),
        (
            CodecTag::LZ4_V1,
            hx("30 616263"),
            2,
            "block longer than raw_len",
        ),
        (CodecTag::ZSTD_V1, hx("00112233"), 3, "not a frame"),
        (
            CodecTag::ZSTD_V1,
            hx("28b52ffd 20 03 190000 616263"),
            2,
            "frame longer than raw_len",
        ),
        (
            CodecTag::ZSTD_V1,
            hx("28b52ffd 20 03 190000 616263"),
            4,
            "frame shorter than raw_len",
        ),
        (
            CodecTag::ZSTD_V1,
            hx("28b52ffd 20 03 190000 6162"),
            3,
            "truncated frame",
        ),
    ];
    let mut out = Vec::new();
    for (tag, body, raw_len, what) in cases {
        let r = codec::decode(tag, 0, &body, raw_len, Deps::default(), &mut out);
        assert!(
            r.is_err(),
            "{} ({what}): accepted, produced {}",
            tag.name(),
            to_hex(&out)
        );
    }
}

#[test]
fn after_merge_generator_goldens() {
    let reg = generator::Registry::builtin();
    let listed: BTreeSet<(u16, u16)> = reg.list().iter().map(|&(id, v, _)| (id, v)).collect();
    for id in [gen_ids::ARITH_U64, gen_ids::BLAKE3_XOF, gen_ids::REPEAT] {
        assert!(
            listed.contains(&(id, 1)),
            "built-in generator {id} v1 is not registered"
        );
    }
    let run = |id: u16, params: &[u8], offset: u64, len: usize| {
        let g = reg.get(id, 1).unwrap();
        let mut out = vec![0u8; len];
        g.generate(params, offset, &mut out).unwrap();
        out
    };

    // 1 = ARITH_U64: count u64 LE values start, start + step, ...
    let p = generator::arith_params(1, 2, 3);
    assert_eq!(
        reg.get(gen_ids::ARITH_U64, 1)
            .unwrap()
            .output_len(&p)
            .unwrap(),
        24
    );
    let full = run(gen_ids::ARITH_U64, &p, 0, 24);
    assert_eq!(
        full,
        hx("0100000000000000 0300000000000000 0500000000000000")
    );
    assert_eq!(
        run(gen_ids::ARITH_U64, &p, 7, 5),
        full[7..12],
        "random access"
    );

    // 3 = REPEAT: motif repeated to total_len.
    let p = generator::repeat_params(5, b"ab");
    assert_eq!(
        reg.get(gen_ids::REPEAT, 1).unwrap().output_len(&p).unwrap(),
        5
    );
    assert_eq!(run(gen_ids::REPEAT, &p, 0, 5), b"ababa");
    assert_eq!(run(gen_ids::REPEAT, &p, 3, 2), b"ba");

    // 2 = BLAKE3_XOF: keyed BLAKE3 XOF (key = params[0..32]) of the empty
    // message, first `len` bytes, random access by position (docs/format.md).
    let key = [7u8; 32];
    let p = generator::blake3_xof_params(&key, 1000);
    assert_eq!(
        reg.get(gen_ids::BLAKE3_XOF, 1)
            .unwrap()
            .output_len(&p)
            .unwrap(),
        1000
    );
    let mut expected = vec![0u8; 1000];
    blake3::Hasher::new_keyed(&key)
        .finalize_xof()
        .fill(&mut expected);
    assert_eq!(run(gen_ids::BLAKE3_XOF, &p, 0, 1000), expected);
    assert_eq!(
        run(gen_ids::BLAKE3_XOF, &p, 333, 100),
        expected[333..433],
        "random access"
    );

    // Invalid params are errors, never wrong bytes.
    let invalid: [(u16, Vec<u8>); 3] = [
        (gen_ids::ARITH_U64, vec![0; 23]),
        (gen_ids::BLAKE3_XOF, vec![0; 39]),
        (gen_ids::REPEAT, generator::repeat_params(5, b"")),
    ];
    for (id, params) in invalid {
        assert!(
            reg.get(id, 1).unwrap().output_len(&params).is_err(),
            "generator {id} accepted params {params:?}"
        );
    }
}

// ===========================================================================
// Property tests
// ===========================================================================

fn arb_codec_tag() -> impl Strategy<Value = CodecTag> {
    prop_oneof![
        prop::sample::select(CodecTag::ALL_V1.to_vec()),
        (any::<u8>(), any::<u8>()).prop_map(|(id, version)| CodecTag { id, version }),
    ]
}

fn arb_envelope_with_raw_len(raw_len: u32) -> impl Strategy<Value = Vec<u8>> {
    (
        arb_codec_tag(),
        any::<u64>(),
        any::<[u8; 32]>(),
        prop::collection::vec(any::<u8>(), 0..48),
    )
        .prop_map(move |(codec, aux, digest, body)| {
            write_envelope(codec, aux, raw_len, &digest, &body)
        })
}

/// (logical_len, body) pairs that satisfy every manifest rule.
fn arb_manifest_body() -> impl Strategy<Value = (u64, ManifestBody)> {
    let inline =
        prop_oneof![0u32..=4096, Just(MAX_UNIT_LEN), 0u32..=MAX_UNIT_LEN].prop_flat_map(|len| {
            arb_envelope_with_raw_len(len)
                .prop_map(move |env| (len as u64, ManifestBody::Inline(env)))
        });
    let chunks =
        prop::collection::vec((1u64..=u64::MAX / 256, 1u64..=u64::MAX), 1..40).prop_map(|parts| {
            let mut end = 0u64;
            let refs = parts
                .into_iter()
                .map(|(len, object_id)| {
                    end += len;
                    ChunkRef {
                        logical_end: end,
                        object_id,
                    }
                })
                .collect();
            (end, ManifestBody::Chunks(refs))
        });
    let generated = (
        any::<u16>(),
        any::<u16>(),
        prop::collection::vec(any::<u8>(), 0..200),
        any::<[u8; 32]>(),
        any::<u64>(),
    )
        .prop_map(|(generator_id, generator_version, params, digest, len)| {
            (
                len,
                ManifestBody::Generated {
                    generator_id,
                    generator_version,
                    params,
                    digest,
                },
            )
        });
    prop_oneof![
        inline,
        chunks,
        generated,
        Just((0u64, ManifestBody::Tombstone))
    ]
}

fn arb_manifest() -> impl Strategy<Value = Manifest> {
    (
        any::<u64>(),
        prop::option::of(any::<u64>()),
        arb_manifest_body(),
    )
        .prop_map(|(revision, source_id, (logical_len, body))| Manifest {
            revision,
            logical_len,
            source_id,
            body,
        })
}

fn arb_source() -> impl Strategy<Value = SourceDescriptor> {
    let last = prop::option::of((any::<u64>(), any::<u64>(), any::<[u8; 32]>(), any::<u64>()));
    (any::<u8>(), ".{0,40}", any::<u16>(), last).prop_map(
        |(kind, location, adapter_version, last)| SourceDescriptor {
            kind,
            location,
            adapter_version,
            last_import: last.map(|(revision, bytes, digest, unix_ms)| ImportInfo {
                revision,
                bytes,
                digest,
                unix_ms,
            }),
        },
    )
}

/// Keys biased towards the bytes that matter for history key escaping.
fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    let byte = prop_oneof![3 => Just(0u8), 3 => Just(0xffu8), 1 => Just(1u8), 1 => Just(0xfeu8), 2 => any::<u8>()];
    prop::collection::vec(byte, 0..6)
}

fn arb_revision() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0u64),
        Just(1u64),
        Just(0xffu64),
        Just(0x100u64),
        Just(u64::MAX),
        any::<u64>()
    ]
}

/// Values around every varint length boundary: 2^b and 2^b - 1.
fn arb_varint_value() -> impl Strategy<Value = u64> {
    prop_oneof![
        any::<u64>(),
        (0u32..64).prop_map(|b| 1u64 << b),
        (1u32..=64).prop_map(|b| u64::MAX >> (64 - b)),
        Just(0u64),
    ]
}

#[derive(Clone, Debug)]
enum Mutation {
    Truncate(prop::sample::Index),
    Set(prop::sample::Index, u8),
    Insert(prop::sample::Index, u8),
    Remove(prop::sample::Index),
}

fn arb_mutations() -> impl Strategy<Value = Vec<Mutation>> {
    let one = prop_oneof![
        any::<prop::sample::Index>().prop_map(Mutation::Truncate),
        (any::<prop::sample::Index>(), any::<u8>()).prop_map(|(i, b)| Mutation::Set(i, b)),
        (any::<prop::sample::Index>(), any::<u8>()).prop_map(|(i, b)| Mutation::Insert(i, b)),
        any::<prop::sample::Index>().prop_map(Mutation::Remove),
    ];
    prop::collection::vec(one, 1..4)
}

fn mutate(mut bytes: Vec<u8>, mutations: &[Mutation]) -> Vec<u8> {
    for m in mutations {
        match m {
            Mutation::Truncate(i) if !bytes.is_empty() => bytes.truncate(i.index(bytes.len())),
            Mutation::Set(i, b) if !bytes.is_empty() => {
                let at = i.index(bytes.len());
                bytes[at] = *b;
            }
            Mutation::Insert(i, b) => {
                let at = i.index(bytes.len() + 1);
                bytes.insert(at, *b);
            }
            Mutation::Remove(i) if !bytes.is_empty() => {
                bytes.remove(i.index(bytes.len()));
            }
            _ => {}
        }
    }
    bytes
}

/// Every decoder must be total (Err instead of panic) and canonical: whenever
/// it accepts bytes, re-encoding the value reproduces exactly those bytes.
fn check_decoders_on(bytes: &[u8], start: usize) -> std::result::Result<(), TestCaseError> {
    if let Ok(m) = Manifest::decode(bytes) {
        prop_assert_eq!(m.encode(), bytes);
    }
    if let Ok((h, body)) = read_envelope(bytes) {
        prop_assert_eq!(h.flags, 0);
        prop_assert_eq!(
            write_envelope(h.codec, h.aux_id, h.raw_len, &h.digest, body),
            bytes
        );
    }
    if let Ok(p) = Param::decode(bytes) {
        prop_assert_eq!(p.encode(), bytes);
    }
    if let Ok(s) = SourceDescriptor::decode(bytes) {
        prop_assert_eq!(s.encode(), bytes);
    }
    if let Ok((key, rev)) = parse_history_key(bytes) {
        prop_assert_eq!(history_key(&key, rev), bytes);
    }
    match decode_id_list(bytes) {
        Ok(ids) => {
            prop_assert_eq!(encode_id_list(&ids), bytes);
        }
        Err(_) => {
            prop_assert!(!bytes.len().is_multiple_of(8));
        }
    }
    let mut pos = start;
    if let Ok(v) = get_varint(bytes, &mut pos) {
        let mut enc = Vec::new();
        put_varint(&mut enc, v);
        prop_assert_eq!(&bytes[start..pos], &enc[..]);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn prop_manifest_roundtrip(m in arb_manifest()) {
        let bytes = m.encode();
        let back = Manifest::decode(&bytes)?;
        prop_assert_eq!(&back, &m);
        prop_assert_eq!(back.encode(), bytes);
    }

    #[test]
    fn prop_manifest_mutations_never_panic(m in arb_manifest(), muts in arb_mutations(), start in 0usize..64) {
        check_decoders_on(&mutate(m.encode(), &muts), start)?;
    }

    #[test]
    fn prop_envelope_roundtrip(
        codec in arb_codec_tag(),
        aux_id in any::<u64>(),
        raw_len in 0u32..=MAX_UNIT_LEN,
        digest in any::<[u8; 32]>(),
        body in prop::collection::vec(any::<u8>(), 0..200),
    ) {
        let env = write_envelope(codec, aux_id, raw_len, &digest, &body);
        prop_assert_eq!(env.len(), format::ENVELOPE_HEADER_LEN + body.len());
        let (h, b) = read_envelope(&env)?;
        let expected = EnvelopeHeader { codec, flags: 0, raw_len, body_len: body.len() as u32, aux_id, digest };
        prop_assert_eq!(h, expected);
        prop_assert_eq!(b, &body[..]);
    }

    #[test]
    fn prop_envelope_mutations_never_panic(
        env in (0u32..=MAX_UNIT_LEN).prop_flat_map(arb_envelope_with_raw_len),
        muts in arb_mutations(),
    ) {
        check_decoders_on(&mutate(env, &muts), 0)?;
    }

    #[test]
    fn prop_source_descriptor_roundtrip(s in arb_source(), muts in arb_mutations()) {
        let bytes = s.encode();
        prop_assert_eq!(&SourceDescriptor::decode(&bytes)?, &s);
        check_decoders_on(&mutate(bytes, &muts), 0)?;
    }

    #[test]
    fn prop_param_roundtrip(
        kind in prop_oneof![Just(param_kind::ZSTD_DICT), Just(param_kind::TEMPLATE)],
        bytes in prop::collection::vec(any::<u8>(), 0..100),
    ) {
        let p = Param { kind, version: 1, bytes };
        prop_assert_eq!(Param::decode(&p.encode())?, p);
    }

    #[test]
    fn prop_history_key_order_matches_key_then_revision(
        k1 in arb_key(), r1 in arb_revision(), k2 in arb_key(), r2 in arb_revision(),
    ) {
        let a = history_key(&k1, r1);
        let b = history_key(&k2, r2);
        prop_assert_eq!(a.cmp(&b), (&k1, r1).cmp(&(&k2, r2)));
        prop_assert_eq!(a.starts_with(&history_prefix(&k2)), k1 == k2);
        prop_assert_eq!(parse_history_key(&a)?, (k1, r1));
    }

    #[test]
    fn prop_varint_roundtrip_is_canonical(v in arb_varint_value()) {
        let mut enc = Vec::new();
        put_varint(&mut enc, v);
        let bits = 64 - v.leading_zeros();
        prop_assert_eq!(enc.len() as u32, bits.div_ceil(7).max(1));
        enc.extend_from_slice(&[0x80, 0x00]);
        let mut pos = 0;
        prop_assert_eq!(get_varint(&enc, &mut pos)?, v);
        prop_assert_eq!(pos, enc.len() - 2);
    }

    #[test]
    fn prop_id_keys_sort_numerically(a in any::<u64>(), b in any::<u64>()) {
        prop_assert_eq!(id_key(a).cmp(&id_key(b)), a.cmp(&b));
        prop_assert_eq!(parse_id_key(&id_key(a))?, a);
        prop_assert_eq!(decode_id_list(&encode_id_list(&[a, b]))?, vec![a, b]);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn prop_decoders_never_panic_on_arbitrary_bytes(
        bytes in prop::collection::vec(any::<u8>(), 0..160),
        start in 0usize..170,
    ) {
        check_decoders_on(&bytes, start)?;
    }

    #[test]
    fn prop_decoders_never_panic_on_plausible_prefixes(
        prefix in prop_oneof![
            Just(b"BBO1".to_vec()),
            Just(vec![1u8]),
            Just(hx("01 01 0100")),
            Just(hx("00ff")),
        ],
        rest in prop::collection::vec(prop_oneof![3 => any::<u8>(), 1 => Just(0u8), 1 => Just(1u8)], 0..120),
    ) {
        check_decoders_on(&[prefix, rest].concat(), 0)?;
    }
}

// ===========================================================================
// Independent table-level validator (docs/format.md: tables, lifetime rule)
// ===========================================================================

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn scan_rows<R: ReadTxn + ?Sized>(r: &R, table: Table) -> Rows {
    let mut rows = Vec::new();
    r.scan(
        table,
        Bound::Unbounded,
        Bound::Unbounded,
        false,
        &mut |k: &[u8], v: &[u8]| {
            rows.push((k.to_vec(), v.to_vec()));
            Ok(true)
        },
    )
    .unwrap_or_else(|e| panic!("scan of table {} failed: {e}", table.name()));
    rows
}

/// BLAKE3 over (u64 LE key length, key, u64 LE value length, value)* in key order.
fn rows_digest(rows: &[(Vec<u8>, Vec<u8>)]) -> String {
    let mut h = blake3::Hasher::new();
    for (k, v) in rows {
        h.update(&(k.len() as u64).to_le_bytes());
        h.update(k);
        h.update(&(v.len() as u64).to_le_bytes());
        h.update(v);
    }
    h.finalize().to_hex().to_string()
}

#[derive(Debug, Default)]
struct Validation {
    problems: Vec<String>,
    mode: Option<Mode>,
}

struct Checker {
    problems: Vec<String>,
    mode: Option<Mode>,
    next_revision: u64,
    /// param id -> kind
    params: BTreeMap<u64, u8>,
    /// object id -> (raw_len, digest)
    objects: BTreeMap<u64, (u32, [u8; 32])>,
    sources: BTreeSet<u64>,
    /// object id -> references found in records + history + pending_imports
    refs: BTreeMap<u64, u64>,
}

impl Checker {
    fn problem(&mut self, msg: String) {
        self.problems.push(msg);
    }

    fn envelope(&mut self, what: &str, h: &EnvelopeHeader) {
        if !CodecTag::ALL_V1.contains(&h.codec) {
            self.problem(format!(
                "{what}: codec 0x{:02x} v{} is not a v1 codec",
                h.codec.id, h.codec.version
            ));
        }
        if self.mode == Some(Mode::BabelPure)
            && (h.codec != CodecTag::BABEL_AFFINE_V1 || h.aux_id != 0)
        {
            self.problem(format!(
                "{what}: babel-pure databases store only BabelAffineV1 units without aux_id (found {})",
                h.codec.name()
            ));
        }
        match codec::required_param(h.codec, h.aux_id) {
            Some(kind) => match self.params.get(&h.aux_id).copied() {
                Some(k) if k == kind => {}
                Some(k) => self.problem(format!(
                    "{what}: param {} has kind {k}, codec needs kind {kind}",
                    h.aux_id
                )),
                None => self.problem(format!("{what}: missing param {} (aux_id)", h.aux_id)),
            },
            None if h.aux_id != 0 => self.problem(format!(
                "{what}: aux_id {} set on a codec without dependency",
                h.aux_id
            )),
            None => {}
        }
    }

    fn manifest(&mut self, what: &str, m: &Manifest) {
        if m.revision == 0 || m.revision >= self.next_revision {
            self.problem(format!(
                "{what}: revision {} outside 1..next_revision ({})",
                m.revision, self.next_revision
            ));
        }
        if let Some(id) = m.source_id
            && !self.sources.contains(&id)
        {
            self.problem(format!("{what}: missing source {id}"));
        }
        match &m.body {
            ManifestBody::Inline(env) => match read_envelope(env) {
                Ok((h, _)) => self.envelope(&format!("{what} (inline)"), &h),
                Err(e) => self.problem(format!("{what}: inline envelope: {e}")),
            },
            ManifestBody::Chunks(refs) => {
                let mut start = 0u64;
                for r in refs {
                    let len = r.logical_end - start;
                    start = r.logical_end;
                    *self.refs.entry(r.object_id).or_default() += 1;
                    match self.objects.get(&r.object_id).copied() {
                        None => self.problem(format!("{what}: chunk references missing object {}", r.object_id)),
                        Some((raw_len, _)) if u64::from(raw_len) != len => self.problem(format!(
                            "{what}: chunk of {len} bytes references object {} whose raw_len is {raw_len}",
                            r.object_id
                        )),
                        Some(_) => {}
                    }
                }
            }
            ManifestBody::Generated { .. } | ManifestBody::Tombstone => {}
        }
    }
}

fn row_id(problems: &mut Vec<String>, table: &str, key: &[u8], next: u64) -> Option<u64> {
    match parse_id_key(key) {
        Ok(0) => {
            problems.push(format!("{table}: id 0 is reserved"));
            None
        }
        Ok(id) if id >= next => {
            problems.push(format!(
                "{table}[{id}]: id not below its meta counter ({next})"
            ));
            Some(id)
        }
        Ok(id) => Some(id),
        Err(e) => {
            problems.push(format!("{table}: key {}: {e}", to_hex(key)));
            None
        }
    }
}

fn meta_counter(meta: &BTreeMap<String, Vec<u8>>, name: &str, problems: &mut Vec<String>) -> u64 {
    match meta.get(name).map(|v| format::decode_u64(v)) {
        Some(Ok(v)) if v >= 1 => v,
        Some(Ok(v)) => {
            problems.push(format!("meta: {name} = {v}, counters start at 1"));
            v
        }
        Some(Err(e)) => {
            problems.push(format!("meta: {name}: {e}"));
            0
        }
        None => {
            problems.push(format!("meta: missing {name}"));
            0
        }
    }
}

const KNOWN_META: [&str; 12] = [
    meta_key::FORMAT_VERSION,
    meta_key::MODE,
    meta_key::BLOCK_SIZE,
    meta_key::INLINE_MAX,
    meta_key::NEXT_OBJECT_ID,
    meta_key::NEXT_REVISION,
    meta_key::NEXT_PARAM_ID,
    meta_key::NEXT_SOURCE_ID,
    meta_key::NEXT_IMPORT_ID,
    meta_key::ACTIVE_ZSTD_DICT,
    meta_key::ACTIVE_TEMPLATE,
    meta_key::CREATED_BY,
];

/// Check every table against docs/format.md using only `babeldb::format`
/// (no engine code): encodings, id counters, references, candidate lists,
/// mode invariants and the object lifetime rule (refcount = references in
/// records + history + pending_imports).
fn validate_tables<R: ReadTxn + ?Sized>(r: &R) -> Validation {
    let rows: Vec<Rows> = Table::ALL.iter().map(|&t| scan_rows(r, t)).collect();
    let mut problems = Vec::new();

    let mut meta: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (k, v) in &rows[Table::Meta.index()] {
        match std::str::from_utf8(k) {
            Ok(name) if KNOWN_META.contains(&name) => {
                meta.insert(name.to_string(), v.clone());
            }
            Ok(name) => problems.push(format!("meta: unknown entry {name:?}")),
            Err(_) => problems.push(format!("meta: key {} is not utf-8", to_hex(k))),
        }
    }
    match meta
        .get(meta_key::FORMAT_VERSION)
        .map(|v| format::decode_u32(v))
    {
        Some(Ok(1)) => {}
        other => problems.push(format!(
            "meta: format_version must be u32 LE 1, found {other:?}"
        )),
    }
    let mode = match meta.get(meta_key::MODE).map(Vec::as_slice) {
        Some([b]) => Mode::from_u8(*b),
        _ => None,
    };
    if mode.is_none() {
        problems.push(format!(
            "meta: mode must be one byte (1 or 2), found {:?}",
            meta.get(meta_key::MODE)
        ));
    }
    let block_size = match meta
        .get(meta_key::BLOCK_SIZE)
        .map(|v| format::decode_u32(v))
    {
        Some(Ok(v)) if (MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&v) => v,
        other => {
            problems.push(format!(
                "meta: block_size must be u32 LE in [512, 1 MiB], found {other:?}"
            ));
            0
        }
    };
    match meta
        .get(meta_key::INLINE_MAX)
        .map(|v| format::decode_u32(v))
    {
        Some(Ok(v)) if v <= block_size => {}
        other => problems.push(format!(
            "meta: inline_max must be u32 LE <= block_size, found {other:?}"
        )),
    }
    let next_object_id = meta_counter(&meta, meta_key::NEXT_OBJECT_ID, &mut problems);
    let next_revision = meta_counter(&meta, meta_key::NEXT_REVISION, &mut problems);
    let next_param_id = meta_counter(&meta, meta_key::NEXT_PARAM_ID, &mut problems);
    let next_source_id = meta_counter(&meta, meta_key::NEXT_SOURCE_ID, &mut problems);
    let next_import_id = meta_counter(&meta, meta_key::NEXT_IMPORT_ID, &mut problems);
    if let Some(v) = meta.get(meta_key::CREATED_BY)
        && std::str::from_utf8(v).is_err()
    {
        problems.push("meta: created_by is not utf-8".to_string());
    }

    let mut c = Checker {
        problems,
        mode,
        next_revision,
        params: BTreeMap::new(),
        objects: BTreeMap::new(),
        sources: BTreeSet::new(),
        refs: BTreeMap::new(),
    };

    for (k, v) in &rows[Table::Params.index()] {
        let Some(id) = row_id(&mut c.problems, "params", k, next_param_id) else {
            continue;
        };
        match Param::decode(v) {
            Ok(p) => {
                c.params.insert(id, p.kind);
            }
            Err(e) => c.problem(format!("params[{id}]: {e}")),
        }
    }
    let active = [
        (meta_key::ACTIVE_ZSTD_DICT, param_kind::ZSTD_DICT),
        (meta_key::ACTIVE_TEMPLATE, param_kind::TEMPLATE),
    ];
    for (name, kind) in active {
        match meta.get(name).map(|v| format::decode_u64(v)) {
            None => {}
            Some(Ok(id)) if c.params.get(&id) == Some(&kind) => {}
            other => c.problem(format!(
                "meta: {name} must name a param of kind {kind}, found {other:?}"
            )),
        }
    }

    for (k, v) in &rows[Table::Sources.index()] {
        let Some(id) = row_id(&mut c.problems, "sources", k, next_source_id) else {
            continue;
        };
        match SourceDescriptor::decode(v) {
            Ok(s) => {
                if let Some(li) = &s.last_import
                    && (li.revision == 0 || li.revision >= next_revision)
                {
                    c.problem(format!(
                        "sources[{id}]: last_import revision {} out of range",
                        li.revision
                    ));
                }
                c.sources.insert(id);
            }
            Err(e) => c.problem(format!("sources[{id}]: {e}")),
        }
    }

    for (k, v) in &rows[Table::Objects.index()] {
        let Some(id) = row_id(&mut c.problems, "objects", k, next_object_id) else {
            continue;
        };
        match read_envelope(v) {
            Ok((h, _)) => {
                c.envelope(&format!("objects[{id}]"), &h);
                c.objects.insert(id, (h.raw_len, h.digest));
            }
            Err(e) => c.problem(format!("objects[{id}]: {e}")),
        }
    }

    let mut current: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
    for (k, v) in &rows[Table::Records.index()] {
        let what = format!("records[{}]", to_hex(k));
        match Manifest::decode(v) {
            Ok(m) => {
                c.manifest(&what, &m);
                current.insert(k.clone(), m.revision);
            }
            Err(e) => c.problem(format!("{what}: {e}")),
        }
    }

    for (k, v) in &rows[Table::History.index()] {
        let (user_key, rev) = match parse_history_key(k) {
            Ok(parsed) => parsed,
            Err(e) => {
                c.problem(format!("history: key {}: {e}", to_hex(k)));
                continue;
            }
        };
        let what = format!("history[{}@{rev}]", to_hex(&user_key));
        match Manifest::decode(v) {
            Ok(m) => {
                if m.revision != rev {
                    c.problem(format!(
                        "{what}: manifest revision {} differs from the key revision",
                        m.revision
                    ));
                }
                if let Some(&cur) = current.get(&user_key)
                    && rev >= cur
                {
                    c.problem(format!(
                        "{what}: not older than the current record (revision {cur})"
                    ));
                }
                c.manifest(&what, &m);
            }
            Err(e) => c.problem(format!("{what}: {e}")),
        }
    }

    for (k, v) in &rows[Table::PendingImports.index()] {
        let Some(id) = row_id(&mut c.problems, "pending_imports", k, next_import_id) else {
            continue;
        };
        match decode_id_list(v) {
            Ok(ids) => {
                for oid in ids {
                    *c.refs.entry(oid).or_default() += 1;
                    if !c.objects.contains_key(&oid) {
                        c.problem(format!("pending_imports[{id}]: missing object {oid}"));
                    }
                }
            }
            Err(e) => c.problem(format!("pending_imports[{id}]: {e}")),
        }
    }

    for (k, v) in &rows[Table::HashCandidates.index()] {
        if k.len() != 36 {
            c.problem(format!(
                "hash_candidates: key of {} bytes (expected 36)",
                k.len()
            ));
            continue;
        }
        let digest: [u8; 32] = k[..32].try_into().unwrap();
        let raw_len = u32::from_le_bytes(k[32..].try_into().unwrap());
        let what = format!("hash_candidates[{}..,{raw_len}]", to_hex(&digest[..4]));
        match decode_id_list(v) {
            Ok(ids) => {
                if ids.is_empty() {
                    c.problem(format!("{what}: empty id list"));
                }
                if ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
                    c.problem(format!("{what}: duplicate ids {ids:?}"));
                }
                for oid in ids {
                    match c.objects.get(&oid).copied() {
                        None => c.problem(format!("{what}: dangling candidate {oid}")),
                        Some((l, d)) if l != raw_len || d != digest => c.problem(format!(
                            "{what}: candidate {oid} has a different digest/raw_len"
                        )),
                        Some(_) => {}
                    }
                }
            }
            Err(e) => c.problem(format!("{what}: {e}")),
        }
    }
    if mode == Some(Mode::BabelPure) {
        if !rows[Table::HashCandidates.index()].is_empty() {
            c.problem("babel-pure: hash_candidates must be empty (no dedupe)".to_string());
        }
        if !rows[Table::Params.index()].is_empty() {
            c.problem("babel-pure: params must be empty".to_string());
        }
    }

    let mut stored: BTreeMap<u64, u64> = BTreeMap::new();
    for (k, v) in &rows[Table::Refcounts.index()] {
        let Some(id) = row_id(&mut c.problems, "refcounts", k, next_object_id) else {
            continue;
        };
        match format::decode_u64(v) {
            Ok(0) => c.problem(format!(
                "refcounts[{id}]: zero refcount row (objects are removed at zero)"
            )),
            Ok(n) => {
                stored.insert(id, n);
            }
            Err(e) => c.problem(format!("refcounts[{id}]: {e}")),
        }
        if !c.objects.contains_key(&id) {
            c.problem(format!("refcounts[{id}]: row for a missing object"));
        }
    }
    let ids: Vec<u64> = c.objects.keys().copied().collect();
    for id in ids {
        let s = stored.get(&id).copied().unwrap_or(0);
        let r = c.refs.get(&id).copied().unwrap_or(0);
        if s != r {
            c.problem(format!("objects[{id}]: refcount {s} != references {r} (records + history + pending_imports)"));
        }
    }
    Validation {
        problems: c.problems,
        mode,
    }
}

/// Rows of one table, in a hand-built database.
type TableRows = BTreeMap<Vec<u8>, Vec<u8>>;

fn put_row(t: &mut [TableRows], table: Table, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
    t[table.index()].insert(key.into(), value.into());
}

fn chunks_manifest(revision: u64, source_id: Option<u64>, refs: &[(u64, u64)]) -> Vec<u8> {
    let refs: Vec<ChunkRef> = refs
        .iter()
        .map(|&(logical_end, object_id)| ChunkRef {
            logical_end,
            object_id,
        })
        .collect();
    let logical_len = refs.last().map_or(0, |r| r.logical_end);
    Manifest {
        revision,
        logical_len,
        source_id,
        body: ManifestBody::Chunks(refs),
    }
    .encode()
}

fn tombstone_manifest(revision: u64) -> Vec<u8> {
    Manifest {
        revision,
        logical_len: 0,
        source_id: None,
        body: ManifestBody::Tombstone,
    }
    .encode()
}

/// A small, fully consistent Adaptive database, built by hand from the format.
fn consistent_tables() -> Vec<TableRows> {
    let mut t: Vec<TableRows> = vec![BTreeMap::new(); Table::ALL.len()];
    let counters = [
        (meta_key::NEXT_OBJECT_ID, 4u64),
        (meta_key::NEXT_REVISION, 8),
        (meta_key::NEXT_PARAM_ID, 2),
        (meta_key::NEXT_SOURCE_ID, 2),
        (meta_key::NEXT_IMPORT_ID, 2),
        (meta_key::ACTIVE_ZSTD_DICT, 1),
    ];
    put_row(
        &mut t,
        Table::Meta,
        meta_key::FORMAT_VERSION.as_bytes(),
        1u32.to_le_bytes(),
    );
    put_row(
        &mut t,
        Table::Meta,
        meta_key::MODE.as_bytes(),
        vec![Mode::Adaptive as u8],
    );
    put_row(
        &mut t,
        Table::Meta,
        meta_key::BLOCK_SIZE.as_bytes(),
        512u32.to_le_bytes(),
    );
    put_row(
        &mut t,
        Table::Meta,
        meta_key::INLINE_MAX.as_bytes(),
        64u32.to_le_bytes(),
    );
    for (name, v) in counters {
        put_row(&mut t, Table::Meta, name.as_bytes(), v.to_le_bytes());
    }
    put_row(
        &mut t,
        Table::Meta,
        meta_key::CREATED_BY.as_bytes(),
        &b"babeldb format_compat"[..],
    );

    let dict = Param {
        kind: param_kind::ZSTD_DICT,
        version: 1,
        bytes: b"dict".to_vec(),
    };
    put_row(&mut t, Table::Params, id_key(1), dict.encode());

    let abc = digest_of(BLAKE3_ABC);
    let two = xof(b"validator/two", 100);
    let two_digest = *blake3::hash(&two).as_bytes();
    let three_digest = digest_of(DIGEST_20_3F);
    put_row(
        &mut t,
        Table::Objects,
        id_key(1),
        write_envelope(CodecTag::RAW_V1, 0, 3, &abc, b"abc"),
    );
    put_row(
        &mut t,
        Table::Objects,
        id_key(2),
        write_envelope(CodecTag::RAW_V1, 0, 100, &two_digest, &two),
    );
    put_row(
        &mut t,
        Table::Objects,
        id_key(3),
        write_envelope(CodecTag::ZSTD_V1, 1, 50, &three_digest, b"frame"),
    );
    put_row(
        &mut t,
        Table::HashCandidates,
        candidate_key(&abc, 3),
        encode_id_list(&[1]),
    );
    put_row(
        &mut t,
        Table::HashCandidates,
        candidate_key(&two_digest, 100),
        encode_id_list(&[2]),
    );
    put_row(
        &mut t,
        Table::HashCandidates,
        candidate_key(&three_digest, 50),
        encode_id_list(&[3]),
    );

    let source = SourceDescriptor {
        kind: source_kind::LOCAL_FILE,
        location: "fixtures/a.bin".to_string(),
        adapter_version: 1,
        last_import: Some(ImportInfo {
            revision: 1,
            bytes: 3,
            digest: abc,
            unix_ms: 0,
        }),
    };
    put_row(&mut t, Table::Sources, id_key(1), source.encode());

    let hi = write_envelope(
        CodecTag::RAW_V1,
        0,
        2,
        blake3::hash(b"hi").as_bytes(),
        b"hi",
    );
    let inline = Manifest {
        revision: 3,
        logical_len: 2,
        source_id: None,
        body: ManifestBody::Inline(hi),
    };
    let generated = Manifest {
        revision: 4,
        logical_len: 24,
        source_id: None,
        body: ManifestBody::Generated {
            generator_id: gen_ids::ARITH_U64,
            generator_version: 1,
            params: generator::arith_params(1, 2, 3),
            digest: [0; 32],
        },
    };
    put_row(
        &mut t,
        Table::Records,
        &b"a"[..],
        chunks_manifest(1, Some(1), &[(3, 1)]),
    );
    put_row(
        &mut t,
        Table::Records,
        &b"b"[..],
        chunks_manifest(2, None, &[(3, 1), (103, 2)]),
    );
    put_row(&mut t, Table::Records, &b"c"[..], inline.encode());
    put_row(&mut t, Table::Records, &b"d"[..], generated.encode());
    put_row(&mut t, Table::Records, &b"e"[..], tombstone_manifest(6));
    put_row(
        &mut t,
        Table::Records,
        &b"f"[..],
        chunks_manifest(7, None, &[(50, 3)]),
    );
    // "e" was deleted with history on: its revision-5 manifest still holds object 2.
    put_row(
        &mut t,
        Table::History,
        history_key(b"e", 5),
        chunks_manifest(5, None, &[(100, 2)]),
    );
    // An unfinished import holds object 1.
    put_row(
        &mut t,
        Table::PendingImports,
        id_key(1),
        encode_id_list(&[1]),
    );
    put_row(&mut t, Table::Refcounts, id_key(1), 3u64.to_le_bytes()); // a, b, pending import 1
    put_row(&mut t, Table::Refcounts, id_key(2), 2u64.to_le_bytes()); // b, history e@5
    put_row(&mut t, Table::Refcounts, id_key(3), 1u64.to_le_bytes()); // f
    t
}

fn validate(t: &[TableRows]) -> Validation {
    let store = MemStore::new();
    let mut w = store.begin_write().unwrap();
    for table in Table::ALL {
        for (k, v) in &t[table.index()] {
            w.put(table, k, v).unwrap();
        }
    }
    w.commit(Durability::Immediate).unwrap();
    let r = store.begin_read().unwrap();
    validate_tables(&r)
}

#[test]
fn table_validator_accepts_a_consistent_database() {
    let v = validate(&consistent_tables());
    assert!(v.problems.is_empty(), "{}", v.problems.join("\n"));
    assert_eq!(v.mode, Some(Mode::Adaptive));
}

#[test]
fn table_validator_flags_each_documented_violation() {
    type Corruption = fn(&mut Vec<TableRows>);
    let cases: [(&str, &str, Corruption); 24] = [
        ("refcount drift", "refcount 2 != references 3", |t| {
            put_row(t, Table::Refcounts, id_key(1), 2u64.to_le_bytes())
        }),
        ("missing object", "missing object 2", |t| {
            t[Table::Objects.index()].remove(&id_key(2)[..]);
        }),
        (
            "candidate of another object",
            "different digest/raw_len",
            |t| {
                put_row(
                    t,
                    Table::HashCandidates,
                    candidate_key(&digest_of(BLAKE3_ABC), 3),
                    encode_id_list(&[2]),
                )
            },
        ),
        ("duplicate candidate ids", "duplicate ids", |t| {
            put_row(
                t,
                Table::HashCandidates,
                candidate_key(&digest_of(BLAKE3_ABC), 3),
                encode_id_list(&[1, 1]),
            )
        }),
        ("dangling candidate", "dangling candidate 9", |t| {
            put_row(
                t,
                Table::HashCandidates,
                candidate_key(&[9; 32], 9),
                encode_id_list(&[9]),
            )
        }),
        (
            "chunk length != object raw_len",
            "whose raw_len is 100",
            |t| {
                put_row(
                    t,
                    Table::Records,
                    &b"b"[..],
                    chunks_manifest(2, None, &[(3, 1), (102, 2)]),
                )
            },
        ),
        ("undecodable manifest", "records[61]", |t| {
            put_row(t, Table::Records, &b"a"[..], vec![2u8])
        }),
        ("history key without terminator", "history: key", |t| {
            put_row(
                t,
                Table::History,
                &b"e"[..],
                chunks_manifest(5, None, &[(100, 2)]),
            )
        }),
        (
            "history revision mismatch",
            "differs from the key revision",
            |t| {
                t[Table::History.index()].clear();
                put_row(
                    t,
                    Table::History,
                    history_key(b"e", 4),
                    chunks_manifest(5, None, &[(100, 2)]),
                );
            },
        ),
        (
            "history not older than the record",
            "not older than the current record",
            |t| put_row(t, Table::Records, &b"e"[..], tombstone_manifest(5)),
        ),
        ("unknown format version", "format_version", |t| {
            put_row(
                t,
                Table::Meta,
                meta_key::FORMAT_VERSION.as_bytes(),
                2u32.to_le_bytes(),
            )
        }),
        ("unknown meta entry", "unknown entry", |t| {
            put_row(t, Table::Meta, &b"generators"[..], vec![0u8])
        }),
        (
            "object id >= next_object_id",
            "objects[3]: id not below",
            |t| {
                put_row(
                    t,
                    Table::Meta,
                    meta_key::NEXT_OBJECT_ID.as_bytes(),
                    3u64.to_le_bytes(),
                )
            },
        ),
        (
            "revision >= next_revision",
            "outside 1..next_revision",
            |t| {
                put_row(
                    t,
                    Table::Meta,
                    meta_key::NEXT_REVISION.as_bytes(),
                    7u64.to_le_bytes(),
                )
            },
        ),
        ("missing param", "missing param 1", |t| {
            t[Table::Params.index()].clear()
        }),
        ("param of the wrong kind", "codec needs kind 1", |t| {
            let tpl = Param {
                kind: param_kind::TEMPLATE,
                version: 1,
                bytes: Vec::new(),
            };
            put_row(t, Table::Params, id_key(1), tpl.encode())
        }),
        ("active dictionary missing", "active_zstd_dict", |t| {
            put_row(
                t,
                Table::Meta,
                meta_key::ACTIVE_ZSTD_DICT.as_bytes(),
                9u64.to_le_bytes(),
            )
        }),
        ("missing source", "missing source 1", |t| {
            t[Table::Sources.index()].clear()
        }),
        (
            "pending import of a missing object",
            "pending_imports[1]: missing object 77",
            |t| put_row(t, Table::PendingImports, id_key(1), encode_id_list(&[77])),
        ),
        ("zero refcount row", "zero refcount", |t| {
            put_row(t, Table::Refcounts, id_key(3), 0u64.to_le_bytes())
        }),
        (
            "refcount row of a missing object",
            "row for a missing object",
            |t| {
                t[Table::Objects.index()].remove(&id_key(3)[..]);
                t[Table::Records.index()].remove(&b"f"[..]);
                t[Table::HashCandidates.index()]
                    .remove(&candidate_key(&digest_of(DIGEST_20_3F), 50)[..]);
            },
        ),
        (
            "babel-pure file with other codecs",
            "babel-pure databases store only",
            |t| {
                put_row(
                    t,
                    Table::Meta,
                    meta_key::MODE.as_bytes(),
                    vec![Mode::BabelPure as u8],
                )
            },
        ),
        ("unknown codec in a v1 file", "is not a v1 codec", |t| {
            let env = write_envelope(
                CodecTag {
                    id: 0x7f,
                    version: 1,
                },
                0,
                3,
                &digest_of(BLAKE3_ABC),
                b"abc",
            );
            put_row(t, Table::Objects, id_key(1), env)
        }),
        (
            "aux_id on a codec without dependency",
            "without dependency",
            |t| {
                let env = write_envelope(CodecTag::RAW_V1, 5, 3, &digest_of(BLAKE3_ABC), b"abc");
                put_row(t, Table::Objects, id_key(1), env)
            },
        ),
    ];
    for (what, needle, corrupt) in cases {
        let mut t = consistent_tables();
        corrupt(&mut t);
        let v = validate(&t);
        assert!(
            v.problems.iter().any(|p| p.contains(needle)),
            "{what}: expected a problem mentioning {needle:?}, got {:#?}",
            v.problems
        );
    }
}

// ===========================================================================
// Frozen database fixtures (tests/data)
// ===========================================================================
//
// Record script (same order in both files; block_size 512, inline_max 64,
// keep_history on, import_batch_bytes 1024):
//
//   inline/hello        "hello, babel"                      inline
//   inline/empty        ""                                  inline, empty value
//   inline/max          64 bytes 00..3f                     inline, len == inline_max
//   blocks/min          65 bytes                            1 chunk (inline_max + 1)
//   blocks/multi        1300 B BLAKE3-XOF                   3 chunks (512, 512, 276)
//   blocks/dup          block A x3                          same block 3x (dedupe in Adaptive)
//   blocks/dup-cross    block B ++ block A                  dedupe across records
//   codec/repeat        "abc" x400                          RepeatV1 candidate
//   codec/arith         160 u64 LE (1000 + 7i)              ArithmeticU64V1 candidate
//   codec/text          15 chat JSON lines                  LZ4/Zstd candidate
//   codec/random        700 B BLAKE3-XOF                    Raw fallback
//   bin\0key\xff        v1 then v2                          key with 0x00/0xFF, escaped history key
//   overwrite           inline v1, 600 B v2, inline v3      history with inline and chunked manifests
//   deleted             700 B, then deleted                 tombstone + history holding objects
//   gen/arith           generator 1 v1 (10, 3, 200)         Generated manifest
//   gen/xof             generator 2 v1 ([7; 32], 5000)      Generated manifest
//   gen/repeat          generator 3 v1 (4096, "babel ")     Generated manifest
//   source/import       1500 B imported with a source       sources + last_import
//   import/failed       source errors after 2048 B          never published; import cleanup runs
//   import/crashed      source panics after 2048 B          never published; the pending_imports row
//                       (simulated crash, caught)           of its committed batches stays for gc
//   (Adaptive only) a forced Zstd dictionary + dict/chat-0..3, a forced template +
//   template/doc-0..2 (best effort: training failures are recorded as notes).
//
// In babel-pure, `put_generated` may be rejected by design (no mixing of
// representations); the rejection is then recorded as a note and the key as absent.

const FIXTURE_BLOCK_SIZE: u32 = 512;
const FIXTURE_INLINE_MAX: u32 = 64;
const EXPECTED_FILE: &str = "v1_expected.txt";
const FIXTURES: [(&str, Mode); 2] = [
    ("v1_adaptive.redb", Mode::Adaptive),
    ("v1_babel_pure.redb", Mode::BabelPure),
];
const REGENERATE_ENV: &str = "BABELDB_REGENERATE_FIXTURES";

fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
}

fn fixture_config(mode: Mode) -> Config {
    let mut cfg = match mode {
        Mode::Adaptive => Config::adaptive(),
        Mode::BabelPure => Config::babel_pure(),
    };
    cfg.block_size = FIXTURE_BLOCK_SIZE;
    cfg.inline_max = FIXTURE_INLINE_MAX;
    cfg.keep_history = true;
    cfg.import_batch_bytes = 1024;
    cfg
}

fn fixture_block_a() -> Vec<u8> {
    xof(b"fixture/block-a", 512)
}

fn fixture_dup_value() -> Vec<u8> {
    fixture_block_a().repeat(3)
}

fn chat_message(i: u64) -> Vec<u8> {
    format!(
        "{{\"id\":\"{}\",\"channel_id\":\"1029384756\",\"author\":{{\"id\":\"{}\",\"username\":\"user{}\"}},\
         \"content\":\"fixture message {i} about the library\",\"timestamp\":\"2026-09-27T12:{:02}:{:02}.000Z\"}}",
        1_300_000_000_000_000_000u64 + i * 4_194_304,
        900_000 + i % 13,
        i % 13,
        (i / 60) % 60,
        i % 60
    )
    .into_bytes()
}

fn chat_lines(start: u64, n: u64) -> Vec<u8> {
    (start..start + n)
        .flat_map(|i| [chat_message(i), b"\n".to_vec()].concat())
        .collect()
}

fn template_doc(i: u64) -> Vec<u8> {
    format!(
        "<!doctype html>\n<html lang=\"pt-BR\">\n<head><meta charset=\"utf-8\"><title>Hexagono {i}</title></head>\n\
         <body>\n<h1>Hexagono {i}</h1>\n<p>Cinco prateleiras por parede, trinta e dois livros por prateleira.</p>\n\
         <p>Livro {} de 410 paginas, 40 linhas por pagina.</p>\n</body>\n</html>\n",
        i * 7 % 32
    )
    .into_bytes()
}

enum Step {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Generated(Vec<u8>, u16, Vec<u8>),
    Import {
        key: Vec<u8>,
        data: Vec<u8>,
        location: &'static str,
    },
    /// The source stops after `fail_at` bytes: with an error, or (`crash`)
    /// with a panic that skips the import's own cleanup, like a killed process.
    InterruptedImport {
        key: Vec<u8>,
        data: Vec<u8>,
        fail_at: usize,
        crash: bool,
    },
    TrainDictionary(Vec<Vec<u8>>),
    TrainTemplate(Vec<Vec<u8>>),
}

fn fixture_script(mode: Mode) -> Vec<Step> {
    let k = |s: &[u8]| s.to_vec();
    let block_a = fixture_block_a();
    let block_b = xof(b"fixture/block-b", 512);
    let mut steps = vec![
        Step::Put(k(b"inline/hello"), b"hello, babel".to_vec()),
        Step::Put(k(b"inline/empty"), Vec::new()),
        Step::Put(k(b"inline/max"), (0..64u8).collect()),
        Step::Put(
            k(b"blocks/min"),
            (0..65u8).map(|i| i.wrapping_mul(37)).collect(),
        ),
        Step::Put(k(b"blocks/multi"), xof(b"fixture/multi", 1300)),
        Step::Put(k(b"blocks/dup"), fixture_dup_value()),
        Step::Put(k(b"blocks/dup-cross"), [block_b, block_a].concat()),
        Step::Put(k(b"codec/repeat"), b"abc".repeat(400)),
        Step::Put(
            k(b"codec/arith"),
            (0..160u64)
                .flat_map(|i| (1000 + 7 * i).to_le_bytes())
                .collect(),
        ),
        Step::Put(k(b"codec/text"), chat_lines(0, 15)),
        Step::Put(k(b"codec/random"), xof(b"fixture/random", 700)),
        Step::Put(k(b"bin\x00key\xff"), b"binary key v1".to_vec()),
        Step::Put(k(b"bin\x00key\xff"), b"binary key v2".to_vec()),
        Step::Put(k(b"overwrite"), b"first version".to_vec()),
        Step::Put(k(b"overwrite"), xof(b"fixture/overwrite-2", 600)),
        Step::Put(k(b"overwrite"), b"third version".to_vec()),
        Step::Put(k(b"deleted"), xof(b"fixture/deleted", 700)),
        Step::Delete(k(b"deleted")),
        Step::Generated(
            k(b"gen/arith"),
            gen_ids::ARITH_U64,
            generator::arith_params(10, 3, 200),
        ),
        Step::Generated(
            k(b"gen/xof"),
            gen_ids::BLAKE3_XOF,
            generator::blake3_xof_params(&[7; 32], 5000),
        ),
        Step::Generated(
            k(b"gen/repeat"),
            gen_ids::REPEAT,
            generator::repeat_params(4096, b"babel "),
        ),
        Step::Import {
            key: k(b"source/import"),
            data: xof(b"fixture/source", 1500),
            location: "fixtures/source.bin",
        },
        Step::InterruptedImport {
            key: k(b"import/failed"),
            data: xof(b"fixture/failed", 4096),
            fail_at: 2048,
            crash: false,
        },
        Step::InterruptedImport {
            key: k(b"import/crashed"),
            data: xof(b"fixture/crashed", 4096),
            fail_at: 2048,
            crash: true,
        },
    ];
    if mode == Mode::Adaptive {
        steps.push(Step::TrainDictionary(
            (1000..1400).map(chat_message).collect(),
        ));
        for i in 0..4u64 {
            steps.push(Step::Put(
                format!("dict/chat-{i}").into_bytes(),
                chat_message(5000 + i),
            ));
        }
        steps.push(Step::TrainTemplate((0..40).map(template_doc).collect()));
        for i in 0..3u64 {
            steps.push(Step::Put(
                format!("template/doc-{i}").into_bytes(),
                template_doc(100 + i),
            ));
        }
    }
    steps
}

/// Expected output of generators 1 and 3 (unambiguous); generator 2 is left to
/// the engine here and pinned by `after_merge_generator_goldens`.
fn generated_value(generator_id: u16, params: &[u8]) -> Option<Vec<u8>> {
    let u = |i: usize| u64::from_le_bytes(params[i * 8..i * 8 + 8].try_into().unwrap());
    match generator_id {
        gen_ids::ARITH_U64 => Some(
            (0..u(2))
                .flat_map(|i| (u(0) + i * u(1)).to_le_bytes())
                .collect(),
        ),
        gen_ids::REPEAT => Some(
            params[8..]
                .iter()
                .copied()
                .cycle()
                .take(u(0) as usize)
                .collect(),
        ),
        _ => None,
    }
}

struct SliceSource<'a> {
    data: &'a [u8],
    pos: usize,
    /// Fail (instead of reporting end of input) once this many bytes were read.
    fail_at: Option<usize>,
    /// Fail by panicking instead of returning an error.
    crash: bool,
}

impl ByteSource for SliceSource<'_> {
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
        let limit = self.fail_at.unwrap_or(self.data.len()).min(self.data.len());
        if self.pos >= limit {
            if self.fail_at.is_some() {
                if self.crash {
                    panic!("simulated crash during an import (fixture)");
                }
                return Err(Error::Io(std::io::Error::other(
                    "simulated source failure (fixture)",
                )));
            }
            return Ok(0);
        }
        let n = buf.len().min(limit - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[derive(Default)]
struct Model {
    /// Current value of every live key (`None`: defined by the engine).
    values: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Every key the script used.
    touched: BTreeSet<Vec<u8>>,
    /// Keys published at least once (their history is recorded).
    written: BTreeSet<Vec<u8>>,
    notes: Vec<String>,
}

fn train_options() -> TrainOptions {
    TrainOptions {
        max_dict_bytes: 4096,
        require_gain: false,
        ..TrainOptions::default()
    }
}

fn run_step(db: &Db, mode: Mode, step: Step, model: &mut Model, name: &str) {
    match step {
        Step::Put(key, value) => {
            db.put(&key, &value, Expect::Any)
                .unwrap_or_else(|e| panic!("{name}: put {}: {e}", show_key(&key)));
            model.touched.insert(key.clone());
            model.written.insert(key.clone());
            model.values.insert(key, Some(value));
        }
        Step::Delete(key) => {
            let deleted = db
                .delete(&key, Expect::Any)
                .unwrap_or_else(|e| panic!("{name}: delete {}: {e}", show_key(&key)));
            assert!(deleted, "{name}: delete {} found nothing", show_key(&key));
            model.values.remove(&key);
        }
        Step::Generated(key, generator_id, params) => {
            model.touched.insert(key.clone());
            match db.put_generated(&key, generator_id, 1, &params, Expect::Any) {
                Ok(_) => {
                    model.written.insert(key.clone());
                    model
                        .values
                        .insert(key, generated_value(generator_id, &params));
                }
                Err(e @ (Error::Unsupported(_) | Error::InvalidArgument(_)))
                    if mode == Mode::BabelPure =>
                {
                    model.notes.push(format!(
                        "put_generated({generator_id}) rejected in babel-pure: {e}"
                    ));
                }
                Err(e) => panic!(
                    "{name}: put_generated {} (generator {generator_id}): {e}",
                    show_key(&key)
                ),
            }
        }
        Step::Import {
            key,
            data,
            location,
        } => {
            let source_id = db
                .register_source(SourceDescriptor::local_file(location))
                .unwrap_or_else(|e| panic!("{name}: register_source: {e}"));
            let mut src = SliceSource {
                data: &data,
                pos: 0,
                fail_at: None,
                crash: false,
            };
            let opts = ImportOptions {
                expect: Expect::Any,
                source_id: Some(source_id),
            };
            db.import(&key, &mut src, &opts)
                .unwrap_or_else(|e| panic!("{name}: import {}: {e}", show_key(&key)));
            model.touched.insert(key.clone());
            model.written.insert(key.clone());
            model.values.insert(key, Some(data));
        }
        Step::InterruptedImport {
            key,
            data,
            fail_at,
            crash,
        } => {
            model.touched.insert(key.clone());
            let mut src = SliceSource {
                data: &data,
                pos: 0,
                fail_at: Some(fail_at),
                crash,
            };
            let how = if crash {
                // The source reads happen outside the write transactions, so the
                // unwinding leaves the committed batches and their pending_imports
                // row behind, as a killed process would.
                eprintln!("{name}: the next panic message is the simulated import crash");
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    db.import(&key, &mut src, &ImportOptions::default())
                }));
                assert!(
                    r.is_err(),
                    "{name}: the simulated crash did not interrupt the import"
                );
                "simulated crash"
            } else {
                let r = db.import(&key, &mut src, &ImportOptions::default());
                assert!(
                    r.is_err(),
                    "{name}: an import whose source fails must not publish (got {r:?})"
                );
                "source error"
            };
            let pending = table_len(db, Table::PendingImports);
            model.notes.push(format!(
                "{} interrupted by a {how}: pending_imports rows now {pending}",
                String::from_utf8_lossy(&key)
            ));
        }
        Step::TrainDictionary(samples) => match db.train_dictionary(&samples, &train_options()) {
            Ok(rep) => model.notes.push(format!(
                "zstd dictionary installed={} param_id={:?}",
                rep.installed, rep.param_id
            )),
            Err(e) => model
                .notes
                .push(format!("zstd dictionary training failed: {e}")),
        },
        Step::TrainTemplate(samples) => match db.train_template(&samples, &train_options()) {
            Ok(rep) => model.notes.push(format!(
                "template installed={} param_id={:?}",
                rep.installed, rep.param_id
            )),
            Err(e) => model.notes.push(format!("template training failed: {e}")),
        },
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FixtureExpect {
    mode: String,
    block_size: u32,
    inline_max: u32,
    notes: Vec<String>,
    live: Vec<LiveExpect>,
    absent: Vec<Vec<u8>>,
    history: Vec<HistoryExpect>,
    sources: Vec<SourceExpect>,
    tables: Vec<TableExpect>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LiveExpect {
    key: Vec<u8>,
    rev: u64,
    len: u64,
    kind: String,
    blake3: String,
    codecs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HistoryExpect {
    key: Vec<u8>,
    rev: u64,
    len: u64,
    tombstone: bool,
    current: bool,
    blake3: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceExpect {
    id: u64,
    kind: u8,
    adapter: u16,
    location: String,
    /// (revision, bytes, BLAKE3 hex) of the last import.
    last: Option<(u64, u64, String)>,
}

impl SourceExpect {
    fn of(id: u64, s: &SourceDescriptor) -> SourceExpect {
        SourceExpect {
            id,
            kind: s.kind,
            adapter: s.adapter_version,
            location: s.location.clone(),
            last: s
                .last_import
                .as_ref()
                .map(|i| (i.revision, i.bytes, to_hex(&i.digest))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableExpect {
    name: String,
    entries: u64,
    blake3: String,
}

fn collect_expectations(db: &Db, model: &Model, name: &str) -> FixtureExpect {
    let mut fx = FixtureExpect {
        mode: db.mode().as_str().to_string(),
        block_size: db.block_size(),
        inline_max: db.inline_max(),
        notes: model.notes.clone(),
        ..FixtureExpect::default()
    };
    for (key, expected) in &model.values {
        let shown = show_key(key);
        let value = db
            .get(key)
            .unwrap()
            .unwrap_or_else(|| panic!("{name}: {shown} is missing after reopen"));
        if let Some(expected) = expected {
            assert!(
                value == *expected,
                "{name}: {shown} reads back different bytes"
            );
        }
        let (rev, len) = db
            .head(key)
            .unwrap()
            .unwrap_or_else(|| panic!("{name}: head {shown}"));
        assert_eq!(len, value.len() as u64, "{name}: head length of {shown}");
        let ins = db
            .inspect(key)
            .unwrap()
            .unwrap_or_else(|| panic!("{name}: inspect {shown}"));
        assert_eq!(ins.revision, rev, "{name}: inspect revision of {shown}");
        fx.live.push(LiveExpect {
            key: key.clone(),
            rev,
            len,
            kind: ins.kind.to_string(),
            blake3: blake3_hex(&value),
            codecs: ins.units.iter().map(|u| u.codec.clone()).collect(),
        });
    }
    for key in model
        .touched
        .iter()
        .filter(|k| !model.values.contains_key(*k))
    {
        assert!(
            db.get(key).unwrap().is_none(),
            "{name}: {} should be absent",
            show_key(key)
        );
        fx.absent.push(key.clone());
    }
    for key in &model.written {
        for e in db.history(key).unwrap() {
            let value = db.get_at(key, e.revision).unwrap();
            fx.history.push(HistoryExpect {
                key: key.clone(),
                rev: e.revision,
                len: e.logical_len,
                tombstone: e.tombstone,
                current: e.current,
                blake3: value.as_deref().map(blake3_hex),
            });
        }
    }
    fx.sources = db
        .sources()
        .unwrap()
        .iter()
        .map(|(id, s)| SourceExpect::of(*id, s))
        .collect();
    let r = db.store().begin_read().unwrap();
    for t in Table::ALL {
        let rows = scan_rows(&r, t);
        fx.tables.push(TableExpect {
            name: t.name().to_string(),
            entries: rows.len() as u64,
            blake3: rows_digest(&rows),
        });
    }
    fx
}

fn build_fixture(path: &Path, mode: Mode) -> FixtureExpect {
    let name = path.file_name().unwrap().to_string_lossy().into_owned();
    let mut model = Model::default();
    {
        let mut db =
            Db::open(path, fixture_config(mode)).unwrap_or_else(|e| panic!("{name}: create: {e}"));
        for step in fixture_script(mode) {
            run_step(&db, mode, step, &mut model, &name);
        }
        match db.compact() {
            Ok(rep) => eprintln!(
                "{name}: compact supported={} apparent {} -> {} bytes",
                rep.supported, rep.apparent_before, rep.apparent_after
            ),
            Err(e) => eprintln!("{name}: compact failed (fixture kept uncompacted): {e}"),
        }
    }
    let db = Db::open(path, fixture_config(mode)).unwrap_or_else(|e| panic!("{name}: reopen: {e}"));
    assert_consistent(&db, &name, "freshly generated");
    let fx = collect_expectations(&db, &model, &name);
    for l in &fx.live {
        let codecs = l.codecs.join(",");
        eprintln!(
            "{name}: {:<44} rev={:<3} len={:<5} {:<9} {codecs}",
            show_key(&l.key),
            l.rev,
            l.len,
            l.kind
        );
    }
    for t in &fx.tables {
        eprintln!("{name}: table {:<16} {} rows", t.name, t.entries);
    }
    for n in &fx.notes {
        eprintln!("{name}: note: {n}");
    }
    fx
}

fn key_token(key: &[u8]) -> String {
    if key.is_empty() {
        "-".to_string()
    } else {
        to_hex(key)
    }
}

fn parse_key_token(t: &str) -> Vec<u8> {
    if t == "-" { Vec::new() } else { hx(t) }
}

const EXPECTED_HEADER: &str = "\
# babeldb persistent format v1: frozen fixtures. DO NOT EDIT, DO NOT REGENERATE.
# Written once by `cargo test --test format_compat generate_v1_fixtures -- --ignored`,
# checked by `open_v1_fixtures` (tests/format_compat.rs, record script `fixture_script`).
# Keys and locations are lowercase hex (\"-\" = empty). Line types:
#   fixture <file> mode=<mode> block_size=<n> inline_max=<n>
#   note    <file> <free text>
#   live    <file> <key> rev=<n> len=<n> kind=<kind> blake3=<hex> codecs=<name,...|->
#   absent  <file> <key>
#   history <file> <key> rev=<n> len=<n> tombstone=<0|1> current=<0|1> blake3=<hex|->
#   source  <file> id=<n> kind=<n> adapter=<n> location=<hex> last=<rev>,<bytes>,<blake3>|-
#   table   <file> <name> entries=<n> blake3=<BLAKE3 of (u64 LE len, key, u64 LE len, value)*>
";

fn render_expected(fixtures: &[(String, FixtureExpect)]) -> String {
    let mut s = String::from(EXPECTED_HEADER);
    for (file, fx) in fixtures {
        writeln!(
            s,
            "fixture {file} mode={} block_size={} inline_max={}",
            fx.mode, fx.block_size, fx.inline_max
        )
        .unwrap();
        for n in &fx.notes {
            writeln!(s, "note {file} {n}").unwrap();
        }
        for l in &fx.live {
            let codecs = if l.codecs.is_empty() {
                "-".to_string()
            } else {
                l.codecs.join(",")
            };
            let key = key_token(&l.key);
            writeln!(
                s,
                "live {file} {key} rev={} len={} kind={} blake3={} codecs={codecs}",
                l.rev, l.len, l.kind, l.blake3
            )
            .unwrap();
        }
        for k in &fx.absent {
            writeln!(s, "absent {file} {}", key_token(k)).unwrap();
        }
        for h in &fx.history {
            writeln!(
                s,
                "history {file} {} rev={} len={} tombstone={} current={} blake3={}",
                key_token(&h.key),
                h.rev,
                h.len,
                u8::from(h.tombstone),
                u8::from(h.current),
                h.blake3.as_deref().unwrap_or("-")
            )
            .unwrap();
        }
        for src in &fx.sources {
            let last = match &src.last {
                Some((rev, bytes, digest)) => format!("{rev},{bytes},{digest}"),
                None => "-".to_string(),
            };
            let location = key_token(src.location.as_bytes());
            writeln!(
                s,
                "source {file} id={} kind={} adapter={} location={location} last={last}",
                src.id, src.kind, src.adapter
            )
            .unwrap();
        }
        for t in &fx.tables {
            writeln!(
                s,
                "table {file} {} entries={} blake3={}",
                t.name, t.entries, t.blake3
            )
            .unwrap();
        }
    }
    s
}

fn field<'a>(tokens: &[&'a str], name: &str) -> &'a str {
    tokens
        .iter()
        .filter_map(|t| t.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
        .unwrap_or_else(|| panic!("{EXPECTED_FILE}: missing field {name} in {tokens:?}"))
}

fn num<T: std::str::FromStr>(tokens: &[&str], name: &str) -> T {
    field(tokens, name)
        .parse()
        .unwrap_or_else(|_| panic!("{EXPECTED_FILE}: bad number for {name} in {tokens:?}"))
}

fn parse_expected(text: &str) -> BTreeMap<String, FixtureExpect> {
    let mut out: BTreeMap<String, FixtureExpect> = BTreeMap::new();
    for line in text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        assert!(
            tokens.len() >= 2,
            "{EXPECTED_FILE}: malformed line {line:?}"
        );
        let fx = out.entry(tokens[1].to_string()).or_default();
        match tokens[0] {
            "fixture" => {
                fx.mode = field(&tokens, "mode").to_string();
                fx.block_size = num(&tokens, "block_size");
                fx.inline_max = num(&tokens, "inline_max");
            }
            "note" => fx.notes.push(tokens[2..].join(" ")),
            "live" => fx.live.push(LiveExpect {
                key: parse_key_token(tokens[2]),
                rev: num(&tokens, "rev"),
                len: num(&tokens, "len"),
                kind: field(&tokens, "kind").to_string(),
                blake3: field(&tokens, "blake3").to_string(),
                codecs: match field(&tokens, "codecs") {
                    "-" => Vec::new(),
                    list => list.split(',').map(str::to_string).collect(),
                },
            }),
            "absent" => fx.absent.push(parse_key_token(tokens[2])),
            "history" => fx.history.push(HistoryExpect {
                key: parse_key_token(tokens[2]),
                rev: num(&tokens, "rev"),
                len: num(&tokens, "len"),
                tombstone: field(&tokens, "tombstone") == "1",
                current: field(&tokens, "current") == "1",
                blake3: match field(&tokens, "blake3") {
                    "-" => None,
                    d => Some(d.to_string()),
                },
            }),
            "source" => fx.sources.push(SourceExpect {
                id: num(&tokens, "id"),
                kind: num(&tokens, "kind"),
                adapter: num(&tokens, "adapter"),
                location: String::from_utf8(parse_key_token(field(&tokens, "location")))
                    .expect("utf-8 location"),
                last: match field(&tokens, "last") {
                    "-" => None,
                    l => {
                        let parts: Vec<&str> = l.split(',').collect();
                        assert_eq!(parts.len(), 3, "{EXPECTED_FILE}: bad last= in {line:?}");
                        Some((
                            parts[0].parse().unwrap(),
                            parts[1].parse().unwrap(),
                            parts[2].to_string(),
                        ))
                    }
                },
            }),
            "table" => fx.tables.push(TableExpect {
                name: tokens[2].to_string(),
                entries: num(&tokens, "entries"),
                blake3: field(&tokens, "blake3").to_string(),
            }),
            other => panic!("{EXPECTED_FILE}: unknown line type {other:?}"),
        }
    }
    out
}

#[test]
fn expected_file_roundtrip() {
    let live_inline = LiveExpect {
        key: b"bin\x00key\xff".to_vec(),
        rev: 13,
        len: 13,
        kind: "inline".to_string(),
        blake3: blake3_hex(b"x"),
        codecs: vec!["RawV1".to_string()],
    };
    let live_generated = LiveExpect {
        key: Vec::new(),
        rev: 1,
        len: 5000,
        kind: "generated".to_string(),
        blake3: blake3_hex(b"y"),
        codecs: Vec::new(),
    };
    let old = HistoryExpect {
        key: b"deleted".to_vec(),
        rev: 17,
        len: 700,
        tombstone: false,
        current: false,
        blake3: Some(blake3_hex(b"z")),
    };
    let tomb = HistoryExpect {
        key: b"deleted".to_vec(),
        rev: 18,
        len: 0,
        tombstone: true,
        current: true,
        blake3: None,
    };
    let fx = FixtureExpect {
        mode: "adaptive".to_string(),
        block_size: 512,
        inline_max: 64,
        notes: vec!["zstd dictionary installed=true param_id=Some(1)".to_string()],
        live: vec![live_inline, live_generated],
        absent: vec![b"deleted".to_vec()],
        history: vec![old, tomb],
        sources: vec![
            SourceExpect {
                id: 1,
                kind: 1,
                adapter: 1,
                location: "fixtures/source bin \u{e7}".to_string(),
                last: Some((22, 1500, blake3_hex(b"s"))),
            },
            SourceExpect {
                id: 2,
                kind: 3,
                adapter: 7,
                location: String::new(),
                last: None,
            },
        ],
        tables: vec![TableExpect {
            name: "records".to_string(),
            entries: 21,
            blake3: blake3_hex(b"t"),
        }],
    };
    let other = FixtureExpect {
        mode: "babel-pure".to_string(),
        block_size: 512,
        inline_max: 64,
        ..FixtureExpect::default()
    };
    let text = render_expected(&[
        ("a.redb".to_string(), fx.clone()),
        ("b.redb".to_string(), other.clone()),
    ]);
    // Checkouts may turn LF into CRLF.
    let parsed = parse_expected(&text.replace('\n', "\r\n"));
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed["a.redb"], fx);
    assert_eq!(parsed["b.redb"], other);
}

/// The `live` entry of `key` in a fixture expectation.
fn live<'a>(fx: &'a FixtureExpect, key: &[u8], name: &str) -> &'a LiveExpect {
    fx.live
        .iter()
        .find(|l| l.key == key)
        .unwrap_or_else(|| panic!("{name}: {} not in {EXPECTED_FILE}", show_key(key)))
}

fn table_len(db: &Db, table: Table) -> u64 {
    db.store().begin_read().unwrap().len(table).unwrap()
}

fn check_live_values(db: &Db, fx: &FixtureExpect, name: &str, skip: &BTreeSet<Vec<u8>>) {
    for l in fx.live.iter().filter(|l| !skip.contains(&l.key)) {
        let key = show_key(&l.key);
        let value = db
            .get(&l.key)
            .unwrap_or_else(|e| panic!("{name}: get {key}: {e}"))
            .unwrap_or_else(|| panic!("{name}: live key {key} not found"));
        assert_eq!(value.len() as u64, l.len, "{name}: length of {key}");
        assert_eq!(
            blake3_hex(&value),
            l.blake3,
            "{name}: bytes of {key} changed"
        );
        assert_eq!(
            db.head(&l.key).unwrap(),
            Some((l.rev, l.len)),
            "{name}: head of {key}"
        );
        let ins = db
            .inspect(&l.key)
            .unwrap()
            .unwrap_or_else(|| panic!("{name}: inspect {key}"));
        assert_eq!(
            (ins.kind, ins.revision),
            (l.kind.as_str(), l.rev),
            "{name}: inspect of {key}"
        );
        let codecs: Vec<String> = ins.units.iter().map(|u| u.codec.clone()).collect();
        assert_eq!(codecs, l.codecs, "{name}: unit codecs of {key}");
        if l.len > 530 {
            let part = db
                .get_range(&l.key, 500, 30)
                .unwrap()
                .unwrap_or_else(|| panic!("{name}: get_range {key}"));
            assert_eq!(
                part,
                value[500..530],
                "{name}: get_range across a block boundary of {key}"
            );
        }
    }
    for k in fx.absent.iter().filter(|k| !skip.contains(*k)) {
        assert_eq!(
            db.get(k).unwrap(),
            None,
            "{name}: {} must stay absent",
            show_key(k)
        );
        assert_eq!(
            db.head(k).unwrap(),
            None,
            "{name}: head of absent {}",
            show_key(k)
        );
    }
}

fn check_history_and_sources(db: &Db, fx: &FixtureExpect, name: &str) {
    let mut by_key: BTreeMap<&[u8], Vec<&HistoryExpect>> = BTreeMap::new();
    for h in &fx.history {
        by_key.entry(&h.key[..]).or_default().push(h);
    }
    for (key, want) in by_key {
        let got: Vec<(u64, u64, bool, bool)> = db
            .history(key)
            .unwrap()
            .iter()
            .map(|e| (e.revision, e.logical_len, e.tombstone, e.current))
            .collect();
        let exp: Vec<(u64, u64, bool, bool)> = want
            .iter()
            .map(|h| (h.rev, h.len, h.tombstone, h.current))
            .collect();
        assert_eq!(got, exp, "{name}: history of {}", show_key(key));
        for h in want {
            let value = db.get_at(key, h.rev).unwrap();
            assert_eq!(
                value.as_deref().map(blake3_hex),
                h.blake3,
                "{name}: {}@{}",
                show_key(key),
                h.rev
            );
        }
    }
    let got: Vec<SourceExpect> = db
        .sources()
        .unwrap()
        .iter()
        .map(|(id, s)| SourceExpect::of(*id, s))
        .collect();
    assert_eq!(got, fx.sources, "{name}: sources");
}

#[track_caller]
fn assert_consistent(db: &Db, name: &str, when: &str) {
    let v = validate_tables(&db.store().begin_read().unwrap());
    assert!(
        v.problems.is_empty(),
        "{name} ({when}): tables violate docs/format.md:\n{}",
        v.problems.join("\n")
    );
    let report = db.verify(true).unwrap();
    assert!(
        report.ok(),
        "{name} ({when}): verify(deep) failed: {report:?}"
    );
}

fn check_fixture(src: &Path, mode: Mode, fx: &FixtureExpect) {
    let name = src.file_name().unwrap().to_string_lossy().into_owned();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join(&name);
    std::fs::copy(src, &path).unwrap();

    // Default creation parameters on purpose: the persisted ones must win.
    let mut db =
        Db::open(&path, Config::adaptive()).unwrap_or_else(|e| panic!("{name}: open failed: {e}"));
    assert_eq!(db.mode(), mode, "{name}: persisted mode");
    assert_eq!(
        db.mode().as_str(),
        fx.mode,
        "{name}: mode in {EXPECTED_FILE}"
    );
    assert_eq!(
        (db.block_size(), db.inline_max()),
        (fx.block_size, fx.inline_max),
        "{name}: persisted sizes"
    );

    // 1. The tables hold exactly the frozen bytes, and they are valid v1.
    let pending_before = {
        let r = db.store().begin_read().unwrap();
        for t in Table::ALL {
            let rows = scan_rows(&r, t);
            match fx.tables.iter().find(|e| e.name == t.name()) {
                Some(e) => {
                    assert_eq!(
                        rows.len() as u64,
                        e.entries,
                        "{name}: entries of table {}",
                        t.name()
                    );
                    assert_eq!(
                        rows_digest(&rows),
                        e.blake3,
                        "{name}: bytes of table {} changed",
                        t.name()
                    );
                }
                None => assert!(
                    rows.is_empty(),
                    "{name}: table {} is not part of the fixture but has rows",
                    t.name()
                ),
            }
        }
        let v = validate_tables(&r);
        assert!(
            v.problems.is_empty(),
            "{name}: frozen tables violate docs/format.md:\n{}",
            v.problems.join("\n")
        );
        assert_eq!(v.mode, Some(mode));
        r.len(Table::PendingImports).unwrap()
    };

    // 2. Every value, history entry and source reads back exactly.
    check_live_values(&db, fx, &name, &BTreeSet::new());
    check_history_and_sources(&db, fx, &name);
    assert!(db.verify(false).unwrap().ok(), "{name}: verify(shallow)");
    assert_consistent(&db, &name, "as frozen");
    let stats = db.stats().unwrap();
    assert_eq!(
        (stats.mode, stats.block_size, stats.inline_max),
        (mode, fx.block_size, fx.inline_max)
    );

    // 3. Writing still works on a v1 file.
    let max_rev = fx
        .live
        .iter()
        .map(|l| l.rev)
        .chain(fx.history.iter().map(|h| h.rev))
        .max()
        .unwrap_or(0);
    let new_value = xof(b"compat/new", 1500);
    let rev_new = db.put(b"compat/new", &new_value, Expect::Absent).unwrap();
    assert!(
        rev_new > max_rev,
        "{name}: new revision {rev_new} must exceed the frozen ones ({max_rev})"
    );
    assert_eq!(
        db.get(b"compat/new").unwrap().as_deref(),
        Some(&new_value[..])
    );
    let hello = live(fx, b"inline/hello", &name);
    db.put(b"inline/hello", b"hello again", Expect::Revision(hello.rev))
        .unwrap_or_else(|e| panic!("{name}: optimistic put against the frozen revision: {e}"));
    assert_eq!(
        db.get(b"inline/hello").unwrap().as_deref(),
        Some(&b"hello again"[..])
    );
    assert!(
        db.delete(b"blocks/multi", Expect::Any).unwrap(),
        "{name}: delete blocks/multi"
    );
    assert_eq!(db.get(b"blocks/multi").unwrap(), None);

    // Dedupe against frozen objects goes through the frozen hash_candidates.
    let dup = fixture_dup_value();
    assert_eq!(
        blake3_hex(&dup),
        live(fx, b"blocks/dup", &name).blake3,
        "{name}: fixture script drifted"
    );
    let objects_before = table_len(&db, Table::Objects);
    db.put(b"compat/dup-again", &dup, Expect::Absent).unwrap();
    let objects_after = table_len(&db, Table::Objects);
    match mode {
        Mode::Adaptive => assert_eq!(
            objects_after, objects_before,
            "{name}: dedupe must reuse the frozen objects"
        ),
        Mode::BabelPure => assert_eq!(
            objects_after,
            objects_before + 3,
            "{name}: babel-pure never dedupes"
        ),
    }
    let changed: BTreeSet<Vec<u8>> = [b"inline/hello".to_vec(), b"blocks/multi".to_vec()].into();
    check_live_values(&db, fx, &name, &changed);
    assert_consistent(&db, &name, "after writes");

    // 4. The interrupted import frozen in pending_imports is collectable.
    let gc = db.gc().unwrap();
    assert_eq!(
        gc.abandoned_imports, pending_before,
        "{name}: abandoned imports collected by gc"
    );
    assert_eq!(
        table_len(&db, Table::PendingImports),
        0,
        "{name}: pending_imports after gc"
    );
    assert_consistent(&db, &name, "after gc");
    drop(db);

    // 5. Reopen with the other default mode: the persisted mode wins again.
    let db = Db::open(&path, Config::babel_pure())
        .unwrap_or_else(|e| panic!("{name}: reopen failed: {e}"));
    assert_eq!(db.mode(), mode, "{name}: persisted mode after reopen");
    check_live_values(&db, fx, &name, &changed);
    assert_eq!(
        db.get(b"compat/new").unwrap().as_deref(),
        Some(&new_value[..])
    );
    assert_eq!(
        db.get(b"compat/dup-again").unwrap().as_deref(),
        Some(&dup[..])
    );
    assert_consistent(&db, &name, "after reopen");
}

#[test]
#[ignore = "regenerates the frozen v1 fixtures in tests/data (run once, then commit)"]
fn generate_v1_fixtures() {
    let dir = data_dir();
    let targets: Vec<PathBuf> = FIXTURES
        .iter()
        .map(|(file, _)| dir.join(file))
        .chain([dir.join(EXPECTED_FILE)])
        .collect();
    if targets.iter().any(|p| p.exists()) && std::env::var(REGENERATE_ENV).as_deref() != Ok("1") {
        eprintln!(
            "generate_v1_fixtures: fixtures already exist in {}; frozen fixtures are never regenerated \
             (set {REGENERATE_ENV}=1 only if they were never published)",
            dir.display()
        );
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut all = Vec::new();
    for (file, mode) in FIXTURES {
        all.push((
            file.to_string(),
            build_fixture(&tmp.path().join(file), mode),
        ));
    }
    std::fs::create_dir_all(&dir).unwrap();
    for (file, _) in FIXTURES {
        std::fs::copy(tmp.path().join(file), dir.join(file)).unwrap();
    }
    std::fs::write(dir.join(EXPECTED_FILE), render_expected(&all)).unwrap();
    // The fresh fixtures must pass exactly what `open_v1_fixtures` will check forever.
    for ((file, mode), (_, fx)) in FIXTURES.iter().zip(&all) {
        check_fixture(&dir.join(file), *mode, fx);
    }
    let written: Vec<String> = targets.iter().map(|p| p.display().to_string()).collect();
    eprintln!(
        "generate_v1_fixtures: wrote {} (commit them)",
        written.join(", ")
    );
}

#[test]
fn open_v1_fixtures() {
    let dir = data_dir();
    let needed: Vec<PathBuf> = FIXTURES
        .iter()
        .map(|(file, _)| dir.join(file))
        .chain([dir.join(EXPECTED_FILE)])
        .collect();
    let missing: Vec<String> = needed
        .iter()
        .filter(|p| !p.exists())
        .map(|p| p.display().to_string())
        .collect();
    if !missing.is_empty() {
        eprintln!(
            "open_v1_fixtures: skipped, fixtures not generated yet (missing: {}). Generate them once with \
             `cargo test --test format_compat generate_v1_fixtures -- --ignored` and commit tests/data.",
            missing.join(", ")
        );
        return;
    }
    let text = std::fs::read_to_string(dir.join(EXPECTED_FILE)).unwrap();
    let expected = parse_expected(&text);
    for (file, mode) in FIXTURES {
        let fx = expected
            .get(file)
            .unwrap_or_else(|| panic!("{EXPECTED_FILE} has no entries for {file}"));
        check_fixture(&dir.join(file), mode, fx);
    }
}
