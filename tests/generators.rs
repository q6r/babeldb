//! Built-in generators: fixed vectors, random access, invalid params, registry.

use std::io::{Read, Seek, SeekFrom};
use std::sync::LazyLock;

use babeldb::Error;
use babeldb::generator::{
    ARITH_U64_PARAMS_LEN, BLAKE3_XOF_PARAMS_LEN, Generator, REPEAT_MAX_MOTIF_LEN, Registry,
    arith_params, blake3_xof_params, ids, repeat_params,
};
use proptest::prelude::*;

static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::builtin);

fn builtin(id: u16) -> &'static dyn Generator {
    REGISTRY.get(id, 1).expect("built-in generator")
}

fn full(g: &dyn Generator, params: &[u8]) -> Vec<u8> {
    let len = g.output_len(params).expect("valid params");
    let mut out = vec![0u8; usize::try_from(len).expect("test output fits in memory")];
    g.generate(params, 0, &mut out).expect("full output");
    out
}

fn range(g: &dyn Generator, params: &[u8], offset: u64, len: usize) -> babeldb::Result<Vec<u8>> {
    let mut out = vec![0u8; len];
    g.generate(params, offset, &mut out)?;
    Ok(out)
}

fn is_invalid<T>(r: babeldb::Result<T>) -> bool {
    matches!(r, Err(Error::InvalidArgument(_)))
}

/// Reference XOF stream, read sequentially from position 0.
fn xof_reference(key: &[u8; 32], len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    blake3::Hasher::new_keyed(key).finalize_xof().fill(&mut out);
    out
}

/// Naive ARITH_U64 oracle: element i is start + step * i.
fn arith_oracle(start: u64, step: u64, count: u64) -> Vec<u8> {
    (0..count)
        .flat_map(|i| (start + step * i).to_le_bytes())
        .collect()
}

/// Naive REPEAT oracle: byte k is motif[k % motif.len()].
fn repeat_oracle(total: usize, motif: &[u8]) -> Vec<u8> {
    (0..total).map(|k| motif[k % motif.len()]).collect()
}

/// Hash the output generated in `chunk`-sized pieces (how the engine streams
/// the digest at write time).
fn streamed_digest(g: &dyn Generator, params: &[u8], chunk: usize) -> blake3::Hash {
    let len = g.output_len(params).expect("valid params");
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; chunk];
    let mut offset = 0u64;
    while offset < len {
        let n = chunk.min((len - offset) as usize);
        g.generate(params, offset, &mut buf[..n]).expect("chunk");
        hasher.update(&buf[..n]);
        offset += n as u64;
    }
    hasher.finalize()
}

// ---------------------------------------------------------------- vectors

#[test]
fn arith_fixed_vector() {
    let g = builtin(ids::ARITH_U64);
    let p = arith_params(0, 1, 4);
    #[rustfmt::skip]
    let expected: [u8; 32] = [
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0,
        2, 0, 0, 0, 0, 0, 0, 0,
        3, 0, 0, 0, 0, 0, 0, 0,
    ];
    assert_eq!(g.output_len(&p).unwrap(), 32);
    assert_eq!(full(g, &p), expected);
    // Partial elements at both ends: the tail of element 0, element 1, the head of element 2.
    assert_eq!(range(g, &p, 5, 14).unwrap(), expected[5..19]);
    // Entirely inside one element.
    assert_eq!(range(g, &p, 17, 3).unwrap(), expected[17..20]);
}

#[test]
fn arith_values_are_little_endian_u64() {
    let g = builtin(ids::ARITH_U64);
    let p = arith_params(0x0102_0304_0506_0708, 0x0100, 2);
    #[rustfmt::skip]
    let expected: [u8; 16] = [
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        0x08, 0x08, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
    ];
    assert_eq!(full(g, &p), expected);
}

#[test]
fn arith_extremes() {
    let g = builtin(ids::ARITH_U64);
    // Values that end exactly at u64::MAX are valid (no wrap needed).
    let p = arith_params(u64::MAX - 2, 1, 3);
    assert_eq!(full(g, &p), arith_oracle(u64::MAX - 2, 1, 3));
    let p = arith_params(0, u64::MAX, 2);
    assert_eq!(
        full(g, &p),
        [0u64.to_le_bytes(), u64::MAX.to_le_bytes()].concat()
    );
    let p = arith_params(15, 16, 1 << 60);
    assert_eq!(g.output_len(&p).unwrap(), 1 << 63);
    let last = range(g, &p, (1 << 63) - 8, 8).unwrap();
    assert_eq!(last, u64::MAX.to_le_bytes());
    // The largest representable output: count = 2^61 - 1 gives 2^64 - 8 bytes.
    let count = (1u64 << 61) - 1;
    let p = arith_params(7, 0, count);
    let len = g.output_len(&p).unwrap();
    assert_eq!(len, u64::MAX - 7);
    let tail = range(g, &p, len - 12, 12).unwrap();
    assert_eq!(
        tail,
        [&7u64.to_le_bytes()[4..], &7u64.to_le_bytes()[..]].concat()
    );
    // Far into a long progression, starting mid-element.
    let (start, step, count) = (5u64, 3u64, 1u64 << 40);
    let p = arith_params(start, step, count);
    let offset = 8 * (count - 3) + 3;
    let expected: Vec<u8> = (count - 3..count)
        .flat_map(|i| (start + step * i).to_le_bytes())
        .collect();
    assert_eq!(range(g, &p, offset, 10).unwrap(), expected[3..13]);
    // count == 0 is a valid, empty progression whatever start and step are.
    let p = arith_params(u64::MAX, u64::MAX, 0);
    assert_eq!(g.output_len(&p).unwrap(), 0);
    assert!(full(g, &p).is_empty());
}

#[test]
fn repeat_fixed_vector() {
    let g = builtin(ids::REPEAT);
    let p = repeat_params(5, b"ab");
    assert_eq!(g.output_len(&p).unwrap(), 5);
    assert_eq!(full(g, &p), b"ababa");
    assert_eq!(range(g, &p, 3, 2).unwrap(), b"ba");
    assert_eq!(range(g, &p, 1, 3).unwrap(), b"bab");
}

#[test]
fn repeat_edge_cases() {
    let g = builtin(ids::REPEAT);
    // Empty output.
    assert!(full(g, &repeat_params(0, b"x")).is_empty());
    // Motif longer than the output: a prefix of the motif.
    assert_eq!(full(g, &repeat_params(3, b"abcdef")), b"abc");
    // Single-byte motif.
    assert_eq!(full(g, &repeat_params(4, b"z")), b"zzzz");
    // The largest motif is accepted, and its phase wraps correctly.
    let motif: Vec<u8> = (0..REPEAT_MAX_MOTIF_LEN).map(|i| (i % 251) as u8).collect();
    let p = repeat_params(3 * REPEAT_MAX_MOTIF_LEN as u64, &motif);
    assert_eq!(full(g, &p), repeat_oracle(3 * REPEAT_MAX_MOTIF_LEN, &motif));
    let offset = 2 * REPEAT_MAX_MOTIF_LEN as u64 - 5;
    assert_eq!(
        range(g, &p, offset, 10).unwrap(),
        [&motif[motif.len() - 5..], &motif[..5]].concat()
    );
    // The very end of a u64::MAX-long output: phase = offset % motif_len.
    let p = repeat_params(u64::MAX, b"abc");
    let offset = u64::MAX - 4;
    let expected: Vec<u8> = (0..4)
        .map(|i| b"abc"[((offset + i) % 3) as usize])
        .collect();
    assert_eq!(range(g, &p, offset, 4).unwrap(), expected);
}

#[test]
fn xof_matches_blake3_reference_stream() {
    let g = builtin(ids::BLAKE3_XOF);
    let key: [u8; 32] = std::array::from_fn(|i| i as u8);
    let p = blake3_xof_params(&key, 1000);
    let expected = xof_reference(&key, 1000);
    assert_eq!(g.output_len(&p).unwrap(), 1000);
    assert_eq!(full(g, &p), expected);
    // The first 32 bytes are the keyed hash of the empty message, which blake3
    // computes by a different path (root compression, not the XOF block loop).
    assert_eq!(&expected[..32], blake3::keyed_hash(&key, b"").as_bytes());
    // Ranges that start, end or straddle 64-byte BLAKE3 output blocks.
    for (offset, len) in [
        (0, 1),
        (1, 63),
        (63, 2),
        (64, 64),
        (100, 300),
        (127, 129),
        (999, 1),
        (1000, 0),
    ] {
        assert_eq!(
            range(g, &p, offset as u64, len).unwrap(),
            expected[offset..offset + len],
            "@{offset}+{len}"
        );
    }
    // Byte-at-a-time generation (partial-block path only) matches too.
    let bytewise: Vec<u8> = (0..300u64)
        .map(|i| range(g, &p, i, 1).unwrap()[0])
        .collect();
    assert_eq!(bytewise, expected[..300]);
}

#[test]
fn xof_far_offsets_seek_without_prefix_work() {
    let g = builtin(ids::BLAKE3_XOF);
    let key = *b"whats the Elvish word for friend";
    let p = blake3_xof_params(&key, u64::MAX);
    assert_eq!(g.output_len(&p).unwrap(), u64::MAX);
    for offset in [u64::MAX - 100, (1 << 40) + 13, 1 << 63] {
        let mut expected = vec![0u8; 100];
        let mut reader = blake3::Hasher::new_keyed(&key).finalize_xof();
        reader.seek(SeekFrom::Start(offset)).unwrap();
        reader.read_exact(&mut expected).unwrap();
        assert_eq!(range(g, &p, offset, 100).unwrap(), expected, "@{offset}");
    }
    // The last byte of the longest stream is addressable; one more is not.
    assert_eq!(range(g, &p, u64::MAX - 1, 1).unwrap().len(), 1);
    assert!(is_invalid(range(g, &p, u64::MAX, 1)));
}

#[test]
fn streamed_generation_matches_full_output() {
    let key = [0x5a; 32];
    let cases: [(u16, Vec<u8>); 3] = [
        (ids::ARITH_U64, arith_params(1, 1 << 32, 1000)),
        (ids::BLAKE3_XOF, blake3_xof_params(&key, 10_000)),
        (ids::REPEAT, repeat_params(10_000, b"0123456")),
    ];
    for (id, p) in cases {
        let g = builtin(id);
        let want = blake3::hash(&full(g, &p));
        for chunk in [1, 7, 64, 4096, 1 << 20] {
            assert_eq!(
                streamed_digest(g, &p, chunk),
                want,
                "{} chunk {chunk}",
                g.name()
            );
        }
    }
}

/// Regression digests that freeze the v1 outputs. They were computed with this
/// implementation after the oracle checks in this file passed, so they catch
/// changes, not first-time bugs. A mismatch means a v1 output changed. Do not
/// update the digest: ship the new behaviour as a new generator version.
#[test]
fn v1_outputs_are_frozen() {
    let key = *b"babeldb/generator/v1 golden key!";
    let motif = b"Library of Babel ";
    let cases: [(u16, Vec<u8>, Vec<u8>, &str); 3] = [
        (
            ids::ARITH_U64,
            arith_params(0x0123_4567_89ab_cdef, 0x1111_1111, 4096),
            arith_oracle(0x0123_4567_89ab_cdef, 0x1111_1111, 4096),
            "c63cdeee1a4bd03058a7dabdb20a6f73ea73172105c5fd36be097ae7aabf3101",
        ),
        (
            ids::BLAKE3_XOF,
            blake3_xof_params(&key, 10_000),
            xof_reference(&key, 10_000),
            "2c6fed11b356e1703d6e55a4dc784c1917b7ffda6d6f0e89e772bbdcdbd678df",
        ),
        (
            ids::REPEAT,
            repeat_params(100_003, motif),
            repeat_oracle(100_003, motif),
            "eb8bc3ce531302987c3f830c270dc1618331bebd38324c932c09dcd60a56fd25",
        ),
    ];
    let mut changed = Vec::new();
    for (id, params, oracle, golden) in cases {
        let g = builtin(id);
        let out = full(g, &params);
        assert!(out == oracle, "{} disagrees with its oracle", g.name());
        let digest = blake3::hash(&out).to_hex();
        if digest.as_str() != golden {
            changed.push(format!("{} v1: digest {digest}, frozen {golden}", g.name()));
        }
    }
    assert!(
        changed.is_empty(),
        "v1 outputs changed:\n{}",
        changed.join("\n")
    );
}

// --------------------------------------------------------- invalid input

#[test]
fn arith_rejects_invalid_params() {
    let g = builtin(ids::ARITH_U64);
    for len in [
        0,
        1,
        8,
        16,
        ARITH_U64_PARAMS_LEN - 1,
        ARITH_U64_PARAMS_LEN + 1,
        32,
        48,
    ] {
        let p = vec![0u8; len];
        assert!(is_invalid(g.output_len(&p)), "len {len}");
        assert!(is_invalid(g.generate(&p, 0, &mut [])), "len {len}");
    }
    let overflowing = [
        arith_params(0, 0, 1 << 61),          // count * 8 overflows
        arith_params(0, 0, u64::MAX),         // count * 8 overflows
        arith_params(u64::MAX, 1, 2),         // last value overflows
        arith_params(u64::MAX - 1, 1, 3),     // last value overflows
        arith_params(1, u64::MAX, 2),         // last value overflows
        arith_params(16, 16, 1 << 60),        // start + step * (count - 1) == 2^64
        arith_params(0, u64::MAX / 2 + 1, 3), // step * (count - 1) overflows
    ];
    for p in overflowing {
        assert!(is_invalid(g.output_len(&p)), "{p:?}");
        assert!(is_invalid(g.generate(&p, 0, &mut [0u8; 8])), "{p:?}");
    }
}

#[test]
fn xof_rejects_invalid_params() {
    let g = builtin(ids::BLAKE3_XOF);
    for len in [
        0,
        8,
        32,
        BLAKE3_XOF_PARAMS_LEN - 1,
        BLAKE3_XOF_PARAMS_LEN + 1,
        48,
        80,
    ] {
        let p = vec![7u8; len];
        assert!(is_invalid(g.output_len(&p)), "len {len}");
        assert!(is_invalid(g.generate(&p, 0, &mut [])), "len {len}");
    }
}

#[test]
fn repeat_rejects_invalid_params() {
    let g = builtin(ids::REPEAT);
    // Missing or truncated total_len, or an empty motif.
    for len in 0..=8 {
        let p = vec![1u8; len];
        assert!(is_invalid(g.output_len(&p)), "len {len}");
        assert!(is_invalid(g.generate(&p, 0, &mut [])), "len {len}");
    }
    assert!(is_invalid(g.output_len(&repeat_params(10, b""))));
    let too_long = vec![b'x'; REPEAT_MAX_MOTIF_LEN + 1];
    assert!(is_invalid(g.output_len(&repeat_params(10, &too_long))));
    assert!(is_invalid(g.generate(
        &repeat_params(10, &too_long),
        0,
        &mut [0u8; 4]
    )));
    assert_eq!(
        g.output_len(&repeat_params(10, &too_long[1..])).unwrap(),
        10
    );
}

#[test]
fn out_of_range_reads_are_rejected() {
    let key = [3u8; 32];
    let cases: [(u16, Vec<u8>, u64); 3] = [
        (ids::ARITH_U64, arith_params(1, 2, 4), 32),
        (ids::BLAKE3_XOF, blake3_xof_params(&key, 1000), 1000),
        (ids::REPEAT, repeat_params(5, b"ab"), 5),
    ];
    for (id, p, len) in cases {
        let g = builtin(id);
        let n = len as usize;
        assert_eq!(g.output_len(&p).unwrap(), len);
        // Empty reads anywhere in [0, len] are fine.
        assert!(g.generate(&p, 0, &mut []).is_ok());
        assert!(g.generate(&p, len, &mut []).is_ok());
        // Past the end, straddling the end, longer than the output, overflowing offset + len.
        assert!(is_invalid(g.generate(&p, len + 1, &mut [])), "{}", g.name());
        assert!(is_invalid(range(g, &p, len - 1, 2)), "{}", g.name());
        assert!(is_invalid(range(g, &p, 0, n + 1)), "{}", g.name());
        assert!(is_invalid(range(g, &p, u64::MAX, 1)), "{}", g.name());
        assert!(is_invalid(range(g, &p, u64::MAX - 1, 4)), "{}", g.name());
        assert!(
            is_invalid(g.generate(&p, u64::MAX, &mut [])),
            "{}",
            g.name()
        );
    }
    // Outputs of length u64::MAX: the range end may reach u64::MAX but not pass it.
    for (id, p) in [
        (ids::BLAKE3_XOF, blake3_xof_params(&key, u64::MAX)),
        (ids::REPEAT, repeat_params(u64::MAX, b"xyz")),
    ] {
        let g = builtin(id);
        assert!(g.generate(&p, u64::MAX, &mut []).is_ok());
        assert!(range(g, &p, u64::MAX - 3, 3).is_ok());
        assert!(is_invalid(range(g, &p, u64::MAX - 3, 4)));
        assert!(is_invalid(range(g, &p, u64::MAX, 1)));
    }
}

#[test]
fn failed_generate_leaves_output_untouched() {
    let key = [9u8; 32];
    let failing: [(u16, Vec<u8>, u64); 5] = [
        (ids::ARITH_U64, arith_params(1, 1, 2), 12), // range past the end
        (ids::ARITH_U64, vec![0u8; 23], 0),          // bad params
        (ids::BLAKE3_XOF, blake3_xof_params(&key, 10), 5),
        (ids::REPEAT, repeat_params(5, b"ab"), 1),
        (ids::REPEAT, repeat_params(5, b""), 0),
    ];
    for (id, p, offset) in failing {
        let mut out = [0xAAu8; 8];
        assert!(is_invalid(builtin(id).generate(&p, offset, &mut out)));
        assert_eq!(out, [0xAA; 8]);
    }
}

// --------------------------------------------------------------- registry

#[test]
fn builtin_registry_has_three_v1_generators() {
    assert_eq!(
        REGISTRY.list(),
        vec![
            (ids::ARITH_U64, 1, "ArithU64"),
            (ids::BLAKE3_XOF, 1, "Blake3Xof"),
            (ids::REPEAT, 1, "Repeat")
        ]
    );
    for (id, version, name) in REGISTRY.list() {
        let g = REGISTRY.get(id, version).unwrap();
        assert_eq!((g.id(), g.version(), g.name()), (id, version, name));
    }
}

#[test]
fn unknown_generators_are_reported() {
    for (id, version) in [
        (1, 0),
        (1, 2),
        (2, 2),
        (0, 1),
        (4, 1),
        (u16::MAX, 1),
        (3, u16::MAX),
    ] {
        match REGISTRY.get(id, version) {
            Err(Error::UnknownGenerator { id: i, version: v }) => assert_eq!((i, v), (id, version)),
            Err(e) => panic!("({id}, {version}): unexpected error {e}"),
            Ok(g) => panic!("({id}, {version}): unexpectedly found {}", g.name()),
        }
    }
    assert!(matches!(
        Registry::empty().get(ids::ARITH_U64, 1),
        Err(Error::UnknownGenerator {
            id: ids::ARITH_U64,
            version: 1
        })
    ));
    assert!(Registry::empty().list().is_empty());
}

struct Fake {
    id: u16,
    version: u16,
}

impl Generator for Fake {
    fn id(&self) -> u16 {
        self.id
    }
    fn version(&self) -> u16 {
        self.version
    }
    fn name(&self) -> &'static str {
        "Fake"
    }
    fn output_len(&self, _params: &[u8]) -> babeldb::Result<u64> {
        Ok(0)
    }
    fn generate(&self, _params: &[u8], _offset: u64, out: &mut [u8]) -> babeldb::Result<()> {
        out.fill(0);
        Ok(())
    }
}

#[test]
fn registration_cannot_shadow_builtins() {
    let mut r = Registry::builtin();
    r.register(Box::new(Fake {
        id: ids::ARITH_U64,
        version: 1,
    }));
    assert_eq!(r.get(ids::ARITH_U64, 1).unwrap().name(), "ArithU64");
    assert_eq!(r.list().len(), 3);
    // A new version or a new id is added normally.
    r.register(Box::new(Fake {
        id: ids::ARITH_U64,
        version: 2,
    }));
    r.register(Box::new(Fake {
        id: 1000,
        version: 1,
    }));
    assert_eq!(r.get(ids::ARITH_U64, 2).unwrap().name(), "Fake");
    assert_eq!(r.get(1000, 1).unwrap().name(), "Fake");
    assert_eq!(r.list().len(), 5);
}

// --------------------------------------------------------------- proptest

/// Valid ARITH_U64 params from raw randomness: step and start are reduced so
/// that `start + step * (count - 1)` fits in u64, extremes included.
fn arith_valid(count: u64, step_raw: u64, start_raw: u64) -> (u64, u64, u64) {
    let span_elems = count.saturating_sub(1);
    let step = match u64::MAX.checked_div(span_elems) {
        Some(max_step) if max_step < u64::MAX => step_raw % (max_step + 1),
        _ => step_raw,
    };
    let span = step * span_elems;
    let start = if span == 0 {
        start_raw
    } else {
        start_raw % (u64::MAX - span + 1)
    };
    (start, step, count)
}

fn sub_range(len: usize) -> impl Strategy<Value = (usize, usize)> {
    (0..=len).prop_flat_map(move |a| (Just(a), a..=len))
}

// Case counts follow proptest's default (256), overridable with PROPTEST_CASES.
proptest! {
    #[test]
    fn arith_random_access(
        (start, step, count) in (0u64..=300, any::<u64>(), any::<u64>())
            .prop_map(|(c, st, s)| arith_valid(c, st, s)),
        picks in prop::collection::vec((any::<prop::sample::Index>(), any::<prop::sample::Index>()), 1..8),
    ) {
        let g = builtin(ids::ARITH_U64);
        let p = arith_params(start, step, count);
        let whole = full(g, &p);
        prop_assert_eq!(&whole, &arith_oracle(start, step, count));
        for (i, j) in picks {
            let (a, b) = { let (x, y) = (i.index(whole.len() + 1), j.index(whole.len() + 1)); (x.min(y), x.max(y)) };
            prop_assert_eq!(range(g, &p, a as u64, b - a).unwrap(), &whole[a..b]);
        }
    }

    #[test]
    fn xof_random_access(
        key in any::<[u8; 32]>(),
        (len, (a, b)) in (0usize..=2048).prop_flat_map(|len| (Just(len), sub_range(len))),
    ) {
        let g = builtin(ids::BLAKE3_XOF);
        let p = blake3_xof_params(&key, len as u64);
        let whole = full(g, &p);
        prop_assert_eq!(&whole, &xof_reference(&key, len));
        prop_assert_eq!(range(g, &p, a as u64, b - a).unwrap(), &whole[a..b]);
    }

    #[test]
    fn repeat_random_access(
        motif in prop::collection::vec(any::<u8>(), 1..=40),
        (total, (a, b)) in (0usize..=2048).prop_flat_map(|t| (Just(t), sub_range(t))),
    ) {
        let g = builtin(ids::REPEAT);
        let p = repeat_params(total as u64, &motif);
        let whole = full(g, &p);
        prop_assert_eq!(&whole, &repeat_oracle(total, &motif));
        prop_assert_eq!(range(g, &p, a as u64, b - a).unwrap(), &whole[a..b]);
    }

    /// Far offsets in huge outputs, checked byte by byte against closed forms.
    #[test]
    fn far_offsets_match_closed_forms(
        (start, step, count) in ((1u64 << 40)..(1u64 << 61), any::<u64>(), any::<u64>())
            .prop_map(|(c, st, s)| arith_valid(c, st, s)),
        total in any::<u64>(),
        motif in prop::collection::vec(any::<u8>(), 1..=16),
        key in any::<[u8; 32]>(),
        back in any::<u64>(),
        n in 0usize..=64,
    ) {
        let n64 = n as u64;
        // ARITH_U64: byte k is byte k % 8 of start + step * (k / 8).
        let g = builtin(ids::ARITH_U64);
        let p = arith_params(start, step, count);
        let len = count * 8;
        let offset = len - n64 - back % (len - n64 + 1);
        let expected: Vec<u8> = (offset..offset + n64)
            .map(|k| (start + step * (k / 8)).to_le_bytes()[(k % 8) as usize])
            .collect();
        prop_assert_eq!(range(g, &p, offset, n).unwrap(), expected);

        // REPEAT: byte k is motif[k % m].
        prop_assume!(total >= n64);
        let g = builtin(ids::REPEAT);
        let p = repeat_params(total, &motif);
        // Any offset in [0, total - n]; (total - n) + 1 overflows only when total - n == u64::MAX.
        let offset = match (total - n64).checked_add(1) {
            Some(choices) => back % choices,
            None => back,
        };
        let m = motif.len() as u64;
        let expected: Vec<u8> = (offset..offset + n64).map(|k| motif[(k % m) as usize]).collect();
        prop_assert_eq!(range(g, &p, offset, n).unwrap(), expected);

        // BLAKE3_XOF: the reference reader seeked with io::Seek.
        let g = builtin(ids::BLAKE3_XOF);
        let p = blake3_xof_params(&key, total);
        let mut expected = vec![0u8; n];
        let mut reader = blake3::Hasher::new_keyed(&key).finalize_xof();
        reader.seek(SeekFrom::Start(offset)).unwrap();
        reader.read_exact(&mut expected).unwrap();
        prop_assert_eq!(range(g, &p, offset, n).unwrap(), expected);
    }

    /// Arbitrary params and ranges never panic. `generate` succeeds exactly
    /// when the params are valid and the range fits, and every failure is
    /// `InvalidArgument`.
    #[test]
    fn arbitrary_input_never_panics(
        id in 1u16..=3,
        params in prop_oneof![
            prop::collection::vec(any::<u8>(), ARITH_U64_PARAMS_LEN),
            prop::collection::vec(any::<u8>(), BLAKE3_XOF_PARAMS_LEN),
            prop::collection::vec(any::<u8>(), 0..=96),
        ],
        offset in prop_oneof![0u64..=256, any::<u64>(), (u64::MAX - 256)..=u64::MAX],
        n in 0usize..=64,
    ) {
        let g = builtin(id);
        let len = g.output_len(&params);
        let mut out = vec![0u8; n];
        let res = g.generate(&params, offset, &mut out);
        let fits = match &len {
            Ok(l) => offset.checked_add(n as u64).is_some_and(|end| end <= *l),
            Err(_) => false,
        };
        prop_assert_eq!(res.is_ok(), fits, "{} params {:?} offset {} n {}", g.name(), params, offset, n);
        if let Err(e) = &len {
            prop_assert!(matches!(e, Error::InvalidArgument(_)), "{e}");
        }
        if let Err(e) = &res {
            prop_assert!(matches!(e, Error::InvalidArgument(_)), "{e}");
        }
    }

    #[test]
    fn registry_lookup_matches_builtins(id in any::<u16>(), version in any::<u16>()) {
        let known = version == 1 && [ids::ARITH_U64, ids::BLAKE3_XOF, ids::REPEAT].contains(&id);
        match REGISTRY.get(id, version) {
            Ok(g) => {
                prop_assert!(known);
                prop_assert_eq!((g.id(), g.version()), (id, version));
            }
            Err(Error::UnknownGenerator { id: i, version: v }) => {
                prop_assert!(!known);
                prop_assert_eq!((i, v), (id, version));
            }
            Err(e) => prop_assert!(false, "unexpected error {e}"),
        }
    }
}
