//! `BabelAffineV1` (O(n) limb arithmetic) against a `num-bigint` oracle that
//! evaluates the definition directly: seed = ((x - 1) * inv(5)) mod 2^(8n),
//! x = (5 * seed + 1) mod 2^(8n), with inv(5) obtained by Newton iteration.

use std::collections::HashSet;
use std::sync::OnceLock;

use babeldb::codec::babel_affine;
use num_bigint::BigUint;
use proptest::prelude::*;

const MAX_LEN: usize = 4096;

fn mask(n: usize) -> BigUint {
    (BigUint::from(1u32) << (8 * n as u64)) - 1u32
}

/// inv(5) mod 2^(8 * MAX_LEN) by Newton iteration inv <- inv * (2 - 5 * inv),
/// which doubles the number of correct low bits each step. Reducing it mod
/// 2^(8n) gives the inverse for every shorter length.
fn inv5() -> &'static BigUint {
    static INV: OnceLock<BigUint> = OnceLock::new();
    INV.get_or_init(|| {
        let bits = 8 * MAX_LEN as u64;
        let m = mask(MAX_LEN);
        let modulus = &m + 1u32;
        let five = BigUint::from(5u32);
        // 5 * 1 = 1 (mod 4): correct to 2 bits.
        let mut inv = BigUint::from(1u32);
        let mut correct_bits = 2u64;
        while correct_bits < bits {
            let five_inv = (&five * &inv) & &m;
            let factor = (&modulus + 2u32 - five_inv) & &m;
            inv = (inv * factor) & &m;
            correct_bits *= 2;
        }
        assert_eq!((&five * &inv) & &m, BigUint::from(1u32));
        inv
    })
}

/// Big-endian bytes of `v`, left-padded with zeros to exactly `n` bytes.
fn to_fixed_be(v: &BigUint, n: usize) -> Vec<u8> {
    let bytes = v.to_bytes_be();
    let skip = bytes.iter().take_while(|&&b| b == 0).count();
    let bytes = &bytes[skip..];
    assert!(bytes.len() <= n);
    let mut out = vec![0u8; n];
    out[n - bytes.len()..].copy_from_slice(bytes);
    out
}

fn oracle_encode(data: &[u8]) -> Vec<u8> {
    let n = data.len();
    if n == 0 {
        return Vec::new();
    }
    let m = mask(n);
    let x = BigUint::from_bytes_be(data);
    // x - 1 mod 2^(8n) == x + (2^(8n) - 1) mod 2^(8n)
    let x_minus_1 = (x + &m) & &m;
    let seed = (x_minus_1 * (inv5() & &m)) & &m;
    to_fixed_be(&seed, n)
}

fn oracle_decode(seed: &[u8]) -> Vec<u8> {
    let n = seed.len();
    if n == 0 {
        return Vec::new();
    }
    let x = (BigUint::from_bytes_be(seed) * 5u32 + 1u32) & mask(n);
    to_fixed_be(&x, n)
}

fn decode(seed: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    babel_affine::decode(seed, seed.len() as u32, &mut out).unwrap();
    out
}

#[test]
fn known_vectors() {
    assert_eq!(babel_affine::encode(b"Hi"), [0xdb, 0x48]);
    assert_eq!(decode(&[0xdb, 0x48]), b"Hi");
    assert_eq!(oracle_encode(b"Hi"), [0xdb, 0x48]);
    assert!(babel_affine::encode(b"").is_empty());
    assert!(decode(b"").is_empty());
    // x = 1 is the fixed point of the seed side: seed 0 <-> bytes 00..01.
    assert_eq!(babel_affine::encode(&[0, 0, 0, 1]), [0, 0, 0, 0]);
    assert_eq!(decode(&[0; 9]), [0, 0, 0, 0, 0, 0, 0, 0, 1]);
}

#[test]
fn newton_inverse_matches_closed_form() {
    // 256^n = 1 (mod 5), so (4 * 2^(8n) + 1) / 5 is an integer and 5 times it is 1 mod 2^(8n).
    for n in [1usize, 2, 3, 7, 8, 9, 16, 100, MAX_LEN] {
        let m = mask(n);
        let closed = ((&m + 1u32) * 4u32 + 1u32) / 5u32;
        assert_eq!(inv5() & &m, closed, "n = {n}");
    }
    assert_eq!(inv5() & mask(1), BigUint::from(205u32));
    assert_eq!(inv5() & mask(8), BigUint::from(0xCCCC_CCCC_CCCC_CCCDu64));
}

#[test]
fn exhaustive_one_byte() {
    let mut seeds = HashSet::new();
    for x in 0..=255u8 {
        let seed = babel_affine::encode(&[x]);
        assert_eq!(seed, oracle_encode(&[x]), "x = {x:#04x}");
        assert_eq!(decode(&seed), [x]);
        assert_eq!(decode(&[x]), oracle_decode(&[x]));
        seeds.insert(seed);
    }
    assert_eq!(seeds.len(), 256);
}

#[test]
fn exhaustive_two_bytes() {
    let mut seen = vec![false; 1 << 16];
    for v in 0..=u16::MAX {
        let x = v.to_be_bytes();
        let seed = babel_affine::encode(&x);
        assert_eq!(seed.len(), 2);
        assert_eq!(seed, oracle_encode(&x), "x = {v:#06x}");
        assert_eq!(decode(&seed), x, "x = {v:#06x}");
        assert_eq!(decode(&x), oracle_decode(&x), "seed = {v:#06x}");
        let s = u16::from_be_bytes([seed[0], seed[1]]) as usize;
        assert!(!seen[s], "seed {s:#06x} produced twice");
        seen[s] = true;
    }
    assert!(seen.iter().all(|&b| b), "65,536 distinct seeds");
}

/// Random bytes reshaped to stress carries and borrows: leading zeros or
/// 0xff (short big-endian integers), trailing zeros (borrow through `x - 1`),
/// trailing 0xff, and constant inputs.
fn shaped_input() -> impl Strategy<Value = Vec<u8>> {
    let bytes = prop_oneof![
        6 => prop::collection::vec(any::<u8>(), 0..=64),
        1 => prop::collection::vec(any::<u8>(), 0..=MAX_LEN),
    ];
    (bytes, 0u8..7, any::<prop::sample::Index>()).prop_map(|(mut v, shape, at)| {
        let k = if v.is_empty() {
            0
        } else {
            at.index(v.len() + 1)
        };
        let n = v.len();
        match shape {
            1 => v[..k].fill(0),
            2 => v[..k].fill(0xff),
            3 => v[n - k..].fill(0),
            4 => v[n - k..].fill(0xff),
            5 => v.fill(0),
            6 => v.fill(0xff),
            _ => {}
        }
        v
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("BABEL_ORACLE_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn matches_bigint_oracle(data in shaped_input()) {
        let seed = babel_affine::encode(&data);
        prop_assert_eq!(seed.len(), data.len());
        prop_assert_eq!(&seed, &oracle_encode(&data));
        prop_assert_eq!(decode(&seed), data.clone());
        // Every byte string is also a seed: the other direction is a bijection too.
        let x = decode(&data);
        prop_assert_eq!(&x, &oracle_decode(&data));
        prop_assert_eq!(babel_affine::encode(&x), data);
    }
}
