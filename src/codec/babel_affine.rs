//! `BabelAffineV1`: reversible affine map over n-byte integers.
//!   M = 2^(8n), x = big-endian integer of the bytes
//!   encode: seed = ((x - 1) * inv(5)) mod M
//!   decode: x    = (5 * seed + 1) mod M
//! The seed is stored in exactly n bytes, big-endian; n = 0 maps to the empty seed.
//! Didactic construction (not the Library of Babel site algorithm, not crypto).
//!
//! Both directions are one O(n) pass from the least significant end (the last
//! byte) toward the first: 64-bit limbs, then the leftover leading bytes one at
//! a time. No bigints, no division.
//!
//! - decode: carry chain of `5 * s + carry` with the carry starting at 1 (the
//!   `+ 1`); the final carry is dropped (mod M). The carry never exceeds 4.
//! - encode: exact 2-adic (Hensel) division of `x - 1` by 5. With a pending
//!   borrow `b` that starts at 1 (the `- 1`), each digit of base B is
//!   `s = ((x_i - b) mod B) * inv5 mod B` and the next borrow is
//!   `b = (b + 5 s - x_i) / B`, an exact non-negative division with `b <= 5`.
//!   For B = 2^64, inv5 = 0xCCCC_CCCC_CCCC_CCCD; for B = 256, inv5 = 205.
//!   The borrow is the same quantity whatever the digit size, so limbs and
//!   bytes can be mixed.
//!
//! `tests/babel_oracle.rs` checks this against a `num-bigint` oracle.

use crate::error::{Error, Result};

/// 5 * 205 = 1025 = 4 * 256 + 1.
const INV5_U8: u8 = 205;
/// 5 * INV5_U64 = 4 * 2^64 + 1.
const INV5_U64: u64 = 0xCCCC_CCCC_CCCC_CCCD;

/// Seed of `data` (same length).
pub fn encode(data: &[u8]) -> Vec<u8> {
    let mut seed = vec![0u8; data.len()];
    encode_to_slice(data, &mut seed);
    seed
}

/// Write the seed of `data` into `seed`, which must have the same length.
pub fn encode_to_slice(data: &[u8], seed: &mut [u8]) {
    assert_eq!(
        data.len(),
        seed.len(),
        "seed buffer length must equal data length"
    );
    let mut borrow: u64 = 1;
    let mut src = data.rchunks_exact(8);
    let mut dst = seed.rchunks_exact_mut(8);
    for (x, s) in (&mut src).zip(&mut dst) {
        let x = u64::from_be_bytes(x.try_into().unwrap());
        let d = x.wrapping_sub(borrow).wrapping_mul(INV5_U64);
        s.copy_from_slice(&d.to_be_bytes());
        // b + 5d - x is a non-negative multiple of 2^64 (see module docs).
        borrow = ((borrow as u128 + 5 * d as u128 - x as u128) >> 64) as u64;
    }
    let head = src.remainder();
    for (x, s) in head.iter().rev().zip(dst.into_remainder().iter_mut().rev()) {
        let d = x.wrapping_sub(borrow as u8).wrapping_mul(INV5_U8);
        *s = d;
        borrow = ((borrow as u32 + 5 * d as u32 - *x as u32) >> 8) as u64;
    }
}

/// Append the bytes addressed by `seed` to `out`.
pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    if body.len() as u64 != raw_len as u64 {
        return Err(Error::format(format!(
            "BabelAffineV1 seed of {} bytes for raw_len {raw_len}",
            body.len()
        )));
    }
    let start = out.len();
    out.resize(start + body.len(), 0);
    decode_to_slice(body, &mut out[start..]);
    Ok(())
}

/// Write the bytes addressed by `seed` into `out`, which must have the same length.
pub fn decode_to_slice(seed: &[u8], out: &mut [u8]) {
    assert_eq!(
        seed.len(),
        out.len(),
        "output length must equal seed length"
    );
    let mut carry: u64 = 1;
    let mut src = seed.rchunks_exact(8);
    let mut dst = out.rchunks_exact_mut(8);
    for (s, x) in (&mut src).zip(&mut dst) {
        let s = u64::from_be_bytes(s.try_into().unwrap());
        let v = 5 * s as u128 + carry as u128;
        x.copy_from_slice(&(v as u64).to_be_bytes());
        carry = (v >> 64) as u64;
    }
    let head = src.remainder();
    for (s, x) in head.iter().rev().zip(dst.into_remainder().iter_mut().rev()) {
        let v = 5 * *s as u32 + carry as u32;
        *x = v as u8;
        carry = (v >> 8) as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-at-a-time reference of the same recurrences (no limbs).
    fn encode_bytewise(data: &[u8]) -> Vec<u8> {
        let mut seed = vec![0u8; data.len()];
        let mut b: u32 = 1;
        for i in (0..data.len()).rev() {
            let d = data[i].wrapping_sub(b as u8).wrapping_mul(INV5_U8);
            seed[i] = d;
            b = (b + 5 * d as u32 - data[i] as u32) >> 8;
        }
        seed
    }

    #[test]
    fn known_vector() {
        assert_eq!(encode(b"Hi"), vec![0xdb, 0x48]);
        let mut out = Vec::new();
        decode(&[0xdb, 0x48], 2, &mut out).unwrap();
        assert_eq!(out, b"Hi");
    }

    #[test]
    fn limbs_match_bytewise() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for n in 0..80 {
            let data: Vec<u8> = (0..n)
                .map(|_| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    (state >> 56) as u8
                })
                .collect();
            let seed = encode(&data);
            assert_eq!(seed, encode_bytewise(&data), "n = {n}");
            let mut out = vec![0xAA];
            decode(&seed, n as u32, &mut out).unwrap();
            assert_eq!(&out[1..], &data[..], "n = {n}");
        }
    }

    #[test]
    fn rejects_length_mismatch() {
        let mut out = Vec::new();
        assert!(decode(&[1, 2, 3], 2, &mut out).is_err());
        assert!(decode(&[], 1, &mut out).is_err());
        assert!(out.is_empty());
    }
}
