//! `BabelAffineV1`: reversible affine map over n-byte integers.
//!   M = 2^(8n), x = big-endian integer of the bytes
//!   encode: seed = ((x - 1) * inv(5)) mod M
//!   decode: x    = (5 * seed + 1) mod M
//! The seed is stored in exactly n bytes, big-endian; n = 0 maps to the empty seed.
//! Didactic construction (not the Library of Babel site algorithm, not crypto).
//! SKELETON — to be implemented by the codec agent with O(n) byte arithmetic.

use crate::error::Result;

/// Seed of `data` (same length).
pub fn encode(data: &[u8]) -> Vec<u8> {
    let _ = data;
    todo!("babel_affine::encode")
}

/// Append the bytes addressed by `seed` to `out`.
pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    let _ = (body, raw_len, out);
    todo!("babel_affine::decode")
}
