//! `BLAKE3_XOF` version 1: the BLAKE3 keyed extendable output of the empty
//! message.
//!
//! - params: exactly 40 bytes, `key: [u8; 32] | len: u64 LE`.
//! - output: the first `len` bytes of
//!   `blake3::Hasher::new_keyed(&key).finalize_xof()`; `output_len = len`.
//!   BLAKE3 defines 2^64 - 1 output bytes, so every `len` is valid.
//! - Random access: the output reader seeks to `offset` directly, so there is
//!   no work proportional to `offset`.
//!
//! This generator stores high-entropy (pseudorandom) data honestly: the 40
//! bytes of params describe the whole value only because the reader has the
//! generator code. BLAKE3 output is fixed by its specification, and the SIMD
//! back-ends produce the same bytes as the portable one.

use super::{Generator, check_range, ids, take_u64_le};
use crate::error::{Error, Result};

/// Exact length of `BLAKE3_XOF` params.
pub const BLAKE3_XOF_PARAMS_LEN: usize = 40;

const NAME: &str = "Blake3Xof";

/// Built-in generator `ids::BLAKE3_XOF`, version 1.
#[derive(Clone, Copy, Debug, Default)]
pub struct Blake3Xof;

fn parse(params: &[u8]) -> Result<(&[u8; 32], u64)> {
    let parsed = (params.len() == BLAKE3_XOF_PARAMS_LEN)
        .then(|| {
            let (key, rest) = params.split_first_chunk::<32>()?;
            let (len, _) = take_u64_le(rest)?;
            Some((key, len))
        })
        .flatten();
    parsed.ok_or_else(|| {
        Error::InvalidArgument(format!(
            "{NAME}: params must be exactly {BLAKE3_XOF_PARAMS_LEN} bytes, got {}",
            params.len()
        ))
    })
}

impl Generator for Blake3Xof {
    fn id(&self) -> u16 {
        ids::BLAKE3_XOF
    }

    fn version(&self) -> u16 {
        1
    }

    fn name(&self) -> &'static str {
        NAME
    }

    fn output_len(&self, params: &[u8]) -> Result<u64> {
        parse(params).map(|(_, len)| len)
    }

    fn generate(&self, params: &[u8], offset: u64, out: &mut [u8]) -> Result<()> {
        let (key, len) = parse(params)?;
        check_range(NAME, len, offset, out.len())?;
        if out.is_empty() {
            return Ok(());
        }
        let mut reader = blake3::Hasher::new_keyed(key).finalize_xof();
        reader.set_position(offset);
        reader.fill(out);
        Ok(())
    }
}
