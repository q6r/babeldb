//! `ARITH_U64` version 1: an arithmetic progression of little-endian `u64`s.
//!
//! - params: exactly 24 bytes, `start: u64 LE | step: u64 LE | count: u64 LE`.
//! - output: the `count` values `start + i * step` for `i in 0..count`, each
//!   written as 8 little-endian bytes; `output_len = count * 8`.
//! - The progression never wraps. Params are rejected when `count * 8`
//!   overflows `u64` or when the last value `start + step * (count - 1)` does.
//!   `count == 0` is valid and gives an empty output.
//! - Random access: byte `k` of the output is byte `k % 8` of element `k / 8`,
//!   so a range may start and end in the middle of an element.

use super::{Generator, check_range, ids, take_u64_le};
use crate::error::{Error, Result};

/// Exact length of `ARITH_U64` params.
pub const ARITH_U64_PARAMS_LEN: usize = 24;

const NAME: &str = "ArithU64";

/// Built-in generator `ids::ARITH_U64`, version 1.
#[derive(Clone, Copy, Debug, Default)]
pub struct ArithU64;

#[derive(Clone, Copy)]
struct Params {
    start: u64,
    step: u64,
    count: u64,
    output_len: u64,
}

impl Params {
    fn parse(params: &[u8]) -> Result<Params> {
        let parsed = (params.len() == ARITH_U64_PARAMS_LEN)
            .then(|| {
                let (start, rest) = take_u64_le(params)?;
                let (step, rest) = take_u64_le(rest)?;
                let (count, _) = take_u64_le(rest)?;
                Some((start, step, count))
            })
            .flatten();
        let Some((start, step, count)) = parsed else {
            return Err(Error::InvalidArgument(format!(
                "{NAME}: params must be exactly {ARITH_U64_PARAMS_LEN} bytes, got {}",
                params.len()
            )));
        };
        let output_len = count.checked_mul(8).ok_or_else(|| {
            Error::InvalidArgument(format!(
                "{NAME}: output length of {count} values overflows u64"
            ))
        })?;
        if let Some(last_index) = count.checked_sub(1) {
            step.checked_mul(last_index)
                .and_then(|d| start.checked_add(d))
                .ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "{NAME}: start {start} + step {step} * {last_index} overflows u64"
                    ))
                })?;
        }
        Ok(Params {
            start,
            step,
            count,
            output_len,
        })
    }

    /// Value of element `index` (`index < count`), exact: no wrap-around.
    fn value_at(&self, index: u64) -> Result<u64> {
        if index >= self.count {
            return Err(Error::InvalidArgument(format!(
                "{NAME}: element {index} out of range"
            )));
        }
        self.step
            .checked_mul(index)
            .and_then(|d| self.start.checked_add(d))
            .ok_or_else(|| Error::InvalidArgument(format!("{NAME}: element {index} overflows u64")))
    }
}

impl Generator for ArithU64 {
    fn id(&self) -> u16 {
        ids::ARITH_U64
    }

    fn version(&self) -> u16 {
        1
    }

    fn name(&self) -> &'static str {
        NAME
    }

    fn output_len(&self, params: &[u8]) -> Result<u64> {
        Params::parse(params).map(|p| p.output_len)
    }

    fn generate(&self, params: &[u8], offset: u64, out: &mut [u8]) -> Result<()> {
        let p = Params::parse(params)?;
        check_range(NAME, p.output_len, offset, out.len())?;
        if out.is_empty() {
            return Ok(());
        }
        // Validated above: every element touched by the range has index < count,
        // and `value_at` re-checks the first one.
        let mut value = p.value_at(offset / 8)?;
        let skip = (offset % 8) as usize;
        let lead = if skip == 0 {
            0
        } else {
            (8 - skip).min(out.len())
        };
        let (head, body) = out.split_at_mut(lead);
        if !head.is_empty() {
            head.copy_from_slice(&value.to_le_bytes()[skip..skip + head.len()]);
            // The value after the last element may not fit in u64; it is never
            // written, so wrapping here cannot corrupt the output.
            value = value.wrapping_add(p.step);
        }
        let mut chunks = body.chunks_exact_mut(8);
        for chunk in &mut chunks {
            chunk.copy_from_slice(&value.to_le_bytes());
            value = value.wrapping_add(p.step);
        }
        let tail = chunks.into_remainder();
        if !tail.is_empty() {
            let n = tail.len();
            tail.copy_from_slice(&value.to_le_bytes()[..n]);
        }
        Ok(())
    }
}
