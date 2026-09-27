//! `REPEAT` version 1: a motif repeated up to a total length.
//!
//! - params: `total_len: u64 LE | motif`, where the motif has
//!   `1..=REPEAT_MAX_MOTIF_LEN` bytes and takes the rest of the params.
//! - output: `total_len` bytes, byte `k` being `motif[k % motif.len()]`. The
//!   last repetition may be partial, and `total_len == 0` is valid.
//! - Random access: the output starts at phase `offset % motif.len()`, so there
//!   is no work proportional to `offset`.

use super::{Generator, check_range, ids, take_u64_le};
use crate::error::{Error, Result};

/// Longest accepted motif.
///
/// With this limit REPEAT params can reach `8 + 65536` bytes, which is more
/// than `format::MAX_PARAMS_LEN` (65536) allows in a manifest. The caller that
/// persists params must enforce that limit on its own.
pub const REPEAT_MAX_MOTIF_LEN: usize = 64 * 1024;

const NAME: &str = "Repeat";

/// Built-in generator `ids::REPEAT`, version 1.
#[derive(Clone, Copy, Debug, Default)]
pub struct Repeat;

fn parse(params: &[u8]) -> Result<(u64, &[u8])> {
    let Some((total_len, motif)) = take_u64_le(params) else {
        return Err(Error::InvalidArgument(format!(
            "{NAME}: params must hold total_len (8 bytes) and a motif, got {} bytes",
            params.len()
        )));
    };
    if motif.is_empty() || motif.len() > REPEAT_MAX_MOTIF_LEN {
        return Err(Error::InvalidArgument(format!(
            "{NAME}: motif must have 1..={REPEAT_MAX_MOTIF_LEN} bytes, got {}",
            motif.len()
        )));
    }
    Ok((total_len, motif))
}

impl Generator for Repeat {
    fn id(&self) -> u16 {
        ids::REPEAT
    }

    fn version(&self) -> u16 {
        1
    }

    fn name(&self) -> &'static str {
        NAME
    }

    fn output_len(&self, params: &[u8]) -> Result<u64> {
        parse(params).map(|(total_len, _)| total_len)
    }

    fn generate(&self, params: &[u8], offset: u64, out: &mut [u8]) -> Result<()> {
        let (total_len, motif) = parse(params)?;
        check_range(NAME, total_len, offset, out.len())?;
        let m = motif.len();
        // `m` is in 1..=65536, so both conversions are lossless.
        let phase = (offset % m as u64) as usize;
        // First, one period (or less) starting at the phase: motif[phase..]
        // followed by motif[..phase].
        let first = m.min(out.len());
        let a = (m - phase).min(first);
        out[..a].copy_from_slice(&motif[phase..phase + a]);
        out[a..first].copy_from_slice(&motif[..first - a]);
        // Then keep doubling the filled prefix. `filled` stays a multiple of
        // `m` (only the last copy can be shorter), so every copy keeps the
        // right phase.
        let mut filled = first;
        while filled < out.len() {
            let n = filled.min(out.len() - filled);
            out.copy_within(..n, filled);
            filled += n;
        }
        Ok(())
    }
}
