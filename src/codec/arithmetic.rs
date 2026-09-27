//! `ArithmeticU64V1`: body = start u64 LE ++ step u64 LE ++ count u64 LE; output
//! = count little-endian u64 values start, start+step, ... without overflow.
//! Requires raw_len == count * 8. The step is unsigned: only non-decreasing
//! sequences whose last value fits in u64 are representable.

use crate::error::{Error, Result};
use crate::format::MAX_UNIT_LEN;

pub const BODY_LEN: usize = 24;
/// Fewest values worth recognizing (the body is 24 bytes, 4 values are 32).
pub const MIN_VALUES: usize = 4;

pub fn recognize(data: &[u8]) -> Option<Vec<u8>> {
    if !data.len().is_multiple_of(8) || data.len() < MIN_VALUES * 8 {
        return None;
    }
    let mut values = data
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()));
    let start = values.next()?;
    let second = values.next()?;
    let step = second.checked_sub(start)?;
    let mut prev = second;
    for v in values {
        if prev.checked_add(step)? != v {
            return None;
        }
        prev = v;
    }
    let count = (data.len() / 8) as u64;
    let mut body = Vec::with_capacity(BODY_LEN);
    body.extend_from_slice(&start.to_le_bytes());
    body.extend_from_slice(&step.to_le_bytes());
    body.extend_from_slice(&count.to_le_bytes());
    Some(body)
}

pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    let Ok(fields) = <&[u8; BODY_LEN]>::try_from(body) else {
        return Err(Error::format(format!(
            "ArithmeticU64V1 body of {} bytes (expected 24)",
            body.len()
        )));
    };
    let field = |i: usize| u64::from_le_bytes(fields[i * 8..i * 8 + 8].try_into().unwrap());
    let (start, step, count) = (field(0), field(1), field(2));
    if count.checked_mul(8) != Some(raw_len as u64) {
        return Err(Error::format(format!(
            "ArithmeticU64V1 count {count} does not match raw_len {raw_len}"
        )));
    }
    if raw_len > MAX_UNIT_LEN {
        return Err(Error::format(
            "ArithmeticU64V1 raw_len exceeds MAX_UNIT_LEN",
        ));
    }
    if count > 0
        && step
            .checked_mul(count - 1)
            .and_then(|d| start.checked_add(d))
            .is_none()
    {
        return Err(Error::format("ArithmeticU64V1 sequence overflows u64"));
    }
    let base = out.len();
    out.resize(base + raw_len as usize, 0);
    let mut v = start;
    for chunk in out[base..].chunks_exact_mut(8) {
        chunk.copy_from_slice(&v.to_le_bytes());
        // Cannot overflow before the last value (checked above); the value
        // computed after the last one is discarded.
        v = v.wrapping_add(step);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(start: u64, step: u64, count: usize) -> Vec<u8> {
        (0..count as u64)
            .flat_map(|i| (start + i * step).to_le_bytes())
            .collect()
    }

    #[test]
    fn roundtrip_and_rejections() {
        let data = seq(1000, 7, 50);
        let body = recognize(&data).unwrap();
        let mut out = Vec::new();
        decode(&body, data.len() as u32, &mut out).unwrap();
        assert_eq!(out, data);
        assert!(recognize(&seq(5, 0, 4)).is_some());
        assert!(recognize(&seq(5, 1, 3)).is_none());
        let mut down = seq(0, 1, 5);
        down.reverse();
        assert!(recognize(&down).is_none());
        let wrap: Vec<u8> = [u64::MAX - 1, u64::MAX, 0, 1]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        assert!(recognize(&wrap).is_none());
        let mut overflow = Vec::new();
        overflow.extend_from_slice(&(u64::MAX - 5).to_le_bytes());
        overflow.extend_from_slice(&3u64.to_le_bytes());
        overflow.extend_from_slice(&3u64.to_le_bytes());
        assert!(decode(&overflow, 24, &mut Vec::new()).is_err());
    }
}
