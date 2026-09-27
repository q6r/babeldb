//! `RepeatV1`: body = period (u32 LE) ++ motif[period]; output = motif repeated
//! up to raw_len (last repetition may be partial).
//!
//! Recognition looks for the minimal period p of the input (`data[i] ==
//! data[i - p]` for every `i >= p`) with the KMP prefix function. It only needs
//! a bounded prefix: once the prefix length k reaches `max_period + q` (q = the
//! minimal period of that prefix), any period `p <= max_period` of the whole
//! input is a multiple of q (Fine–Wilf), so q is the only candidate left and a
//! single comparison over the whole input settles it. Work and memory are
//! O(min(n, 2 * max_period)) plus one O(n) comparison.

use crate::error::{Error, Result};
use crate::format::MAX_UNIT_LEN;

/// Bytes of the `period` field.
pub const HEADER_LEN: usize = 4;

/// Body if `data` is an exact repetition of a motif of at most `max_period` bytes.
/// Returns `None` unless the body is strictly smaller than `data`.
pub fn recognize(data: &[u8], max_period: usize) -> Option<Vec<u8>> {
    // The body is HEADER_LEN + p bytes, so only p < n - HEADER_LEN can pay off.
    let limit = max_period.min(data.len().checked_sub(HEADER_LEN + 1)?);
    let p = minimal_period(data, limit)?;
    let period = u32::try_from(p).ok()?;
    let mut body = Vec::with_capacity(HEADER_LEN + p);
    body.extend_from_slice(&period.to_le_bytes());
    body.extend_from_slice(&data[..p]);
    Some(body)
}

/// Smallest `p` in `1..=max_period` such that `data[i] == data[i - p]` for every
/// `i >= p`, if any.
pub fn minimal_period(data: &[u8], max_period: usize) -> Option<usize> {
    let n = data.len();
    let max_period = max_period.min(n);
    if max_period == 0 {
        return None;
    }
    let limit = n.min(max_period.saturating_mul(2));
    // pi[k] = length of the longest proper border of data[..=k].
    let mut pi: Vec<u32> = Vec::with_capacity(limit);
    pi.push(0);
    let mut q = 1;
    let mut k = 1;
    while k < limit {
        let c = data[k];
        let mut j = pi[k - 1] as usize;
        while j > 0 && c != data[j] {
            j = pi[j - 1] as usize;
        }
        if c == data[j] {
            j += 1;
        }
        pi.push(j as u32);
        k += 1;
        // Minimal period of data[..k]; it never decreases as k grows.
        q = k - j;
        if q > max_period {
            return None;
        }
        if k >= max_period + q {
            break;
        }
    }
    (data[q..] == data[..n - q]).then_some(q)
}

pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    let Some((header, motif)) = body.split_first_chunk::<HEADER_LEN>() else {
        return Err(Error::format(
            "RepeatV1 body shorter than its 4-byte header",
        ));
    };
    let period = u32::from_le_bytes(*header) as usize;
    if period == 0 {
        return Err(Error::format("RepeatV1 period 0"));
    }
    if motif.len() != period {
        return Err(Error::format(format!(
            "RepeatV1 motif of {} bytes for period {period}",
            motif.len()
        )));
    }
    if raw_len == 0 {
        return Err(Error::format("RepeatV1 with raw_len 0"));
    }
    if raw_len > MAX_UNIT_LEN {
        return Err(Error::format("RepeatV1 raw_len exceeds MAX_UNIT_LEN"));
    }
    let n = raw_len as usize;
    if period > n {
        return Err(Error::format(format!(
            "RepeatV1 period {period} > raw_len {raw_len}"
        )));
    }
    let start = out.len();
    out.reserve(n);
    out.extend_from_slice(motif);
    // Double the produced prefix (always a whole number of periods) until done.
    loop {
        let have = out.len() - start;
        if have >= n {
            break;
        }
        out.extend_from_within(start..start + have.min(n - have));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naive_period(data: &[u8], max_period: usize) -> Option<usize> {
        (1..=max_period.min(data.len())).find(|&p| data[p..] == data[..data.len() - p])
    }

    #[test]
    fn period_matches_naive() {
        let mut state = 7u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as usize
        };
        for _ in 0..3000 {
            let alphabet = 1 + next() % 3;
            let motif_len = 1 + next() % 12;
            let motif: Vec<u8> = (0..motif_len).map(|_| (next() % alphabet) as u8).collect();
            let n = next() % 80;
            let mut data: Vec<u8> = motif.iter().cycle().take(n).copied().collect();
            if next() % 3 == 0 && n > 0 {
                let i = next() % n;
                data[i] ^= 1;
            }
            for max_period in [1, 2, 3, 5, 8, 13, 40, 100] {
                assert_eq!(
                    minimal_period(&data, max_period),
                    naive_period(&data, max_period),
                    "{data:?} max_period {max_period}"
                );
            }
        }
    }

    #[test]
    fn recognize_and_decode() {
        let data: Vec<u8> = b"abc".iter().cycle().take(100).copied().collect();
        let body = recognize(&data, 4096).unwrap();
        assert_eq!(body, [3, 0, 0, 0, b'a', b'b', b'c']);
        let mut out = Vec::new();
        decode(&body, 100, &mut out).unwrap();
        assert_eq!(out, data);
        assert!(recognize(b"abcdefgh", 4096).is_none());
        assert!(recognize(b"aaaaa", 4096).is_none());
        assert_eq!(recognize(b"aaaaaa", 4096).unwrap(), [1, 0, 0, 0, b'a']);
        assert!(recognize(&data, 2).is_none());
    }
}
