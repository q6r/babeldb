//! `Lz4V1`: one independent LZ4 block (block format, no frame, no size
//! prefix); raw_len comes from the envelope. Encoded with `lz4_flex` (safe
//! encoder and decoder).

use lz4_flex::block;

use crate::error::{Error, Result};
use crate::format::MAX_UNIT_LEN;

/// An LZ4 block never expands its input by more than this factor: a match of
/// length 19 + 255 k costs at least 3 + k body bytes, a literal costs 1.
const MAX_EXPANSION: usize = 255;

pub fn encode(data: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    encode_into(data, &mut body);
    body
}

/// `encode` into a reusable buffer: `out` holds exactly the body afterwards.
/// Only the part of the buffer beyond its current length is zeroed, so a
/// buffer reused for inputs of similar size costs no allocation.
pub fn encode_into(data: &[u8], out: &mut Vec<u8>) {
    let max = block::get_maximum_output_size(data.len());
    if out.len() < max {
        out.resize(max, 0);
    }
    match block::compress_into(data, &mut out[..max]) {
        Ok(len) => out.truncate(len),
        // Unreachable with a buffer of the documented maximum size.
        Err(_) => *out = block::compress(data),
    }
}

/// Append exactly `raw_len` decoded bytes to `out`, or fail.
pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    if raw_len > MAX_UNIT_LEN {
        return Err(Error::format("Lz4V1 raw_len exceeds MAX_UNIT_LEN"));
    }
    let n = raw_len as usize;
    if n > body.len().saturating_mul(MAX_EXPANSION) {
        return Err(Error::format(format!(
            "Lz4V1 body of {} bytes cannot produce {n} bytes",
            body.len()
        )));
    }
    let start = out.len();
    out.resize(start + n, 0);
    // The output slice has exactly raw_len bytes: the decoder fails instead of
    // writing past it, and a shorter result is rejected below.
    let res = block::decompress_into(body, &mut out[start..]);
    match res {
        Ok(written) if written == n => Ok(()),
        Ok(written) => {
            out.truncate(start);
            Err(Error::format(format!(
                "Lz4V1 produced {written} bytes, expected {n}"
            )))
        }
        Err(e) => {
            out.truncate(start);
            Err(Error::format(format!("Lz4V1: {e}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_exact_size() {
        for data in [&b""[..], b"a", b"hello hello hello hello hello hello hello"] {
            let body = encode(data);
            let mut out = Vec::new();
            decode(&body, data.len() as u32, &mut out).unwrap();
            assert_eq!(out, data);
        }
        let data = vec![7u8; 1000];
        let body = encode(&data);
        assert!(body.len() < 20);
        assert!(decode(&body, 999, &mut Vec::new()).is_err());
        assert!(decode(&body, 1001, &mut Vec::new()).is_err());
        let mut out = vec![1, 2, 3];
        assert!(decode(&body, 5000, &mut out).is_err());
        assert_eq!(out, [1, 2, 3]);
    }

    #[test]
    fn reused_buffer_matches_fresh_encoding() {
        let mut buf = vec![0xAA; 7];
        for len in [0usize, 1, 50, 3000, 10, 600, 0, 4096] {
            let data: Vec<u8> = (0..len).map(|i| (i * 7 % 13) as u8).collect();
            encode_into(&data, &mut buf);
            assert_eq!(buf, encode(&data), "len {len}");
        }
    }
}
