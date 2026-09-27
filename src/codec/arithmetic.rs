//! `ArithmeticU64V1`: body = start u64 LE ++ step u64 LE ++ count u64 LE; output
//! = count little-endian u64 values start, start+step, ... without overflow.
//! Requires raw_len == count * 8.
//! SKELETON — to be implemented by the codec agent.

use crate::error::Result;

pub fn recognize(data: &[u8]) -> Option<Vec<u8>> {
    let _ = data;
    None
}

pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    let _ = (body, raw_len, out);
    todo!("arithmetic::decode")
}
