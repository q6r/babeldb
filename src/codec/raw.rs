//! `RawV1`: the original bytes. Reference for correctness and final fallback.

use crate::error::{Error, Result};

pub fn encode(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}

pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    if body.len() != raw_len as usize {
        return Err(Error::format("RawV1 body length != raw_len"));
    }
    out.extend_from_slice(body);
    Ok(())
}
