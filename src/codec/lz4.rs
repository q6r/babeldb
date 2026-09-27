//! `Lz4V1`: one independent LZ4 block (no frame); raw_len comes from the envelope.
//! SKELETON — to be implemented by the codec agent.

use crate::error::Result;

pub fn encode(data: &[u8]) -> Vec<u8> {
    let _ = data;
    todo!("lz4::encode")
}

pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    let _ = (body, raw_len, out);
    todo!("lz4::decode")
}
