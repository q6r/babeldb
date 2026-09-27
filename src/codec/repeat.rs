//! `RepeatV1`: body = period (u32 LE) ++ motif[period]; output = motif repeated
//! up to raw_len (last repetition may be partial).
//! SKELETON — to be implemented by the codec agent.

use crate::error::Result;

/// Body if `data` is an exact repetition of a motif of at most `max_period` bytes.
pub fn recognize(data: &[u8], max_period: usize) -> Option<Vec<u8>> {
    let _ = (data, max_period);
    None
}

pub fn decode(body: &[u8], raw_len: u32, out: &mut Vec<u8>) -> Result<()> {
    let _ = (body, raw_len, out);
    todo!("repeat::decode")
}
