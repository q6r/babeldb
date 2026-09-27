//! `ZstdV1`: one independent Zstandard frame, optionally with an identified,
//! immutable dictionary (envelope `aux_id` = param id).
//! SKELETON — to be implemented by the codec agent.

use crate::error::Result;

/// Prepared dictionary, built once per param id and shared (Send + Sync).
pub struct ZstdDict {
    pub id: u64,
    bytes: Vec<u8>,
}

impl ZstdDict {
    pub fn new(id: u64, bytes: Vec<u8>, level: i32) -> Result<ZstdDict> {
        let _ = level;
        Ok(ZstdDict { id, bytes })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub fn encode(data: &[u8], level: i32, dict: Option<&ZstdDict>) -> Result<Vec<u8>> {
    let _ = (data, level, dict);
    todo!("zstd::encode")
}

pub fn decode(body: &[u8], raw_len: u32, dict: Option<&ZstdDict>, out: &mut Vec<u8>) -> Result<()> {
    let _ = (body, raw_len, dict, out);
    todo!("zstd::decode")
}

/// Train a dictionary of at most `max_size` bytes from samples.
pub fn train_dictionary(samples: &[Vec<u8>], max_size: usize) -> Result<Vec<u8>> {
    let _ = (samples, max_size);
    todo!("zstd::train_dictionary")
}
