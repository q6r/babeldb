//! Codec contract. The envelope (header, digest) lives in `format`; codecs
//! only produce and consume bodies. Every decoder must produce exactly
//! `raw_len` bytes or fail, validating lengths before allocating.

pub mod arithmetic;
pub mod babel_affine;
pub mod lz4;
pub mod raw;
pub mod repeat;
pub mod template_patch;
pub mod zstd;

use crate::error::{Error, Result};
use crate::format::{codec_id, param_kind, CodecTag, MAX_UNIT_LEN};

pub use template_patch::Template;
pub use zstd::ZstdDict;

/// Shared decoding dependencies, prepared once per param id by the engine.
#[derive(Default, Clone, Copy)]
pub struct Deps<'a> {
    pub zstd_dict: Option<&'a ZstdDict>,
    pub template: Option<&'a Template>,
}

/// A body produced by a codec for one unit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Encoded {
    pub codec: CodecTag,
    /// Param id the body depends on, or 0.
    pub aux_id: u64,
    pub body: Vec<u8>,
}

/// Param kind required by a codec (the envelope's `aux_id` must then be non-zero).
pub fn required_param(codec: CodecTag, aux_id: u64) -> Option<u8> {
    match codec.id {
        codec_id::ZSTD if aux_id != 0 => Some(param_kind::ZSTD_DICT),
        codec_id::TEMPLATE_PATCH => Some(param_kind::TEMPLATE),
        _ => None,
    }
}

/// Decode `body` into `out` (cleared first).
pub fn decode(codec: CodecTag, aux_id: u64, body: &[u8], raw_len: u32, deps: Deps<'_>, out: &mut Vec<u8>) -> Result<()> {
    if raw_len > MAX_UNIT_LEN {
        return Err(Error::format("raw_len exceeds MAX_UNIT_LEN"));
    }
    out.clear();
    match (codec.id, codec.version) {
        (codec_id::RAW, 1) => raw::decode(body, raw_len, out)?,
        (codec_id::REPEAT, 1) => repeat::decode(body, raw_len, out)?,
        (codec_id::ARITH_U64, 1) => arithmetic::decode(body, raw_len, out)?,
        (codec_id::LZ4, 1) => lz4::decode(body, raw_len, out)?,
        (codec_id::ZSTD, 1) => {
            let dict = if aux_id == 0 {
                None
            } else {
                Some(deps.zstd_dict.ok_or(Error::MissingDependency { param_id: aux_id })?)
            };
            zstd::decode(body, raw_len, dict, out)?
        }
        (codec_id::BABEL_AFFINE, 1) => babel_affine::decode(body, raw_len, out)?,
        (codec_id::TEMPLATE_PATCH, 1) => {
            let t = deps.template.ok_or(Error::MissingDependency { param_id: aux_id })?;
            template_patch::decode(body, raw_len, t, out)?
        }
        _ => return Err(Error::UnknownCodec { id: codec.id, version: codec.version }),
    }
    if out.len() != raw_len as usize {
        return Err(Error::format(format!(
            "{} produced {} bytes, expected {raw_len}",
            codec.name(),
            out.len()
        )));
    }
    Ok(())
}
