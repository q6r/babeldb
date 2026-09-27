//! Persistent binary formats, format version 1 (see `docs/format.md`).
//!
//! Every integer of the envelope, manifest and tables is little-endian unless
//! stated otherwise. Integer *keys* are big-endian so byte order == numeric order.
//! Fields are serialized explicitly; no struct memory is ever written as-is.

use crate::error::{Error, Result};

pub const FORMAT_VERSION: u32 = 1;
pub const OBJECT_MAGIC: [u8; 4] = *b"BBO1";
pub const ENVELOPE_HEADER_LEN: usize = 64;
/// Upper bound of one decoded unit (block or inline value); checked before allocating.
pub const MAX_UNIT_LEN: u32 = 1 << 24;
/// Upper bound of generator parameters stored in a manifest.
pub const MAX_PARAMS_LEN: usize = 64 * 1024;
pub const MANIFEST_VERSION: u8 = 1;

/// Permanent codec identifiers. Never renumber; never derive from enum order.
pub mod codec_id {
    pub const RAW: u8 = 0x01;
    pub const REPEAT: u8 = 0x02;
    pub const ARITH_U64: u8 = 0x03;
    pub const LZ4: u8 = 0x10;
    pub const ZSTD: u8 = 0x11;
    pub const BABEL_AFFINE: u8 = 0x20;
    pub const TEMPLATE_PATCH: u8 = 0x30;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CodecTag {
    pub id: u8,
    pub version: u8,
}

impl CodecTag {
    pub const RAW_V1: CodecTag = CodecTag { id: codec_id::RAW, version: 1 };
    pub const REPEAT_V1: CodecTag = CodecTag { id: codec_id::REPEAT, version: 1 };
    pub const ARITH_U64_V1: CodecTag = CodecTag { id: codec_id::ARITH_U64, version: 1 };
    pub const LZ4_V1: CodecTag = CodecTag { id: codec_id::LZ4, version: 1 };
    pub const ZSTD_V1: CodecTag = CodecTag { id: codec_id::ZSTD, version: 1 };
    pub const BABEL_AFFINE_V1: CodecTag = CodecTag { id: codec_id::BABEL_AFFINE, version: 1 };
    pub const TEMPLATE_PATCH_V1: CodecTag = CodecTag { id: codec_id::TEMPLATE_PATCH, version: 1 };

    pub const ALL_V1: [CodecTag; 7] = [
        CodecTag::RAW_V1,
        CodecTag::REPEAT_V1,
        CodecTag::ARITH_U64_V1,
        CodecTag::LZ4_V1,
        CodecTag::ZSTD_V1,
        CodecTag::BABEL_AFFINE_V1,
        CodecTag::TEMPLATE_PATCH_V1,
    ];

    pub fn name(&self) -> &'static str {
        match (self.id, self.version) {
            (codec_id::RAW, 1) => "RawV1",
            (codec_id::REPEAT, 1) => "RepeatV1",
            (codec_id::ARITH_U64, 1) => "ArithmeticU64V1",
            (codec_id::LZ4, 1) => "Lz4V1",
            (codec_id::ZSTD, 1) => "ZstdV1",
            (codec_id::BABEL_AFFINE, 1) => "BabelAffineV1",
            (codec_id::TEMPLATE_PATCH, 1) => "TemplatePatchV1",
            _ => "Unknown",
        }
    }
}

// ---------------------------------------------------------------------------
// Object envelope (64-byte header + body)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvelopeHeader {
    pub codec: CodecTag,
    pub flags: u16,
    pub raw_len: u32,
    pub body_len: u32,
    /// Param id (dictionary/template) or 0.
    pub aux_id: u64,
    /// BLAKE3 of the original (decoded) bytes.
    pub digest: [u8; 32],
}

/// offset 0 magic[4] | 4 codec_id u8 | 5 codec_version u8 | 6 flags u16 | 8 raw_len u32
/// | 12 body_len u32 | 16 aux_id u64 | 24 digest[32] | 56 reserved u64 = 0 | 64 body
pub fn write_envelope(codec: CodecTag, aux_id: u64, raw_len: u32, digest: &[u8; 32], body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ENVELOPE_HEADER_LEN + body.len());
    out.extend_from_slice(&OBJECT_MAGIC);
    out.push(codec.id);
    out.push(codec.version);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&raw_len.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&aux_id.to_le_bytes());
    out.extend_from_slice(digest);
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Parse and validate an envelope. Codec support is checked later, at decode time.
pub fn read_envelope(bytes: &[u8]) -> Result<(EnvelopeHeader, &[u8])> {
    if bytes.len() < ENVELOPE_HEADER_LEN {
        return Err(Error::format(format!("envelope too short: {} bytes", bytes.len())));
    }
    if bytes[0..4] != OBJECT_MAGIC {
        return Err(Error::format("bad envelope magic"));
    }
    let codec = CodecTag { id: bytes[4], version: bytes[5] };
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    if flags != 0 {
        return Err(Error::format(format!("unknown envelope flags 0x{flags:04x}")));
    }
    let raw_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let body_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let aux_id = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&bytes[24..56]);
    if bytes[56..64] != [0u8; 8] {
        return Err(Error::format("non-zero reserved envelope field"));
    }
    if raw_len > MAX_UNIT_LEN {
        return Err(Error::format(format!("raw_len {raw_len} exceeds MAX_UNIT_LEN")));
    }
    let body = &bytes[ENVELOPE_HEADER_LEN..];
    if body.len() != body_len as usize {
        return Err(Error::format(format!(
            "body_len {body_len} does not match stored body of {} bytes",
            body.len()
        )));
    }
    Ok((EnvelopeHeader { codec, flags, raw_len, body_len, aux_id, digest }, body))
}

// ---------------------------------------------------------------------------
// Varint (unsigned LEB128)
// ---------------------------------------------------------------------------

pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn get_varint(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut v: u64 = 0;
    for i in 0..10 {
        let b = *bytes.get(*pos).ok_or_else(|| Error::format("truncated varint"))?;
        *pos += 1;
        let payload = (b & 0x7f) as u64;
        if i == 9 && payload > 1 {
            return Err(Error::format("varint overflow"));
        }
        v |= payload << (7 * i);
        if b & 0x80 == 0 {
            if i > 0 && b == 0 {
                return Err(Error::format("non-canonical varint"));
            }
            return Ok(v);
        }
    }
    Err(Error::format("varint too long"))
}

// ---------------------------------------------------------------------------
// Little helpers for fixed-width fields
// ---------------------------------------------------------------------------

pub fn take<'a>(bytes: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = pos.checked_add(n).ok_or_else(|| Error::format("length overflow"))?;
    let s = bytes.get(*pos..end).ok_or_else(|| Error::format("truncated field"))?;
    *pos = end;
    Ok(s)
}

pub fn take_u8(bytes: &[u8], pos: &mut usize) -> Result<u8> {
    Ok(take(bytes, pos, 1)?[0])
}

pub fn take_u16(bytes: &[u8], pos: &mut usize) -> Result<u16> {
    Ok(u16::from_le_bytes(take(bytes, pos, 2)?.try_into().unwrap()))
}

pub fn take_u32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(take(bytes, pos, 4)?.try_into().unwrap()))
}

pub fn take_u64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(take(bytes, pos, 8)?.try_into().unwrap()))
}

pub fn take_digest(bytes: &[u8], pos: &mut usize) -> Result<[u8; 32]> {
    let mut d = [0u8; 32];
    d.copy_from_slice(take(bytes, pos, 32)?);
    Ok(d)
}

pub fn encode_u64(v: u64) -> [u8; 8] {
    v.to_le_bytes()
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = bytes.try_into().map_err(|_| Error::format("expected 8-byte u64"))?;
    Ok(u64::from_le_bytes(arr))
}

pub fn decode_u32(bytes: &[u8]) -> Result<u32> {
    let arr: [u8; 4] = bytes.try_into().map_err(|_| Error::format("expected 4-byte u32"))?;
    Ok(u32::from_le_bytes(arr))
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkRef {
    /// Exclusive logical end of this chunk; the start is the previous end (or 0).
    pub logical_end: u64,
    pub object_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestBody {
    /// Complete envelope stored inside the manifest (small values, no dedupe).
    Inline(Vec<u8>),
    /// Ordered references to immutable objects.
    Chunks(Vec<ChunkRef>),
    /// Output of a registered, versioned deterministic generator.
    Generated { generator_id: u16, generator_version: u16, params: Vec<u8>, digest: [u8; 32] },
    /// Deletion marker (only written when history is kept).
    Tombstone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub revision: u64,
    pub logical_len: u64,
    pub source_id: Option<u64>,
    pub body: ManifestBody,
}

const KIND_INLINE: u8 = 0;
const KIND_CHUNKS: u8 = 1;
const KIND_GENERATED: u8 = 2;
const KIND_TOMBSTONE: u8 = 3;
const MFLAG_HAS_SOURCE: u8 = 1;

impl Manifest {
    /// u8 version=1 | u64 revision | u64 logical_len | u8 kind | u8 flags (bit0 has_source)
    /// | [u64 source_id] | body:
    ///   Inline:    varint len, envelope
    ///   Chunks:    varint count, count x (u64 logical_end, u64 object_id)
    ///   Generated: u16 id, u16 version, varint params_len, params, digest[32]
    ///   Tombstone: (empty)
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        out.push(MANIFEST_VERSION);
        out.extend_from_slice(&self.revision.to_le_bytes());
        out.extend_from_slice(&self.logical_len.to_le_bytes());
        let kind = match &self.body {
            ManifestBody::Inline(_) => KIND_INLINE,
            ManifestBody::Chunks(_) => KIND_CHUNKS,
            ManifestBody::Generated { .. } => KIND_GENERATED,
            ManifestBody::Tombstone => KIND_TOMBSTONE,
        };
        out.push(kind);
        out.push(if self.source_id.is_some() { MFLAG_HAS_SOURCE } else { 0 });
        if let Some(s) = self.source_id {
            out.extend_from_slice(&s.to_le_bytes());
        }
        match &self.body {
            ManifestBody::Inline(env) => {
                put_varint(&mut out, env.len() as u64);
                out.extend_from_slice(env);
            }
            ManifestBody::Chunks(refs) => {
                put_varint(&mut out, refs.len() as u64);
                for r in refs {
                    out.extend_from_slice(&r.logical_end.to_le_bytes());
                    out.extend_from_slice(&r.object_id.to_le_bytes());
                }
            }
            ManifestBody::Generated { generator_id, generator_version, params, digest } => {
                out.extend_from_slice(&generator_id.to_le_bytes());
                out.extend_from_slice(&generator_version.to_le_bytes());
                put_varint(&mut out, params.len() as u64);
                out.extend_from_slice(params);
                out.extend_from_slice(digest);
            }
            ManifestBody::Tombstone => {}
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Manifest> {
        let mut pos = 0usize;
        let version = take_u8(bytes, &mut pos)?;
        if version != MANIFEST_VERSION {
            return Err(Error::format(format!("unknown manifest version {version}")));
        }
        let revision = take_u64(bytes, &mut pos)?;
        let logical_len = take_u64(bytes, &mut pos)?;
        let kind = take_u8(bytes, &mut pos)?;
        let flags = take_u8(bytes, &mut pos)?;
        if flags & !MFLAG_HAS_SOURCE != 0 {
            return Err(Error::format(format!("unknown manifest flags 0x{flags:02x}")));
        }
        let source_id = if flags & MFLAG_HAS_SOURCE != 0 { Some(take_u64(bytes, &mut pos)?) } else { None };
        let body = match kind {
            KIND_INLINE => {
                let n = get_varint(bytes, &mut pos)? as usize;
                let env = take(bytes, &mut pos, n)?.to_vec();
                let (h, _) = read_envelope(&env)?;
                if h.raw_len as u64 != logical_len {
                    return Err(Error::format("inline envelope raw_len != logical_len"));
                }
                ManifestBody::Inline(env)
            }
            KIND_CHUNKS => {
                let count = get_varint(bytes, &mut pos)?;
                let remaining = (bytes.len() - pos) as u64;
                if count == 0 || count.checked_mul(16).map_or(true, |need| need > remaining) {
                    return Err(Error::format("invalid chunk count"));
                }
                let mut refs = Vec::with_capacity(count as usize);
                let mut prev_end = 0u64;
                for _ in 0..count {
                    let logical_end = take_u64(bytes, &mut pos)?;
                    let object_id = take_u64(bytes, &mut pos)?;
                    if logical_end <= prev_end {
                        return Err(Error::format("chunk ends must be strictly increasing"));
                    }
                    if object_id == 0 {
                        return Err(Error::format("object id 0 is reserved"));
                    }
                    prev_end = logical_end;
                    refs.push(ChunkRef { logical_end, object_id });
                }
                if prev_end != logical_len {
                    return Err(Error::format("last chunk end != logical_len"));
                }
                ManifestBody::Chunks(refs)
            }
            KIND_GENERATED => {
                let generator_id = take_u16(bytes, &mut pos)?;
                let generator_version = take_u16(bytes, &mut pos)?;
                let n = get_varint(bytes, &mut pos)? as usize;
                if n > MAX_PARAMS_LEN {
                    return Err(Error::format("generator params too long"));
                }
                let params = take(bytes, &mut pos, n)?.to_vec();
                let digest = take_digest(bytes, &mut pos)?;
                ManifestBody::Generated { generator_id, generator_version, params, digest }
            }
            KIND_TOMBSTONE => {
                if logical_len != 0 {
                    return Err(Error::format("tombstone with non-zero length"));
                }
                ManifestBody::Tombstone
            }
            k => return Err(Error::format(format!("unknown manifest kind {k}"))),
        };
        if pos != bytes.len() {
            return Err(Error::format("trailing bytes after manifest"));
        }
        Ok(Manifest { revision, logical_len, source_id, body })
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self.body, ManifestBody::Tombstone)
    }

    /// Object ids referenced by this manifest, in order, with repetitions.
    pub fn object_ids(&self) -> Vec<u64> {
        match &self.body {
            ManifestBody::Chunks(refs) => refs.iter().map(|r| r.object_id).collect(),
            _ => Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Table keys and small values
// ---------------------------------------------------------------------------

/// Integer keys are big-endian so that byte order equals numeric order.
pub fn id_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

pub fn parse_id_key(k: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = k.try_into().map_err(|_| Error::format("id key must be 8 bytes"))?;
    Ok(u64::from_be_bytes(arr))
}

/// `hash_candidates` key: digest[32] ++ raw_len (u32 LE).
pub fn candidate_key(digest: &[u8; 32], raw_len: u32) -> [u8; 36] {
    let mut k = [0u8; 36];
    k[..32].copy_from_slice(digest);
    k[32..].copy_from_slice(&raw_len.to_le_bytes());
    k
}

/// Lists of object ids (candidates, pending imports): concatenated u64 LE.
pub fn encode_id_list(ids: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

pub fn decode_id_list(bytes: &[u8]) -> Result<Vec<u64>> {
    if bytes.len() % 8 != 0 {
        return Err(Error::format("id list length not a multiple of 8"));
    }
    Ok(bytes.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect())
}

/// History key: escape(user_key) ++ 0x00 0x00 ++ revision (u64 BE).
/// Escaping maps 0x00 -> 0x00 0xFF, so the terminator is unambiguous and
/// entries sort by (user_key, revision).
pub fn history_prefix(user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + 2);
    for &b in user_key {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.push(0);
    out.push(0);
    out
}

pub fn history_key(user_key: &[u8], revision: u64) -> Vec<u8> {
    let mut out = history_prefix(user_key);
    out.extend_from_slice(&revision.to_be_bytes());
    out
}

pub fn parse_history_key(k: &[u8]) -> Result<(Vec<u8>, u64)> {
    let mut key = Vec::with_capacity(k.len());
    let mut i = 0;
    while i < k.len() {
        let b = k[i];
        if b == 0 {
            match k.get(i + 1) {
                Some(0xFF) => {
                    key.push(0);
                    i += 2;
                }
                Some(0) => {
                    let rev_bytes = k.get(i + 2..).ok_or_else(|| Error::format("truncated history key"))?;
                    let arr: [u8; 8] =
                        rev_bytes.try_into().map_err(|_| Error::format("bad history revision"))?;
                    return Ok((key, u64::from_be_bytes(arr)));
                }
                _ => return Err(Error::format("bad escape in history key")),
            }
        } else {
            key.push(b);
            i += 1;
        }
    }
    Err(Error::format("history key without terminator"))
}

/// Names of the `meta` table entries.
pub mod meta_key {
    pub const FORMAT_VERSION: &str = "format_version"; // u32 LE
    pub const MODE: &str = "mode"; // u8
    pub const BLOCK_SIZE: &str = "block_size"; // u32 LE
    pub const INLINE_MAX: &str = "inline_max"; // u32 LE
    pub const NEXT_OBJECT_ID: &str = "next_object_id"; // u64 LE, starts at 1 (0 = none)
    pub const NEXT_REVISION: &str = "next_revision"; // u64 LE, starts at 1
    pub const NEXT_PARAM_ID: &str = "next_param_id"; // u64 LE, starts at 1
    pub const NEXT_SOURCE_ID: &str = "next_source_id"; // u64 LE, starts at 1
    pub const NEXT_IMPORT_ID: &str = "next_import_id"; // u64 LE, starts at 1
    pub const ACTIVE_ZSTD_DICT: &str = "active_zstd_dict"; // u64 LE param id (absent = none)
    pub const ACTIVE_TEMPLATE: &str = "active_template"; // u64 LE param id (absent = none)
    pub const CREATED_BY: &str = "created_by"; // utf-8 crate version
}

/// Kinds of shared decoding dependencies stored in `params`.
pub mod param_kind {
    pub const ZSTD_DICT: u8 = 1;
    pub const TEMPLATE: u8 = 2;
}

/// Immutable decoding dependency: u8 kind | u16 LE version | bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Param {
    pub kind: u8,
    pub version: u16,
    pub bytes: Vec<u8>,
}

impl Param {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(3 + self.bytes.len());
        out.push(self.kind);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&self.bytes);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Param> {
        let mut pos = 0;
        let kind = take_u8(bytes, &mut pos)?;
        let version = take_u16(bytes, &mut pos)?;
        if kind != param_kind::ZSTD_DICT && kind != param_kind::TEMPLATE {
            return Err(Error::format(format!("unknown param kind {kind}")));
        }
        if version != 1 {
            return Err(Error::format(format!("unknown param version {version}")));
        }
        Ok(Param { kind, version, bytes: bytes[pos..].to_vec() })
    }
}

/// Kinds of import sources.
pub mod source_kind {
    pub const LOCAL_FILE: u8 = 1;
    pub const GENERATOR: u8 = 2;
    pub const EXTERNAL: u8 = 3;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportInfo {
    /// Revision of the record published by the import.
    pub revision: u64,
    pub bytes: u64,
    /// BLAKE3 of the imported bytes (identity of the source version actually read).
    pub digest: [u8; 32],
    pub unix_ms: u64,
}

/// Optional provenance shared by imported records. Never holds credentials,
/// and never replaces the stored content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub kind: u8,
    pub location: String,
    pub adapter_version: u16,
    pub last_import: Option<ImportInfo>,
}

impl SourceDescriptor {
    /// u8 version=1 | u8 kind | u16 adapter_version | varint len, location utf-8
    /// | u8 has_last | [u64 revision, u64 bytes, digest[32], u64 unix_ms]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.location.len());
        out.push(1);
        out.push(self.kind);
        out.extend_from_slice(&self.adapter_version.to_le_bytes());
        put_varint(&mut out, self.location.len() as u64);
        out.extend_from_slice(self.location.as_bytes());
        match &self.last_import {
            None => out.push(0),
            Some(i) => {
                out.push(1);
                out.extend_from_slice(&i.revision.to_le_bytes());
                out.extend_from_slice(&i.bytes.to_le_bytes());
                out.extend_from_slice(&i.digest);
                out.extend_from_slice(&i.unix_ms.to_le_bytes());
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<SourceDescriptor> {
        let mut pos = 0;
        let version = take_u8(bytes, &mut pos)?;
        if version != 1 {
            return Err(Error::format(format!("unknown source descriptor version {version}")));
        }
        let kind = take_u8(bytes, &mut pos)?;
        let adapter_version = take_u16(bytes, &mut pos)?;
        let n = get_varint(bytes, &mut pos)? as usize;
        let location = String::from_utf8(take(bytes, &mut pos, n)?.to_vec())
            .map_err(|_| Error::format("source location is not utf-8"))?;
        let last_import = match take_u8(bytes, &mut pos)? {
            0 => None,
            1 => Some(ImportInfo {
                revision: take_u64(bytes, &mut pos)?,
                bytes: take_u64(bytes, &mut pos)?,
                digest: take_digest(bytes, &mut pos)?,
                unix_ms: take_u64(bytes, &mut pos)?,
            }),
            _ => return Err(Error::format("bad has_last flag")),
        };
        if pos != bytes.len() {
            return Err(Error::format("trailing bytes after source descriptor"));
        }
        Ok(SourceDescriptor { kind, location, adapter_version, last_import })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let d = [7u8; 32];
        let env = write_envelope(CodecTag::RAW_V1, 0, 3, &d, b"abc");
        assert_eq!(env.len(), 67);
        let (h, body) = read_envelope(&env).unwrap();
        assert_eq!(h.codec, CodecTag::RAW_V1);
        assert_eq!(h.raw_len, 3);
        assert_eq!(body, b"abc");
    }

    #[test]
    fn manifest_roundtrip() {
        let m = Manifest {
            revision: 9,
            logical_len: 20,
            source_id: Some(3),
            body: ManifestBody::Chunks(vec![
                ChunkRef { logical_end: 16, object_id: 1 },
                ChunkRef { logical_end: 20, object_id: 2 },
            ]),
        };
        assert_eq!(Manifest::decode(&m.encode()).unwrap(), m);
    }

    #[test]
    fn history_key_order_and_parse() {
        let a = history_key(b"a\0b", 5);
        let (k, r) = parse_history_key(&a).unwrap();
        assert_eq!(k, b"a\0b");
        assert_eq!(r, 5);
        assert!(history_key(b"a", 9) < history_key(b"a\0", 1));
        assert!(history_key(b"a", 1) < history_key(b"a", 2));
    }

    #[test]
    fn varint_edges() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            let mut p = 0;
            assert_eq!(get_varint(&b, &mut p).unwrap(), v);
            assert_eq!(p, b.len());
        }
    }
}
