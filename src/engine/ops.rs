//! Typed transactional helpers shared by the engine, ingestion and maintenance.
//! Object lifetime rule: every manifest reference (current record, retained
//! history, pending import) holds one refcount; an object is removed together
//! with its hash candidate and refcount row when the count reaches zero.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};

use crate::codec::{self, Deps, Template, ZstdDict};
use crate::error::{Error, Result};
use crate::format::{
    self, id_key, meta_key, param_kind, CodecTag, EnvelopeHeader, Manifest, Param, ENVELOPE_HEADER_LEN,
    MANIFEST_VERSION, OBJECT_MAGIC,
};
use crate::hash::{self, Digest};
use crate::planner::Planner;
use crate::store::{ReadTxn, Table, WriteTxn};

/// One unit (block or inline value) encoded outside the write transaction.
#[derive(Clone, Debug)]
pub struct PreparedUnit {
    pub digest: Digest,
    pub raw_len: u32,
    pub envelope: Vec<u8>,
}

/// Choose the representation of `data` and wrap it in an envelope. The
/// chosen body is written into the envelope straight from the planner (one
/// allocation, one copy).
pub fn prepare_unit(planner: &Planner, data: &[u8]) -> PreparedUnit {
    assert!(data.len() <= format::MAX_UNIT_LEN as usize, "unit larger than MAX_UNIT_LEN");
    let digest = hash::digest(data);
    let raw_len = data.len() as u32;
    let envelope = planner.encode_unit_with(data, |codec, aux_id, body| {
        let mut envelope = Vec::with_capacity(ENVELOPE_HEADER_LEN + body.len());
        append_envelope(&mut envelope, codec, aux_id, raw_len, &digest, body);
        envelope
    });
    PreparedUnit { digest, raw_len, envelope }
}

/// Append an envelope (§5 of `docs/format.md`) to `out`: the bytes of
/// `format::write_envelope`, without allocating a separate buffer.
pub(crate) fn append_envelope(out: &mut Vec<u8>, codec: CodecTag, aux_id: u64, raw_len: u32, digest: &Digest, body: &[u8]) {
    out.reserve(ENVELOPE_HEADER_LEN + body.len());
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
}

/// Manifest kind byte of Inline manifests (§6).
const KIND_INLINE: u8 = 0;
/// Where the revision (u64 LE) sits in every manifest (§6).
pub(crate) const MANIFEST_REVISION: Range<usize> = 1..9;

/// Encode `data` as a complete Inline manifest (§6) without a source and with
/// revision 0, ready to be published once `set_manifest_revision` wrote the
/// revision allocated inside the write transaction. The bytes are those of
/// `Manifest { body: ManifestBody::Inline(envelope), .. }.encode()`, built in
/// one allocation and outside the transaction.
pub(crate) fn prepare_inline_manifest(planner: &Planner, data: &[u8]) -> Vec<u8> {
    assert!(data.len() <= format::MAX_UNIT_LEN as usize, "unit larger than MAX_UNIT_LEN");
    let digest = hash::digest(data);
    let raw_len = data.len() as u32;
    planner.encode_unit_with(data, |codec, aux_id, body| {
        let envelope_len = ENVELOPE_HEADER_LEN + body.len();
        let mut m = Vec::with_capacity(MANIFEST_REVISION.end + 8 + 2 + 10 + envelope_len);
        m.push(MANIFEST_VERSION);
        m.extend_from_slice(&0u64.to_le_bytes());
        m.extend_from_slice(&(data.len() as u64).to_le_bytes());
        m.push(KIND_INLINE);
        m.push(0); // flags: no source
        format::put_varint(&mut m, envelope_len as u64);
        append_envelope(&mut m, codec, aux_id, raw_len, &digest, body);
        m
    })
}

/// Write `revision` into an encoded manifest.
pub(crate) fn set_manifest_revision(manifest: &mut [u8], revision: u64) {
    manifest[MANIFEST_REVISION].copy_from_slice(&revision.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Params (shared decoding dependencies)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub enum ParamEntry {
    Dict(Arc<ZstdDict>),
    Template(Arc<Template>),
}

/// Prepared params by id. Params are immutable, so entries never go stale.
pub struct ParamCache {
    map: RwLock<HashMap<u64, ParamEntry>>,
    zstd_level: i32,
}

impl ParamCache {
    pub fn new(zstd_level: i32) -> ParamCache {
        ParamCache { map: RwLock::new(HashMap::new()), zstd_level }
    }

    pub fn get(&self, id: u64) -> Option<ParamEntry> {
        self.map.read().ok()?.get(&id).cloned()
    }

    pub fn insert(&self, id: u64, entry: ParamEntry) {
        if let Ok(mut m) = self.map.write() {
            m.insert(id, entry);
        }
    }

    pub fn remove(&self, id: u64) {
        if let Ok(mut m) = self.map.write() {
            m.remove(&id);
        }
    }

    pub fn prepare(&self, id: u64, param: Param) -> Result<ParamEntry> {
        Ok(match param.kind {
            param_kind::ZSTD_DICT => ParamEntry::Dict(Arc::new(ZstdDict::new(id, param.bytes, self.zstd_level)?)),
            param_kind::TEMPLATE => ParamEntry::Template(Arc::new(Template::new(id, param.bytes))),
            k => return Err(Error::format(format!("unknown param kind {k}"))),
        })
    }

    /// Cached entry, or load + prepare it from the `params` table.
    pub fn load<T: ReadTxn + ?Sized>(&self, t: &T, id: u64) -> Result<ParamEntry> {
        if let Some(e) = self.get(id) {
            return Ok(e);
        }
        let bytes = t.get(Table::Params, &id_key(id))?.ok_or(Error::MissingDependency { param_id: id })?;
        let entry = self.prepare(id, Param::decode(&bytes)?)?;
        self.insert(id, entry.clone());
        Ok(entry)
    }

    /// Load the dependency of an envelope, if any (call while the txn is alive).
    pub fn ensure_for_envelope<T: ReadTxn + ?Sized>(&self, t: &T, envelope: &[u8]) -> Result<()> {
        let (h, _) = format::read_envelope(envelope)?;
        self.dependency(t, &h).map(|_| ())
    }

    /// The dependency an envelope with header `h` needs (loaded through `t`
    /// if it is not prepared yet), or None when it needs none.
    pub(crate) fn dependency<T: ReadTxn + ?Sized>(&self, t: &T, h: &EnvelopeHeader) -> Result<Option<ParamEntry>> {
        match codec::required_param(h.codec, h.aux_id) {
            Some(_) => self.load(t, h.aux_id).map(Some),
            None => Ok(None),
        }
    }

    /// `dependency` without a transaction: `Ok(None)` when none is needed,
    /// `Err(())` when it is needed but not prepared yet.
    pub(crate) fn cached_dependency(&self, h: &EnvelopeHeader) -> std::result::Result<Option<ParamEntry>, ()> {
        match codec::required_param(h.codec, h.aux_id) {
            Some(_) => self.get(h.aux_id).map(Some).ok_or(()),
            None => Ok(None),
        }
    }
}

/// Decode an envelope into the original bytes. Dependencies must already be in
/// `params` (see `ParamCache::ensure_for_envelope`). With `verify`, the length
/// and BLAKE3 digest are checked.
pub fn decode_envelope(envelope: &[u8], params: &ParamCache, verify: bool, object_id: Option<u64>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    decode_envelope_into(envelope, params, verify, object_id, &mut out)?;
    Ok(out)
}

/// `decode_envelope` into a reusable buffer (cleared first). On success `out`
/// holds exactly the header's `raw_len` bytes.
pub fn decode_envelope_into(
    envelope: &[u8],
    params: &ParamCache,
    verify: bool,
    object_id: Option<u64>,
    out: &mut Vec<u8>,
) -> Result<()> {
    let (h, body) = format::read_envelope(envelope)?;
    let entry = match codec::required_param(h.codec, h.aux_id) {
        Some(_) => Some(params.get(h.aux_id).ok_or(Error::MissingDependency { param_id: h.aux_id })?),
        None => None,
    };
    decode_body_into(&h, body, entry.as_ref(), verify, object_id, out)
}

/// Decode the body of an envelope whose header `h` was already parsed and
/// validated (`format::read_envelope`), with its dependency `entry` (None when
/// the codec needs none). `out` is cleared first and holds exactly `raw_len`
/// bytes on success; with `verify` their BLAKE3 digest must equal the header's.
pub(crate) fn decode_body_into(
    h: &EnvelopeHeader,
    body: &[u8],
    entry: Option<&ParamEntry>,
    verify: bool,
    object_id: Option<u64>,
    out: &mut Vec<u8>,
) -> Result<()> {
    let deps = deps_for(h, entry, object_id)?;
    out.clear();
    out.reserve(h.raw_len as usize);
    codec::decode(h.codec, h.aux_id, body, h.raw_len, deps, out)?;
    if verify && hash::digest(out) != h.digest {
        return Err(Error::integrity(object_id, "BLAKE3 digest mismatch"));
    }
    Ok(())
}

/// Decoding dependencies of an envelope: the prepared param must be of the
/// kind its codec requires.
fn deps_for<'a>(h: &EnvelopeHeader, entry: Option<&'a ParamEntry>, object_id: Option<u64>) -> Result<Deps<'a>> {
    match (codec::required_param(h.codec, h.aux_id), entry) {
        (None, _) => Ok(Deps::default()),
        (Some(param_kind::ZSTD_DICT), Some(ParamEntry::Dict(d))) => Ok(Deps { zstd_dict: Some(d.as_ref()), template: None }),
        (Some(param_kind::TEMPLATE), Some(ParamEntry::Template(t))) => Ok(Deps { zstd_dict: None, template: Some(t.as_ref()) }),
        (Some(_), None) => Err(Error::MissingDependency { param_id: h.aux_id }),
        _ => Err(Error::integrity(object_id, "param kind does not match codec")),
    }
}

// ---------------------------------------------------------------------------
// Meta and id allocation
// ---------------------------------------------------------------------------

pub fn get_meta<T: ReadTxn + ?Sized>(t: &T, name: &str) -> Result<Option<Vec<u8>>> {
    t.get(Table::Meta, name.as_bytes())
}

pub fn get_meta_u64<T: ReadTxn + ?Sized>(t: &T, name: &str) -> Result<Option<u64>> {
    get_meta(t, name)?.map(|v| format::decode_u64(&v)).transpose()
}

pub fn put_meta_u64<W: WriteTxn + ?Sized>(w: &mut W, name: &str, v: u64) -> Result<()> {
    w.put(Table::Meta, name.as_bytes(), &format::encode_u64(v))
}

/// Allocate the next value of a monotonic counter. Identifiers are never reused;
/// exhaustion is an error, never a silent wrap.
pub fn alloc_id<W: WriteTxn + ?Sized>(w: &mut W, counter: &str, what: &'static str) -> Result<u64> {
    let next = get_meta_u64(w, counter)?.ok_or_else(|| Error::format(format!("missing meta counter {counter}")))?;
    if next == u64::MAX {
        return Err(Error::IdExhausted(what));
    }
    put_meta_u64(w, counter, next + 1)?;
    Ok(next)
}

pub fn alloc_revision<W: WriteTxn + ?Sized>(w: &mut W) -> Result<u64> {
    alloc_id(w, meta_key::NEXT_REVISION, "revision")
}

// ---------------------------------------------------------------------------
// Manifests
// ---------------------------------------------------------------------------

pub fn load_manifest<T: ReadTxn + ?Sized>(t: &T, key: &[u8]) -> Result<Option<Manifest>> {
    t.get(Table::Records, key)?.map(|b| Manifest::decode(&b)).transpose()
}

pub fn put_manifest<W: WriteTxn + ?Sized>(w: &mut W, key: &[u8], m: &Manifest) -> Result<()> {
    w.put(Table::Records, key, &m.encode())
}

// ---------------------------------------------------------------------------
// Refcounts, candidates, objects
// ---------------------------------------------------------------------------

pub fn get_refcount<T: ReadTxn + ?Sized>(t: &T, id: u64) -> Result<u64> {
    Ok(t.get(Table::Refcounts, &id_key(id))?.map(|v| format::decode_u64(&v)).transpose()?.unwrap_or(0))
}

pub fn incref<W: WriteTxn + ?Sized>(w: &mut W, id: u64) -> Result<u64> {
    let n = get_refcount(w, id)?.checked_add(1).ok_or(Error::IdExhausted("refcount"))?;
    w.put(Table::Refcounts, &id_key(id), &format::encode_u64(n))?;
    Ok(n)
}

/// Drop one reference. At zero the object, its candidate entry and its
/// refcount row are removed. Returns whether the object was removed.
pub fn decref<W: WriteTxn + ?Sized>(w: &mut W, id: u64) -> Result<bool> {
    let n = get_refcount(w, id)?;
    match n {
        0 => Err(Error::integrity(Some(id), "refcount underflow")),
        1 => {
            remove_object(w, id)?;
            Ok(true)
        }
        _ => {
            w.put(Table::Refcounts, &id_key(id), &format::encode_u64(n - 1))?;
            Ok(false)
        }
    }
}

/// Remove an object with its candidate entry and refcount row (no checks).
pub fn remove_object<W: WriteTxn + ?Sized>(w: &mut W, id: u64) -> Result<()> {
    if let Some(env) = w.get(Table::Objects, &id_key(id))? {
        let (h, _) = format::read_envelope(&env)?;
        remove_candidate(w, &h.digest, h.raw_len, id)?;
        w.remove(Table::Objects, &id_key(id))?;
    }
    w.remove(Table::Refcounts, &id_key(id))?;
    Ok(())
}

pub fn candidates<T: ReadTxn + ?Sized>(t: &T, digest: &Digest, raw_len: u32) -> Result<Vec<u64>> {
    match t.get(Table::HashCandidates, &format::candidate_key(digest, raw_len))? {
        Some(v) => format::decode_id_list(&v),
        None => Ok(Vec::new()),
    }
}

pub fn add_candidate<W: WriteTxn + ?Sized>(w: &mut W, digest: &Digest, raw_len: u32, id: u64) -> Result<()> {
    let mut ids = candidates(w, digest, raw_len)?;
    if !ids.contains(&id) {
        ids.push(id);
        w.put(Table::HashCandidates, &format::candidate_key(digest, raw_len), &format::encode_id_list(&ids))?;
    }
    Ok(())
}

pub fn remove_candidate<W: WriteTxn + ?Sized>(w: &mut W, digest: &Digest, raw_len: u32, id: u64) -> Result<()> {
    let key = format::candidate_key(digest, raw_len);
    let mut ids = candidates(w, digest, raw_len)?;
    let before = ids.len();
    ids.retain(|&x| x != id);
    if ids.len() != before {
        if ids.is_empty() {
            w.remove(Table::HashCandidates, &key)?;
        } else {
            w.put(Table::HashCandidates, &key, &format::encode_id_list(&ids))?;
        }
    }
    Ok(())
}

/// Store (or, with dedupe, reuse) the object of one prepared unit and take one
/// reference to it. A candidate is reused only after its decoded bytes are
/// compared byte for byte with `raw`: a digest is never treated as proof.
/// Returns (object id, reused).
pub fn store_unit<W: WriteTxn + ?Sized>(
    w: &mut W,
    unit: &PreparedUnit,
    raw: &[u8],
    dedupe: bool,
    params: &ParamCache,
) -> Result<(u64, bool)> {
    debug_assert_eq!(raw.len(), unit.raw_len as usize);
    if dedupe {
        let mut bytes = Vec::new();
        for cand in candidates(w, &unit.digest, unit.raw_len)? {
            if let Some(env) = w.get(Table::Objects, &id_key(cand))? {
                let (h, body) = format::read_envelope(&env)?;
                let entry = params.dependency(w, &h)?;
                decode_body_into(&h, body, entry.as_ref(), false, Some(cand), &mut bytes)?;
                if bytes == raw {
                    incref(w, cand)?;
                    return Ok((cand, true));
                }
            }
        }
    }
    let id = alloc_id(w, meta_key::NEXT_OBJECT_ID, "object id")?;
    w.put(Table::Objects, &id_key(id), &unit.envelope)?;
    if dedupe {
        add_candidate(w, &unit.digest, unit.raw_len, id)?;
    }
    incref(w, id)?;
    Ok((id, false))
}

/// Take one reference per chunk of a manifest.
pub fn retain_manifest<W: WriteTxn + ?Sized>(w: &mut W, m: &Manifest) -> Result<()> {
    for id in m.object_ids() {
        incref(w, id)?;
    }
    Ok(())
}

/// Release every chunk reference of a manifest; returns removed object ids
/// (to evict from the block cache after commit).
pub fn release_manifest<W: WriteTxn + ?Sized>(w: &mut W, m: &Manifest) -> Result<Vec<u64>> {
    let mut removed = Vec::new();
    for id in m.object_ids() {
        if decref(w, id)? {
            removed.push(id);
        }
    }
    Ok(removed)
}

pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CodecPolicy, Mode};
    use crate::format::ManifestBody;

    fn inputs() -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = vec![Vec::new(), b"x".to_vec(), vec![0; 300], b"abc".repeat(50)];
        v.push((0..700u32).map(|i| (i * 31 % 251) as u8).collect());
        v.push(br#"{"id":"1190000000000123","content":"hello there, the deploy worked"}"#.repeat(3));
        v.push((0..20u64).flat_map(|i| (1000 + 3 * i).to_le_bytes()).collect());
        v
    }

    #[test]
    fn envelopes_match_the_format_writer() {
        for planner in [
            Planner::new(Mode::Adaptive, CodecPolicy::default()),
            Planner::new(Mode::Adaptive, CodecPolicy::raw_only()),
            Planner::new(Mode::BabelPure, CodecPolicy::default()),
        ] {
            for data in inputs() {
                let unit = prepare_unit(&planner, &data);
                let enc = planner.encode_unit(&data);
                let expected = format::write_envelope(enc.codec, enc.aux_id, data.len() as u32, &hash::digest(&data), &enc.body);
                assert_eq!(unit.envelope, expected);
                assert_eq!((unit.raw_len as usize, unit.digest), (data.len(), hash::digest(&data)));
            }
        }
    }

    #[test]
    fn prepared_inline_manifests_match_manifest_encode() {
        let planner = Planner::new(Mode::Adaptive, CodecPolicy::default());
        for data in inputs() {
            for revision in [1u64, 258, u64::MAX - 1] {
                let mut prepared = prepare_inline_manifest(&planner, &data);
                set_manifest_revision(&mut prepared, revision);
                let envelope = prepare_unit(&planner, &data).envelope;
                let expected = Manifest { revision, logical_len: data.len() as u64, source_id: None, body: ManifestBody::Inline(envelope) }.encode();
                assert_eq!(prepared, expected);
                let m = Manifest::decode(&prepared).unwrap();
                assert_eq!((m.revision, m.logical_len), (revision, data.len() as u64));
            }
        }
    }

    #[test]
    fn body_decoding_checks_dependencies_and_digest() {
        let params = ParamCache::new(3);
        let data = b"hello hello hello hello hello hello hello hello".to_vec();
        let planner = Planner::new(Mode::Adaptive, CodecPolicy::default());
        let env = prepare_unit(&planner, &data).envelope;
        let (h, body) = format::read_envelope(&env).unwrap();
        let mut out = vec![9; 3];
        decode_body_into(&h, body, None, true, None, &mut out).unwrap();
        assert_eq!(out, data);
        assert_eq!(decode_envelope(&env, &params, true, None).unwrap(), data);
        let mut bad = h.clone();
        bad.digest[0] ^= 1;
        assert!(matches!(decode_body_into(&bad, body, None, true, Some(4), &mut out), Err(Error::Integrity { object_id: Some(4), .. })));
        decode_body_into(&bad, body, None, false, None, &mut out).unwrap();
        // A template envelope without its param, or with a param of the wrong kind.
        let tpl = format::write_envelope(CodecTag::TEMPLATE_PATCH_V1, 77, 1, &hash::digest(b"a"), &[2, b'a']);
        let (th, tbody) = format::read_envelope(&tpl).unwrap();
        assert!(matches!(decode_body_into(&th, tbody, None, true, None, &mut out), Err(Error::MissingDependency { param_id: 77 })));
        let dict = ParamEntry::Dict(Arc::new(ZstdDict::new(77, vec![1; 300], 3).unwrap()));
        assert!(matches!(decode_body_into(&th, tbody, Some(&dict), true, None, &mut out), Err(Error::Integrity { .. })));
        assert_eq!(params.cached_dependency(&th).err(), Some(()));
        assert!(matches!(params.cached_dependency(&h), Ok(None)));
        params.insert(77, ParamEntry::Template(Arc::new(Template::new(77, b"abc".to_vec()))));
        assert!(matches!(params.cached_dependency(&th), Ok(Some(ParamEntry::Template(_)))));
    }
}
