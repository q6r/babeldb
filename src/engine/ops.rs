//! Typed transactional helpers shared by the engine, ingestion and maintenance.
//! Object lifetime rule: every manifest reference (current record, retained
//! history, pending import) holds one refcount; an object is removed together
//! with its hash candidate and refcount row when the count reaches zero.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::codec::{self, Deps, Template, ZstdDict};
use crate::error::{Error, Result};
use crate::format::{self, id_key, meta_key, param_kind, Manifest, Param};
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

/// Choose the representation of `data` and wrap it in an envelope.
pub fn prepare_unit(planner: &Planner, data: &[u8]) -> PreparedUnit {
    assert!(data.len() <= format::MAX_UNIT_LEN as usize, "unit larger than MAX_UNIT_LEN");
    let digest = hash::digest(data);
    let enc = planner.encode_unit(data);
    let envelope = format::write_envelope(enc.codec, enc.aux_id, data.len() as u32, &digest, &enc.body);
    PreparedUnit { digest, raw_len: data.len() as u32, envelope }
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
        if codec::required_param(h.codec, h.aux_id).is_some() {
            self.load(t, h.aux_id)?;
        }
        Ok(())
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
    let needed = codec::required_param(h.codec, h.aux_id);
    let entry = match needed {
        Some(_) => Some(params.get(h.aux_id).ok_or(Error::MissingDependency { param_id: h.aux_id })?),
        None => None,
    };
    let deps = match (&entry, needed) {
        (None, None) => Deps::default(),
        (Some(ParamEntry::Dict(d)), Some(param_kind::ZSTD_DICT)) => Deps { zstd_dict: Some(d.as_ref()), template: None },
        (Some(ParamEntry::Template(t)), Some(param_kind::TEMPLATE)) => Deps { zstd_dict: None, template: Some(t.as_ref()) },
        _ => return Err(Error::integrity(object_id, "param kind does not match codec")),
    };
    out.clear();
    out.reserve(h.raw_len as usize);
    codec::decode(h.codec, h.aux_id, body, h.raw_len, deps, out)?;
    if verify && hash::digest(out) != h.digest {
        return Err(Error::integrity(object_id, "BLAKE3 digest mismatch"));
    }
    Ok(())
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
        for cand in candidates(w, &unit.digest, unit.raw_len)? {
            if let Some(env) = w.get(Table::Objects, &id_key(cand))? {
                params.ensure_for_envelope(w, &env)?;
                let bytes = decode_envelope(&env, params, false, Some(cand))?;
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
