//! Diagnostics: per-record inspection and full space accounting.

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;

use super::{Db, Inspection, UnitInfo, ops};
use crate::error::{Error, Result};
use crate::format::{self, CodecTag, Manifest, ManifestBody, SourceDescriptor};
use crate::stats::{CodecUsage, Stats};
use crate::store::{ReadTxn, Store, Table};
use crate::sys;

/// Codec name, with the raw tag for codecs this build does not know.
fn codec_label(tag: CodecTag) -> String {
    match tag.name() {
        "Unknown" => format!("Unknown(0x{:02x} v{})", tag.id, tag.version),
        name => name.to_string(),
    }
}

fn unit_info(envelope: &[u8], object_id: Option<u64>, refcount: Option<u64>) -> Result<UnitInfo> {
    let (h, _) = format::read_envelope(envelope)?;
    Ok(UnitInfo {
        object_id,
        codec: codec_label(h.codec),
        raw_len: h.raw_len,
        body_len: h.body_len,
        aux_id: h.aux_id,
        refcount,
    })
}

pub(super) fn inspect<S: Store>(db: &Db<S>, key: &[u8]) -> Result<Option<Inspection>> {
    let r = db.store.begin_read()?;
    let Some(raw) = r.get(Table::Records, key)? else {
        return Ok(None);
    };
    let m = Manifest::decode(&raw)?;
    let mut units = Vec::new();
    let mut encoded_bytes = 0u64;
    let mut generator = None;
    let kind = match &m.body {
        ManifestBody::Inline(envelope) => {
            units.push(unit_info(envelope, None, None)?);
            encoded_bytes = envelope.len() as u64;
            "inline"
        }
        ManifestBody::Chunks(refs) => {
            let mut counted = HashSet::new();
            for c in refs {
                let envelope = r
                    .get(Table::Objects, &format::id_key(c.object_id))?
                    .ok_or_else(|| Error::integrity(Some(c.object_id), "referenced object is missing"))?;
                let refcount = ops::get_refcount(&r, c.object_id)?;
                units.push(unit_info(&envelope, Some(c.object_id), Some(refcount))?);
                if counted.insert(c.object_id) {
                    encoded_bytes += envelope.len() as u64;
                }
            }
            "chunks"
        }
        ManifestBody::Generated { generator_id, generator_version, params, .. } => {
            let name = db
                .generators
                .get(*generator_id, *generator_version)
                .map_or_else(|_| "unknown".to_string(), |g| g.name().to_string());
            generator = Some((*generator_id, *generator_version, name, params.len()));
            "generated"
        }
        ManifestBody::Tombstone => "tombstone",
    };
    let source = match m.source_id {
        Some(id) => r
            .get(Table::Sources, &format::id_key(id))?
            .map(|v| SourceDescriptor::decode(&v))
            .transpose()?
            .map(|d| (id, d)),
        None => None,
    };
    Ok(Some(Inspection {
        key: key.to_vec(),
        revision: m.revision,
        logical_len: m.logical_len,
        kind,
        manifest_bytes: raw.len() as u64,
        encoded_bytes,
        units,
        source,
        generator,
    }))
}

fn add_codec_usage(codecs: &mut BTreeMap<CodecTag, CodecUsage>, envelope: &[u8]) -> Result<()> {
    let (h, _) = format::read_envelope(envelope)?;
    let usage = codecs
        .entry(h.codec)
        .or_insert_with(|| CodecUsage { codec: codec_label(h.codec), ..CodecUsage::default() });
    usage.units += 1;
    usage.raw_bytes += u64::from(h.raw_len);
    usage.body_bytes += u64::from(h.body_len);
    usage.envelope_bytes += envelope.len() as u64;
    Ok(())
}

/// (entries, Σ key.len() + value.len()) of a table.
fn table_totals<T: ReadTxn + ?Sized>(t: &T, table: Table) -> Result<(u64, u64)> {
    let (mut entries, mut bytes) = (0u64, 0u64);
    t.scan(table, Bound::Unbounded, Bound::Unbounded, false, &mut |k: &[u8], v: &[u8]| {
        entries += 1;
        bytes += (k.len() + v.len()) as u64;
        Ok(true)
    })?;
    Ok((entries, bytes))
}

pub(super) fn stats<S: Store>(db: &Db<S>) -> Result<Stats> {
    let r = db.store.begin_read()?;
    let mut s = Stats {
        backend: db.store.backend_name(),
        mode: db.mode,
        block_size: db.block_size,
        inline_max: db.inline_max,
        records: 0,
        tombstones: 0,
        objects: 0,
        hash_candidates: 0,
        params: 0,
        history_entries: 0,
        sources: 0,
        pending_imports: 0,
        logical_bytes: 0,
        key_bytes: 0,
        manifest_bytes: 0,
        inline_envelope_bytes: 0,
        object_bytes: 0,
        candidate_bytes: 0,
        refcount_bytes: 0,
        param_bytes: 0,
        history_bytes: 0,
        source_bytes: 0,
        meta_bytes: 0,
        pending_import_bytes: 0,
        per_codec: Vec::new(),
        files: Vec::new(),
        cache: Default::default(),
        planner: Default::default(),
        counters: Default::default(),
    };
    let mut codecs: BTreeMap<CodecTag, CodecUsage> = BTreeMap::new();
    r.scan(Table::Records, Bound::Unbounded, Bound::Unbounded, false, &mut |k: &[u8], v: &[u8]| {
        s.key_bytes += k.len() as u64;
        s.manifest_bytes += v.len() as u64;
        let m = Manifest::decode(v)?;
        if m.is_tombstone() {
            s.tombstones += 1;
        } else {
            s.records += 1;
            s.logical_bytes += m.logical_len;
        }
        if let ManifestBody::Inline(envelope) = &m.body {
            s.inline_envelope_bytes += envelope.len() as u64;
            add_codec_usage(&mut codecs, envelope)?;
        }
        Ok(true)
    })?;
    r.scan(Table::Objects, Bound::Unbounded, Bound::Unbounded, false, &mut |k: &[u8], v: &[u8]| {
        s.objects += 1;
        s.object_bytes += (k.len() + v.len()) as u64;
        add_codec_usage(&mut codecs, v)?;
        Ok(true)
    })?;
    (s.hash_candidates, s.candidate_bytes) = table_totals(&r, Table::HashCandidates)?;
    (_, s.refcount_bytes) = table_totals(&r, Table::Refcounts)?;
    (s.params, s.param_bytes) = table_totals(&r, Table::Params)?;
    (s.history_entries, s.history_bytes) = table_totals(&r, Table::History)?;
    (s.sources, s.source_bytes) = table_totals(&r, Table::Sources)?;
    (s.pending_imports, s.pending_import_bytes) = table_totals(&r, Table::PendingImports)?;
    (_, s.meta_bytes) = table_totals(&r, Table::Meta)?;
    drop(r);
    s.per_codec = codecs.into_values().collect();
    s.files = db.store.files().iter().filter_map(|p| sys::file_size(p).ok()).collect();
    s.cache = db.cache.stats();
    s.planner = db.planner().snapshot();
    s.counters = db.counters.snapshot();
    Ok(s)
}
