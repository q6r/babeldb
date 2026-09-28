//! Maintenance and training: `Db::verify`, `Db::gc`, `Db::compact`,
//! `Db::train_dictionary`, `Db::train_template`.
//!
//! Reference model (see `engine::ops`): every chunk reference of a current
//! record, of a retained history manifest and of a pending import holds one
//! refcount. `verify` recomputes those counts from one read snapshot (imports
//! may be running, so pending references count). `gc` holds `&mut Db`, so no
//! import can be running: every pending import is abandoned and the true
//! counts come from records and history only.
//!
//! Memory: `verify` and `gc` keep one small entry per distinct referenced
//! object (O(objects)); everything else is streamed, and `gc` repairs in
//! pages of bounded size (never writing from inside a scan).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use crate::codec;
use crate::config::Mode;
use crate::engine::Db;
use crate::engine::ops::{self, ParamEntry};
use crate::error::{Error, Result};
use crate::format::{
    self, CodecTag, MAX_UNIT_LEN, Manifest, ManifestBody, Param, SourceDescriptor, id_key,
    meta_key, param_kind,
};
use crate::generator::Generator;
use crate::hash::{self, Digest, StreamHasher};
use crate::planner::{self, Planner, TrainOptions, TrainReport};
use crate::store::{Durability, ReadTxn, ScanFn, Store, Table, WriteTxn};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyReport {
    pub deep: bool,
    pub records_checked: u64,
    pub history_checked: u64,
    pub objects_checked: u64,
    /// Deep only: objects decoded and verified (length + BLAKE3) successfully.
    pub objects_decoded: u64,
    /// Distinct referenced object ids absent from `objects`.
    pub missing_objects: u64,
    /// Existing referenced objects whose refcount row differs from the number
    /// of references (records + history + pending imports), or is missing.
    pub refcount_mismatches: u64,
    /// Deep only: units that fail to decode or whose bytes do not match their
    /// digest (objects, inline envelopes, regenerated `Generated` records).
    pub digest_failures: u64,
    /// Candidate ids pointing to an object with another digest/length, to a
    /// missing object that is still referenced, or to the reserved id 0,
    /// plus malformed candidate entries.
    pub dangling_candidates: u64,
    /// Candidate ids of released objects (missing and referenced by
    /// nothing). Releasing an object leaves its id in its candidate list;
    /// ids are never reused and dedupe skips such ids, so they are harmless
    /// and only take space until the list is rewritten or `gc` drops them.
    pub stale_candidates: u64,
    /// Distinct param ids that an envelope needs (or that meta marks active)
    /// but that are missing, undecodable, of the wrong kind or unpreparable.
    pub missing_params: u64,
    /// Objects with refcount but no reference (collectable by gc). Also
    /// counts unreferenced objects without a row and rows without an object.
    pub orphan_objects: u64,
    pub pending_imports: u64,
    /// Entries that cannot be decoded or contradict each other: manifests,
    /// history keys, id lists, envelope headers, unknown codecs, params,
    /// sources, chunk lengths that differ from the object length, generator
    /// params, and meta counters that could hand out an id already in use.
    pub format_errors: u64,
    /// `Generated` records whose generator (id, version) is not registered.
    pub unknown_generators: u64,
    /// Manifests attached to a source id absent from `sources` (provenance
    /// only: the content is not affected, so `ok()` ignores it).
    pub dangling_sources: u64,
    /// Human-readable description of the first problems found (bounded).
    pub issues: Vec<String>,
}

impl VerifyReport {
    /// No inconsistency that could return wrong bytes or lose data.
    pub fn ok(&self) -> bool {
        self.missing_objects == 0
            && self.refcount_mismatches == 0
            && self.digest_failures == 0
            && self.dangling_candidates == 0
            && self.missing_params == 0
            && self.format_errors == 0
            && self.unknown_generators == 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Pending imports dropped (while gc holds the database exclusively no
    /// import can be running); their references are released.
    pub abandoned_imports: u64,
    pub objects_removed: u64,
    /// Candidate ids dropped from candidate lists (mismatching object,
    /// missing object that is still referenced, duplicate) plus malformed
    /// candidate entries removed.
    pub candidates_removed: u64,
    /// Stale candidate ids dropped: ids of released objects
    /// (`VerifyReport::stale_candidates`), expected residue rather than drift.
    pub stale_candidates_removed: u64,
    pub params_removed: u64,
    /// Refcount rows rewritten, created or removed because they disagreed
    /// with the references (releases of abandoned imports are not drift).
    pub refcounts_fixed: u64,
    /// Steps skipped or reconciled, with the reason.
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactReport {
    pub supported: bool,
    pub apparent_before: u64,
    pub apparent_after: u64,
    pub allocated_before: Option<u64>,
    pub allocated_after: Option<u64>,
}

/// Maximum number of entries of `VerifyReport::issues`.
pub const MAX_ISSUES: usize = 100;

/// Chunk length unknown (reference from a pending import). Never a valid
/// unit length, since `MAX_UNIT_LEN < u32::MAX`.
const LEN_UNKNOWN: u32 = u32::MAX;
const SEEN_OBJECT: u8 = 1;
const SEEN_ROW: u8 = 2;
/// Entries read per page by `gc`.
const GC_PAGE: usize = 1024;

/// Serializes dictionary/template installs so that the planner always
/// reflects the last committed install.
static INSTALL_LOCK: Mutex<()> = Mutex::new(());

fn scan_all<R: ReadTxn + ?Sized>(r: &R, table: Table, f: &mut ScanFn<'_>) -> Result<()> {
    r.scan(table, Bound::Unbounded, Bound::Unbounded, false, f)
}

/// Up to `limit` entries after `after` (exclusive), values mapped by `map`.
/// Lets `gc` read and write between pages instead of inside a scan callback.
fn next_page<T, X, F>(
    t: &T,
    table: Table,
    after: Option<&[u8]>,
    limit: usize,
    mut map: F,
) -> Result<Vec<(Vec<u8>, X)>>
where
    T: ReadTxn + ?Sized,
    F: FnMut(&[u8]) -> X,
{
    let start = after.map_or(Bound::Unbounded, Bound::Excluded);
    let mut page = Vec::new();
    t.scan(
        table,
        start,
        Bound::Unbounded,
        false,
        &mut |k: &[u8], v: &[u8]| {
            page.push((k.to_vec(), map(v)));
            Ok(page.len() < limit)
        },
    )?;
    Ok(page)
}

/// Printable, bounded rendering of a user key.
fn show_key(key: &[u8]) -> String {
    const MAX: usize = 64;
    let mut s = String::with_capacity(key.len().min(MAX) + 8);
    s.push('"');
    for &b in key.iter().take(MAX) {
        if (0x20..0x7f).contains(&b) && b != b'"' && b != b'\\' {
            s.push(b as char);
        } else {
            s.push_str(&format!("\\x{b:02x}"));
        }
    }
    if key.len() > MAX {
        s.push_str("...");
    }
    s.push('"');
    s
}

fn history_name(k: &[u8]) -> String {
    match format::parse_history_key(k) {
        Ok((key, rev)) => format!("history {} rev {rev}", show_key(&key)),
        Err(_) => format!("history entry {}", show_key(k)),
    }
}

fn codec_known(codec: CodecTag) -> bool {
    CodecTag::ALL_V1.contains(&codec)
}

/// BLAKE3 of the output of a generator, produced in bounded chunks.
fn regenerate_digest(g: &dyn Generator, params: &[u8], len: u64) -> Result<Digest> {
    const CHUNK: u64 = 64 * 1024;
    let mut buf = vec![0u8; CHUNK.min(len) as usize];
    let mut hasher = StreamHasher::new();
    let mut offset = 0u64;
    while offset < len {
        let n = CHUNK.min(len - offset) as usize;
        g.generate(params, offset, &mut buf[..n])?;
        hasher.update(&buf[..n]);
        offset += n as u64;
    }
    Ok(hasher.finalize())
}

/// What candidate id `id` of candidate key `key` names.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CandidateStatus {
    /// An existing object with that digest and length.
    Live,
    /// No object (released, unless something still references it).
    Missing,
    /// An object with another digest or length, or an undecodable one.
    Mismatch,
}

fn candidate_status<T: ReadTxn + ?Sized>(t: &T, key: &[u8], id: u64) -> Result<CandidateStatus> {
    Ok(match t.get(Table::Objects, &id_key(id))? {
        Some(env) => match format::read_envelope(&env) {
            Ok((h, _)) if format::candidate_key(&h.digest, h.raw_len).as_slice() == key => CandidateStatus::Live,
            _ => CandidateStatus::Mismatch,
        },
        None => CandidateStatus::Missing,
    })
}

/// Remove an object, its refcount row and (unlike `ops::remove_object`,
/// which leaves a stale id) its candidate entry, tolerating an undecodable
/// envelope (its entry cannot be located; the candidate pass drops it). The
/// refcount row is removed either way. Returns whether an object was removed.
fn remove_object_lenient<W: WriteTxn + ?Sized>(w: &mut W, id: u64) -> Result<bool> {
    let key = id_key(id);
    let existed = match w.get(Table::Objects, &key)? {
        Some(env) => {
            if let Ok((h, _)) = format::read_envelope(&env) {
                ops::remove_candidate(w, &h.digest, h.raw_len, id)?;
            }
            w.remove(Table::Objects, &key)?;
            true
        }
        None => false,
    };
    w.remove(Table::Refcounts, &key)?;
    Ok(existed)
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

/// References to one object seen by `verify`.
struct Expected {
    count: u64,
    /// Length implied by chunk references (`LEN_UNKNOWN` for pending ones).
    len: u32,
    flags: u8,
}

enum ParamStatus {
    Kind(u8),
    Undecodable,
}

struct ParamProblem {
    reason: String,
    units: u64,
}

struct Verifier<'d, S: Store> {
    db: &'d Db<S>,
    rep: VerifyReport,
    expected: HashMap<u64, Expected>,
    params: HashMap<u64, ParamStatus>,
    param_problems: BTreeMap<u64, ParamProblem>,
    sources: HashSet<u64>,
    /// Highest ids in use, checked against the meta counters.
    max_object_id: u64,
    max_revision: u64,
    max_param_id: u64,
    max_source_id: u64,
    max_import_id: u64,
}

impl<'d, S: Store> Verifier<'d, S> {
    fn new(db: &'d Db<S>, deep: bool) -> Self {
        Verifier {
            db,
            rep: VerifyReport {
                deep,
                ..VerifyReport::default()
            },
            expected: HashMap::new(),
            params: HashMap::new(),
            param_problems: BTreeMap::new(),
            sources: HashSet::new(),
            max_object_id: 0,
            max_revision: 0,
            max_param_id: 0,
            max_source_id: 0,
            max_import_id: 0,
        }
    }

    fn issue(&mut self, msg: impl FnOnce() -> String) {
        if self.rep.issues.len() < MAX_ISSUES {
            self.rep.issues.push(msg());
        }
    }

    fn format_error(&mut self, msg: impl FnOnce() -> String) {
        self.rep.format_errors += 1;
        self.issue(msg);
    }

    fn scan_params<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        scan_all(r, Table::Params, &mut |k: &[u8], v: &[u8]| {
            let Ok(id) = format::parse_id_key(k) else {
                self.format_error(|| format!("params: malformed key {}", show_key(k)));
                return Ok(true);
            };
            self.max_param_id = self.max_param_id.max(id);
            let status = match Param::decode(v) {
                Ok(p) => ParamStatus::Kind(p.kind),
                Err(e) => {
                    self.format_error(|| format!("param {id}: {e}"));
                    ParamStatus::Undecodable
                }
            };
            self.params.insert(id, status);
            Ok(true)
        })
    }

    fn scan_sources<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        scan_all(r, Table::Sources, &mut |k: &[u8], v: &[u8]| {
            let Ok(id) = format::parse_id_key(k) else {
                self.format_error(|| format!("sources: malformed key {}", show_key(k)));
                return Ok(true);
            };
            self.max_source_id = self.max_source_id.max(id);
            self.sources.insert(id);
            if let Err(e) = SourceDescriptor::decode(v) {
                self.format_error(|| format!("source {id}: {e}"));
            }
            Ok(true)
        })
    }

    /// Active dependencies named in meta must exist with the right kind.
    fn check_active_params<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        for (name, kind) in [
            (meta_key::ACTIVE_ZSTD_DICT, param_kind::ZSTD_DICT),
            (meta_key::ACTIVE_TEMPLATE, param_kind::TEMPLATE),
        ] {
            let Some(bytes) = ops::get_meta(r, name)? else {
                continue;
            };
            let id = match format::decode_u64(&bytes) {
                Ok(id) => id,
                Err(e) => {
                    self.format_error(|| format!("meta {name}: {e}"));
                    continue;
                }
            };
            self.max_param_id = self.max_param_id.max(id);
            let reason = match self.params.get(&id) {
                None => Some("is missing"),
                Some(ParamStatus::Undecodable) => Some("is undecodable"),
                Some(ParamStatus::Kind(k)) if *k != kind => Some("has the wrong kind"),
                Some(ParamStatus::Kind(_)) => None,
            };
            if let Some(reason) = reason {
                self.param_problems
                    .entry(id)
                    .or_insert_with(|| ParamProblem {
                        reason: format!("{reason} (active in meta {name})"),
                        units: 0,
                    });
            }
        }
        Ok(())
    }

    fn scan_records<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        scan_all(r, Table::Records, &mut |k: &[u8], v: &[u8]| {
            self.rep.records_checked += 1;
            match Manifest::decode(v) {
                Ok(m) => self.check_manifest(r, &m, &|| format!("record {}", show_key(k)))?,
                Err(e) => self
                    .format_error(|| format!("record {}: undecodable manifest: {e}", show_key(k))),
            }
            Ok(true)
        })
    }

    fn scan_history<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        scan_all(r, Table::History, &mut |k: &[u8], v: &[u8]| {
            self.rep.history_checked += 1;
            let rev = match format::parse_history_key(k) {
                Ok((_, rev)) => rev,
                Err(e) => {
                    self.format_error(|| format!("{}: malformed key: {e}", history_name(k)));
                    return Ok(true);
                }
            };
            match Manifest::decode(v) {
                Ok(m) => {
                    if m.revision != rev {
                        let actual = m.revision;
                        self.format_error(|| {
                            format!("{}: manifest has revision {actual}", history_name(k))
                        });
                    }
                    self.check_manifest(r, &m, &|| history_name(k))?;
                }
                Err(e) => {
                    self.format_error(|| format!("{}: undecodable manifest: {e}", history_name(k)))
                }
            }
            Ok(true)
        })
    }

    fn scan_pending<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        scan_all(r, Table::PendingImports, &mut |k: &[u8], v: &[u8]| {
            self.rep.pending_imports += 1;
            let Ok(import_id) = format::parse_id_key(k) else {
                self.format_error(|| format!("pending_imports: malformed key {}", show_key(k)));
                return Ok(true);
            };
            self.max_import_id = self.max_import_id.max(import_id);
            match format::decode_id_list(v) {
                Ok(ids) => {
                    for (i, id) in ids.into_iter().enumerate() {
                        if id == 0 {
                            self.format_error(|| {
                                format!(
                                    "pending import {import_id}: entry {i} is the reserved id 0"
                                )
                            });
                        } else {
                            self.add_reference(
                                id,
                                LEN_UNKNOWN,
                                &|| format!("pending import {import_id}"),
                                i,
                            );
                        }
                    }
                }
                Err(e) => self.format_error(|| format!("pending import {import_id}: {e}")),
            }
            Ok(true)
        })
    }

    fn add_reference(&mut self, id: u64, len: u32, what: &dyn Fn() -> String, index: usize) {
        self.max_object_id = self.max_object_id.max(id);
        let e = self.expected.entry(id).or_insert(Expected {
            count: 0,
            len: LEN_UNKNOWN,
            flags: 0,
        });
        e.count = e.count.saturating_add(1);
        let mut conflict = None;
        if len != LEN_UNKNOWN {
            if e.len == LEN_UNKNOWN {
                e.len = len;
            } else if e.len != len {
                conflict = Some(e.len);
            }
        }
        if let Some(other) = conflict {
            self.format_error(|| {
                format!("{}: chunk {index} expects object {id} to hold {len} bytes, another chunk expects {other}", what())
            });
        }
    }

    fn check_manifest<R: ReadTxn + ?Sized>(
        &mut self,
        r: &R,
        m: &Manifest,
        what: &dyn Fn() -> String,
    ) -> Result<()> {
        self.max_revision = self.max_revision.max(m.revision);
        if let Some(sid) = m.source_id {
            if !self.sources.contains(&sid) {
                self.rep.dangling_sources += 1;
                self.issue(|| format!("{}: source {sid} is not registered", what()));
            }
            self.max_source_id = self.max_source_id.max(sid);
        }
        match &m.body {
            ManifestBody::Chunks(refs) => {
                let mut start = 0u64;
                for (i, c) in refs.iter().enumerate() {
                    let span = c.logical_end.saturating_sub(start);
                    start = c.logical_end;
                    let len = if span <= u64::from(MAX_UNIT_LEN) {
                        span as u32
                    } else {
                        self.format_error(|| {
                            format!("{}: chunk {i} spans {span} bytes (> MAX_UNIT_LEN)", what())
                        });
                        LEN_UNKNOWN
                    };
                    self.add_reference(c.object_id, len, what, i);
                }
            }
            ManifestBody::Inline(envelope) => self.check_inline(r, envelope, what)?,
            ManifestBody::Generated {
                generator_id,
                generator_version,
                params,
                digest,
            } => self.check_generated(
                m.logical_len,
                *generator_id,
                *generator_version,
                params,
                digest,
                what,
            ),
            ManifestBody::Tombstone => {}
        }
        Ok(())
    }

    fn check_inline<R: ReadTxn + ?Sized>(
        &mut self,
        r: &R,
        envelope: &[u8],
        what: &dyn Fn() -> String,
    ) -> Result<()> {
        let header = match format::read_envelope(envelope) {
            Ok((h, _)) => h,
            Err(e) => {
                self.format_error(|| format!("{}: inline envelope: {e}", what()));
                return Ok(());
            }
        };
        if !codec_known(header.codec) {
            let c = header.codec;
            self.format_error(|| {
                format!("{}: unknown codec 0x{:02x} v{}", what(), c.id, c.version)
            });
            return Ok(());
        }
        let usable = self.check_param(r, header.codec, header.aux_id)?;
        if self.rep.deep
            && usable
            && let Err(e) = ops::decode_envelope(envelope, &self.db.params, true, None)
        {
            self.rep.digest_failures += 1;
            self.issue(|| format!("{}: inline value fails verification: {e}", what()));
        }
        Ok(())
    }

    fn check_generated(
        &mut self,
        logical_len: u64,
        id: u16,
        version: u16,
        params: &[u8],
        digest: &Digest,
        what: &dyn Fn() -> String,
    ) {
        let db = self.db;
        let Ok(generator) = db.generators.get(id, version) else {
            self.rep.unknown_generators += 1;
            self.issue(|| format!("{}: generator {id} v{version} is not registered", what()));
            return;
        };
        match generator.output_len(params) {
            Ok(len) if len == logical_len => {}
            Ok(len) => {
                self.format_error(|| {
                    format!(
                        "{}: generator output is {len} bytes, manifest says {logical_len}",
                        what()
                    )
                });
                return;
            }
            Err(e) => {
                self.format_error(|| {
                    format!(
                        "{}: generator {} rejects its params: {e}",
                        what(),
                        generator.name()
                    )
                });
                return;
            }
        }
        if self.rep.deep {
            match regenerate_digest(generator, params, logical_len) {
                Ok(d) if d == *digest => {}
                Ok(_) => {
                    self.rep.digest_failures += 1;
                    self.issue(|| {
                        format!(
                            "{}: regenerated bytes do not match the stored digest",
                            what()
                        )
                    });
                }
                Err(e) => {
                    self.rep.digest_failures += 1;
                    self.issue(|| format!("{}: regeneration failed: {e}", what()));
                }
            }
        }
    }

    /// Whether the dependency of an envelope is available. Problems are
    /// aggregated per param id; deep mode also prepares the param.
    fn check_param<R: ReadTxn + ?Sized>(
        &mut self,
        r: &R,
        codec: CodecTag,
        aux_id: u64,
    ) -> Result<bool> {
        let Some(kind) = codec::required_param(codec, aux_id) else {
            return Ok(true);
        };
        self.max_param_id = self.max_param_id.max(aux_id);
        let mut problem = match self.params.get(&aux_id) {
            None => Some("is missing".to_string()),
            Some(ParamStatus::Undecodable) => Some("is undecodable".to_string()),
            Some(ParamStatus::Kind(k)) if *k != kind => Some(format!(
                "has kind {k} but {} needs kind {kind}",
                codec.name()
            )),
            Some(ParamStatus::Kind(_)) => None,
        };
        if problem.is_none() && self.rep.deep {
            match self.db.params.load(r, aux_id) {
                Ok(_) => {}
                Err(e @ (Error::Io(_) | Error::Backend(_))) => return Err(e),
                Err(e) => problem = Some(format!("cannot be prepared: {e}")),
            }
        }
        match problem {
            None => Ok(true),
            Some(reason) => {
                self.param_problems
                    .entry(aux_id)
                    .or_insert_with(|| ParamProblem { reason, units: 0 })
                    .units += 1;
                Ok(false)
            }
        }
    }

    fn scan_objects<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        scan_all(r, Table::Objects, &mut |k: &[u8], v: &[u8]| {
            self.rep.objects_checked += 1;
            let Ok(id) = format::parse_id_key(k) else {
                self.format_error(|| format!("objects: malformed key {}", show_key(k)));
                return Ok(true);
            };
            self.max_object_id = self.max_object_id.max(id);
            let expected_len = match self.expected.get_mut(&id) {
                Some(e) => {
                    e.flags |= SEEN_OBJECT;
                    Some(e.len)
                }
                None => None,
            };
            if expected_len.is_none() {
                self.rep.orphan_objects += 1;
            }
            let header = match format::read_envelope(v) {
                Ok((h, _)) => h,
                Err(e) => {
                    self.format_error(|| format!("object {id}: {e}"));
                    return Ok(true);
                }
            };
            if !codec_known(header.codec) {
                let c = header.codec;
                self.format_error(|| {
                    format!("object {id}: unknown codec 0x{:02x} v{}", c.id, c.version)
                });
                return Ok(true);
            }
            if let Some(len) = expected_len.filter(|l| *l != LEN_UNKNOWN && *l != header.raw_len) {
                let raw_len = header.raw_len;
                self.format_error(|| {
                    format!("object {id} holds {raw_len} bytes but its chunks expect {len}")
                });
            }
            let usable = self.check_param(r, header.codec, header.aux_id)?;
            if self.rep.deep && usable {
                match ops::decode_envelope(v, &self.db.params, true, Some(id)) {
                    Ok(_) => self.rep.objects_decoded += 1,
                    Err(e) => {
                        self.rep.digest_failures += 1;
                        self.issue(|| format!("object {id} fails verification: {e}"));
                    }
                }
            }
            Ok(true)
        })
    }

    fn scan_refcounts<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        scan_all(r, Table::Refcounts, &mut |k: &[u8], v: &[u8]| {
            let Ok(id) = format::parse_id_key(k) else {
                self.format_error(|| format!("refcounts: malformed key {}", show_key(k)));
                return Ok(true);
            };
            self.max_object_id = self.max_object_id.max(id);
            let stored = match format::decode_u64(v) {
                Ok(n) => Some(n),
                Err(e) => {
                    self.format_error(|| format!("refcount row of object {id}: {e}"));
                    None
                }
            };
            let referenced = match self.expected.get_mut(&id) {
                Some(e) => {
                    e.flags |= SEEN_ROW;
                    Some((e.count, e.flags & SEEN_OBJECT != 0))
                }
                None => None,
            };
            match referenced {
                // A missing object is reported once, as missing.
                Some((count, true)) => {
                    if let Some(stored) = stored.filter(|s| *s != count) {
                        self.rep.refcount_mismatches += 1;
                        self.issue(|| {
                            format!("object {id}: refcount {stored}, but {count} reference(s)")
                        });
                    }
                }
                Some((_, false)) => {}
                // Unreferenced objects were counted by the object scan.
                None => {
                    if r.get(Table::Objects, k)?.is_none() {
                        self.rep.orphan_objects += 1;
                    }
                }
            }
            Ok(true)
        })
    }

    /// Referenced objects that are missing or have no refcount row.
    fn check_references<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        let mut missing = HashSet::new();
        let mut no_row = Vec::new();
        for (&id, e) in &self.expected {
            if e.flags & SEEN_OBJECT == 0 {
                missing.insert(id);
            } else if e.flags & SEEN_ROW == 0 {
                no_row.push((id, e.count));
            }
        }
        no_row.sort_unstable();
        for (id, count) in no_row {
            self.rep.refcount_mismatches += 1;
            self.issue(|| format!("object {id}: no refcount row, but {count} reference(s)"));
        }
        self.rep.missing_objects = missing.len() as u64;
        if missing.is_empty() || self.rep.issues.len() >= MAX_ISSUES {
            return Ok(());
        }
        // Name the manifests and imports that lost data (one issue each).
        for table in [Table::Records, Table::History, Table::PendingImports] {
            scan_all(r, table, &mut |k: &[u8], v: &[u8]| {
                if self.rep.issues.len() >= MAX_ISSUES {
                    return Ok(false);
                }
                let ids = match table {
                    Table::PendingImports => format::decode_id_list(v).unwrap_or_default(),
                    _ => Manifest::decode(v)
                        .map(|m| m.object_ids())
                        .unwrap_or_default(),
                };
                let mut hits = ids.iter().filter(|id| missing.contains(*id));
                if let Some(&first) = hits.next() {
                    let n = 1 + hits.count();
                    let name = match table {
                        Table::Records => format!("record {}", show_key(k)),
                        Table::History => history_name(k),
                        _ => format!("pending import {}", show_key(k)),
                    };
                    self.issue(|| {
                        format!(
                            "{name}: {n} reference(s) to missing objects (first: object {first})"
                        )
                    });
                }
                Ok(true)
            })?;
        }
        Ok(())
    }

    fn scan_candidates<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        scan_all(r, Table::HashCandidates, &mut |k: &[u8], v: &[u8]| {
            if k.len() != 36 {
                self.rep.dangling_candidates += 1;
                self.issue(|| format!("hash_candidates: malformed key of {} bytes", k.len()));
                return Ok(true);
            }
            let name = || {
                let raw_len = u32::from_le_bytes([k[32], k[33], k[34], k[35]]);
                format!("candidate {}../{raw_len}", hash::to_hex(&k[..8]))
            };
            let ids = match format::decode_id_list(v) {
                Ok(ids) if !ids.is_empty() => ids,
                Ok(_) => {
                    self.rep.dangling_candidates += 1;
                    self.issue(|| format!("{}: empty id list", name()));
                    return Ok(true);
                }
                Err(e) => {
                    self.rep.dangling_candidates += 1;
                    self.issue(|| format!("{}: {e}", name()));
                    return Ok(true);
                }
            };
            for id in ids {
                // A stale id must still be below the counter (checked by
                // `check_counters`): otherwise a new object could get it.
                self.max_object_id = self.max_object_id.max(id);
                let problem = match candidate_status(r, k, id)? {
                    _ if id == 0 => "is the reserved id 0",
                    CandidateStatus::Live => continue,
                    CandidateStatus::Missing if !self.expected.contains_key(&id) => {
                        self.rep.stale_candidates += 1;
                        continue;
                    }
                    CandidateStatus::Missing => "is missing but still referenced",
                    CandidateStatus::Mismatch => "has another digest/length",
                };
                self.rep.dangling_candidates += 1;
                self.issue(|| format!("{}: object {id} {problem}", name()));
            }
            Ok(true)
        })
    }

    /// Every counter must be above the ids in use, or an allocation would
    /// hand out an id that already names something.
    fn check_counters<R: ReadTxn + ?Sized>(&mut self, r: &R) -> Result<()> {
        let counters = [
            (meta_key::NEXT_OBJECT_ID, self.max_object_id),
            (meta_key::NEXT_REVISION, self.max_revision),
            (meta_key::NEXT_PARAM_ID, self.max_param_id),
            (meta_key::NEXT_SOURCE_ID, self.max_source_id),
            (meta_key::NEXT_IMPORT_ID, self.max_import_id),
        ];
        for (name, max_used) in counters {
            match ops::get_meta(r, name)?.map(|b| format::decode_u64(&b)) {
                None => self.format_error(|| format!("meta {name} is missing")),
                Some(Err(e)) => self.format_error(|| format!("meta {name}: {e}")),
                Some(Ok(0)) => self.format_error(|| format!("meta {name} is 0 (ids start at 1)")),
                Some(Ok(next)) if next <= max_used => self.format_error(|| {
                    format!("meta {name} is {next} but id {max_used} is already in use")
                }),
                Some(Ok(_)) => {}
            }
        }
        Ok(())
    }

    fn finish(mut self) -> VerifyReport {
        for (id, p) in std::mem::take(&mut self.param_problems) {
            self.rep.missing_params += 1;
            self.issue(|| match p.units {
                0 => format!("param {id} {}", p.reason),
                n => format!("param {id} {}; needed by {n} unit(s)", p.reason),
            });
        }
        let orphans = self.rep.orphan_objects;
        if orphans > 0 {
            self.issue(|| {
                format!("{orphans} orphan object(s) without references (collectable by gc)")
            });
        }
        let pending = self.rep.pending_imports;
        if pending > 0 {
            self.issue(|| {
                format!(
                    "{pending} pending import(s): running now, or abandoned and collectable by gc"
                )
            });
        }
        self.rep
    }
}

impl<S: Store> Db<S> {
    /// Consistency check. `deep` also decodes every object and inline value
    /// (checking length and BLAKE3) and regenerates `Generated` records.
    ///
    /// Runs on one read snapshot, so it may run next to writers and imports:
    /// the references of a running import are pending and counted as such.
    pub fn verify(&self, deep: bool) -> Result<VerifyReport> {
        let r = self.store.begin_read()?;
        let mut v = Verifier::new(self, deep);
        v.scan_params(&r)?;
        v.scan_sources(&r)?;
        v.check_active_params(&r)?;
        v.scan_records(&r)?;
        v.scan_history(&r)?;
        v.scan_pending(&r)?;
        v.scan_objects(&r)?;
        v.scan_refcounts(&r)?;
        v.check_references(&r)?;
        v.scan_candidates(&r)?;
        v.check_counters(&r)?;
        Ok(v.finish())
    }

    /// Exclusive maintenance: abandoned imports, orphan objects/candidates/params,
    /// refcount drift.
    ///
    /// One durable transaction. The true reference counts are recomputed from
    /// records and history first, so a drifted refcount can never make gc
    /// delete an object that a manifest still references. If any manifest
    /// cannot be decoded, its references are unknown: gc then returns a
    /// `Format` error and changes nothing.
    pub fn gc(&mut self) -> Result<GcReport> {
        let mut rep = GcReport::default();
        let mut w = self.store.begin_write()?;

        // 1. True references: current records and retained history.
        let mut live: HashMap<u64, u64> = HashMap::new();
        let mut live_params: HashSet<u64> = HashSet::new();
        let mut undecodable: Option<String> = None;
        for table in [Table::Records, Table::History] {
            scan_all(
                &w,
                table,
                &mut |k: &[u8], v: &[u8]| match Manifest::decode(v) {
                    Ok(m) => {
                        for id in m.object_ids() {
                            let n = live.entry(id).or_insert(0);
                            *n = n.saturating_add(1);
                        }
                        if let ManifestBody::Inline(env) = &m.body
                            && let Ok((h, _)) = format::read_envelope(env)
                            && h.aux_id != 0
                        {
                            live_params.insert(h.aux_id);
                        }
                        Ok(true)
                    }
                    Err(e) => {
                        let name = if table == Table::Records {
                            format!("record {}", show_key(k))
                        } else {
                            history_name(k)
                        };
                        undecodable = Some(format!("{name}: {e}"));
                        Ok(false)
                    }
                },
            )?;
            if let Some(what) = undecodable.take() {
                return Err(Error::Format(format!(
                    "gc refused: undecodable manifest ({what}); its references are unknown, nothing was changed"
                )));
            }
        }

        // 2. Abandoned imports: drop the entries; their references disappear
        //    with the recount below.
        let mut released: HashMap<u64, u64> = HashMap::new();
        let mut pending_keys: Vec<Vec<u8>> = Vec::new();
        let mut undecodable_lists = 0u64;
        scan_all(&w, Table::PendingImports, &mut |k: &[u8], v: &[u8]| {
            pending_keys.push(k.to_vec());
            match format::decode_id_list(v) {
                Ok(ids) => {
                    for id in ids {
                        let n = released.entry(id).or_insert(0);
                        *n = n.saturating_add(1);
                    }
                }
                Err(_) => undecodable_lists += 1,
            }
            Ok(true)
        })?;
        for k in &pending_keys {
            w.remove(Table::PendingImports, k)?;
        }
        rep.abandoned_imports = pending_keys.len() as u64;
        if undecodable_lists > 0 {
            rep.notes.push(format!(
                "{undecodable_lists} pending import(s) had an undecodable id list; the recount released their references"
            ));
        }

        // 3. Refcount rows: remove unreferenced objects, rewrite drifted counts.
        let mut after: Option<Vec<u8>> = None;
        loop {
            let page = next_page(&w, Table::Refcounts, after.as_deref(), GC_PAGE, |v| {
                format::decode_u64(v).ok()
            })?;
            let Some(last) = page.last().map(|(k, _)| k.clone()) else {
                break;
            };
            for (k, stored) in page {
                let Ok(id) = format::parse_id_key(&k) else {
                    w.remove(Table::Refcounts, &k)?;
                    rep.refcounts_fixed += 1;
                    continue;
                };
                let target = live.get(&id).copied().unwrap_or(0);
                if target == 0 {
                    if remove_object_lenient(&mut w, id)? {
                        rep.objects_removed += 1;
                    } else {
                        rep.refcounts_fixed += 1;
                    }
                } else if stored != Some(target) {
                    w.put(Table::Refcounts, &k, &format::encode_u64(target))?;
                    let with_released = target.checked_add(released.get(&id).copied().unwrap_or(0));
                    if stored != with_released {
                        rep.refcounts_fixed += 1;
                    }
                }
            }
            after = Some(last);
        }

        // 4. Objects without a refcount row; collect the params still in use.
        let mut params_known = true;
        after = None;
        loop {
            let page = next_page(&w, Table::Objects, after.as_deref(), GC_PAGE, |v| {
                format::read_envelope(v).ok().map(|(h, _)| h.aux_id)
            })?;
            let Some(last) = page.last().map(|(k, _)| k.clone()) else {
                break;
            };
            for (k, aux_id) in page {
                let Ok(id) = format::parse_id_key(&k) else {
                    // Unreachable: object ids are 8-byte keys.
                    w.remove(Table::Objects, &k)?;
                    rep.objects_removed += 1;
                    continue;
                };
                if w.get(Table::Refcounts, &k)?.is_none() {
                    let target = live.get(&id).copied().unwrap_or(0);
                    if target == 0 {
                        remove_object_lenient(&mut w, id)?;
                        rep.objects_removed += 1;
                        continue;
                    }
                    w.put(Table::Refcounts, &k, &format::encode_u64(target))?;
                    rep.refcounts_fixed += 1;
                }
                match aux_id {
                    Some(0) => {}
                    Some(aux) => {
                        live_params.insert(aux);
                    }
                    None => params_known = false,
                }
            }
            after = Some(last);
        }

        // 5. Candidate lists: keep ids of existing objects with that digest/length.
        after = None;
        loop {
            let page = next_page(&w, Table::HashCandidates, after.as_deref(), GC_PAGE, |v| {
                v.to_vec()
            })?;
            let Some(last) = page.last().map(|(k, _)| k.clone()) else {
                break;
            };
            for (k, v) in page {
                let ids = if k.len() == 36 {
                    format::decode_id_list(&v).ok()
                } else {
                    None
                };
                let Some(ids) = ids.filter(|ids| !ids.is_empty()) else {
                    w.remove(Table::HashCandidates, &k)?;
                    rep.candidates_removed += 1;
                    continue;
                };
                let mut keep: Vec<u64> = Vec::with_capacity(ids.len());
                let mut stale = 0u64;
                for (i, &id) in ids.iter().enumerate() {
                    if ids[..i].contains(&id) {
                        continue; // duplicate
                    }
                    match candidate_status(&w, &k, id)? {
                        CandidateStatus::Live => keep.push(id),
                        CandidateStatus::Missing if id != 0 && !live.contains_key(&id) => stale += 1,
                        _ => {}
                    }
                }
                if keep.len() != ids.len() {
                    rep.stale_candidates_removed += stale;
                    rep.candidates_removed += (ids.len() - keep.len()) as u64 - stale;
                    if keep.is_empty() {
                        w.remove(Table::HashCandidates, &k)?;
                    } else {
                        w.put(Table::HashCandidates, &k, &format::encode_id_list(&keep))?;
                    }
                }
            }
            after = Some(last);
        }

        // 6. Params referenced by no envelope aux_id and not active in meta.
        //    Any non-zero aux_id counts as a reference (conservative for
        //    codecs this build does not know).
        for name in [meta_key::ACTIVE_ZSTD_DICT, meta_key::ACTIVE_TEMPLATE] {
            if let Some(b) = ops::get_meta(&w, name)? {
                match format::decode_u64(&b) {
                    Ok(id) => {
                        live_params.insert(id);
                    }
                    Err(_) => params_known = false,
                }
            }
        }
        let mut removed_params = Vec::new();
        if params_known {
            let mut unused: Vec<Vec<u8>> = Vec::new();
            scan_all(&w, Table::Params, &mut |k: &[u8], _v: &[u8]| {
                match format::parse_id_key(k) {
                    Ok(id) if live_params.contains(&id) => {}
                    _ => unused.push(k.to_vec()),
                }
                Ok(true)
            })?;
            for k in unused {
                w.remove(Table::Params, &k)?;
                rep.params_removed += 1;
                if let Ok(id) = format::parse_id_key(&k) {
                    removed_params.push(id);
                }
            }
        } else {
            rep.notes.push(
                "params kept: an object envelope or an active param id is undecodable".into(),
            );
        }

        w.commit(Durability::Immediate)?;
        self.counters.commits.fetch_add(1, Ordering::Relaxed);
        for id in removed_params {
            self.params.remove(id);
        }
        self.cache.clear();
        Ok(rep)
    }

    /// Exclusive: ask the backend to return free space to the file system.
    /// Sizes are measured on `Store::files` before and after.
    pub fn compact(&mut self) -> Result<CompactReport> {
        let (apparent_before, allocated_before) = self.file_usage()?;
        let supported = self.store.compact()?;
        let (apparent_after, allocated_after) = self.file_usage()?;
        Ok(CompactReport {
            supported,
            apparent_before,
            apparent_after,
            allocated_before,
            allocated_after,
        })
    }

    fn file_usage(&self) -> Result<(u64, Option<u64>)> {
        let mut apparent = 0u64;
        let mut allocated = Some(0u64);
        for path in self.store.files() {
            let size = crate::sys::file_size(&path)?;
            apparent += size.apparent_bytes;
            allocated = allocated.zip(size.allocated_bytes).map(|(a, b)| a + b);
        }
        Ok((apparent, allocated))
    }

    /// Train a Zstd dictionary from samples, evaluate it on held-out samples and
    /// install it as the active dictionary only if the projected net gain is positive.
    ///
    /// Only the Adaptive planner uses dictionaries: in BabelPure mode this
    /// returns `Error::Unsupported` without training.
    pub fn train_dictionary(
        &self,
        samples: &[Vec<u8>],
        opts: &TrainOptions,
    ) -> Result<TrainReport> {
        self.require_adaptive("Zstd dictionaries")?;
        let (bytes, report) =
            planner::train_zstd_dictionary(samples, self.cfg.codecs.zstd_level, opts)?;
        self.finish_training(param_kind::ZSTD_DICT, bytes, report)
    }

    /// Same for a `TemplatePatchV1` template (`Error::Unsupported` in BabelPure mode).
    pub fn train_template(&self, samples: &[Vec<u8>], opts: &TrainOptions) -> Result<TrainReport> {
        self.require_adaptive("templates")?;
        let (bytes, report) = planner::train_template(samples, opts)?;
        self.finish_training(param_kind::TEMPLATE, bytes, report)
    }

    fn require_adaptive(&self, what: &str) -> Result<()> {
        match self.mode {
            Mode::Adaptive => Ok(()),
            mode => Err(Error::Unsupported(format!(
                "{what} are only used by the Adaptive planner; this database is {}",
                mode.as_str()
            ))),
        }
    }

    fn finish_training(
        &self,
        kind: u8,
        bytes: Option<Vec<u8>>,
        mut report: TrainReport,
    ) -> Result<TrainReport> {
        report.installed = false;
        report.param_id = None;
        if let Some(bytes) = bytes {
            report.param_id = Some(self.install_param(kind, bytes)?);
            report.installed = true;
        }
        Ok(report)
    }

    /// Store a new immutable param, make it the active one of its kind
    /// (durably) and switch the planner to it, keeping the other active
    /// dependency. Earlier params stay as long as objects reference them.
    /// Also used by the automatic dictionary (`engine::autodict`).
    pub(crate) fn install_param(&self, kind: u8, bytes: Vec<u8>) -> Result<u64> {
        let _serial = INSTALL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let param = Param {
            kind,
            version: 1,
            bytes,
        };
        let encoded = param.encode();
        let active_key = match kind {
            param_kind::ZSTD_DICT => meta_key::ACTIVE_ZSTD_DICT,
            _ => meta_key::ACTIVE_TEMPLATE,
        };
        let mut w = self.store.begin_write()?;
        let id = ops::alloc_id(&mut w, meta_key::NEXT_PARAM_ID, "param id")?;
        // Prepared before the commit: a dependency that cannot be prepared is never installed.
        let entry = self.params.prepare(id, param)?;
        w.put(Table::Params, &id_key(id), &encoded)?;
        ops::put_meta_u64(&mut w, active_key, id)?;
        w.commit(Durability::Immediate)?;
        self.counters.commits.fetch_add(1, Ordering::Relaxed);
        self.params.insert(id, entry.clone());

        // Installs are serialized, so the current planner holds the other
        // active dependency.
        let current = self.planner();
        let (dict, template) = match entry {
            ParamEntry::Dict(d) => (Some(d), current.template().cloned()),
            ParamEntry::Template(t) => (current.dictionary().cloned(), Some(t)),
        };
        self.replace_planner(
            Planner::new(self.mode, self.cfg.codecs.clone()).with_params(dict, template),
        );
        Ok(id)
    }
}
