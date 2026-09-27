//! Read path: key -> manifest (one lookup) -> units intersecting the range ->
//! decode -> verify -> bytes. Blocks come from the cache when possible; the
//! envelopes of the others are copied out and the snapshot is released before
//! they are decoded (up to `DETACHED_ENVELOPE_BYTES`; beyond that, blocks are
//! decoded while the snapshot is still held so memory stays bounded).
//!
//! Inline records (the common case for small values) are parsed once, in
//! place (`inline_view`): no copy of the envelope, one header parse, one
//! dependency lookup. A `RawV1` value is verified in the record buffer and
//! returned in it (the only copy is the backend's); other codecs decode into
//! one exactly sized buffer. Verified inline values of the other codecs are
//! offered to the value namespace of the block cache, keyed by the revision
//! of their manifest (unique and immutable); from their second recent read on
//! they are kept, so
//! repeated reads skip decoding and hashing while one-off reads (uniform
//! access over a large dataset) cost no copy and evict nothing. Scans with
//! values resolve inline records inside the backend scan, from the borrowed
//! record bytes.

use std::ops::{Bound, Range};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::ops::{self, ParamEntry};
use super::{Db, HistoryEntry, Revision, ScanItem, ScanOptions, prefix_successor};
use crate::chunk;
use crate::codec;
use crate::error::{Error, Result};
use crate::format::{self, ChunkRef, CodecTag, EnvelopeHeader, MAX_UNIT_LEN, Manifest, ManifestBody, SourceDescriptor};
use crate::hash::{self, Digest};
use crate::store::{ReadTxn, Store, Table};

/// Envelope bytes held outside the snapshot before decoding starts in place.
const DETACHED_ENVELOPE_BYTES: usize = 8 << 20;

/// Scans with values and a `limit` of at most this many items keep the inline
/// values they decode in the value cache (larger or unlimited scans only read
/// it, so a bulk export does not flush the hot set).
pub const VALUE_CACHE_SCAN_LIMIT: usize = 1024;

/// Requested bytes of a value, clamped to its length.
#[derive(Clone, Copy, Debug)]
struct Span {
    offset: u64,
    len: u64,
    /// The whole value (digests of generated values are only checked then).
    whole: bool,
}

impl Span {
    fn whole(logical_len: u64) -> Span {
        Span { offset: 0, len: logical_len, whole: true }
    }

    fn range(logical_len: u64, offset: u64, len: u64) -> Result<Span> {
        if offset > logical_len {
            return Err(Error::InvalidArgument(format!(
                "offset {offset} is beyond the value length {logical_len}"
            )));
        }
        let len = len.min(logical_len - offset);
        Ok(Span { offset, len, whole: offset == 0 && len == logical_len })
    }

    /// `range = None` is the whole value.
    fn of(logical_len: u64, range: Option<(u64, u64)>) -> Result<Span> {
        match range {
            None => Ok(Span::whole(logical_len)),
            Some((offset, len)) => Span::range(logical_len, offset, len),
        }
    }

    /// Nothing to decode: an empty part of a value (whole empty values are
    /// still decoded, so their envelope is checked).
    fn is_empty_part(&self) -> bool {
        self.len == 0 && !self.whole
    }

    /// The span as a range of an inline value (at most `u32::MAX` bytes).
    fn bytes(&self) -> Range<usize> {
        self.offset as usize..(self.offset + self.len) as usize
    }
}

/// An Inline manifest located inside the record bytes, without copying its
/// envelope out (the common case for small values).
struct InlineView {
    revision: u64,
    logical_len: u64,
    /// Parsed and validated envelope header.
    header: EnvelopeHeader,
    /// The envelope body inside the record bytes.
    body: Range<usize>,
}

/// The checks of `Manifest::decode` for the Inline kind (format v1: version,
/// kind 0, flags bit0 = has_source, canonical varint length, envelope ending
/// the record, `raw_len == logical_len`). `None` for any other kind or any
/// deviation: the caller then decodes the manifest, which reports the error.
fn inline_view(raw: &[u8]) -> Option<InlineView> {
    const KIND_INLINE: u8 = 0;
    const FLAG_HAS_SOURCE: u8 = 1;
    let mut pos = 0usize;
    if format::take_u8(raw, &mut pos).ok()? != format::MANIFEST_VERSION {
        return None;
    }
    let revision = format::take_u64(raw, &mut pos).ok()?;
    let logical_len = format::take_u64(raw, &mut pos).ok()?;
    if format::take_u8(raw, &mut pos).ok()? != KIND_INLINE {
        return None;
    }
    match format::take_u8(raw, &mut pos).ok()? {
        0 => {}
        FLAG_HAS_SOURCE => {
            format::take_u64(raw, &mut pos).ok()?;
        }
        _ => return None,
    }
    let len = usize::try_from(format::get_varint(raw, &mut pos).ok()?).ok()?;
    let end = pos.checked_add(len)?;
    if end != raw.len() {
        return None;
    }
    let (header, _) = format::read_envelope(&raw[pos..end]).ok()?;
    if u64::from(header.raw_len) != logical_len {
        return None;
    }
    Some(InlineView { revision, logical_len, header, body: pos + format::ENVELOPE_HEADER_LEN..end })
}

/// Revision of a valid Inline manifest; None for other kinds and any deviation.
pub(super) fn inline_revision(raw: &[u8]) -> Option<u64> {
    inline_view(raw).map(|view| view.revision)
}

/// (revision, logical_len, tombstone) of a manifest, fully validated, without
/// copying an inline envelope.
fn manifest_head(raw: &[u8]) -> Result<(u64, u64, bool)> {
    match inline_view(raw) {
        Some(view) => Ok((view.revision, view.logical_len, false)),
        None => {
            let m = Manifest::decode(raw)?;
            Ok((m.revision, m.logical_len, m.is_tombstone()))
        }
    }
}

/// A record read from `records` or `history`.
enum Record {
    Inline { raw: Vec<u8>, view: InlineView },
    Other(Manifest),
}

impl Record {
    fn parse(raw: Vec<u8>) -> Result<Record> {
        match inline_view(&raw) {
            Some(view) => Ok(Record::Inline { raw, view }),
            None => Ok(Record::Other(Manifest::decode(&raw)?)),
        }
    }

    fn revision(&self) -> u64 {
        match self {
            Record::Inline { view, .. } => view.revision,
            Record::Other(m) => m.revision,
        }
    }
}

/// Decode work of one read, added to the counters once.
#[derive(Default)]
struct Work {
    units: u64,
    bytes: u64,
}

/// What a read took from the snapshot; decoded by `Db::finish`.
enum Pending {
    /// Inline envelope (its dependency is loaded).
    Inline { revision: u64, envelope: Vec<u8> },
    Chunks(ChunkRead),
    Generated { generator_id: u16, generator_version: u16, params: Vec<u8>, digest: Digest, logical_len: u64 },
    Empty,
}

enum Piece {
    Cached { block: Arc<[u8]>, range: Range<usize> },
    Envelope { object_id: u64, envelope: Vec<u8>, range: Range<usize> },
}

/// Blocks of a chunked read, in order; `out` holds what is already assembled.
#[derive(Default)]
struct ChunkRead {
    out: Vec<u8>,
    pieces: Vec<Piece>,
    pending_bytes: usize,
    scratch: Vec<u8>,
    work: Work,
}

/// Output buffer sized from persisted lengths: failure is an error, not an abort.
fn alloc_output(len: u64) -> Result<Vec<u8>> {
    let too_large = || Error::LimitExceeded(format!("cannot allocate {len} bytes for a value"));
    let n = usize::try_from(len).map_err(|_| too_large())?;
    let mut out = Vec::new();
    out.try_reserve_exact(n).map_err(|_| too_large())?;
    Ok(out)
}

/// The requested part of a decoded unit, reusing its buffer when it is all of it.
fn slice_owned(unit: Vec<u8>, range: Range<usize>, object_id: Option<u64>) -> Result<Vec<u8>> {
    if range.start == 0 && range.end == unit.len() {
        return Ok(unit);
    }
    unit.get(range)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| Error::integrity(object_id, "decoded unit is shorter than its manifest entry"))
}

/// A deferred item of a scan with values, resolved after the backend scan.
enum Deferred {
    /// Inline record whose dependency is not prepared yet.
    Inline { revision: u64, header: EnvelopeHeader, body: Vec<u8> },
    Manifest(Manifest),
}

impl<S: Store> Db<S> {
    /// Whether verified inline values go through the value cache.
    fn value_cache_on(&self) -> bool {
        self.cfg.cache_values && self.cfg.verify_on_read && self.cfg.cache_bytes > 0
    }

    /// Whether the value of an inline record with header `h` may be cached:
    /// `RawV1` values are not (a hit would only save the digest check, and
    /// the bytes would be held twice, next to the backend's page cache).
    fn caches(h: &EnvelopeHeader) -> bool {
        h.codec != CodecTag::RAW_V1
    }

    /// The whole cached value of an inline record, if the value cache has it.
    fn cached_inline(&self, view: &InlineView) -> Option<Arc<[u8]>> {
        if !self.value_cache_on() || !Self::caches(&view.header) {
            return None;
        }
        // A cached value always has the manifest's length; anything else is
        // never served (the record is decoded instead).
        self.cache.get_value(view.revision).filter(|v| v.len() as u64 == view.logical_len)
    }

    /// Offer a verified value to the cache (stored on its second recent offer).
    fn remember_inline(&self, revision: u64, header: &EnvelopeHeader, value: &[u8], cache: bool) {
        if cache && Self::caches(header) {
            self.cache.offer_value(revision, value);
        }
    }

    /// Decode (and verify, per `verify_on_read`) the whole value of an inline
    /// record from its envelope body, borrowed from the record bytes.
    fn decode_inline_body(
        &self,
        revision: u64,
        header: &EnvelopeHeader,
        body: &[u8],
        entry: Option<&ParamEntry>,
        cache: bool,
        work: &mut Work,
    ) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        ops::decode_body_into(header, body, entry, self.cfg.verify_on_read, None, &mut out)?;
        work.units += 1;
        work.bytes += out.len() as u64;
        self.remember_inline(revision, header, &out, cache);
        Ok(out)
    }

    /// The requested span of an inline record whose bytes (`raw`) the caller
    /// owns. A `RawV1` value is verified where it lies and moved to the front
    /// of `raw`, which becomes the result: no second buffer.
    fn finish_inline_record(
        &self,
        mut raw: Vec<u8>,
        view: &InlineView,
        entry: Option<ParamEntry>,
        span: Span,
        work: &mut Work,
    ) -> Result<Vec<u8>> {
        let h = &view.header;
        if h.codec != CodecTag::RAW_V1 {
            let cache = self.value_cache_on();
            let value = self.decode_inline_body(view.revision, h, &raw[view.body.clone()], entry.as_ref(), cache, work)?;
            return slice_owned(value, span.bytes(), None);
        }
        // `codec::decode` of RawV1 followed by the digest check, in place.
        let body = &raw[view.body.clone()];
        if body.len() != h.raw_len as usize {
            return Err(Error::format("RawV1 body length != raw_len"));
        }
        if self.cfg.verify_on_read && hash::digest(body) != h.digest {
            return Err(Error::integrity(None, "BLAKE3 digest mismatch"));
        }
        work.units += 1;
        work.bytes += body.len() as u64;
        let part = span.bytes();
        let start = view.body.start + part.start;
        let len = part.len();
        raw.copy_within(start..start + len, 0);
        raw.truncate(len);
        Ok(raw)
    }

    /// The requested span of an inline record, from the value cache or decoded.
    /// The snapshot `t` is only used to load a dependency that is not prepared.
    fn read_inline<T: ReadTxn>(&self, t: T, raw: Vec<u8>, view: &InlineView, span: Span) -> Result<Vec<u8>> {
        let mut work = Work::default();
        let value = match self.cached_inline(view) {
            Some(v) => {
                drop(t);
                v[span.bytes()].to_vec()
            }
            None => {
                let entry = self.params.dependency(&t, &view.header)?;
                drop(t);
                self.finish_inline_record(raw, view, entry, span, &mut work)?
            }
        };
        self.note_read(&work, value.len());
        Ok(value)
    }

    /// Read (part of) the value of `m`, releasing the snapshot before decoding.
    fn read_detached<T: ReadTxn>(&self, t: T, m: Manifest, range: Option<(u64, u64)>) -> Result<Vec<u8>> {
        let span = Span::of(m.logical_len, range)?;
        if span.is_empty_part() {
            return Ok(Vec::new());
        }
        let pending = self.fetch(&t, m, span)?;
        drop(t);
        self.finish(pending, span)
    }

    /// Phase 1 (snapshot held): load decoding dependencies and copy the
    /// envelopes of blocks that are not cached.
    fn fetch<T: ReadTxn + ?Sized>(&self, t: &T, m: Manifest, span: Span) -> Result<Pending> {
        Ok(match m.body {
            ManifestBody::Inline(envelope) => {
                self.params.ensure_for_envelope(t, &envelope)?;
                Pending::Inline { revision: m.revision, envelope }
            }
            ManifestBody::Chunks(refs) => Pending::Chunks(self.fetch_chunks(t, &refs, span)?),
            ManifestBody::Generated { generator_id, generator_version, params, digest } => {
                Pending::Generated { generator_id, generator_version, params, digest, logical_len: m.logical_len }
            }
            ManifestBody::Tombstone => Pending::Empty,
        })
    }

    /// Phase 2 (snapshot may be gone): decode, verify, cache, slice.
    fn finish(&self, pending: Pending, span: Span) -> Result<Vec<u8>> {
        let mut work = Work::default();
        let value = match pending {
            Pending::Inline { revision, envelope } => {
                let (h, body) = format::read_envelope(&envelope)?;
                let entry = match codec::required_param(h.codec, h.aux_id) {
                    Some(_) => Some(self.params.get(h.aux_id).ok_or(Error::MissingDependency { param_id: h.aux_id })?),
                    None => None,
                };
                let value = self.decode_inline_body(revision, &h, body, entry.as_ref(), self.value_cache_on(), &mut work)?;
                slice_owned(value, span.bytes(), None)?
            }
            Pending::Chunks(read) => {
                let (value, chunk_work) = self.finish_chunks(read, span.len)?;
                work = chunk_work;
                value
            }
            Pending::Generated { generator_id, generator_version, params, digest, logical_len } => {
                let generator = self.generators.get(generator_id, generator_version)?;
                let len = generator.output_len(&params)?;
                if len != logical_len {
                    return Err(Error::integrity(
                        None,
                        format!(
                            "generator {generator_id} v{generator_version} yields {len} bytes, the manifest says {logical_len}"
                        ),
                    ));
                }
                let mut out = alloc_output(span.len)?;
                out.resize(span.len as usize, 0);
                generator.generate(&params, span.offset, &mut out)?;
                work.units = 1;
                work.bytes = span.len;
                // Range reads of generated values are not digest-verified.
                if span.whole && self.cfg.verify_on_read && hash::digest(&out) != digest {
                    return Err(Error::integrity(None, "generated value does not match its digest"));
                }
                out
            }
            Pending::Empty => Vec::new(),
        };
        self.note_read(&work, value.len());
        Ok(value)
    }

    fn fetch_chunks<T: ReadTxn + ?Sized>(&self, t: &T, refs: &[ChunkRef], span: Span) -> Result<ChunkRead> {
        let mut read = ChunkRead::default();
        let Some((first, last)) = chunk::locate(refs, span.offset, span.len) else {
            return Ok(read);
        };
        let end = span.offset + span.len;
        read.pieces.reserve(last - first + 1);
        for idx in first..=last {
            let object_id = refs[idx].object_id;
            let start = chunk::chunk_start(refs, idx);
            let chunk_end = refs[idx].logical_end;
            let chunk_len = chunk_end - start;
            if chunk_len > u64::from(MAX_UNIT_LEN) {
                return Err(Error::integrity(
                    Some(object_id),
                    format!("chunk of {chunk_len} bytes exceeds MAX_UNIT_LEN"),
                ));
            }
            let range = (span.offset.max(start) - start) as usize..(end.min(chunk_end) - start) as usize;
            if let Some(block) = self.cache.get(object_id) {
                if block.len() as u64 != chunk_len {
                    return Err(Error::integrity(Some(object_id), "cached block length differs from the manifest"));
                }
                read.pieces.push(Piece::Cached { block, range });
                continue;
            }
            let envelope = t
                .get(Table::Objects, &format::id_key(object_id))?
                .ok_or_else(|| Error::integrity(Some(object_id), "referenced object is missing"))?;
            let (header, _) = format::read_envelope(&envelope)?;
            if u64::from(header.raw_len) != chunk_len {
                return Err(Error::integrity(
                    Some(object_id),
                    format!("object holds {} bytes, the manifest chunk {chunk_len}", header.raw_len),
                ));
            }
            if codec::required_param(header.codec, header.aux_id).is_some() {
                self.params.load(t, header.aux_id)?;
            }
            read.pending_bytes += envelope.len();
            read.pieces.push(Piece::Envelope { object_id, envelope, range });
            if read.pending_bytes > DETACHED_ENVELOPE_BYTES {
                self.drain_pieces(&mut read, span.len)?;
            }
        }
        Ok(read)
    }

    /// Decode the copied envelopes and append every piece to `out`, in order.
    fn drain_pieces(&self, read: &mut ChunkRead, total: u64) -> Result<()> {
        if read.out.capacity() == 0 {
            read.out = alloc_output(total)?;
        }
        for piece in read.pieces.drain(..) {
            match piece {
                Piece::Cached { block, range } => read.out.extend_from_slice(&block[range]),
                Piece::Envelope { object_id, envelope, range } => {
                    self.decode_block(object_id, &envelope, &mut read.scratch, &mut read.work)?;
                    read.out.extend_from_slice(&read.scratch[range]);
                }
            }
        }
        read.pending_bytes = 0;
        Ok(())
    }

    fn finish_chunks(&self, mut read: ChunkRead, total: u64) -> Result<(Vec<u8>, Work)> {
        // One block and nothing assembled yet: return its buffer without a copy.
        if read.out.capacity() == 0 && read.pieces.len() == 1 {
            match read.pieces.pop() {
                Some(Piece::Envelope { object_id, envelope, range }) => {
                    let mut block = Vec::new();
                    self.decode_block(object_id, &envelope, &mut block, &mut read.work)?;
                    return Ok((slice_owned(block, range, Some(object_id))?, read.work));
                }
                Some(Piece::Cached { block, range }) => return Ok((block[range].to_vec(), read.work)),
                None => {}
            }
        }
        self.drain_pieces(&mut read, total)?;
        Ok((read.out, read.work))
    }

    /// Decode one object into `buf` (exactly its `raw_len` bytes, verified per
    /// `verify_on_read`) and offer it to the cache.
    fn decode_block(&self, object_id: u64, envelope: &[u8], buf: &mut Vec<u8>, work: &mut Work) -> Result<()> {
        ops::decode_envelope_into(envelope, &self.params, self.cfg.verify_on_read, Some(object_id), buf)?;
        work.units += 1;
        work.bytes += buf.len() as u64;
        if self.cfg.cache_bytes > 0 {
            self.cache.insert(object_id, Arc::from(buf.as_slice()));
        }
        Ok(())
    }

    fn note_read(&self, work: &Work, returned: usize) {
        let c = &self.counters;
        c.bytes_requested.fetch_add(returned as u64, Ordering::Relaxed);
        if work.units > 0 {
            c.units_decoded.fetch_add(work.units, Ordering::Relaxed);
            c.bytes_reconstructed.fetch_add(work.bytes, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------
// Entry points (called by the public methods in `engine/mod.rs`)
// ---------------------------------------------------------------------------

/// Current value (or part of it) with its revision; tombstones read as None.
pub(super) fn get_value<S: Store>(
    db: &Db<S>,
    key: &[u8],
    range: Option<(u64, u64)>,
) -> Result<Option<(Revision, Vec<u8>)>> {
    db.counters.gets.fetch_add(1, Ordering::Relaxed);
    let r = db.store.begin_read()?;
    let Some(raw) = r.get(Table::Records, key)? else {
        return Ok(None);
    };
    // Small values: decode straight from the record bytes.
    if let Some(view) = inline_view(&raw) {
        let span = Span::of(view.logical_len, range)?;
        if span.is_empty_part() {
            return Ok(Some((view.revision, Vec::new())));
        }
        let revision = view.revision;
        return Ok(Some((revision, db.read_inline(r, raw, &view, span)?)));
    }
    let m = Manifest::decode(&raw)?;
    drop(raw);
    if m.is_tombstone() {
        return Ok(None);
    }
    let revision = m.revision;
    Ok(Some((revision, db.read_detached(r, m, range)?)))
}

pub(super) fn head<S: Store>(db: &Db<S>, key: &[u8]) -> Result<Option<(Revision, u64)>> {
    let r = db.store.begin_read()?;
    let Some(raw) = r.get(Table::Records, key)? else {
        return Ok(None);
    };
    drop(r);
    let (revision, logical_len, tombstone) = manifest_head(&raw)?;
    Ok((!tombstone).then_some((revision, logical_len)))
}

pub(super) fn get_at<S: Store>(db: &Db<S>, key: &[u8], revision: Revision) -> Result<Option<Vec<u8>>> {
    db.counters.gets.fetch_add(1, Ordering::Relaxed);
    let r = db.store.begin_read()?;
    let current = r.get(Table::Records, key)?.map(Record::parse).transpose()?;
    let record = match current {
        Some(rec) if rec.revision() == revision => rec,
        _ => match r.get(Table::History, &format::history_key(key, revision))? {
            Some(raw) => {
                let rec = Record::parse(raw)?;
                if rec.revision() != revision {
                    return Err(Error::format("history entry revision differs from its key"));
                }
                rec
            }
            None => return Ok(None),
        },
    };
    match record {
        Record::Inline { raw, view } => {
            let span = Span::whole(view.logical_len);
            db.read_inline(r, raw, &view, span).map(Some)
        }
        Record::Other(m) if m.is_tombstone() => Ok(None),
        Record::Other(m) => db.read_detached(r, m, None).map(Some),
    }
}

/// Bounds that cannot select anything (start after end, or equal with one side
/// excluded). Backends are not asked to scan them: ordered-map range APIs may
/// panic on such bounds.
fn range_is_empty(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(s), Bound::Included(e)) => s > e,
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => s >= e,
        _ => false,
    }
}

fn slice_bound(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    b.as_ref().map(Vec::as_slice)
}

pub(super) fn scan<S: Store>(db: &Db<S>, opts: &ScanOptions) -> Result<Vec<ScanItem>> {
    if range_is_empty(&opts.start, &opts.end) {
        return Ok(Vec::new());
    }
    let r = db.store.begin_read()?;
    let full = |n: usize| opts.limit != 0 && n >= opts.limit;
    let (start, end) = (slice_bound(&opts.start), slice_bound(&opts.end));
    let mut items: Vec<ScanItem> = Vec::new();
    if !opts.with_values {
        r.scan(Table::Records, start, end, opts.reverse, &mut |k: &[u8], v: &[u8]| {
            let (revision, logical_len, tombstone) = manifest_head(v)?;
            if !tombstone {
                items.push(ScanItem { key: k.to_vec(), revision, logical_len, value: None });
            }
            Ok(!full(items.len()))
        })?;
        return Ok(items);
    }
    // The backend scan stops at the limit. Inline values are resolved inside
    // it, from the borrowed record bytes, when their dependency is prepared;
    // the other records are resolved afterwards with the same snapshot.
    let cache = db.value_cache_on() && opts.limit != 0 && opts.limit <= VALUE_CACHE_SCAN_LIMIT;
    let mut deferred: Vec<(usize, Deferred)> = Vec::new();
    let mut work = Work::default();
    let mut inline_bytes = 0usize;
    r.scan(Table::Records, start, end, opts.reverse, &mut |k: &[u8], v: &[u8]| {
        let Some(view) = inline_view(v) else {
            let m = Manifest::decode(v)?;
            if !m.is_tombstone() {
                let (revision, logical_len) = (m.revision, m.logical_len);
                deferred.push((items.len(), Deferred::Manifest(m)));
                items.push(ScanItem { key: k.to_vec(), revision, logical_len, value: None });
            }
            return Ok(!full(items.len()));
        };
        let value = match db.cached_inline(&view) {
            Some(value) => Some(value.to_vec()),
            None => match db.params.cached_dependency(&view.header) {
                Ok(entry) => Some(db.decode_inline_body(
                    view.revision,
                    &view.header,
                    &v[view.body.clone()],
                    entry.as_ref(),
                    cache,
                    &mut work,
                )?),
                Err(()) => {
                    let body = v[view.body.clone()].to_vec();
                    deferred.push((items.len(), Deferred::Inline { revision: view.revision, header: view.header.clone(), body }));
                    None
                }
            },
        };
        inline_bytes += value.as_ref().map_or(0, Vec::len);
        items.push(ScanItem { key: k.to_vec(), revision: view.revision, logical_len: view.logical_len, value });
        Ok(!full(items.len()))
    })?;
    for (idx, item) in deferred {
        let value = match item {
            Deferred::Inline { revision, header, body } => {
                let entry = db.params.dependency(&r, &header)?;
                let value = db.decode_inline_body(revision, &header, &body, entry.as_ref(), cache, &mut work)?;
                inline_bytes += value.len();
                value
            }
            Deferred::Manifest(m) => {
                let span = Span::whole(m.logical_len);
                let pending = db.fetch(&r, m, span)?;
                db.finish(pending, span)?
            }
        };
        items[idx].value = Some(value);
    }
    if inline_bytes > 0 || work.units > 0 {
        db.note_read(&work, inline_bytes);
    }
    Ok(items)
}

fn history_entry(revision: u64, logical_len: u64, tombstone: bool, current: bool) -> HistoryEntry {
    HistoryEntry { revision, logical_len, tombstone, current }
}

pub(super) fn history<S: Store>(db: &Db<S>, key: &[u8]) -> Result<Vec<HistoryEntry>> {
    let r = db.store.begin_read()?;
    let prefix = format::history_prefix(key);
    let end_key = prefix_successor(&prefix);
    let end = end_key.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
    let mut entries = Vec::new();
    r.scan(Table::History, Bound::Included(&prefix), end, false, &mut |k: &[u8], v: &[u8]| {
        let (_, revision) = format::parse_history_key(k)?;
        let (rev, logical_len, tombstone) = manifest_head(v)?;
        if rev != revision {
            return Err(Error::format("history entry revision differs from its key"));
        }
        entries.push(history_entry(rev, logical_len, tombstone, false));
        Ok(true)
    })?;
    if let Some(raw) = r.get(Table::Records, key)? {
        let (rev, logical_len, tombstone) = manifest_head(&raw)?;
        entries.push(history_entry(rev, logical_len, tombstone, true));
    }
    Ok(entries)
}

pub(super) fn sources<S: Store>(db: &Db<S>) -> Result<Vec<(u64, SourceDescriptor)>> {
    let r = db.store.begin_read()?;
    let mut out = Vec::new();
    r.scan(Table::Sources, Bound::Unbounded, Bound::Unbounded, false, &mut |k: &[u8], v: &[u8]| {
        out.push((format::parse_id_key(k)?, SourceDescriptor::decode(v)?));
        Ok(true)
    })?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::engine::Expect;
    use crate::format::CodecTag;
    use crate::store::mem::MemStore;
    use crate::store::{Durability, WriteTxn};

    fn mem_db(cfg: Config) -> Db<MemStore> {
        Db::with_store(MemStore::new(), cfg).unwrap()
    }

    /// Compressible text of `n` bytes.
    fn text(n: usize, seed: u64) -> Vec<u8> {
        let words = ["the", "deploy", "worked", "thanks", "prod", "is", "slow", "see", "you", "lol"];
        let mut s = seed | 1;
        let mut out = Vec::with_capacity(n + 8);
        while out.len() < n {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            out.extend_from_slice(words[(s % 10) as usize].as_bytes());
            out.push(b' ');
        }
        out.truncate(n);
        out
    }

    fn tamper_last_byte(db: &Db<MemStore>, key: &[u8]) {
        let mut w = db.store.begin_write().unwrap();
        let mut raw = w.get(Table::Records, key).unwrap().unwrap();
        *raw.last_mut().unwrap() ^= 0x40;
        w.put(Table::Records, key, &raw).unwrap();
        w.commit(Durability::Immediate).unwrap();
    }

    #[test]
    fn raw_values_skip_the_value_cache() {
        let db = mem_db(Config::raw_only());
        let v = text(700, 1);
        db.put(b"k", &v, Expect::Any).unwrap();
        let before = db.counters.snapshot().units_decoded;
        for _ in 0..3 {
            assert_eq!(db.get(b"k").unwrap().unwrap(), v);
        }
        assert_eq!(db.get_range(b"k", 5, 10).unwrap().unwrap(), v[5..15]);
        let items = db.scan(&ScanOptions::all().limit(5).with_values(true)).unwrap();
        assert_eq!(items[0].value.as_deref(), Some(&v[..]));
        assert_eq!(db.counters.snapshot().units_decoded - before, 5, "verified on every read");
        let s = db.cache.stats();
        assert_eq!((s.entries, s.hits, s.misses), (0, 0, 0), "{s:?}");
    }

    #[test]
    fn inline_values_are_served_from_the_value_cache() {
        {
            let db = mem_db(Config::adaptive());
            let v1 = text(700, 1);
            let rev1 = db.put(b"k", &v1, Expect::Any).unwrap();
            let c0 = db.counters.snapshot();
            // Read once: decoded, offered, not kept yet. Read again: kept.
            for n in 1..=2 {
                assert_eq!(db.get(b"k").unwrap().unwrap(), v1);
                assert_eq!(db.cache.stats().entries, n - 1);
            }
            let c1 = db.counters.snapshot();
            assert_eq!((c1.units_decoded - c0.units_decoded, c1.bytes_reconstructed - c0.bytes_reconstructed), (2, 1400));
            // Hits: nothing decoded, the requested bytes are counted.
            assert_eq!(db.get(b"k").unwrap().unwrap(), v1);
            assert_eq!(db.get_range(b"k", 10, 20).unwrap().unwrap(), v1[10..30]);
            assert_eq!(db.get_at(b"k", rev1).unwrap().unwrap(), v1);
            let c2 = db.counters.snapshot();
            assert_eq!(c2.units_decoded, c1.units_decoded);
            assert_eq!(c2.bytes_requested - c1.bytes_requested, 700 + 20 + 700);
            // A new revision is a new entry; the old one is never served for it.
            let v2 = text(650, 2);
            let rev2 = db.put(b"k", &v2, Expect::Any).unwrap();
            assert_eq!(db.get_with_revision(b"k").unwrap().unwrap(), (rev2, v2.clone()));
            assert_eq!(db.get_range(b"k", 600, 100).unwrap().unwrap(), v2[600..]);
            assert_eq!(db.get_range(b"k", 0, 5).unwrap().unwrap(), v2[..5]);
            let s = db.cache.stats();
            assert_eq!(s.entries, 2, "{s:?}");
            assert!(s.hits >= 4, "{s:?}");
        }
    }

    #[test]
    fn only_verified_values_are_cached_and_damage_is_never_returned() {
        let db = mem_db(Config::adaptive());
        let v = text(500, 3);
        db.put(b"k", &v, Expect::Any).unwrap();
        tamper_last_byte(&db, b"k");
        // Never read before the damage: decoding reports it.
        assert!(db.get(b"k").is_err());
        assert_eq!(db.cache.stats().entries, 0, "a failed verification caches nothing");
        db.put(b"k", &v, Expect::Any).unwrap();
        for _ in 0..2 {
            assert_eq!(db.get(b"k").unwrap().unwrap(), v);
        }
        assert_eq!(db.cache.stats().entries, 1);
        tamper_last_byte(&db, b"k");
        // The verified value of that revision is served; once the cache is
        // dropped the damage is reported again, never returned.
        assert_eq!(db.get(b"k").unwrap().unwrap(), v);
        db.clear_cache();
        assert!(db.get(b"k").is_err());
        assert!(db.get(b"k").is_err());

        // Without verification nothing goes through the value cache.
        let db = mem_db(Config { verify_on_read: false, ..Config::adaptive() });
        db.put(b"k", &v, Expect::Any).unwrap();
        let before = db.counters.snapshot().units_decoded;
        for _ in 0..3 {
            assert_eq!(db.get(b"k").unwrap().unwrap(), v);
        }
        assert_eq!(db.counters.snapshot().units_decoded - before, 3);
        assert_eq!(db.cache.stats().entries, 0);
        // Nor when it is switched off.
        let db = mem_db(Config { cache_values: false, ..Config::adaptive() });
        db.put(b"k", &v, Expect::Any).unwrap();
        for _ in 0..3 {
            db.get(b"k").unwrap();
        }
        assert_eq!(db.cache.stats().entries, 0);
    }

    #[test]
    fn bounded_scans_fill_the_value_cache_and_unbounded_ones_only_read_it() {
        let db = mem_db(Config::adaptive());
        let values: Vec<Vec<u8>> = (0..30).map(|i| text(200 + i * 7, i as u64 + 10)).collect();
        for (i, v) in values.iter().enumerate() {
            db.put(format!("s/{i:02}").as_bytes(), v, Expect::Any).unwrap();
        }
        for _ in 0..3 {
            let all = db.scan(&ScanOptions::prefix(b"s/").with_values(true)).unwrap();
            assert_eq!(all.len(), 30);
        }
        assert_eq!(db.cache.stats().entries, 0, "unlimited scans do not fill the cache");
        let all = db.scan(&ScanOptions::prefix(b"s/").with_values(true)).unwrap();
        let opts = ScanOptions::prefix(b"s/").reverse(true).limit(10).with_values(true);
        let latest = db.scan(&opts).unwrap();
        assert_eq!(db.cache.stats().entries, 0, "first offer");
        assert_eq!(db.scan(&opts).unwrap(), latest);
        assert_eq!(db.cache.stats().entries, 10);
        let before = db.counters.snapshot();
        assert_eq!(db.scan(&opts).unwrap(), latest);
        let after = db.counters.snapshot();
        assert_eq!(after.units_decoded, before.units_decoded, "served from the cache");
        assert_eq!(after.bytes_requested - before.bytes_requested, latest.iter().map(|i| i.logical_len).sum::<u64>());
        for (i, item) in all.iter().enumerate() {
            assert_eq!(item.value.as_deref(), Some(&values[i][..]));
            assert_eq!(item.value, db.get(&item.key).unwrap());
        }
        for (j, item) in latest.iter().enumerate() {
            assert_eq!(item.value.as_deref(), Some(&values[29 - j][..]));
        }
    }

    fn inline_manifest(len: usize, source_id: Option<u64>) -> Vec<u8> {
        let data: Vec<u8> = (0..len).map(|i| i as u8).collect();
        let envelope = format::write_envelope(CodecTag::RAW_V1, 0, len as u32, &hash::digest(&data), &data);
        Manifest { revision: 42, logical_len: len as u64, source_id, body: ManifestBody::Inline(envelope) }.encode()
    }

    fn samples() -> Vec<Vec<u8>> {
        let chunks = ManifestBody::Chunks(vec![ChunkRef { logical_end: 10, object_id: 3 }]);
        let generated =
            ManifestBody::Generated { generator_id: 1, generator_version: 1, params: vec![1, 2, 3], digest: [9; 32] };
        vec![
            inline_manifest(0, None),
            inline_manifest(5, Some(7)),
            // 64 + 200 bytes: two-byte varint length.
            inline_manifest(200, None),
            Manifest { revision: 1, logical_len: 10, source_id: None, body: chunks }.encode(),
            Manifest { revision: 2, logical_len: 0, source_id: None, body: ManifestBody::Tombstone }.encode(),
            Manifest { revision: 3, logical_len: 9, source_id: Some(1), body: generated }.encode(),
        ]
    }

    /// The view accepts exactly the valid Inline manifests, with the same fields.
    fn check(bytes: &[u8]) {
        let decoded = Manifest::decode(bytes);
        match inline_view(bytes) {
            Some(view) => {
                let m = decoded.as_ref().expect("the view accepted bytes that Manifest::decode rejects");
                assert_eq!((m.revision, m.logical_len), (view.revision, view.logical_len));
                let envelope = &bytes[view.body.start - format::ENVELOPE_HEADER_LEN..view.body.end];
                assert_eq!(m.body, ManifestBody::Inline(envelope.to_vec()));
                let (h, body) = format::read_envelope(envelope).unwrap();
                assert_eq!((h, body), (view.header.clone(), &bytes[view.body.clone()]));
            }
            None => assert!(
                !matches!(decoded, Ok(Manifest { body: ManifestBody::Inline(_), .. })),
                "the view missed a valid inline manifest"
            ),
        }
        // `manifest_head` agrees with `Manifest::decode` on every input.
        match (manifest_head(bytes), decoded) {
            (Ok(head), Ok(m)) => assert_eq!(head, (m.revision, m.logical_len, m.is_tombstone())),
            (Err(_), Err(_)) => {}
            (a, b) => panic!("manifest_head {a:?} vs decode {b:?}"),
        }
    }

    #[test]
    fn inline_view_agrees_with_manifest_decode() {
        for sample in samples() {
            check(&sample);
            for cut in 0..sample.len() {
                check(&sample[..cut]);
            }
            let mut longer = sample.clone();
            longer.push(0);
            check(&longer);
            for i in 0..sample.len() {
                for mask in [0x01, 0x02, 0x40, 0x80, 0xFF] {
                    let mut b = sample.clone();
                    b[i] ^= mask;
                    check(&b);
                }
            }
        }
    }

    #[test]
    fn spans() {
        assert!(matches!(Span::range(10, 11, 0), Err(Error::InvalidArgument(_))));
        let s = Span::range(10, 10, 5).unwrap();
        assert!(s.len == 0 && !s.whole && s.is_empty_part());
        let s = Span::range(10, 0, u64::MAX).unwrap();
        assert!(s.whole && s.len == 10);
        let s = Span::range(10, 3, u64::MAX).unwrap();
        assert!(!s.whole && s.len == 7);
        assert_eq!(s.bytes(), 3..10);
        let s = Span::whole(0);
        assert!(s.whole && !s.is_empty_part());
        assert!(Span::of(0, Some((0, 0))).unwrap().whole);
    }

    #[test]
    fn empty_scan_ranges() {
        let b = |v: &[u8]| v.to_vec();
        assert!(range_is_empty(&Bound::Included(b(b"b")), &Bound::Included(b(b"a"))));
        assert!(!range_is_empty(&Bound::Included(b(b"a")), &Bound::Included(b(b"a"))));
        assert!(range_is_empty(&Bound::Included(b(b"a")), &Bound::Excluded(b(b"a"))));
        assert!(range_is_empty(&Bound::Excluded(b(b"a")), &Bound::Included(b(b"a"))));
        assert!(range_is_empty(&Bound::Excluded(b(b"a")), &Bound::Excluded(b(b"a"))));
        assert!(!range_is_empty(&Bound::Excluded(b(b"a")), &Bound::Excluded(b(b"b"))));
        assert!(!range_is_empty(&Bound::Included(b(b"z")), &Bound::Unbounded));
    }
}
