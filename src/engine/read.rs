//! Read path: key -> manifest (one lookup) -> units intersecting the range ->
//! decode -> verify -> bytes. Blocks come from the cache when possible; the
//! envelopes of the others are copied out and the snapshot is released before
//! they are decoded (up to `DETACHED_ENVELOPE_BYTES`; beyond that, blocks are
//! decoded while the snapshot is still held so memory stays bounded).

use std::ops::{Bound, Range};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{Db, HistoryEntry, Revision, ScanItem, ScanOptions, ops, prefix_successor};
use crate::chunk;
use crate::codec;
use crate::error::{Error, Result};
use crate::format::{self, ChunkRef, MAX_UNIT_LEN, Manifest, ManifestBody, SourceDescriptor};
use crate::hash::{self, Digest};
use crate::store::{ReadTxn, Store, Table};

/// Envelope bytes held outside the snapshot before decoding starts in place.
const DETACHED_ENVELOPE_BYTES: usize = 8 << 20;

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
}

/// An Inline manifest located inside the record bytes, without copying its
/// envelope out (the common case for small values).
struct InlineView {
    revision: u64,
    logical_len: u64,
    envelope: Range<usize>,
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
    let envelope = pos..pos.checked_add(len)?;
    if envelope.end != raw.len() {
        return None;
    }
    let (header, _) = format::read_envelope(&raw[envelope.clone()]).ok()?;
    if u64::from(header.raw_len) != logical_len {
        return None;
    }
    Some(InlineView { revision, logical_len, envelope })
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
    Inline(Vec<u8>),
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

impl<S: Store> Db<S> {
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

    /// Decode an inline envelope (dependencies loaded) and keep the span.
    fn decode_inline(&self, envelope: &[u8], span: Span, work: &mut Work) -> Result<Vec<u8>> {
        let unit = ops::decode_envelope(envelope, &self.params, self.cfg.verify_on_read, None)?;
        work.units += 1;
        work.bytes += unit.len() as u64;
        // Inline values are at most `u32::MAX` bytes (`raw_len`), so the span fits.
        slice_owned(unit, span.offset as usize..(span.offset + span.len) as usize, None)
    }

    /// Phase 1 (snapshot held): load decoding dependencies and copy the
    /// envelopes of blocks that are not cached.
    fn fetch<T: ReadTxn + ?Sized>(&self, t: &T, m: Manifest, span: Span) -> Result<Pending> {
        Ok(match m.body {
            ManifestBody::Inline(envelope) => {
                self.params.ensure_for_envelope(t, &envelope)?;
                Pending::Inline(envelope)
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
            Pending::Inline(envelope) => self.decode_inline(&envelope, span, &mut work)?,
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
        let envelope = &raw[view.envelope];
        db.params.ensure_for_envelope(&r, envelope)?;
        drop(r);
        let mut work = Work::default();
        let value = db.decode_inline(envelope, span, &mut work)?;
        db.note_read(&work, value.len());
        return Ok(Some((view.revision, value)));
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
    if let Some(view) = inline_view(&raw) {
        return Ok(Some((view.revision, view.logical_len)));
    }
    let m = Manifest::decode(&raw)?;
    Ok((!m.is_tombstone()).then_some((m.revision, m.logical_len)))
}

pub(super) fn get_at<S: Store>(db: &Db<S>, key: &[u8], revision: Revision) -> Result<Option<Vec<u8>>> {
    db.counters.gets.fetch_add(1, Ordering::Relaxed);
    let r = db.store.begin_read()?;
    let m = match ops::load_manifest(&r, key)? {
        Some(m) if m.revision == revision => m,
        _ => match r.get(Table::History, &format::history_key(key, revision))? {
            Some(raw) => {
                let m = Manifest::decode(&raw)?;
                if m.revision != revision {
                    return Err(Error::format("history entry revision differs from its key"));
                }
                m
            }
            None => return Ok(None),
        },
    };
    if m.is_tombstone() {
        return Ok(None);
    }
    db.read_detached(r, m, None).map(Some)
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
    if !opts.with_values {
        let mut items = Vec::new();
        r.scan(Table::Records, slice_bound(&opts.start), slice_bound(&opts.end), opts.reverse, &mut |k: &[u8], v: &[u8]| {
            let m = Manifest::decode(v)?;
            if !m.is_tombstone() {
                items.push(ScanItem { key: k.to_vec(), revision: m.revision, logical_len: m.logical_len, value: None });
            }
            Ok(!full(items.len()))
        })?;
        return Ok(items);
    }
    // The backend scan stops at the limit; values are then reconstructed
    // with the same snapshot.
    let mut found: Vec<(Vec<u8>, Manifest)> = Vec::new();
    r.scan(Table::Records, slice_bound(&opts.start), slice_bound(&opts.end), opts.reverse, &mut |k: &[u8], v: &[u8]| {
        let m = Manifest::decode(v)?;
        if !m.is_tombstone() {
            found.push((k.to_vec(), m));
        }
        Ok(!full(found.len()))
    })?;
    let mut items = Vec::with_capacity(found.len());
    for (key, m) in found {
        let (revision, logical_len) = (m.revision, m.logical_len);
        let span = Span::whole(logical_len);
        let pending = db.fetch(&r, m, span)?;
        let value = db.finish(pending, span)?;
        items.push(ScanItem { key, revision, logical_len, value: Some(value) });
    }
    Ok(items)
}

fn history_entry(m: &Manifest, current: bool) -> HistoryEntry {
    HistoryEntry { revision: m.revision, logical_len: m.logical_len, tombstone: m.is_tombstone(), current }
}

pub(super) fn history<S: Store>(db: &Db<S>, key: &[u8]) -> Result<Vec<HistoryEntry>> {
    let r = db.store.begin_read()?;
    let prefix = format::history_prefix(key);
    let end_key = prefix_successor(&prefix);
    let end = end_key.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
    let mut entries = Vec::new();
    r.scan(Table::History, Bound::Included(&prefix), end, false, &mut |k: &[u8], v: &[u8]| {
        let (_, revision) = format::parse_history_key(k)?;
        let m = Manifest::decode(v)?;
        if m.revision != revision {
            return Err(Error::format("history entry revision differs from its key"));
        }
        entries.push(history_entry(&m, false));
        Ok(true)
    })?;
    if let Some(m) = ops::load_manifest(&r, key)? {
        entries.push(history_entry(&m, true));
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
    use crate::format::CodecTag;

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
                let m = decoded.expect("the view accepted bytes that Manifest::decode rejects");
                assert_eq!((m.revision, m.logical_len), (view.revision, view.logical_len));
                assert_eq!(m.body, ManifestBody::Inline(bytes[view.envelope].to_vec()));
            }
            None => assert!(
                !matches!(decoded, Ok(Manifest { body: ManifestBody::Inline(_), .. })),
                "the view missed a valid inline manifest"
            ),
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
