//! Streaming ingestion with bounded memory. Objects are prepared in batches
//! (registered in `pending_imports`); the manifest that makes the record
//! visible, and the source's `last_import`, are published in one final
//! durable transaction. An interrupted import publishes nothing; its objects
//! are released by a best-effort cleanup or, failing that, collected by `Db::gc`.
//!
//! Memory is bounded by one read buffer of `block_size` bytes, the staged
//! batch (at most `Config::import_batch_bytes` plus one block, counting the
//! encoded envelopes and the raw copies kept for the byte-for-byte dedupe
//! comparison) and 16 bytes per block for the chunk list of the manifest.

pub mod file;

use std::io::{ErrorKind, Read};
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::engine::ops::{self, PreparedUnit};
use crate::engine::{Db, Expect, Revision};
use crate::error::{Error, Result};
use crate::format::{
    self, ChunkRef, CodecTag, ENVELOPE_HEADER_LEN, ImportInfo, Manifest, ManifestBody,
    SourceDescriptor, id_key, meta_key,
};
use crate::hash::{Digest, StreamHasher};
use crate::source::{self, LOCAL_FILE_ADAPTER_VERSION, source_kind};
use crate::store::{Durability, ReadTxn, Store, Table, WriteTxn};

use file::FileSource;

/// A source of bytes read block by block.
pub trait ByteSource {
    /// Fill `buf` as much as possible; return 0 at end of input.
    ///
    /// Short reads are tolerated: the importer keeps calling until its block
    /// is full or 0 is returned, so blocks always have the database block size.
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize>;
}

/// In-memory bytes; the slice advances as it is read.
impl ByteSource for &[u8] {
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = buf.len().min(self.len());
        let (head, tail) = self.split_at(n);
        buf[..n].copy_from_slice(head);
        *self = tail;
        Ok(n)
    }
}

/// Adapter for any `std::io::Read` (stdin, pipes, cursors).
pub struct ReaderSource<R: Read> {
    inner: R,
}

impl<R: Read> ReaderSource<R> {
    pub fn new(inner: R) -> Self {
        ReaderSource { inner }
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> ByteSource for ReaderSource<R> {
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
        read_full(&mut self.inner, buf)
    }
}

/// Read until `buf` is full or the reader reports end of input
/// (`Interrupted` is retried).
pub(crate) fn read_full<R: Read + ?Sized>(r: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let rest = buf.len() - filled;
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) if n <= rest => filled += n,
            Ok(n) => {
                return Err(Error::InvalidArgument(format!(
                    "reader returned {n} bytes for a {rest}-byte buffer"
                )));
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(filled)
}

#[derive(Clone, Debug)]
pub struct ImportOptions {
    pub expect: Expect,
    /// Existing source id to attach and update.
    pub source_id: Option<u64>,
}

impl Default for ImportOptions {
    fn default() -> Self {
        ImportOptions {
            expect: Expect::Any,
            source_id: None,
        }
    }
}

/// Provenance of an import, resolved inside the final transaction so that a
/// failed import never leaves a descriptor behind.
enum SourceSpec {
    Detached,
    Existing(u64),
    LocalFile(String),
}

/// A block prepared outside the write transaction, waiting for its batch.
struct StagedBlock {
    unit: PreparedUnit,
    /// Original bytes for the byte-for-byte comparison of dedupe candidates;
    /// `None` when the envelope is `RawV1`, whose body already is those bytes.
    raw: Option<Vec<u8>>,
}

impl StagedBlock {
    fn new(unit: PreparedUnit, data: &[u8]) -> Result<StagedBlock> {
        let (header, body) = format::read_envelope(&unit.envelope)?;
        let raw = if header.codec == CodecTag::RAW_V1 && body == data {
            None
        } else {
            Some(data.to_vec())
        };
        Ok(StagedBlock { unit, raw })
    }

    fn raw(&self) -> &[u8] {
        match &self.raw {
            Some(raw) => raw,
            None => self
                .unit
                .envelope
                .get(ENVELOPE_HEADER_LEN..)
                .unwrap_or_default(),
        }
    }

    /// Bytes held in memory until the batch is committed.
    fn held_bytes(&self) -> usize {
        self.unit.envelope.len() + self.raw.as_ref().map_or(0, Vec::len)
    }
}

/// Committed part of an import (its references are pending until publication).
#[derive(Default)]
struct ImportRun {
    /// Allocated by the first committed batch.
    import_id: Option<u64>,
    /// Chunk list of the manifest (committed blocks only).
    refs: Vec<ChunkRef>,
    logical_end: u64,
}

fn unknown_source(id: u64) -> Error {
    Error::InvalidArgument(format!("unknown source id {id}"))
}

/// Optimistic concurrency check against the current record (a tombstone counts as absent).
fn check_expect(key: &[u8], current: Option<&Manifest>, expect: Expect) -> Result<()> {
    let live = current.filter(|m| !m.is_tombstone()).map(|m| m.revision);
    let (holds, expected) = match expect {
        Expect::Any => (true, String::new()),
        Expect::Absent => (live.is_none(), "absent".to_string()),
        Expect::Revision(r) => (live == Some(r), format!("revision {r}")),
    };
    if holds {
        Ok(())
    } else {
        Err(Error::RevisionConflict {
            key: key.to_vec(),
            expected,
            actual: live,
        })
    }
}

/// Read until `buf` is full or the source reports end of input, so a short
/// result always means end of input.
fn fill_block(source: &mut dyn ByteSource, buf: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let rest = &mut buf[filled..];
        let n = source.read_block(rest)?;
        if n == 0 {
            break;
        }
        if n > rest.len() {
            return Err(Error::InvalidArgument(format!(
                "ByteSource reported {n} bytes for a {}-byte buffer",
                rest.len()
            )));
        }
        filled += n;
    }
    Ok(filled)
}

fn resolve_source<W: WriteTxn + ?Sized>(
    w: &mut W,
    spec: &SourceSpec,
) -> Result<Option<(u64, SourceDescriptor)>> {
    match spec {
        SourceSpec::Detached => Ok(None),
        SourceSpec::Existing(id) => match source::load_source(w, *id)? {
            Some(desc) => Ok(Some((*id, desc))),
            None => Err(unknown_source(*id)),
        },
        SourceSpec::LocalFile(location) => {
            if let Some((id, mut desc)) =
                source::find_source_by_location(w, source_kind::LOCAL_FILE, location)?
            {
                desc.adapter_version = LOCAL_FILE_ADAPTER_VERSION;
                return Ok(Some((id, desc)));
            }
            let id = ops::alloc_id(w, meta_key::NEXT_SOURCE_ID, "source id")?;
            Ok(Some((id, SourceDescriptor::local_file(location.clone()))))
        }
    }
}

impl<S: Store> Db<S> {
    /// Stream `source` into `key` with memory bounded by the block size and
    /// `Config::import_batch_bytes`. Nothing becomes visible before the final commit.
    ///
    /// Values of at most `inline_max` bytes are stored inline, exactly like
    /// `put`. `opts.expect` is checked inside the final transaction; on any
    /// error nothing is published and the references taken by committed
    /// batches are released (or left to `gc` if that cleanup fails too).
    pub fn import(
        &self,
        key: &[u8],
        source: &mut dyn ByteSource,
        opts: &ImportOptions,
    ) -> Result<Revision> {
        self.check_import_key(key)?;
        let spec = match opts.source_id {
            Some(id) => SourceSpec::Existing(id),
            None => SourceSpec::Detached,
        };
        self.run_import(key, source, opts.expect, spec)
    }

    /// Import a local file byte-for-byte. When `opts.source_id` is None a
    /// `LOCAL_FILE` source is registered (or reused by location) and attached.
    ///
    /// The location is the canonical path (`source::local_file_location`).
    /// The source is registered in the same transaction that publishes the
    /// record, so a failed import leaves no descriptor behind.
    pub fn import_file(&self, key: &[u8], path: &Path, opts: &ImportOptions) -> Result<Revision> {
        self.check_import_key(key)?;
        let mut file = FileSource::open(path)?;
        let spec = match opts.source_id {
            Some(id) => SourceSpec::Existing(id),
            None => SourceSpec::LocalFile(source::local_file_location(path)?),
        };
        self.run_import(key, &mut file, opts.expect, spec)
    }

    fn check_import_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidArgument("empty key".into()));
        }
        if key.len() > self.cfg.max_key_len {
            return Err(Error::LimitExceeded(format!(
                "key of {} bytes exceeds max_key_len {}",
                key.len(),
                self.cfg.max_key_len
            )));
        }
        Ok(())
    }

    fn run_import(
        &self,
        key: &[u8],
        source: &mut dyn ByteSource,
        expect: Expect,
        spec: SourceSpec,
    ) -> Result<Revision> {
        if let SourceSpec::Existing(id) = spec {
            // Fail fast; the final transaction checks again.
            let r = self.store.begin_read()?;
            if source::load_source(&r, id)?.is_none() {
                return Err(unknown_source(id));
            }
        }
        let mut run = ImportRun::default();
        let result = self.stream_import(key, source, expect, &spec, &mut run);
        if result.is_err()
            && let Some(import_id) = run.import_id
        {
            // Best effort: if this fails too, the pending entry stays and
            // `gc` releases its references later.
            let _ = self.abandon_import(import_id);
        }
        result
    }

    fn stream_import(
        &self,
        key: &[u8],
        source: &mut dyn ByteSource,
        expect: Expect,
        spec: &SourceSpec,
        run: &mut ImportRun,
    ) -> Result<Revision> {
        let block_size = self.block_size as usize;
        let inline_max = u64::from(self.inline_max);
        let max_value_len = self.cfg.max_value_len;
        let batch_budget = self.cfg.import_batch_bytes;
        let planner = self.planner();
        let dedupe = self.dedupe();

        let mut buf = vec![0u8; block_size];
        let mut hasher = StreamHasher::new();
        let mut total: u64 = 0;
        let mut staged: Vec<StagedBlock> = Vec::new();
        let mut staged_bytes = 0usize;
        loop {
            let n = fill_block(source, &mut buf)?;
            if n == 0 {
                break;
            }
            total = total
                .checked_add(n as u64)
                .filter(|t| *t <= max_value_len)
                .ok_or_else(|| {
                    Error::LimitExceeded(format!(
                        "imported value exceeds max_value_len {max_value_len}"
                    ))
                })?;
            let data = &buf[..n];
            hasher.update(data);
            let block = StagedBlock::new(ops::prepare_unit(&planner, data), data)?;
            staged_bytes += block.held_bytes();
            staged.push(block);
            // While the value may still turn out inline, nothing is written.
            if total > inline_max && staged_bytes >= batch_budget {
                self.commit_batch(&mut staged, run, dedupe)?;
                staged_bytes = 0;
            }
            if n < block_size {
                break; // `fill_block` only stops early at end of input
            }
        }
        let digest = hasher.finalize();

        let body = if total <= inline_max {
            // inline_max <= block_size, so at most one block was read and
            // nothing was committed; its envelope is the inline envelope.
            let envelope = match staged.pop() {
                Some(block) => block.unit.envelope,
                None => ops::prepare_unit(&planner, &[]).envelope,
            };
            ManifestBody::Inline(envelope)
        } else {
            self.commit_batch(&mut staged, run, dedupe)?;
            ManifestBody::Chunks(std::mem::take(&mut run.refs))
        };
        self.publish_import(key, expect, spec, run.import_id, total, digest, body)
    }

    /// One intermediate transaction: store (or dedupe) every staged block,
    /// taking one provisional reference each, and append the object ids to
    /// the pending entry of the import.
    fn commit_batch(
        &self,
        staged: &mut Vec<StagedBlock>,
        run: &mut ImportRun,
        dedupe: bool,
    ) -> Result<()> {
        if staged.is_empty() {
            return Ok(());
        }
        let mut w = self.store.begin_write()?;
        let import_id = match run.import_id {
            Some(id) => id,
            None => ops::alloc_id(&mut w, meta_key::NEXT_IMPORT_ID, "import id")?,
        };
        let mut ids = Vec::with_capacity(staged.len());
        let mut reused = 0u64;
        let mut object_ids = ops::ObjectIds::default();
        for block in staged.iter() {
            let (id, was_reused) = ops::store_unit_with(
                &mut w,
                &block.unit,
                block.raw(),
                dedupe,
                &self.params,
                &mut object_ids,
            )?;
            reused += u64::from(was_reused);
            ids.push(id);
        }
        object_ids.finish(&mut w)?;
        let pending_key = id_key(import_id);
        let mut list = w
            .get(Table::PendingImports, &pending_key)?
            .unwrap_or_default();
        list.extend_from_slice(&format::encode_id_list(&ids));
        w.put(Table::PendingImports, &pending_key, &list)?;
        w.commit(Durability::Deferred)?;

        self.counters.commits.fetch_add(1, Ordering::Relaxed);
        self.counters
            .objects_written
            .fetch_add(ids.len() as u64 - reused, Ordering::Relaxed);
        self.counters
            .dedupe_hits
            .fetch_add(reused, Ordering::Relaxed);
        // Only committed references become part of the run.
        run.import_id = Some(import_id);
        for (block, id) in staged.drain(..).zip(ids) {
            run.logical_end += u64::from(block.unit.raw_len);
            run.refs.push(ChunkRef {
                logical_end: run.logical_end,
                object_id: id,
            });
        }
        Ok(())
    }

    /// Final durable transaction: check `expect`, publish the manifest (the
    /// pending references become its references), retire the previous
    /// manifest and update the source.
    #[allow(clippy::too_many_arguments)]
    fn publish_import(
        &self,
        key: &[u8],
        expect: Expect,
        spec: &SourceSpec,
        import_id: Option<u64>,
        total: u64,
        digest: Digest,
        body: ManifestBody,
    ) -> Result<Revision> {
        let mut w = self.store.begin_write()?;
        let current = ops::load_manifest(&w, key)?;
        check_expect(key, current.as_ref(), expect)?;
        let source = resolve_source(&mut w, spec)?;
        let revision = ops::alloc_revision(&mut w)?;
        let manifest = Manifest {
            revision,
            logical_len: total,
            source_id: source.as_ref().map(|(id, _)| *id),
            body,
        };
        ops::put_manifest(&mut w, key, &manifest)?;
        if let Some(id) = import_id {
            w.remove(Table::PendingImports, &id_key(id))?;
        }
        let mut removed = Vec::new();
        if let Some(old) = current {
            if self.cfg.keep_history {
                // The retained manifest keeps its references.
                w.put(
                    Table::History,
                    &format::history_key(key, old.revision),
                    &old.encode(),
                )?;
            } else {
                removed = ops::release_manifest(&mut w, &old)?;
            }
        }
        if let Some((id, mut desc)) = source {
            desc.last_import = Some(ImportInfo {
                revision,
                bytes: total,
                digest,
                unix_ms: ops::now_unix_ms(),
            });
            source::put_source(&mut w, id, &desc)?;
        }
        w.commit(Durability::Immediate)?;

        self.counters.commits.fetch_add(1, Ordering::Relaxed);
        for id in removed {
            self.cache.remove(id);
        }
        Ok(revision)
    }

    /// Release every provisional reference of an unfinished import and drop
    /// its pending entry. Reads the list from the store, so references are
    /// never released twice.
    fn abandon_import(&self, import_id: u64) -> Result<()> {
        let mut w = self.store.begin_write()?;
        let pending_key = id_key(import_id);
        let Some(list) = w.get(Table::PendingImports, &pending_key)? else {
            return Ok(());
        };
        let mut removed = Vec::new();
        for id in format::decode_id_list(&list)? {
            if ops::decref(&mut w, id)? {
                removed.push(id);
            }
        }
        w.remove(Table::PendingImports, &pending_key)?;
        w.commit(Durability::Immediate)?;

        self.counters.commits.fetch_add(1, Ordering::Relaxed);
        for id in removed {
            self.cache.remove(id);
        }
        Ok(())
    }
}
