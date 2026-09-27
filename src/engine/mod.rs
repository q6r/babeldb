//! Engine: coordinates codecs, dedupe, cache and transactions.
//!
//! Read path: key -> manifest (1 lookup) -> objects intersecting the range
//! -> decode -> verify (len + BLAKE3) -> bytes. The hash index is only used
//! on writes (dedupe candidates), never to find the current version.
//!
//! The public signatures are the contract used by the CLI, benches and tests.
//! Maintenance/training methods live in `crate::maintenance`, imports in
//! `crate::ingest` (both as `impl Db` blocks using `ops`).

pub mod ops;

use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::cache::BlockCache;
use crate::config::{Config, Mode, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE};
use crate::error::{Error, Result};
use crate::format::{self, meta_key, SourceDescriptor, FORMAT_VERSION};
use crate::generator::{Generator, Registry};
use crate::planner::Planner;
use crate::stats::{EngineCounters, Stats};
use crate::store::redb::RedbStore;
use crate::store::{Durability, Store, Table, WriteTxn};

use ops::{ParamCache, ParamEntry};

pub type Revision = u64;

/// Optimistic concurrency condition checked inside the write transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    /// Last writer wins.
    Any,
    /// The key must not exist (a tombstone counts as absent).
    Absent,
    /// The current revision must be exactly this one.
    Revision(Revision),
}

#[derive(Clone, Copy, Debug)]
pub enum BatchOp<'a> {
    Put { key: &'a [u8], value: &'a [u8], expect: Expect },
    Delete { key: &'a [u8], expect: Expect },
}

#[derive(Clone, Debug)]
pub struct ScanOptions {
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
    pub reverse: bool,
    /// Maximum number of items (0 = unlimited).
    pub limit: usize,
    /// Reconstruct values (otherwise only key/revision/len).
    pub with_values: bool,
}

impl ScanOptions {
    pub fn all() -> ScanOptions {
        ScanOptions { start: Bound::Unbounded, end: Bound::Unbounded, reverse: false, limit: 0, with_values: false }
    }

    /// Every key starting with `prefix`.
    pub fn prefix(prefix: &[u8]) -> ScanOptions {
        let end = match prefix_successor(prefix) {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        };
        ScanOptions { start: Bound::Included(prefix.to_vec()), end, reverse: false, limit: 0, with_values: false }
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    pub fn reverse(mut self, r: bool) -> Self {
        self.reverse = r;
        self
    }

    pub fn with_values(mut self, v: bool) -> Self {
        self.with_values = v;
        self
    }
}

/// Smallest key greater than every key that starts with `prefix`.
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanItem {
    pub key: Vec<u8>,
    pub revision: Revision,
    pub logical_len: u64,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryEntry {
    pub revision: Revision,
    pub logical_len: u64,
    pub tombstone: bool,
    /// True for the current manifest (from `records`), false for `history`.
    pub current: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnitInfo {
    /// None for inline units.
    pub object_id: Option<u64>,
    pub codec: String,
    pub raw_len: u32,
    pub body_len: u32,
    pub aux_id: u64,
    /// Reference count of the object (None for inline units).
    pub refcount: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inspection {
    pub key: Vec<u8>,
    pub revision: Revision,
    pub logical_len: u64,
    /// "inline" | "chunks" | "generated" | "tombstone"
    pub kind: &'static str,
    pub manifest_bytes: u64,
    /// Sum of envelope sizes (64-byte headers + bodies) of the units referenced.
    /// Shared objects are counted fully here; see `stats` for the shared total.
    pub encoded_bytes: u64,
    pub units: Vec<UnitInfo>,
    pub source: Option<(u64, SourceDescriptor)>,
    /// (generator id, version, name, params length)
    pub generator: Option<(u16, u16, String, usize)>,
}

pub struct Db<S: Store = RedbStore> {
    pub(crate) store: S,
    pub(crate) cfg: Config,
    /// Persisted creation parameters (win over `cfg`).
    pub(crate) mode: Mode,
    pub(crate) block_size: u32,
    pub(crate) inline_max: u32,
    /// Replaced (not mutated) when a dictionary/template is installed.
    pub(crate) planner: RwLock<Arc<Planner>>,
    pub(crate) params: ParamCache,
    pub(crate) cache: BlockCache,
    pub(crate) generators: Registry,
    pub(crate) counters: EngineCounters,
}

impl Db<RedbStore> {
    /// Open or create a redb-backed database file.
    pub fn open(path: impl AsRef<Path>, cfg: Config) -> Result<Db<RedbStore>> {
        let store = RedbStore::open(path, cfg.backend_cache_bytes)?;
        Db::with_store(store, cfg)
    }
}

fn meta_u32<T: crate::store::ReadTxn + ?Sized>(t: &T, name: &str) -> Result<u32> {
    let v = ops::get_meta(t, name)?.ok_or_else(|| Error::format(format!("missing meta {name}")))?;
    format::decode_u32(&v)
}

impl<S: Store> Db<S> {
    /// Initialize `meta` on first use (format version, mode, block size,
    /// inline_max, id counters) or validate it (unknown format version => error).
    pub fn with_store(store: S, cfg: Config) -> Result<Db<S>> {
        cfg.validate()?;
        let (mode, block_size, inline_max) = {
            let mut w = store.begin_write()?;
            match ops::get_meta(&w, meta_key::FORMAT_VERSION)? {
                None => {
                    w.put(Table::Meta, meta_key::FORMAT_VERSION.as_bytes(), &FORMAT_VERSION.to_le_bytes())?;
                    w.put(Table::Meta, meta_key::MODE.as_bytes(), &[cfg.mode as u8])?;
                    w.put(Table::Meta, meta_key::BLOCK_SIZE.as_bytes(), &cfg.block_size.to_le_bytes())?;
                    w.put(Table::Meta, meta_key::INLINE_MAX.as_bytes(), &cfg.inline_max.to_le_bytes())?;
                    for counter in [
                        meta_key::NEXT_OBJECT_ID,
                        meta_key::NEXT_REVISION,
                        meta_key::NEXT_PARAM_ID,
                        meta_key::NEXT_SOURCE_ID,
                        meta_key::NEXT_IMPORT_ID,
                    ] {
                        ops::put_meta_u64(&mut w, counter, 1)?;
                    }
                    let created_by = format!("babeldb {}", env!("CARGO_PKG_VERSION"));
                    w.put(Table::Meta, meta_key::CREATED_BY.as_bytes(), created_by.as_bytes())?;
                    w.commit(Durability::Immediate)?;
                    (cfg.mode, cfg.block_size, cfg.inline_max)
                }
                Some(v) => {
                    let version = format::decode_u32(&v)?;
                    if version != FORMAT_VERSION {
                        return Err(Error::Unsupported(format!(
                            "format version {version} (this build reads {FORMAT_VERSION})"
                        )));
                    }
                    let mode = ops::get_meta(&w, meta_key::MODE)?
                        .and_then(|b| if b.len() == 1 { Mode::from_u8(b[0]) } else { None })
                        .ok_or_else(|| Error::format("bad mode in meta"))?;
                    let bs = meta_u32(&w, meta_key::BLOCK_SIZE)?;
                    let im = meta_u32(&w, meta_key::INLINE_MAX)?;
                    if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&bs) || im > bs {
                        return Err(Error::format("invalid persisted block_size/inline_max"));
                    }
                    drop(w);
                    (mode, bs, im)
                }
            }
        };

        let params = ParamCache::new(cfg.codecs.zstd_level);
        let (dict, template) = {
            let r = store.begin_read()?;
            let dict = match ops::get_meta_u64(&r, meta_key::ACTIVE_ZSTD_DICT)? {
                Some(id) => match params.load(&r, id)? {
                    ParamEntry::Dict(d) => Some(d),
                    ParamEntry::Template(_) => return Err(Error::format("active dictionary is not a dictionary")),
                },
                None => None,
            };
            let template = match ops::get_meta_u64(&r, meta_key::ACTIVE_TEMPLATE)? {
                Some(id) => match params.load(&r, id)? {
                    ParamEntry::Template(t) => Some(t),
                    ParamEntry::Dict(_) => return Err(Error::format("active template is not a template")),
                },
                None => None,
            };
            (dict, template)
        };
        let planner = Planner::new(mode, cfg.codecs.clone()).with_params(dict, template);

        Ok(Db {
            cache: BlockCache::new(cfg.cache_bytes),
            generators: Registry::builtin(),
            counters: EngineCounters::default(),
            planner: RwLock::new(Arc::new(planner)),
            params,
            mode,
            block_size,
            inline_max,
            store,
            cfg,
        })
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// Effective (persisted) mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Effective (persisted) block size.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Effective (persisted) inline threshold.
    pub fn inline_max(&self) -> u32 {
        self.inline_max
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    /// Current planner (cheap `Arc` clone).
    pub(crate) fn planner(&self) -> Arc<Planner> {
        self.planner.read().map(|p| p.clone()).unwrap_or_else(|e| e.into_inner().clone())
    }

    pub(crate) fn replace_planner(&self, p: Planner) {
        match self.planner.write() {
            Ok(mut g) => *g = Arc::new(p),
            Err(e) => *e.into_inner() = Arc::new(p),
        }
    }

    /// Dedupe is only active in Adaptive mode.
    pub(crate) fn dedupe(&self) -> bool {
        self.cfg.effective_dedupe(self.mode)
    }

    /// Register an extra generator (tests, experiments).
    pub fn register_generator(&mut self, g: Box<dyn Generator>) {
        self.generators.register(g);
    }

    pub fn clear_cache(&self) {
        self.cache.clear();
    }
}

#[allow(unused_variables)]
impl<S: Store> Db<S> {
    /// Durable when it returns (Immediate commit).
    pub fn put(&self, key: &[u8], value: &[u8], expect: Expect) -> Result<Revision> {
        todo!()
    }

    /// Store a record described by a registered generator + params (no objects).
    pub fn put_generated(&self, key: &[u8], generator_id: u16, generator_version: u16, params: &[u8], expect: Expect) -> Result<Revision> {
        todo!()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub fn get_with_revision(&self, key: &[u8]) -> Result<Option<(Revision, Vec<u8>)>> {
        todo!()
    }

    /// Bytes `[offset, offset+len)` clamped to the value length. `offset >
    /// logical_len` is an `InvalidArgument` error; `offset == logical_len` returns empty.
    pub fn get_range(&self, key: &[u8], offset: u64, len: u64) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    /// Current (revision, logical_len), tombstones excluded.
    pub fn head(&self, key: &[u8]) -> Result<Option<(Revision, u64)>> {
        todo!()
    }

    /// Returns whether a live record was deleted.
    pub fn delete(&self, key: &[u8], expect: Expect) -> Result<bool> {
        todo!()
    }

    /// Apply every op atomically in one durable commit (group commit).
    /// Any failed expectation aborts the whole batch. Returns the new revision
    /// for puts, and `Some(rev)`/`None` for deletes (deleted / nothing to delete).
    pub fn write_batch(&self, ops: &[BatchOp<'_>]) -> Result<Vec<Option<Revision>>> {
        todo!()
    }

    /// Group-commit primitive: apply, in order and in ONE commit with the given
    /// durability, every op whose expectation holds. Ops whose expectation fails
    /// (or that are invalid) are skipped and reported individually; the others
    /// are still applied. The outer `Err` is reserved for failures that abort the
    /// whole transaction (I/O, backend, integrity).
    pub fn write_batch_each(&self, ops: &[BatchOp<'_>], durability: Durability) -> Result<Vec<Result<Option<Revision>>>> {
        todo!()
    }

    /// Make every previously committed `Durability::Deferred` transaction
    /// durable (an empty `Immediate` commit).
    pub fn sync(&self) -> Result<()> {
        todo!()
    }

    /// Ordered range scan over live records.
    pub fn scan(&self, opts: &ScanOptions) -> Result<Vec<ScanItem>> {
        todo!()
    }

    /// Retained history of a key, oldest first, plus the current manifest.
    pub fn history(&self, key: &[u8]) -> Result<Vec<HistoryEntry>> {
        todo!()
    }

    /// Value of a key at a specific revision (current or retained history).
    pub fn get_at(&self, key: &[u8], revision: Revision) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    /// Drop retained history entries, keeping the newest `keep_last` per key.
    /// Returns the number of history entries removed.
    pub fn prune_history(&self, key: Option<&[u8]>, keep_last: usize) -> Result<u64> {
        todo!()
    }

    pub fn register_source(&self, desc: SourceDescriptor) -> Result<u64> {
        todo!()
    }

    pub fn sources(&self) -> Result<Vec<(u64, SourceDescriptor)>> {
        todo!()
    }

    pub fn inspect(&self, key: &[u8]) -> Result<Option<Inspection>> {
        todo!()
    }

    /// Full accounting (scans every table).
    pub fn stats(&self) -> Result<Stats> {
        todo!()
    }
}
