//! Transactional storage contract.
//!
//! Every logical table is reachable from one transaction, so objects, hash
//! candidates, refcounts and the manifest are published atomically. Backends
//! only store bytes; all typed encoding lives in `format` and `engine`.
//! Integer keys are encoded big-endian (`format::id_key`).

use std::ops::Bound;
use std::path::PathBuf;

use crate::error::Result;

pub mod conformance;
pub mod mem;
pub mod redb;
#[cfg(feature = "lmdb")]
pub mod heed;

/// Logical tables (identical contract on every backend).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Table {
    /// name (utf-8) -> value (see `format::meta_key`)
    Meta,
    /// user key -> encoded `Manifest`
    Records,
    /// object id (u64 BE) -> envelope
    Objects,
    /// `format::candidate_key` -> id list (u64 LE each)
    HashCandidates,
    /// object id (u64 BE) -> u64 LE reference count
    Refcounts,
    /// param id (u64 BE) -> encoded `Param`
    Params,
    /// `format::history_key` -> encoded `Manifest`
    History,
    /// source id (u64 BE) -> encoded `SourceDescriptor`
    Sources,
    /// import id (u64 BE) -> id list of objects prepared by an unfinished import
    PendingImports,
}

impl Table {
    pub const ALL: [Table; 9] = [
        Table::Meta,
        Table::Records,
        Table::Objects,
        Table::HashCandidates,
        Table::Refcounts,
        Table::Params,
        Table::History,
        Table::Sources,
        Table::PendingImports,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Table::Meta => "meta",
            Table::Records => "records",
            Table::Objects => "objects",
            Table::HashCandidates => "hash_candidates",
            Table::Refcounts => "refcounts",
            Table::Params => "params",
            Table::History => "history",
            Table::Sources => "sources",
            Table::PendingImports => "pending_imports",
        }
    }

    pub fn index(self) -> usize {
        self as usize
    }
}

/// Commit durability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Durable when `commit` returns (fsync or equivalent).
    Immediate,
    /// Visible to later transactions; may be lost on crash until a later
    /// `Immediate` commit (redb `Durability::None`). Backends without an
    /// equivalent must treat it as `Immediate`.
    Deferred,
}

/// Callback used by `scan`; return `Ok(false)` to stop early.
pub type ScanFn<'f> = dyn FnMut(&[u8], &[u8]) -> Result<bool> + 'f;

pub trait ReadTxn {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Ordered visit of the entries with keys inside `(start, end)`.
    /// `reverse` visits in descending key order.
    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()>;

    /// Number of entries in a table.
    fn len(&self, table: Table) -> Result<u64>;
}

pub trait WriteTxn: ReadTxn {
    fn put(&mut self, table: Table, key: &[u8], value: &[u8]) -> Result<()>;

    /// Returns whether the key existed.
    fn remove(&mut self, table: Table, key: &[u8]) -> Result<bool>;

    /// Publish every change atomically. Dropping without commit aborts.
    fn commit(self, durability: Durability) -> Result<()>;
}

/// A storage backend. Readers see consistent snapshots (MVCC); there is at
/// most one write transaction at a time.
pub trait Store: Send + Sync + 'static {
    type Read<'a>: ReadTxn
    where
        Self: 'a;
    type Write<'a>: WriteTxn
    where
        Self: 'a;

    fn begin_read(&self) -> Result<Self::Read<'_>>;

    /// Blocks until no other write transaction is active.
    fn begin_write(&self) -> Result<Self::Write<'_>>;

    /// Try to return free space to the file system. Requires no live transactions.
    /// Returns `Ok(false)` when the backend does not support it.
    fn compact(&mut self) -> Result<bool>;

    /// Files that hold the database (for size accounting).
    fn files(&self) -> Vec<PathBuf>;

    fn backend_name(&self) -> &'static str;
}
