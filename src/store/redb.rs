//! redb backend: one file of copy-on-write B+trees, MVCC readers, one writer.
//!
//! Every logical table is a `TableDefinition<&[u8], &[u8]>` named `Table::name()`.
//! `open` creates the missing ones, so read transactions never see a missing table.
//!
//! Durability, as implemented by redb 4.3.0 (checked in its sources):
//! - `Immediate` maps to `redb::Durability::Immediate`: a checksummed 1-phase commit that
//!   ends with one fsync (`FlushFileBuffers` on Windows) before `commit` returns.
//! - `Deferred` maps to `redb::Durability::None`: the commit is published in memory (every
//!   later transaction sees it) and its pages are written without fsync, while the header
//!   on disk still names the last durable commit. The next `Immediate` commit is built on
//!   top of that state and makes every earlier deferred commit durable with it; closing the
//!   store cleanly (dropping it) also ends with a durable commit. If the process dies first,
//!   reopening rolls back to the last durable commit (whole transactions only).
//!
//! A redb `Table` borrows its `WriteTransaction`, so write transactions reopen the table on
//! every call instead of caching handles next to it. Scans inside a write transaction copy
//! entries out in bounded batches and close the table before running the callback, so the
//! callback may read the same table again (redb allows one open handle per table and
//! transaction). Read transactions open each table at most once, on first use, and scan
//! without copying.

use std::marker::PhantomData;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use ::redb::{
    AccessGuard, Builder, Database, Durability as RedbDurability, ReadOnlyTable, ReadTransaction,
    ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageError, TableDefinition,
    TableHandle, WriteTransaction,
};

use super::{Durability, ReadTxn, ScanFn, Store, Table, WriteTxn};
use crate::error::{Error, Result};

type Bytes = &'static [u8];
type ReadTable = ReadOnlyTable<Bytes, Bytes>;
type Entry<'a> =
    std::result::Result<(AccessGuard<'a, Bytes>, AccessGuard<'a, Bytes>), StorageError>;

/// Entries copied per table open by a write-transaction scan: starts small so short or
/// early-stopped scans stay cheap, then doubles.
const WRITE_SCAN_FIRST_BATCH: usize = 32;
const WRITE_SCAN_MAX_BATCH: usize = 4096;
/// A batch also ends once it holds this many key+value bytes (it always holds one entry).
const WRITE_SCAN_BATCH_BYTES: usize = 1 << 20;

fn definition(table: Table) -> TableDefinition<'static, Bytes, Bytes> {
    TableDefinition::new(table.name())
}

pub struct RedbStore {
    db: Database,
    path: PathBuf,
}

impl RedbStore {
    /// Open or create the database file. Creates every table of `Table::ALL`
    /// so that read transactions never observe a missing table.
    pub fn open(path: impl AsRef<Path>, cache_bytes: usize) -> Result<RedbStore> {
        let path = path.as_ref().to_path_buf();
        let db = Builder::new()
            .set_cache_size(cache_bytes)
            .create(&path)
            .map_err(Error::backend)?;
        create_tables(&db)?;
        Ok(RedbStore { db, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Opens every table in one write transaction (which also checks the types of existing
/// ones) and commits only when some table was missing, so reopening costs no fsync.
fn create_tables(db: &Database) -> Result<()> {
    let txn = db.begin_write().map_err(Error::backend)?;
    let existing: Vec<String> = txn
        .list_tables()
        .map_err(Error::backend)?
        .map(|h| h.name().to_string())
        .collect();
    let mut missing = false;
    for table in Table::ALL {
        missing |= !existing.iter().any(|name| name == table.name());
        txn.open_table(definition(table)).map_err(Error::backend)?;
    }
    if missing {
        txn.commit().map_err(Error::backend)
    } else {
        txn.abort().map_err(Error::backend)
    }
}

fn bound_slice(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    b.as_ref().map(Vec::as_slice)
}

/// Zero-copy visit of a redb range.
fn visit<'a>(entries: impl Iterator<Item = Entry<'a>>, f: &mut ScanFn<'_>) -> Result<()> {
    for entry in entries {
        let (k, v) = entry.map_err(Error::backend)?;
        if !f(k.value(), v.value())? {
            break;
        }
    }
    Ok(())
}

/// Keys and values copied out of a table, stored back to back in one buffer.
#[derive(Default)]
struct Batch {
    data: Vec<u8>,
    /// (key end, value end) offsets into `data`; each entry starts where the previous ended.
    ends: Vec<(usize, usize)>,
}

impl Batch {
    fn clear(&mut self) {
        self.data.clear();
        self.ends.clear();
    }

    /// Copies up to `limit` entries; returns true when `entries` ran out.
    fn fill<'a>(&mut self, entries: impl Iterator<Item = Entry<'a>>, limit: usize) -> Result<bool> {
        for entry in entries {
            let (k, v) = entry.map_err(Error::backend)?;
            self.data.extend_from_slice(k.value());
            let key_end = self.data.len();
            self.data.extend_from_slice(v.value());
            self.ends.push((key_end, self.data.len()));
            if self.ends.len() >= limit || self.data.len() >= WRITE_SCAN_BATCH_BYTES {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let mut start = 0;
        self.ends.iter().map(move |&(key_end, value_end)| {
            let entry = (&self.data[start..key_end], &self.data[key_end..value_end]);
            start = value_end;
            entry
        })
    }

    fn last_key(&self) -> Option<&[u8]> {
        let (key_end, _) = *self.ends.last()?;
        let start = match self.ends.len() {
            1 => 0,
            n => self.ends[n - 2].1,
        };
        Some(&self.data[start..key_end])
    }
}

/// Read transaction over a consistent snapshot. Tables are opened lazily, once.
pub struct RedbRead {
    txn: ReadTransaction,
    tables: [OnceLock<ReadTable>; Table::ALL.len()],
}

impl RedbRead {
    fn table(&self, table: Table) -> Result<&ReadTable> {
        let slot = self
            .tables
            .get(table.index())
            .ok_or_else(|| Error::backend(format!("table {table:?} missing from Table::ALL")))?;
        if let Some(t) = slot.get() {
            return Ok(t);
        }
        let opened = self
            .txn
            .open_table(definition(table))
            .map_err(Error::backend)?;
        Ok(slot.get_or_init(|| opened))
    }
}

impl ReadTxn for RedbRead {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = self.table(table)?.get(key).map_err(Error::backend)?;
        Ok(guard.map(|v| v.value().to_vec()))
    }

    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()> {
        let range = self
            .table(table)?
            .range::<&[u8]>((start, end))
            .map_err(Error::backend)?;
        if reverse {
            visit(range.rev(), f)
        } else {
            visit(range, f)
        }
    }

    fn len(&self, table: Table) -> Result<u64> {
        self.table(table)?.len().map_err(Error::backend)
    }
}

/// Write transaction. Dropping it without `commit` aborts it: redb 4.3 rolls back an
/// uncompleted `WriteTransaction` in its `Drop`.
pub struct RedbWrite<'a> {
    txn: WriteTransaction,
    _store: PhantomData<&'a RedbStore>,
}

impl RedbWrite<'_> {
    fn open(&self, table: Table) -> Result<::redb::Table<'_, Bytes, Bytes>> {
        self.txn
            .open_table(definition(table))
            .map_err(Error::backend)
    }
}

impl ReadTxn for RedbWrite<'_> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let t = self.open(table)?;
        let value = t
            .get(key)
            .map_err(Error::backend)?
            .map(|v| v.value().to_vec());
        Ok(value)
    }

    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()> {
        let mut lower = start.map(<[u8]>::to_vec);
        let mut upper = end.map(<[u8]>::to_vec);
        let mut batch = Batch::default();
        let mut limit = WRITE_SCAN_FIRST_BATCH;
        loop {
            batch.clear();
            let exhausted = {
                let t = self.open(table)?;
                let range = t
                    .range::<&[u8]>((bound_slice(&lower), bound_slice(&upper)))
                    .map_err(Error::backend)?;
                if reverse {
                    batch.fill(range.rev(), limit)?
                } else {
                    batch.fill(range, limit)?
                }
            };
            for (k, v) in batch.iter() {
                if !f(k, v)? {
                    return Ok(());
                }
            }
            let resume = match batch.last_key() {
                Some(last) if !exhausted => Bound::Excluded(last.to_vec()),
                _ => return Ok(()),
            };
            if reverse {
                upper = resume;
            } else {
                lower = resume;
            }
            limit = (limit * 2).min(WRITE_SCAN_MAX_BATCH);
        }
    }

    fn len(&self, table: Table) -> Result<u64> {
        self.open(table)?.len().map_err(Error::backend)
    }
}

impl WriteTxn for RedbWrite<'_> {
    fn put(&mut self, table: Table, key: &[u8], value: &[u8]) -> Result<()> {
        let mut t = self.open(table)?;
        t.insert(key, value).map_err(Error::backend)?;
        Ok(())
    }

    fn remove(&mut self, table: Table, key: &[u8]) -> Result<bool> {
        let mut t = self.open(table)?;
        let existed = t.remove(key).map_err(Error::backend)?.is_some();
        Ok(existed)
    }

    fn commit(self, durability: Durability) -> Result<()> {
        let mut txn = self.txn;
        txn.set_durability(match durability {
            Durability::Immediate => RedbDurability::Immediate,
            Durability::Deferred => RedbDurability::None,
        })
        .map_err(Error::backend)?;
        txn.commit().map_err(Error::backend)
    }
}

impl Store for RedbStore {
    type Read<'a> = RedbRead;
    type Write<'a> = RedbWrite<'a>;

    fn begin_read(&self) -> Result<RedbRead> {
        let txn = self.db.begin_read().map_err(Error::backend)?;
        Ok(RedbRead {
            txn,
            tables: Default::default(),
        })
    }

    fn begin_write(&self) -> Result<RedbWrite<'_>> {
        let txn = self.db.begin_write().map_err(Error::backend)?;
        Ok(RedbWrite {
            txn,
            _store: PhantomData,
        })
    }

    /// Runs redb compaction; `Ok(true)` means it ran (whether redb moved any page is not
    /// part of the contract). Fails while any `RedbRead` is still alive.
    fn compact(&mut self) -> Result<bool> {
        self.db.compact().map_err(Error::backend)?;
        Ok(true)
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn backend_name(&self) -> &'static str {
        "redb"
    }
}
