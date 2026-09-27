//! redb backend (single file, B+trees copy-on-write, MVCC, one writer).
//! SKELETON — to be implemented by the store agent.

use std::ops::Bound;
use std::path::{Path, PathBuf};

use super::{Durability, ReadTxn, ScanFn, Store, Table, WriteTxn};
use crate::error::Result;

pub struct RedbStore {
    path: PathBuf,
}

impl RedbStore {
    /// Open or create the database file. Creates every table of `Table::ALL`
    /// so that read transactions never observe a missing table.
    pub fn open(path: impl AsRef<Path>, cache_bytes: usize) -> Result<RedbStore> {
        let _ = cache_bytes;
        let _ = path.as_ref();
        todo!("RedbStore::open")
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct RedbRead {}

pub struct RedbWrite<'a> {
    _store: &'a RedbStore,
}

impl ReadTxn for RedbRead {
    fn get(&self, _table: Table, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }
    fn scan(&self, _table: Table, _start: Bound<&[u8]>, _end: Bound<&[u8]>, _reverse: bool, _f: &mut ScanFn<'_>) -> Result<()> {
        todo!()
    }
    fn len(&self, _table: Table) -> Result<u64> {
        todo!()
    }
}

impl ReadTxn for RedbWrite<'_> {
    fn get(&self, _table: Table, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }
    fn scan(&self, _table: Table, _start: Bound<&[u8]>, _end: Bound<&[u8]>, _reverse: bool, _f: &mut ScanFn<'_>) -> Result<()> {
        todo!()
    }
    fn len(&self, _table: Table) -> Result<u64> {
        todo!()
    }
}

impl WriteTxn for RedbWrite<'_> {
    fn put(&mut self, _table: Table, _key: &[u8], _value: &[u8]) -> Result<()> {
        todo!()
    }
    fn remove(&mut self, _table: Table, _key: &[u8]) -> Result<bool> {
        todo!()
    }
    fn commit(self, _durability: Durability) -> Result<()> {
        todo!()
    }
}

impl Store for RedbStore {
    type Read<'a> = RedbRead;
    type Write<'a> = RedbWrite<'a>;

    fn begin_read(&self) -> Result<RedbRead> {
        todo!()
    }
    fn begin_write(&self) -> Result<RedbWrite<'_>> {
        todo!()
    }
    fn compact(&mut self) -> Result<bool> {
        todo!()
    }
    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }
    fn backend_name(&self) -> &'static str {
        "redb"
    }
}
