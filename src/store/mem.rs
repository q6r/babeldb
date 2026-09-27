//! In-memory reference backend used by tests. Snapshots are `Arc`s of the whole
//! table set; a write transaction clones the tables and swaps them on commit.
//! Not durable and O(size) per write: never use it for benchmarks.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use super::{Durability, ReadTxn, ScanFn, Store, Table, WriteTxn};
use crate::error::{Error, Result};

type Tables = Vec<BTreeMap<Vec<u8>, Vec<u8>>>;

pub struct MemStore {
    current: RwLock<Arc<Tables>>,
    writer: Mutex<()>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStore {
    pub fn new() -> Self {
        MemStore {
            current: RwLock::new(Arc::new(vec![BTreeMap::new(); Table::ALL.len()])),
            writer: Mutex::new(()),
        }
    }
}

pub struct MemRead {
    snap: Arc<Tables>,
}

pub struct MemWrite<'a> {
    store: &'a MemStore,
    tables: Tables,
    _guard: MutexGuard<'a, ()>,
}

fn scan_map(
    map: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
    f: &mut ScanFn<'_>,
) -> Result<()> {
    if let (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) = (start, end) {
        if s > e {
            return Ok(());
        }
    }
    let range = map.range::<[u8], _>((start, end));
    if reverse {
        for (k, v) in range.rev() {
            if !f(k, v)? {
                break;
            }
        }
    } else {
        for (k, v) in range {
            if !f(k, v)? {
                break;
            }
        }
    }
    Ok(())
}

impl ReadTxn for MemRead {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snap[table.index()].get(key).cloned())
    }

    fn scan(&self, table: Table, start: Bound<&[u8]>, end: Bound<&[u8]>, reverse: bool, f: &mut ScanFn<'_>) -> Result<()> {
        scan_map(&self.snap[table.index()], start, end, reverse, f)
    }

    fn len(&self, table: Table) -> Result<u64> {
        Ok(self.snap[table.index()].len() as u64)
    }
}

impl ReadTxn for MemWrite<'_> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.tables[table.index()].get(key).cloned())
    }

    fn scan(&self, table: Table, start: Bound<&[u8]>, end: Bound<&[u8]>, reverse: bool, f: &mut ScanFn<'_>) -> Result<()> {
        scan_map(&self.tables[table.index()], start, end, reverse, f)
    }

    fn len(&self, table: Table) -> Result<u64> {
        Ok(self.tables[table.index()].len() as u64)
    }
}

impl WriteTxn for MemWrite<'_> {
    fn put(&mut self, table: Table, key: &[u8], value: &[u8]) -> Result<()> {
        self.tables[table.index()].insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn remove(&mut self, table: Table, key: &[u8]) -> Result<bool> {
        Ok(self.tables[table.index()].remove(key).is_some())
    }

    fn commit(self, _durability: Durability) -> Result<()> {
        let mut cur = self.store.current.write().map_err(|_| Error::backend("poisoned lock"))?;
        *cur = Arc::new(self.tables);
        Ok(())
    }
}

impl Store for MemStore {
    type Read<'a> = MemRead;
    type Write<'a> = MemWrite<'a>;

    fn begin_read(&self) -> Result<MemRead> {
        let snap = self.current.read().map_err(|_| Error::backend("poisoned lock"))?.clone();
        Ok(MemRead { snap })
    }

    fn begin_write(&self) -> Result<MemWrite<'_>> {
        let guard = self.writer.lock().map_err(|_| Error::backend("poisoned lock"))?;
        let tables = (**self.current.read().map_err(|_| Error::backend("poisoned lock"))?).clone();
        Ok(MemWrite { store: self, tables, _guard: guard })
    }

    fn compact(&mut self) -> Result<bool> {
        Ok(false)
    }

    fn files(&self) -> Vec<PathBuf> {
        Vec::new()
    }

    fn backend_name(&self) -> &'static str {
        "mem"
    }
}
