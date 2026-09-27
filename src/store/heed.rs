//! LMDB backend through heed (feature `lmdb`), for the backend comparison.
//! SKELETON — to be implemented by the LMDB agent.

use std::path::{Path, PathBuf};

use crate::error::Result;

pub struct HeedStore {
    dir: PathBuf,
}

impl HeedStore {
    /// Open or create an LMDB environment in `dir` with the given map size.
    pub fn open(dir: impl AsRef<Path>, map_size: usize) -> Result<HeedStore> {
        let _ = (dir.as_ref(), map_size);
        todo!("HeedStore::open")
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}
