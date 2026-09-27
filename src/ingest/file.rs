//! Exact import of local files (no normalization of any byte).

use std::fs::File;
use std::path::Path;

use super::{ByteSource, read_full};
use crate::error::Result;

/// Reads a local file block by block, byte for byte.
pub struct FileSource {
    file: File,
}

impl FileSource {
    pub fn open(path: &Path) -> Result<FileSource> {
        Ok(FileSource {
            file: File::open(path)?,
        })
    }
}

impl ByteSource for FileSource {
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
        read_full(&mut self.file, buf)
    }
}
