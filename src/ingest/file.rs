//! Exact import of local files (no normalization of any byte).

use std::fs::File;
use std::io::Read;
use std::path::Path;

use super::ByteSource;
use crate::error::Result;

pub struct FileSource {
    file: File,
}

impl FileSource {
    pub fn open(path: &Path) -> Result<FileSource> {
        Ok(FileSource { file: File::open(path)? })
    }
}

impl ByteSource for FileSource {
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = self.file.read(&mut buf[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        Ok(filled)
    }
}
