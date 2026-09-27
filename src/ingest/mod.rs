//! Streaming ingestion with bounded memory. Objects are prepared in batches
//! (registered in `pending_imports`); the manifest that makes the record
//! visible, and the source's `last_import`, are published in one final
//! durable transaction. An interrupted import publishes nothing; its objects
//! are collected by `Db::gc`.

pub mod file;

use crate::engine::Expect;
use crate::error::Result;

/// A source of bytes read block by block.
pub trait ByteSource {
    /// Fill `buf` as much as possible; return 0 at end of input.
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize>;
}

#[derive(Clone, Debug)]
pub struct ImportOptions {
    pub expect: Expect,
    /// Existing source id to attach and update.
    pub source_id: Option<u64>,
}

impl Default for ImportOptions {
    fn default() -> Self {
        ImportOptions { expect: Expect::Any, source_id: None }
    }
}

#[allow(unused_variables)]
impl<S: crate::store::Store> crate::engine::Db<S> {
    /// Stream `source` into `key` with memory bounded by the block size and
    /// `Config::import_batch_bytes`. Nothing becomes visible before the final commit.
    pub fn import(&self, key: &[u8], source: &mut dyn ByteSource, opts: &ImportOptions) -> Result<crate::engine::Revision> {
        todo!()
    }

    /// Import a local file byte-for-byte. When `opts.source_id` is None a
    /// `LOCAL_FILE` source is registered (or reused by location) and attached.
    pub fn import_file(&self, key: &[u8], path: &std::path::Path, opts: &ImportOptions) -> Result<crate::engine::Revision> {
        todo!()
    }
}
