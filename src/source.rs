//! Optional import provenance. The descriptor never replaces stored content:
//! a confirmed import stays readable after its source disappears.
//!
//! Typed helpers over `Table::Sources` (source id, u64 BE -> encoded
//! `SourceDescriptor`), usable inside any read or write transaction.

use std::ops::Bound;
use std::path::Path;

use crate::engine::ops;
use crate::error::Result;
use crate::format::{self, id_key, meta_key};
use crate::store::{ReadTxn, Table, WriteTxn};

pub use crate::format::{ImportInfo, SourceDescriptor, source_kind};

/// Adapter version of the local-file importer.
pub const LOCAL_FILE_ADAPTER_VERSION: u16 = 1;

impl SourceDescriptor {
    pub fn local_file(location: impl Into<String>) -> SourceDescriptor {
        SourceDescriptor {
            kind: source_kind::LOCAL_FILE,
            location: location.into(),
            adapter_version: LOCAL_FILE_ADAPTER_VERSION,
            last_import: None,
        }
    }
}

/// Descriptor stored under `id`, if any. Undecodable descriptors are format errors.
pub fn load_source<T: ReadTxn + ?Sized>(t: &T, id: u64) -> Result<Option<SourceDescriptor>> {
    t.get(Table::Sources, &id_key(id))?
        .map(|b| SourceDescriptor::decode(&b))
        .transpose()
}

/// Write (insert or replace) the descriptor of `id`.
pub fn put_source<W: WriteTxn + ?Sized>(w: &mut W, id: u64, desc: &SourceDescriptor) -> Result<()> {
    w.put(Table::Sources, &id_key(id), &desc.encode())
}

/// Register a new descriptor under a freshly allocated source id (never reused).
pub fn insert_source<W: WriteTxn + ?Sized>(w: &mut W, desc: &SourceDescriptor) -> Result<u64> {
    let id = ops::alloc_id(w, meta_key::NEXT_SOURCE_ID, "source id")?;
    put_source(w, id, desc)?;
    Ok(id)
}

/// Lowest-id source of `kind` whose location is exactly `location`.
/// Scans `sources` (O(number of sources)); an undecodable entry is an error.
pub fn find_source_by_location<T: ReadTxn + ?Sized>(
    t: &T,
    kind: u8,
    location: &str,
) -> Result<Option<(u64, SourceDescriptor)>> {
    let mut found = None;
    t.scan(
        Table::Sources,
        Bound::Unbounded,
        Bound::Unbounded,
        false,
        &mut |k: &[u8], v: &[u8]| {
            let desc = SourceDescriptor::decode(v)?;
            if desc.kind == kind && desc.location == location {
                found = Some((format::parse_id_key(k)?, desc));
                return Ok(false);
            }
            Ok(true)
        },
    )?;
    Ok(found)
}

/// Every registered source, ordered by id.
pub fn list_sources<T: ReadTxn + ?Sized>(t: &T) -> Result<Vec<(u64, SourceDescriptor)>> {
    let mut out = Vec::new();
    t.scan(
        Table::Sources,
        Bound::Unbounded,
        Bound::Unbounded,
        false,
        &mut |k: &[u8], v: &[u8]| {
            out.push((format::parse_id_key(k)?, SourceDescriptor::decode(v)?));
            Ok(true)
        },
    )?;
    Ok(out)
}

/// Location recorded for a local file: its canonical absolute path (symlinks
/// resolved; on Windows the verbatim `\?\` form returned by the OS). Paths
/// that are not valid Unicode are converted lossily: the location is
/// provenance only and never used to read the stored content back.
pub fn local_file_location(path: &Path) -> Result<String> {
    let canonical = std::fs::canonicalize(path)?;
    Ok(canonical.to_string_lossy().into_owned())
}
