//! Optional import provenance. The descriptor never replaces stored content:
//! a confirmed import stays readable after its source disappears.

pub use crate::format::{source_kind, ImportInfo, SourceDescriptor};

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
