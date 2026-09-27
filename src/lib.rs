//! babeldb — embedded key-value store inspired by the Library of Babel.
//!
//! Two explicitly separated variants:
//! - `Mode::BabelPure`: every unit stored as the seed of a reversible affine map
//!   (demonstrates addressing without search; saves no space by construction).
//! - `Mode::Adaptive`: smallest exact representation among recipes, raw, LZ4,
//!   Zstd, templates and shared (deduplicated) objects.
//!
//! Every byte needed to recover data is persisted locally and accounted for.

pub mod cache;
pub mod chunk;
pub mod cli;
pub mod codec;
pub mod config;
pub mod datasets;
pub mod engine;
pub mod error;
pub mod format;
pub mod generator;
pub mod hash;
pub mod ingest;
pub mod maintenance;
pub mod planner;
pub mod source;
pub mod stats;
pub mod store;
pub mod sys;

pub use config::{CodecPolicy, Config, Mode};
pub use engine::{BatchOp, Db, Expect, Revision, ScanItem, ScanOptions};
pub use error::{Error, Result};
pub use ingest::ImportOptions;
pub use store::mem::MemStore;
pub use store::redb::RedbStore;
