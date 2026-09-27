//! Maintenance and training: `Db::verify`, `Db::gc`, `Db::compact`,
//! `Db::train_dictionary`, `Db::train_template`.

use crate::engine::Db;
use crate::error::Result;
use crate::planner::{TrainOptions, TrainReport};
use crate::store::Store;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyReport {
    pub deep: bool,
    pub records_checked: u64,
    pub history_checked: u64,
    pub objects_checked: u64,
    pub objects_decoded: u64,
    pub missing_objects: u64,
    pub refcount_mismatches: u64,
    pub digest_failures: u64,
    pub dangling_candidates: u64,
    pub missing_params: u64,
    /// Objects with refcount but no reference (collectable by gc).
    pub orphan_objects: u64,
    pub pending_imports: u64,
    /// Human-readable description of the first problems found (bounded).
    pub issues: Vec<String>,
}

impl VerifyReport {
    /// No inconsistency that could return wrong bytes or lose data.
    pub fn ok(&self) -> bool {
        self.missing_objects == 0
            && self.refcount_mismatches == 0
            && self.digest_failures == 0
            && self.dangling_candidates == 0
            && self.missing_params == 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub abandoned_imports: u64,
    pub objects_removed: u64,
    pub candidates_removed: u64,
    pub params_removed: u64,
    pub refcounts_fixed: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactReport {
    pub supported: bool,
    pub apparent_before: u64,
    pub apparent_after: u64,
    pub allocated_before: Option<u64>,
    pub allocated_after: Option<u64>,
}

#[allow(unused_variables)]
impl<S: Store> Db<S> {
    /// Consistency check. `deep` also decodes every object and checks digests.
    pub fn verify(&self, deep: bool) -> Result<VerifyReport> {
        todo!()
    }

    /// Exclusive maintenance: abandoned imports, orphan objects/candidates/params,
    /// refcount drift.
    pub fn gc(&mut self) -> Result<GcReport> {
        todo!()
    }

    /// Exclusive: ask the backend to return free space to the file system.
    pub fn compact(&mut self) -> Result<CompactReport> {
        todo!()
    }

    /// Train a Zstd dictionary from samples, evaluate it on held-out samples and
    /// install it as the active dictionary only if the projected net gain is positive.
    pub fn train_dictionary(&self, samples: &[Vec<u8>], opts: &TrainOptions) -> Result<TrainReport> {
        todo!()
    }

    /// Same for a `TemplatePatchV1` template.
    pub fn train_template(&self, samples: &[Vec<u8>], opts: &TrainOptions) -> Result<TrainReport> {
        todo!()
    }
}
