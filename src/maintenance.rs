//! Maintenance reports (the procedures are `Db::verify`, `Db::gc`, `Db::compact`).

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
