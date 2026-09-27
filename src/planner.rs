//! Representation selector. BabelPure always uses `BabelAffineV1`. Adaptive
//! evaluates a small family of bounded-work candidates and keeps the smallest
//! serialized body that passes the decode-cost policy and an exact roundtrip;
//! otherwise it falls back to `RawV1` and records the fallback.
//! SKELETON — minimal behavior; the codec agent implements the full selection
//! and the training helpers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::codec::{self, Deps, Encoded, Template, ZstdDict};
use crate::config::{CodecPolicy, Mode};
use crate::error::Result;
use crate::format::CodecTag;

/// Counters per codec (indexed by position in `CodecTag::ALL_V1`).
#[derive(Default)]
pub struct PlannerStats {
    pub chosen: [AtomicU64; 7],
    /// No candidate satisfied the decode budget; Raw was used.
    pub budget_fallbacks: AtomicU64,
    /// A candidate failed its exact roundtrip (bug guard); Raw was used.
    pub roundtrip_failures: AtomicU64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlannerSnapshot {
    pub chosen: Vec<(String, u64)>,
    pub budget_fallbacks: u64,
    pub roundtrip_failures: u64,
}

pub struct Planner {
    mode: Mode,
    policy: CodecPolicy,
    dict: Option<Arc<ZstdDict>>,
    template: Option<Arc<Template>>,
    stats: PlannerStats,
}

impl Planner {
    pub fn new(mode: Mode, policy: CodecPolicy) -> Planner {
        Planner { mode, policy, dict: None, template: None, stats: PlannerStats::default() }
    }

    pub fn with_params(mut self, dict: Option<Arc<ZstdDict>>, template: Option<Arc<Template>>) -> Planner {
        self.dict = dict;
        self.template = template;
        self
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn policy(&self) -> &CodecPolicy {
        &self.policy
    }

    pub fn dictionary(&self) -> Option<&Arc<ZstdDict>> {
        self.dict.as_ref()
    }

    pub fn template(&self) -> Option<&Arc<Template>> {
        self.template.as_ref()
    }

    /// Exact representation of one unit (block or inline value).
    pub fn encode_unit(&self, data: &[u8]) -> Encoded {
        let enc = match self.mode {
            Mode::BabelPure => Encoded { codec: CodecTag::BABEL_AFFINE_V1, aux_id: 0, body: codec::babel_affine::encode(data) },
            Mode::Adaptive => Encoded { codec: CodecTag::RAW_V1, aux_id: 0, body: codec::raw::encode(data) },
        };
        self.record(enc.codec);
        enc
    }

    /// Deps needed to decode what this planner produced.
    pub fn deps(&self) -> Deps<'_> {
        Deps { zstd_dict: self.dict.as_deref(), template: self.template.as_deref() }
    }

    fn record(&self, codec: CodecTag) {
        if let Some(i) = CodecTag::ALL_V1.iter().position(|c| *c == codec) {
            self.stats.chosen[i].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn stats(&self) -> &PlannerStats {
        &self.stats
    }

    pub fn snapshot(&self) -> PlannerSnapshot {
        PlannerSnapshot {
            chosen: CodecTag::ALL_V1
                .iter()
                .zip(self.stats.chosen.iter())
                .map(|(c, n)| (c.name().to_string(), n.load(Ordering::Relaxed)))
                .collect(),
            budget_fallbacks: self.stats.budget_fallbacks.load(Ordering::Relaxed),
            roundtrip_failures: self.stats.roundtrip_failures.load(Ordering::Relaxed),
        }
    }
}

/// Result of evaluating a shared dependency (dictionary or template) on
/// validation samples that were NOT used for training.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TrainReport {
    pub kind: String,
    pub param_bytes: usize,
    pub train_samples: usize,
    pub validation_samples: usize,
    /// Sum of best body sizes on validation samples without the dependency.
    pub validation_bytes_without: u64,
    /// Same with the dependency.
    pub validation_bytes_with: u64,
    /// (without - with) / validation_samples * expected_uses - param_bytes - overhead.
    pub projected_net_gain: i64,
    pub installed: bool,
    pub param_id: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct TrainOptions {
    /// How many future units are expected to use the dependency (amortization).
    pub expected_uses: u64,
    /// Fraction of samples held out for validation (0 < f < 1).
    pub validation_fraction: f64,
    pub max_dict_bytes: usize,
    /// Install only if projected_net_gain > 0 (set false to force for experiments).
    pub require_gain: bool,
}

impl Default for TrainOptions {
    fn default() -> Self {
        TrainOptions { expected_uses: 10_000, validation_fraction: 0.2, max_dict_bytes: 64 * 1024, require_gain: true }
    }
}

/// Train and evaluate a Zstd dictionary. Returns the dictionary bytes when the
/// projected net gain is positive (or `require_gain` is false).
pub fn train_zstd_dictionary(samples: &[Vec<u8>], level: i32, opts: &TrainOptions) -> Result<(Option<Vec<u8>>, TrainReport)> {
    let _ = (samples, level, opts);
    todo!("planner::train_zstd_dictionary")
}

/// Train and evaluate a template for `TemplatePatchV1`.
pub fn train_template(samples: &[Vec<u8>], opts: &TrainOptions) -> Result<(Option<Vec<u8>>, TrainReport)> {
    let _ = (samples, opts);
    todo!("planner::train_template")
}
