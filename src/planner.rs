//! Representation selector. BabelPure always uses `BabelAffineV1`. Adaptive
//! evaluates a small family of bounded-work candidates and keeps the smallest
//! serialized body that passes the decode-cost policy and an exact roundtrip;
//! otherwise it falls back to `RawV1` and records the fallback.
//!
//! Adaptive candidates, in tie-break order (cheaper decode first): Raw, Repeat,
//! ArithmeticU64, LZ4, TemplatePatch (active template), Zstd, Zstd with the
//! active dictionary. The envelope costs the same 64 bytes for every codec and
//! installed dependencies are already paid for, so the smallest body wins.
//!
//! Every chosen encoding is decoded again (`codec::decode` with the planner's
//! deps) and compared with the input before it is returned; a mismatch returns
//! `RawV1` and increments `roundtrip_failures`. `RawV1` itself is the input and
//! needs no check.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::codec::{self, Deps, Encoded, Template, ZstdDict};
use crate::config::{CodecPolicy, DecodeCostModel, Mode};
use crate::error::{Error, Result};
use crate::format::{CodecTag, MAX_UNIT_LEN, codec_id};

/// Inputs shorter than this skip the general-purpose compressors (LZ4, Zstd,
/// dictionary, template): their framing rarely pays off below it.
pub const MIN_COMPRESS_LEN: usize = 32;
/// Longest motif `RepeatV1` looks for.
pub const MAX_REPEAT_PERIOD: usize = 4096;
/// Once a body this small exists, LZ4 and Zstd (whose block/frame framing
/// alone is larger) are not tried.
const FRAMED_MIN_BODY: usize = 8;
/// Bookkeeping bytes charged per installed dependency in `projected_net_gain`.
pub const PARAM_OVERHEAD_BYTES: i64 = 64;
/// Param id used for dependencies built only to evaluate training.
const EVAL_PARAM_ID: u64 = u64::MAX;
/// Verification scratch kept per thread between calls.
const SCRATCH_KEEP_BYTES: usize = 2 << 20;

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

/// Adaptive candidates; declaration order is the tie-break (decode cost) order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Candidate {
    Raw,
    Repeat,
    Arith,
    Lz4,
    Template,
    Zstd,
    ZstdDict,
}

impl Candidate {
    /// Evaluation order: recipes, then the template (which can beat framed
    /// codecs on tiny bodies), then LZ4 and Zstd.
    const EVAL_ORDER: [Candidate; 6] = [
        Candidate::Repeat,
        Candidate::Arith,
        Candidate::Template,
        Candidate::Lz4,
        Candidate::Zstd,
        Candidate::ZstdDict,
    ];

    fn codec(self) -> CodecTag {
        match self {
            Candidate::Raw => CodecTag::RAW_V1,
            Candidate::Repeat => CodecTag::REPEAT_V1,
            Candidate::Arith => CodecTag::ARITH_U64_V1,
            Candidate::Lz4 => CodecTag::LZ4_V1,
            Candidate::Template => CodecTag::TEMPLATE_PATCH_V1,
            Candidate::Zstd | Candidate::ZstdDict => CodecTag::ZSTD_V1,
        }
    }

    fn framed(self) -> bool {
        matches!(self, Candidate::Lz4 | Candidate::Zstd | Candidate::ZstdDict)
    }

    fn bit(self) -> u8 {
        1 << self as u8
    }
}

thread_local! {
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
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
        Planner {
            mode,
            policy,
            dict: None,
            template: None,
            stats: PlannerStats::default(),
        }
    }

    pub fn with_params(
        mut self,
        dict: Option<Arc<ZstdDict>>,
        template: Option<Arc<Template>>,
    ) -> Planner {
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
    ///
    /// Units longer than `MAX_UNIT_LEN` cannot be checked by `codec::decode`:
    /// BabelPure then returns the seed unchecked and Adaptive returns `RawV1`.
    pub fn encode_unit(&self, data: &[u8]) -> Encoded {
        let oversized = data.len() > MAX_UNIT_LEN as usize;
        let enc = match self.mode {
            Mode::BabelPure => {
                let enc = Encoded {
                    codec: CodecTag::BABEL_AFFINE_V1,
                    aux_id: 0,
                    body: codec::babel_affine::encode(data),
                };
                if oversized {
                    enc
                } else {
                    self.verified_or_raw(enc, data)
                }
            }
            Mode::Adaptive if oversized => raw(data),
            Mode::Adaptive => {
                let (kind, body) = self.select(data);
                match body {
                    Some(body) => {
                        let enc = Encoded {
                            codec: kind.codec(),
                            aux_id: self.aux_id(kind),
                            body,
                        };
                        self.verified_or_raw(enc, data)
                    }
                    None => raw(data),
                }
            }
        };
        self.record(enc.codec);
        enc
    }

    /// Deps needed to decode what this planner produced.
    pub fn deps(&self) -> Deps<'_> {
        Deps {
            zstd_dict: self.dict.as_deref(),
            template: self.template.as_deref(),
        }
    }

    fn aux_id(&self, kind: Candidate) -> u64 {
        match kind {
            Candidate::ZstdDict => self.dict.as_ref().map_or(0, |d| d.id),
            Candidate::Template => self.template.as_ref().map_or(0, |t| t.id),
            _ => 0,
        }
    }

    /// Whether `kind` applies to an input of `n` bytes under the policy.
    /// Dependencies with id 0 are never used (aux_id 0 means "none").
    fn enabled(&self, kind: Candidate, n: usize) -> bool {
        let p = &self.policy;
        let big = n >= MIN_COMPRESS_LEN;
        match kind {
            Candidate::Raw => true,
            Candidate::Repeat | Candidate::Arith => p.recipes,
            Candidate::Lz4 => p.lz4 && big,
            Candidate::Zstd => p.zstd && big,
            Candidate::ZstdDict => {
                p.zstd_dictionary && big && self.dict.as_ref().is_some_and(|d| d.id != 0)
            }
            Candidate::Template => {
                p.template && big && self.template.as_ref().is_some_and(|t| t.id != 0)
            }
        }
    }

    fn within_budget(&self, kind: Candidate, n: usize) -> bool {
        match self.policy.decode_budget_ns {
            Some(budget) if kind != Candidate::Raw => self
                .policy
                .cost_model
                .estimate_ns(kind.codec().id, n)
                .is_none_or(|est| est <= budget as f64),
            _ => true,
        }
    }

    /// Body of `kind` for `data`, if the codec applies.
    fn try_candidate(&self, kind: Candidate, data: &[u8]) -> Option<Vec<u8>> {
        match kind {
            Candidate::Raw => None,
            Candidate::Repeat => codec::repeat::recognize(data, MAX_REPEAT_PERIOD),
            Candidate::Arith => codec::arithmetic::recognize(data),
            Candidate::Lz4 => Some(codec::lz4::encode(data)),
            Candidate::Template => codec::template_patch::encode(data, self.template.as_deref()?),
            Candidate::Zstd => codec::zstd::encode(data, self.policy.zstd_level, None).ok(),
            Candidate::ZstdDict => {
                let d = self.dict.as_deref()?;
                codec::zstd::encode(data, d.level(), Some(d)).ok()
            }
        }
    }

    /// Smallest applicable body (None = Raw), counting a budget fallback when
    /// the budget left only Raw although a skipped codec would have been smaller.
    fn select(&self, data: &[u8]) -> (Candidate, Option<Vec<u8>>) {
        let n = data.len();
        let mut best: Option<(Candidate, Vec<u8>)> = None;
        let mut skipped = 0u8;
        for kind in Candidate::EVAL_ORDER {
            if !self.enabled(kind, n) {
                continue;
            }
            let (best_kind, best_len) = best
                .as_ref()
                .map_or((Candidate::Raw, n), |(k, b)| (*k, b.len()));
            if kind.framed() && best_len <= FRAMED_MIN_BODY {
                continue;
            }
            if !self.within_budget(kind, n) {
                skipped |= kind.bit();
                continue;
            }
            if let Some(body) = self.try_candidate(kind, data)
                && (body.len(), kind) < (best_len, best_kind)
            {
                best = Some((kind, body));
            }
        }
        match best {
            Some((kind, body)) => (kind, Some(body)),
            None => {
                if skipped != 0
                    && Candidate::EVAL_ORDER.iter().any(|&k| {
                        skipped & k.bit() != 0
                            && self.try_candidate(k, data).is_some_and(|b| b.len() < n)
                    })
                {
                    self.stats.budget_fallbacks.fetch_add(1, Ordering::Relaxed);
                }
                (Candidate::Raw, None)
            }
        }
    }

    /// Size of the body `encode_unit` would choose, without verification or
    /// statistics (training evaluation).
    fn best_body_len(&self, data: &[u8]) -> usize {
        match self.mode {
            Mode::BabelPure => data.len(),
            Mode::Adaptive => self.select(data).1.map_or(data.len(), |b| b.len()),
        }
    }

    fn verified_or_raw(&self, enc: Encoded, data: &[u8]) -> Encoded {
        if self.roundtrip_ok(&enc, data) {
            enc
        } else {
            self.stats
                .roundtrip_failures
                .fetch_add(1, Ordering::Relaxed);
            raw(data)
        }
    }

    /// Decode `enc` into a per-thread scratch buffer and compare with `data`.
    fn roundtrip_ok(&self, enc: &Encoded, data: &[u8]) -> bool {
        let Ok(raw_len) = u32::try_from(data.len()) else {
            return false;
        };
        let deps = self.deps();
        let check = |buf: &mut Vec<u8>| {
            codec::decode(enc.codec, enc.aux_id, &enc.body, raw_len, deps, buf).is_ok()
                && buf.as_slice() == data
        };
        SCRATCH
            .try_with(|cell| match cell.try_borrow_mut() {
                Ok(mut buf) => {
                    let ok = check(&mut buf);
                    if buf.capacity() > SCRATCH_KEEP_BYTES {
                        *buf = Vec::new();
                    }
                    ok
                }
                Err(_) => check(&mut Vec::new()),
            })
            .unwrap_or_else(|_| check(&mut Vec::new()))
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

fn raw(data: &[u8]) -> Encoded {
    Encoded {
        codec: CodecTag::RAW_V1,
        aux_id: 0,
        body: codec::raw::encode(data),
    }
}

/// Decode-cost estimates measured with `benches/codec.rs`.
///
/// Empty until measured (see the report of the codec work).
pub fn measured_cost_model() -> DecodeCostModel {
    let _ = codec_id::RAW;
    DecodeCostModel::default()
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
        TrainOptions {
            expected_uses: 10_000,
            validation_fraction: 0.2,
            max_dict_bytes: 64 * 1024,
            require_gain: true,
        }
    }
}

/// (training samples, validation samples).
type Split<'a> = (Vec<&'a [u8]>, Vec<&'a [u8]>);

/// Deterministic split: `round(n * f)` validation samples (at least 1, at most
/// n - 1) spread evenly over the input order; the rest trains.
fn split<'a>(samples: &'a [Vec<u8>], opts: &TrainOptions) -> Result<Split<'a>> {
    let f = opts.validation_fraction;
    if !(f > 0.0 && f < 1.0) {
        return Err(Error::InvalidArgument(format!(
            "validation_fraction {f} outside (0, 1)"
        )));
    }
    let n = samples.len();
    if n < 2 {
        return Err(Error::InvalidArgument(format!(
            "training needs at least 2 samples, got {n}"
        )));
    }
    let v = ((n as f64 * f).round() as usize).clamp(1, n - 1);
    let mut validation_mask = vec![false; n];
    for j in 0..v {
        let idx = ((2 * j as u128 + 1) * n as u128 / (2 * v as u128)) as usize;
        validation_mask[idx] = true;
    }
    let mut train = Vec::with_capacity(n - v);
    let mut validation = Vec::with_capacity(v);
    for (s, is_validation) in samples.iter().zip(validation_mask) {
        if is_validation {
            validation.push(s.as_slice());
        } else {
            train.push(s.as_slice());
        }
    }
    Ok((train, validation))
}

fn projected_net_gain(
    without: u64,
    with: u64,
    validation: usize,
    expected_uses: u64,
    param_bytes: usize,
) -> i64 {
    let per_use_total = (without as i128 - with as i128) * expected_uses as i128;
    let gain = per_use_total.div_euclid(validation.max(1) as i128)
        - param_bytes as i128
        - PARAM_OVERHEAD_BYTES as i128;
    gain.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Sum of the bodies `planner` would choose for `samples`.
fn total_best(planner: &Planner, samples: &[&[u8]]) -> u64 {
    samples
        .iter()
        .map(|s| planner.best_body_len(s) as u64)
        .sum()
}

/// Adaptive evaluation planner at `level` with every codec enabled and no budget.
fn eval_planner(level: i32) -> Planner {
    Planner::new(
        Mode::Adaptive,
        CodecPolicy {
            zstd_level: level,
            decode_budget_ns: None,
            ..CodecPolicy::default()
        },
    )
}

/// Train and evaluate a Zstd dictionary. Returns the dictionary bytes when the
/// projected net gain is positive (or `require_gain` is false).
///
/// Validation sizes are what the Adaptive planner (every codec enabled, Zstd at
/// `level`) would store for each held-out sample, without and with the
/// dictionary as an extra candidate.
pub fn train_zstd_dictionary(
    samples: &[Vec<u8>],
    level: i32,
    opts: &TrainOptions,
) -> Result<(Option<Vec<u8>>, TrainReport)> {
    let (train, validation) = split(samples, opts)?;
    let bytes = codec::zstd::train_dictionary(&train, opts.max_dict_bytes)?;
    let dict = Arc::new(ZstdDict::new(EVAL_PARAM_ID, bytes, level)?);
    let without = total_best(&eval_planner(level), &validation);
    let with = total_best(
        &eval_planner(level).with_params(Some(dict.clone()), None),
        &validation,
    );
    let gain = projected_net_gain(
        without,
        with,
        validation.len(),
        opts.expected_uses,
        dict.bytes().len(),
    );
    let report = TrainReport {
        kind: "zstd_dict".into(),
        param_bytes: dict.bytes().len(),
        train_samples: train.len(),
        validation_samples: validation.len(),
        validation_bytes_without: without,
        validation_bytes_with: with,
        projected_net_gain: gain,
        installed: false,
        param_id: None,
    };
    let keep = gain > 0 || !opts.require_gain;
    Ok((keep.then(|| dict.bytes().to_vec()), report))
}

/// Train and evaluate a template for `TemplatePatchV1`.
///
/// The template is the training sample (at most `max_dict_bytes` long) that
/// minimizes the total patch size (`template_patch::train_bounded`). Validation
/// sizes are the best body among the other Adaptive candidates (Zstd at the
/// default level) versus the same with the template as an extra candidate.
/// When no training sample can serve as a template, returns `None` with
/// `param_bytes == 0` and a zero gain.
pub fn train_template(
    samples: &[Vec<u8>],
    opts: &TrainOptions,
) -> Result<(Option<Vec<u8>>, TrainReport)> {
    let (train, validation) = split(samples, opts)?;
    let level = CodecPolicy::default().zstd_level;
    let without = total_best(&eval_planner(level), &validation);
    let mut report = TrainReport {
        kind: "template".into(),
        param_bytes: 0,
        train_samples: train.len(),
        validation_samples: validation.len(),
        validation_bytes_without: without,
        validation_bytes_with: without,
        projected_net_gain: 0,
        installed: false,
        param_id: None,
    };
    let Some(bytes) = codec::template_patch::train_bounded(&train, opts.max_dict_bytes) else {
        return Ok((None, report));
    };
    let template = Arc::new(Template::new(EVAL_PARAM_ID, bytes));
    let with = total_best(
        &eval_planner(level).with_params(None, Some(template.clone())),
        &validation,
    );
    report.param_bytes = template.bytes().len();
    report.validation_bytes_with = with;
    report.projected_net_gain = projected_net_gain(
        without,
        with,
        validation.len(),
        opts.expected_uses,
        report.param_bytes,
    );
    let keep = report.projected_net_gain > 0 || !opts.require_gain;
    Ok((keep.then(|| template.bytes().to_vec()), report))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_is_deterministic_and_balanced() {
        let samples: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i]).collect();
        let opts = TrainOptions::default();
        let (t, v) = split(&samples, &opts).unwrap();
        assert_eq!((t.len(), v.len()), (8, 2));
        assert_eq!(v, vec![&[2u8][..], &[7u8][..]]);
        let (t, v) = split(&samples[..2], &opts).unwrap();
        assert_eq!((t.len(), v.len()), (1, 1));
        assert!(split(&samples[..1], &opts).is_err());
        let bad = TrainOptions {
            validation_fraction: 1.0,
            ..TrainOptions::default()
        };
        assert!(split(&samples, &bad).is_err());
    }

    #[test]
    fn net_gain_formula() {
        assert_eq!(
            projected_net_gain(1000, 600, 4, 100, 500),
            400 / 4 * 100 - 500 - 64
        );
        assert_eq!(projected_net_gain(600, 1000, 4, 1, 0), -100 - 64);
        assert_eq!(projected_net_gain(1, 0, 3, 1, 0), -64);
    }

    #[test]
    fn ties_prefer_cheaper_decode() {
        assert!(Candidate::Raw < Candidate::Repeat);
        assert!(Candidate::Arith < Candidate::Lz4);
        assert!(Candidate::Lz4 < Candidate::Template);
        assert!(Candidate::Template < Candidate::Zstd);
        assert!(Candidate::Zstd < Candidate::ZstdDict);
    }

    #[test]
    fn budget_fallback_is_counted() {
        let text = b"the quick brown fox jumps over the lazy dog, ".repeat(20);
        let mut data = text.clone();
        data.push(b'!');
        let policy = CodecPolicy {
            decode_budget_ns: Some(1),
            cost_model: DecodeCostModel {
                entries: vec![
                    (codec_id::REPEAT, 1e6, 0.0),
                    (codec_id::LZ4, 1e6, 0.0),
                    (codec_id::ZSTD, 1e6, 0.0),
                ],
            },
            ..CodecPolicy::default()
        };
        let p = Planner::new(Mode::Adaptive, policy);
        let enc = p.encode_unit(&data);
        assert_eq!(enc.codec, CodecTag::RAW_V1);
        assert_eq!(p.snapshot().budget_fallbacks, 1);
        let unrestricted = Planner::new(Mode::Adaptive, CodecPolicy::default());
        assert_ne!(unrestricted.encode_unit(&data).codec, CodecTag::RAW_V1);
        assert_eq!(unrestricted.snapshot().budget_fallbacks, 0);
    }
}
