//! Configuration. `mode`, `block_size` and `inline_max` are creation parameters:
//! they are persisted in `meta` when the database is created and the persisted
//! values win when an existing database is opened.

use crate::error::{Error, Result};
use crate::format::codec_id;

/// Database variant. Results of the two modes are always reported separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Mode {
    /// Every unit is stored as the seed of the reversible affine map (`BabelAffineV1`).
    BabelPure = 1,
    /// Smallest exact representation among recipes, raw, LZ4, Zstd, templates.
    Adaptive = 2,
}

impl Mode {
    pub fn from_u8(v: u8) -> Option<Mode> {
        match v {
            1 => Some(Mode::BabelPure),
            2 => Some(Mode::Adaptive),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Mode::BabelPure => "babel-pure",
            Mode::Adaptive => "adaptive",
        }
    }
}

/// Estimated decode cost of a codec: `fixed_ns + ns_per_byte * raw_len`.
/// Values come from measurements (`benches/codec.rs`); empty = no estimate.
#[derive(Clone, Debug, Default)]
pub struct DecodeCostModel {
    /// (codec id, fixed ns, ns per byte)
    pub entries: Vec<(u8, f64, f64)>,
}

impl DecodeCostModel {
    pub fn estimate_ns(&self, codec_id: u8, raw_len: usize) -> Option<f64> {
        self.entries
            .iter()
            .find(|(id, _, _)| *id == codec_id)
            .map(|(_, fixed, per_byte)| fixed + per_byte * raw_len as f64)
    }

    /// Decode cost of `codec::decode` per codec (digest verification, the
    /// same for every codec, excluded), fitted on `cargo bench --bench codec
    /// -- ^decode/` (chat-like JSON, and the recipes on their own inputs, at
    /// 512 B, 4 KiB and 64 KiB) on the reference machine of
    /// `docs/benchmarks.md` §6, 27/09/2026. ZstdV1 is the cost without a
    /// dictionary (with one it is lower for small inputs: 0.4–1.4 µs at
    /// 512 B instead of 2.2 µs). Averages for text-like data, not bounds:
    /// highly repetitive inputs with many short matches decode slower.
    pub fn measured() -> DecodeCostModel {
        DecodeCostModel {
            entries: vec![
                (codec_id::RAW, 12.0, 0.021),
                (codec_id::REPEAT, 25.0, 0.016),
                (codec_id::ARITH_U64, 25.0, 0.026),
                (codec_id::LZ4, 40.0, 0.26),
                (codec_id::TEMPLATE_PATCH, 30.0, 0.45),
                (codec_id::ZSTD, 2000.0, 0.40),
                (codec_id::BABEL_AFFINE, 10.0, 0.10),
            ],
        }
    }
}

/// Which representations the Adaptive planner may choose. Ignored in BabelPure.
#[derive(Clone, Debug)]
pub struct CodecPolicy {
    /// `RepeatV1` and `ArithmeticU64V1` recognizers.
    pub recipes: bool,
    pub lz4: bool,
    pub zstd: bool,
    pub zstd_level: i32,
    /// Try the active Zstd dictionary, if one is installed.
    pub zstd_dictionary: bool,
    /// Try the active template (`TemplatePatchV1`), if one is installed.
    pub template: bool,
    /// Skip codecs whose estimated decode time for a unit exceeds this budget.
    /// Violations are counted in planner statistics, never hidden.
    pub decode_budget_ns: Option<u64>,
    pub cost_model: DecodeCostModel,
    /// Stored bytes that one microsecond of estimated decode time (from
    /// `cost_model`) is worth when candidates are compared: the planner keeps
    /// the candidate with the smallest `body_len + weight * decode_us`, so a
    /// codec that decodes 1 µs slower must save at least this many bytes to
    /// win (e.g. LZ4 over Zstd when Zstd saves little). 0 = always the
    /// smallest body.
    pub decode_weight_bytes_per_us: f64,
}

impl Default for CodecPolicy {
    fn default() -> Self {
        CodecPolicy {
            recipes: true,
            lz4: true,
            zstd: true,
            zstd_level: 3,
            zstd_dictionary: true,
            template: true,
            decode_budget_ns: None,
            cost_model: DecodeCostModel::measured(),
            decode_weight_bytes_per_us: 32.0,
        }
    }
}

impl CodecPolicy {
    /// Only `RawV1`: the "engine + Raw" baseline that measures format overhead.
    pub fn raw_only() -> Self {
        CodecPolicy {
            recipes: false,
            lz4: false,
            zstd: false,
            zstd_dictionary: false,
            template: false,
            ..CodecPolicy::default()
        }
    }
}

/// Automatic Zstd dictionary for small values. Active in Adaptive mode when
/// the policy allows `zstd` and `zstd_dictionary`, `samples > 0`, and no
/// dictionary is active yet: the first `samples` small values written after
/// the database is opened are copied aside, a dictionary is trained and
/// evaluated on held-out samples (`planner::train_zstd_dictionary`), and it
/// is installed as the active dictionary (`params`, `meta.active_zstd_dict`)
/// only when its projected net gain is positive. Values written before keep
/// their representation (params are immutable). One attempt per open.
#[derive(Clone, Debug)]
pub struct AutoDictionary {
    /// Small values sampled before training; 0 disables automatic training.
    pub samples: usize,
    /// Values are sampled when they are stored inline (`len <= inline_max`)
    /// and have `planner::MIN_COMPRESS_LEN..=max_sample_len` bytes.
    pub max_sample_len: usize,
    /// Largest dictionary trained; also capped at 1/16 of the sampled bytes.
    pub max_dict_bytes: usize,
    /// Fraction of the samples held out to validate the dictionary.
    pub validation_fraction: f64,
    /// Future values over which the dictionary must pay for itself; 0 = as
    /// many as the small values written since the database was opened.
    pub expected_uses: u64,
    /// Train on a background thread and install from a later write (true),
    /// or train and install inside the write that completes the samples,
    /// before its own transaction (false). Never inside a write transaction.
    pub background: bool,
}

impl Default for AutoDictionary {
    fn default() -> Self {
        AutoDictionary {
            samples: 2000,
            max_sample_len: 4096,
            max_dict_bytes: 64 * 1024,
            validation_fraction: 0.2,
            expected_uses: 0,
            background: true,
        }
    }
}

impl AutoDictionary {
    /// No automatic training.
    pub fn disabled() -> Self {
        AutoDictionary { samples: 0, ..AutoDictionary::default() }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    // --- creation parameters (persisted) ---
    pub mode: Mode,
    /// Fixed block size used to split values larger than `inline_max`.
    pub block_size: u32,
    /// Values with `len <= inline_max` are stored inside the manifest.
    pub inline_max: u32,

    // --- runtime parameters ---
    pub max_key_len: usize,
    pub max_value_len: u64,
    /// Content-hash deduplication of blocks (Adaptive only; forced off in BabelPure).
    pub dedupe: bool,
    /// Keep previous manifests in `history` (off by default).
    pub keep_history: bool,
    /// Verify length + BLAKE3 digest of every decoded unit before returning it.
    pub verify_on_read: bool,
    /// Byte budget of the application block cache (verified decoded blocks,
    /// and verified values of inline records when `cache_values`).
    pub cache_bytes: usize,
    /// Cache budget handed to the storage backend (redb page cache).
    pub backend_cache_bytes: usize,
    pub codecs: CodecPolicy,
    /// Encoded bytes accumulated per intermediate import transaction.
    pub import_batch_bytes: usize,
    /// Keep verified values of inline records in the block cache, keyed by
    /// the revision of their manifest (needs `verify_on_read`): repeated reads
    /// skip decoding and hashing. A value is kept from its second recent read
    /// on (point reads and scans of at most `engine::VALUE_CACHE_SCAN_LIMIT`
    /// items), so one-off reads cost nothing extra. `RawV1` values are never
    /// kept (they are verified in place on every read).
    pub cache_values: bool,
    /// Automatic Zstd dictionary for small values (Adaptive only).
    pub auto_dictionary: AutoDictionary,
}

pub const MIN_BLOCK_SIZE: u32 = 512;
pub const MAX_BLOCK_SIZE: u32 = 1 << 20;

impl Default for Config {
    fn default() -> Self {
        Config {
            mode: Mode::Adaptive,
            block_size: 16 * 1024,
            inline_max: 1024,
            max_key_len: 4096,
            max_value_len: 4 << 30,
            dedupe: true,
            keep_history: false,
            verify_on_read: true,
            cache_bytes: 64 << 20,
            backend_cache_bytes: 64 << 20,
            codecs: CodecPolicy::default(),
            import_batch_bytes: 64 << 20,
            cache_values: true,
            auto_dictionary: AutoDictionary::default(),
        }
    }
}

impl Config {
    pub fn adaptive() -> Self {
        Config::default()
    }

    pub fn babel_pure() -> Self {
        Config { mode: Mode::BabelPure, dedupe: false, ..Config::default() }
    }

    /// Adaptive engine restricted to `RawV1`, without dedupe: format-overhead baseline.
    pub fn raw_only() -> Self {
        Config { codecs: CodecPolicy::raw_only(), dedupe: false, ..Config::default() }
    }

    /// Dedupe only applies to Adaptive.
    pub fn effective_dedupe(&self, mode: Mode) -> bool {
        self.dedupe && mode == Mode::Adaptive
    }

    /// Whether automatic dictionary training applies to a database of `mode`.
    pub fn auto_dictionary_applies(&self, mode: Mode) -> bool {
        mode == Mode::Adaptive
            && self.auto_dictionary.samples > 0
            && self.codecs.zstd
            && self.codecs.zstd_dictionary
    }

    pub fn validate(&self) -> Result<()> {
        if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&self.block_size) {
            return Err(Error::InvalidArgument(format!(
                "block_size {} outside [{MIN_BLOCK_SIZE}, {MAX_BLOCK_SIZE}]",
                self.block_size
            )));
        }
        if self.inline_max > self.block_size {
            return Err(Error::InvalidArgument("inline_max must be <= block_size".into()));
        }
        if self.max_key_len == 0 || self.max_key_len > 64 * 1024 {
            return Err(Error::InvalidArgument("max_key_len must be in 1..=65536".into()));
        }
        let w = self.codecs.decode_weight_bytes_per_us;
        if !(w.is_finite() && w >= 0.0) {
            return Err(Error::InvalidArgument(format!(
                "decode_weight_bytes_per_us {w} must be finite and >= 0"
            )));
        }
        let f = self.auto_dictionary.validation_fraction;
        if self.auto_dictionary.samples > 0 && !(f > 0.0 && f < 1.0) {
            return Err(Error::InvalidArgument(format!(
                "auto_dictionary.validation_fraction {f} outside (0, 1)"
            )));
        }
        Ok(())
    }
}
