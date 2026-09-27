//! Configuration. `mode`, `block_size` and `inline_max` are creation parameters:
//! they are persisted in `meta` when the database is created and the persisted
//! values win when an existing database is opened.

use crate::error::{Error, Result};

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
            cost_model: DecodeCostModel::default(),
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
    /// Byte budget of the application block cache (verified decoded blocks).
    pub cache_bytes: usize,
    /// Cache budget handed to the storage backend (redb page cache).
    pub backend_cache_bytes: usize,
    pub codecs: CodecPolicy,
    /// Encoded bytes accumulated per intermediate import transaction.
    pub import_batch_bytes: usize,
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
        Ok(())
    }
}
