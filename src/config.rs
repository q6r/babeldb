//! Configuration. `mode`, `block_size` and `inline_max` are creation parameters:
//! they are persisted in `meta` when the database is created and the persisted
//! values win when an existing database is opened.

use std::path::{Path, PathBuf};

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

/// Smallest and largest WAL file (`WalConfig::segment_bytes`).
pub const MIN_WAL_SEGMENT: u64 = 64 << 10;
pub const MAX_WAL_SEGMENT: u64 = 64 << 30;

/// How a WAL write is made durable (`store::wal`, "Why").
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WalSync {
    /// `FILE_FLAG_WRITE_THROUGH` on a cached handle (Linux: `O_DSYNC`).
    /// PostgreSQL's default on Windows (`wal_sync_method = open_datasync`) and
    /// its guarantee: on Windows the drive's volatile cache is not necessarily
    /// written through (see the `CreateFile` documentation).
    #[default]
    WriteThrough,
    /// Windows only: `FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING` with
    /// 4 KiB-aligned writes, the combination for which the OS also asks the
    /// drive to write through its cache (FUA).
    WriteThroughUnbuffered,
    /// A cached write followed by `FlushFileBuffers` (`fdatasync`): the drive
    /// cache flush of redb's `Immediate` commits (PostgreSQL's `fsync`).
    Flush,
}

/// Write-ahead log in front of the storage backend (`store::wal::WalStore`,
/// `Db::open_wal`); layout in `docs/format.md` §19.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalConfig {
    /// Directory of the WAL file; `None` = next to the database file. See
    /// `wal_path` for the name.
    pub dir: Option<PathBuf>,
    /// Size of the WAL file, preallocated (zero-filled) when it is created and
    /// rounded up to 4 KiB. It is also the checkpoint threshold: when the log
    /// of a commit does not fit in what is left, that commit becomes a
    /// checkpoint (one `Immediate` commit of the backend) and the log restarts
    /// at the start of the file. An existing WAL of another size is replaced
    /// after recovery. Default 16 MiB (PostgreSQL's segment size): a larger file
    /// checkpoints less often but lets more dirty backend pages pile up in
    /// between, which measured slower under sustained load.
    pub segment_bytes: u64,
    /// `Deferred` commits keep their log records in memory until the next
    /// `Immediate` commit; past this many bytes they are written (and so made
    /// durable) at once. Bounds memory and the window of deferred commits a
    /// crash can lose.
    pub max_pending_bytes: usize,
    /// Transactions whose log record would be larger than this are not logged:
    /// their commit is a checkpoint instead (one fsync rather than copying,
    /// say, a large import batch into the log). Capped at what fits in the file.
    pub max_record_bytes: usize,
    /// How each WAL write is made durable.
    pub sync: WalSync,
    /// Open even when the WAL file is missing although the database was not
    /// closed cleanly, accepting the loss of the commits that only it held.
    pub recreate_missing: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            dir: None,
            segment_bytes: 16 << 20,
            max_pending_bytes: 4 << 20,
            max_record_bytes: 4 << 20,
            sync: WalSync::default(),
            recreate_missing: false,
        }
    }
}

impl WalConfig {
    pub fn validate(&self) -> Result<()> {
        if !(MIN_WAL_SEGMENT..=MAX_WAL_SEGMENT).contains(&self.segment_bytes) {
            return Err(Error::InvalidArgument(format!(
                "WAL segment_bytes {} outside [{MIN_WAL_SEGMENT}, {MAX_WAL_SEGMENT}]",
                self.segment_bytes
            )));
        }
        if self.max_record_bytes < 64 {
            return Err(Error::InvalidArgument("WAL max_record_bytes must be >= 64".into()));
        }
        Ok(())
    }

    /// Size of the WAL file: `segment_bytes` rounded up to 4 KiB.
    pub fn segment_capacity(&self) -> u64 {
        self.segment_bytes.div_ceil(4096) * 4096
    }

    /// Largest record that is logged: `max_record_bytes`, capped by what fits
    /// after the 4 KiB file header and by the u32 length field.
    pub fn record_limit(&self) -> usize {
        let fits = usize::try_from(self.segment_capacity() - 4096).unwrap_or(usize::MAX);
        let field = usize::try_from(u64::from(u32::MAX) + 32).unwrap_or(usize::MAX);
        self.max_record_bytes.min(fits).min(field)
    }

    /// Path of the WAL of the database file `db_path`: `<file name>.wal` next
    /// to it, or with `dir`, `<file name>.<16 hex digits>.wal` in `dir`, the
    /// digits starting the BLAKE3 of the database's absolute path, so that
    /// databases with the same file name never share a WAL.
    pub fn wal_path(&self, db_path: &Path) -> PathBuf {
        let mut name = db_path
            .file_name()
            .map_or_else(|| "babeldb".into(), |n| n.to_os_string());
        match &self.dir {
            None => {
                name.push(".wal");
                db_path.with_file_name(name)
            }
            Some(dir) => {
                let abs = std::path::absolute(db_path).unwrap_or_else(|_| db_path.to_path_buf());
                let hash = blake3::hash(abs.as_os_str().as_encoded_bytes()).to_hex();
                name.push(format!(".{}.wal", &hash[..16]));
                dir.join(name)
            }
        }
    }
}
