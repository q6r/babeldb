//! Shared helpers for the integration suites (`model`, `recovery`, `ingest`,
//! `concurrency`).
//!
//! Every suite includes this file with `mod common;`, so it is compiled once
//! per suite and some helpers are unused in a given suite.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use babeldb::maintenance::VerifyReport;
use babeldb::store::Store;
use babeldb::{Config, Db, Error, Expect, MemStore, Mode, RedbStore, Revision, ScanOptions};

/// Block size of the small test databases (the minimum `Config::validate` accepts).
pub const BLOCK: u32 = 512;
/// Inline threshold of the small test databases.
pub const INLINE: u32 = 64;

/// Adaptive (default codecs + dedupe) or BabelPure configuration with small
/// blocks, so that multi-block values stay cheap.
pub fn small_config(mode: Mode) -> Config {
    let base = match mode {
        Mode::Adaptive => Config::adaptive(),
        Mode::BabelPure => Config::babel_pure(),
    };
    Config {
        block_size: BLOCK,
        inline_max: INLINE,
        ..base
    }
}

pub fn mem_db(cfg: Config) -> Db<MemStore> {
    Db::with_store(MemStore::new(), cfg)
        .unwrap_or_else(|e| panic!("Db::with_store(MemStore) failed: {e}"))
}

pub fn temp_dir(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create a temporary directory")
}

#[track_caller]
pub fn open_redb(path: &Path, cfg: Config) -> Db<RedbStore> {
    Db::open(path, cfg).unwrap_or_else(|e| panic!("Db::open({}) failed: {e}", path.display()))
}

/// Open a redb database that a killed process had open. On Windows the file
/// handles and locks of a terminated process can take a moment to be
/// released, so failures are retried until `timeout`.
#[track_caller]
pub fn open_redb_retry(path: &Path, cfg: &Config, timeout: Duration) -> Db<RedbStore> {
    let start = Instant::now();
    loop {
        match Db::open(path, cfg.clone()) {
            Ok(db) => return db,
            Err(e) if start.elapsed() < timeout => {
                let _ = e;
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!(
                "Db::open({}) still failing after {timeout:?}: {e}",
                path.display()
            ),
        }
    }
}

pub fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Deterministic pseudo-random data
// ---------------------------------------------------------------------------

/// SplitMix64: tiny deterministic PRNG with identical output on every platform.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform-ish value in `0..n` (0 when `n == 0`).
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    /// Value in `lo..hi` (`lo` when the range is empty).
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi.saturating_sub(lo))
    }

    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut v = vec![0; n];
        self.fill(&mut v);
        v
    }
}

/// Stateless 64-bit mix of two values (deterministic per-index choices).
pub fn mix(a: u64, b: u64) -> u64 {
    Rng::new(a.rotate_left(17) ^ b.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03)
        .next_u64()
}

/// Value shapes chosen to drive every representation of the Adaptive planner
/// (RepeatV1, ArithmeticU64V1, LZ4, Zstd, Raw) and the dedupe path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pattern {
    /// All zero bytes.
    Zeros,
    /// A short (sometimes long) random motif repeated, last repetition partial.
    Motif,
    /// Little-endian u64 arithmetic sequence (sometimes wrapping past u64::MAX,
    /// which the recipe must refuse).
    ArithU64,
    /// Incompressible bytes.
    Random,
    /// UTF-8 text with multi-byte characters, CR/LF and tabs (truncation may
    /// split a character: bytes are bytes).
    Text,
    /// Concatenation of segments of the other shapes, not block aligned.
    Mixed,
}

impl Pattern {
    pub const ALL: [Pattern; 6] = [
        Pattern::Zeros,
        Pattern::Motif,
        Pattern::ArithU64,
        Pattern::Random,
        Pattern::Text,
        Pattern::Mixed,
    ];
    const SIMPLE: [Pattern; 5] = [
        Pattern::Zeros,
        Pattern::Motif,
        Pattern::ArithU64,
        Pattern::Random,
        Pattern::Text,
    ];
}

const WORDS: &[&str] = &[
    "the",
    "library",
    "of",
    "babel",
    "contains",
    "every",
    "book",
    "exactly",
    "once",
    "ção",
    "naïve",
    "日本語",
    "🦀",
    "Ωmega",
    "straße",
    "{\"id\":",
    "\"text\":",
    "}",
    "0123456789",
    "\t",
    "\r\n",
    "\n",
    "  ",
];

fn text_bytes(len: usize, rng: &mut Rng) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 16);
    while out.len() < len {
        let word = WORDS[rng.below(WORDS.len() as u64) as usize];
        out.extend_from_slice(word.as_bytes());
        if rng.chance(3, 4) {
            out.push(b' ');
        }
    }
    out.truncate(len);
    out
}

/// Deterministic value of `len` bytes with the given shape.
pub fn pattern_bytes(pattern: Pattern, len: usize, seed: u64) -> Vec<u8> {
    let mut rng = Rng::new(seed ^ 0x6A09_E667_F3BC_C908);
    match pattern {
        Pattern::Zeros => vec![0; len],
        Pattern::Motif => {
            let period = (if rng.chance(1, 8) {
                1 + rng.below(700)
            } else {
                1 + rng.below(24)
            }) as usize;
            let motif = rng.bytes(period);
            (0..len).map(|i| motif[i % period]).collect()
        }
        Pattern::ArithU64 => {
            let shift = rng.below(64);
            let mut x = rng.next_u64() >> shift;
            if rng.chance(1, 6) {
                x = u64::MAX - rng.below(64 * 1024);
            }
            let step = match rng.below(4) {
                0 => 0,
                1 => 1,
                2 => rng.below(1 << 16),
                _ => {
                    let shift = rng.below(64);
                    rng.next_u64() >> shift
                }
            };
            let mut out = Vec::with_capacity(len + 8);
            while out.len() < len {
                out.extend_from_slice(&x.to_le_bytes());
                x = x.wrapping_add(step);
            }
            out.truncate(len);
            out
        }
        Pattern::Random => rng.bytes(len),
        Pattern::Text => text_bytes(len, &mut rng),
        Pattern::Mixed => {
            let mut out = Vec::with_capacity(len);
            while out.len() < len {
                let seg = (1 + rng.below(700) as usize).min(len - out.len());
                let shape = Pattern::SIMPLE[rng.below(Pattern::SIMPLE.len() as u64) as usize];
                let seg_seed = rng.next_u64();
                out.extend_from_slice(&pattern_bytes(shape, seg, seg_seed));
            }
            out
        }
    }
}

// ---------------------------------------------------------------------------
// Byte comparison with readable failures
// ---------------------------------------------------------------------------

/// Printable form of a (possibly binary or very long) key.
pub fn key_str(key: &[u8]) -> String {
    if key.len() > 40 {
        format!("{}...({} bytes)", key[..24].escape_ascii(), key.len())
    } else {
        format!("\"{}\"", key.escape_ascii())
    }
}

pub fn hex_preview(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "<end>".to_string();
    }
    let mut s = String::new();
    for b in bytes.iter().take(16) {
        let _ = write!(s, "{b:02x}");
    }
    if bytes.len() > 16 {
        s.push_str("..");
    }
    s
}

/// One-line description of how two byte strings differ.
pub fn describe_diff(actual: &[u8], expected: &[u8]) -> String {
    if actual == expected {
        return "identical".to_string();
    }
    let first = actual
        .iter()
        .zip(expected)
        .position(|(a, b)| a != b)
        .unwrap_or_else(|| actual.len().min(expected.len()));
    format!(
        "actual {} bytes vs expected {} bytes, first difference at offset {first}: \
         actual[{first}..]={} expected[{first}..]={}",
        actual.len(),
        expected.len(),
        hex_preview(actual.get(first..).unwrap_or(&[])),
        hex_preview(expected.get(first..).unwrap_or(&[])),
    )
}

#[track_caller]
pub fn assert_bytes_eq(actual: &[u8], expected: &[u8], ctx: &str) {
    if actual != expected {
        panic!("{ctx}: bytes differ: {}", describe_diff(actual, expected));
    }
}

// ---------------------------------------------------------------------------
// Whole-database views
// ---------------------------------------------------------------------------

/// key -> (revision, value)
pub type Snapshot = BTreeMap<Vec<u8>, (Revision, Vec<u8>)>;
/// key -> value
pub type State = BTreeMap<Vec<u8>, Vec<u8>>;

/// Every live record through ONE `scan` (a single snapshot), cross-checked
/// against `get_with_revision` for each key.
#[track_caller]
pub fn snapshot<S: Store>(db: &Db<S>, ctx: &str) -> Snapshot {
    let items = db
        .scan(&ScanOptions::all().with_values(true))
        .unwrap_or_else(|e| panic!("{ctx}: scan(all, with_values) failed: {e}"));
    let mut out = Snapshot::new();
    for item in items {
        let value = item.value.unwrap_or_else(|| {
            panic!(
                "{ctx}: scan with_values returned no value for {}",
                key_str(&item.key)
            )
        });
        assert_eq!(
            value.len() as u64,
            item.logical_len,
            "{ctx}: scan logical_len of {} does not match its value",
            key_str(&item.key)
        );
        if let Some((last, _)) = out.last_key_value() {
            assert!(
                *last < item.key,
                "{ctx}: scan keys not strictly ascending ({} then {})",
                key_str(last),
                key_str(&item.key)
            );
        }
        out.insert(item.key, (item.revision, value));
    }
    for (key, (rev, value)) in &out {
        let point = db
            .get_with_revision(key)
            .unwrap_or_else(|e| panic!("{ctx}: get_with_revision({}) failed: {e}", key_str(key)));
        match point {
            Some((r, v)) => {
                assert_eq!(
                    r,
                    *rev,
                    "{ctx}: scan and get_with_revision disagree on the revision of {}",
                    key_str(key)
                );
                assert_bytes_eq(
                    &v,
                    value,
                    &format!("{ctx}: get_with_revision vs scan for {}", key_str(key)),
                );
            }
            None => panic!(
                "{ctx}: scan returned {} but get_with_revision says it is absent",
                key_str(key)
            ),
        }
    }
    out
}

pub fn values_of(snap: &Snapshot) -> State {
    snap.iter()
        .map(|(k, (_, v))| (k.clone(), v.clone()))
        .collect()
}

/// Human-readable list of the keys whose value differs (at most 20 entries).
pub fn state_diff(actual: &State, expected: &State) -> String {
    let keys: BTreeSet<&Vec<u8>> = actual.keys().chain(expected.keys()).collect();
    let mut out = String::new();
    let mut shown = 0;
    for key in keys {
        let line = match (actual.get(key), expected.get(key)) {
            (Some(a), Some(e)) if a != e => format!("  {}: {}", key_str(key), describe_diff(a, e)),
            (Some(a), None) => format!("  {}: unexpected ({} bytes)", key_str(key), a.len()),
            (None, Some(e)) => format!("  {}: missing ({} bytes expected)", key_str(key), e.len()),
            _ => continue,
        };
        shown += 1;
        if shown > 20 {
            out.push_str("  ...\n");
            break;
        }
        out.push_str(&line);
        out.push('\n');
    }
    out
}

#[track_caller]
pub fn assert_state_eq(actual: &State, expected: &State, ctx: &str) {
    if actual != expected {
        panic!(
            "{ctx}: content differs from the expected state:\n{}",
            state_diff(actual, expected)
        );
    }
}

#[track_caller]
pub fn assert_snapshot_eq(actual: &Snapshot, expected: &Snapshot, ctx: &str) {
    if actual == expected {
        return;
    }
    let mut revisions = String::new();
    for (key, (rev, _)) in actual {
        if let Some((want, _)) = expected.get(key)
            && want != rev
        {
            let _ = writeln!(
                revisions,
                "  {}: revision {rev}, expected {want}",
                key_str(key)
            );
        }
    }
    panic!(
        "{ctx}: database content differs from the model:\n{}{}",
        state_diff(&values_of(actual), &values_of(expected)),
        revisions
    );
}

// ---------------------------------------------------------------------------
// Maintenance helpers
// ---------------------------------------------------------------------------

#[track_caller]
pub fn verify_ok<S: Store>(db: &Db<S>, deep: bool, ctx: &str) -> VerifyReport {
    let report = db
        .verify(deep)
        .unwrap_or_else(|e| panic!("{ctx}: verify({deep}) failed: {e}"));
    assert!(
        report.ok(),
        "{ctx}: verify({deep}) found inconsistencies: {report:#?}"
    );
    report
}

/// Delete every record and check that no object, hash candidate or pending
/// import survives (i.e. no reference was leaked). Only for databases without
/// `keep_history` (retained history legitimately keeps objects alive).
#[track_caller]
pub fn delete_everything_and_check_no_leaks<S: Store>(db: &mut Db<S>, ctx: &str) {
    let items = db
        .scan(&ScanOptions::all())
        .unwrap_or_else(|e| panic!("{ctx}: scan failed: {e}"));
    for item in &items {
        let deleted = db
            .delete(&item.key, Expect::Any)
            .unwrap_or_else(|e| panic!("{ctx}: delete({}) failed: {e}", key_str(&item.key)));
        assert!(
            deleted,
            "{ctx}: delete({}) of a scanned record reported nothing deleted",
            key_str(&item.key)
        );
    }
    let st = db
        .stats()
        .unwrap_or_else(|e| panic!("{ctx}: stats failed: {e}"));
    assert_eq!(
        st.logical_bytes, 0,
        "{ctx}: logical bytes left after deleting everything"
    );
    assert_eq!(
        st.objects, 0,
        "{ctx}: {} objects still stored after deleting every record (leaked references)",
        st.objects
    );
    assert_eq!(
        st.hash_candidates, 0,
        "{ctx}: hash candidates left after deleting everything"
    );
    assert_eq!(st.pending_imports, 0, "{ctx}: pending imports left");
    verify_ok(db, true, ctx);
    db.gc().unwrap_or_else(|e| panic!("{ctx}: gc failed: {e}"));
    let st = db
        .stats()
        .unwrap_or_else(|e| panic!("{ctx}: stats failed: {e}"));
    assert_eq!(st.objects, 0, "{ctx}: objects appeared after gc");
    verify_ok(db, true, ctx);
}

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

pub fn is_conflict(e: &Error) -> bool {
    matches!(e, Error::RevisionConflict { .. })
}

pub fn is_invalid_argument(e: &Error) -> bool {
    matches!(e, Error::InvalidArgument(_))
}

/// Errors acceptable for an input that breaks a configured limit
/// (`max_key_len`, `max_value_len`).
pub fn is_rejected_input(e: &Error) -> bool {
    matches!(e, Error::InvalidArgument(_) | Error::LimitExceeded(_))
}
