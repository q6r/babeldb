//! Deterministic synthetic datasets for the benchmark (scenarios S1–S6,
//! documented in `benchdata/manifest.json` and `docs/benchmarks.md`).
//!
//! The scenarios are explicit hypotheses standing in for a production corpus,
//! which is not available (spec §11). Every generator is a pure function of
//! `(scenario, seed, i, value_size)` built only from integer arithmetic,
//! splitmix64 and BLAKE3 (XOF), so records are byte-identical on every
//! platform. S6 additionally depends on the output of the pinned libzstd
//! (level 3). Changing any generator changes the dataset digests: bump
//! [`DATASETS_VERSION`] and regenerate `benchdata/manifest.json`
//! (`cargo bench --bench engine -- gen-manifest`).
//!
//! The module also holds the workload helpers shared by the benchmark
//! harness: the [`SplitMix64`] generator, the [`Zipf`] sampler and the
//! [`scramble`] permutation.

use std::io::Write as _;

/// Version of the generators. Any change to the bytes they produce bumps it.
pub const DATASETS_VERSION: u32 = 1;
/// Seed used by the benchmark and the manifest unless overridden.
pub const DEFAULT_SEED: u64 = 42;

/// S3: number of chat channels.
pub const S3_CHANNELS: u64 = 1000;
/// S3: number of distinct authors.
pub const S3_USERS: u64 = 10_000;
/// Discord epoch (2015-01-01T00:00:00Z) in Unix milliseconds.
pub const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;
/// S3: send time of message 0 (2025-01-01T00:00:00Z).
pub const S3_BASE_MS: u64 = 1_735_689_600_000;
/// S3: message `i` is sent at `S3_BASE_MS + i * S3_GAP_MS + jitter`,
/// `jitter < S3_GAP_MS`, so send times (and snowflakes) strictly increase with `i`.
pub const S3_GAP_MS: u64 = 100;
/// S4: exact duplicates in every group of 10 consecutive records.
pub const S4_DUPLICATES_PER_10: u64 = 3;
/// S6: zstd level of the stored frames.
pub const S6_ZSTD_LEVEL: i32 = 3;

/// Benchmark scenario identifiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scenario {
    /// S1: zeros and exact repetitions.
    Repetitive,
    /// S2: little-endian u64 arithmetic sequences.
    Sequences,
    /// S3: chat-message JSON (snowflake ids, channel, author, text) from a fixed template.
    ChatJson,
    /// S4: 30 % exact duplicates of other values.
    Duplicates,
    /// S5: high entropy (BLAKE3 XOF with a seed unknown to the planner).
    HighEntropy,
    /// S6: already-compressed payloads (zstd frames of text).
    Compressed,
}

impl Scenario {
    pub const ALL: [Scenario; 6] = [
        Scenario::Repetitive,
        Scenario::Sequences,
        Scenario::ChatJson,
        Scenario::Duplicates,
        Scenario::HighEntropy,
        Scenario::Compressed,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Scenario::Repetitive => "s1-repetitive",
            Scenario::Sequences => "s2-sequences",
            Scenario::ChatJson => "s3-chat-json",
            Scenario::Duplicates => "s4-duplicates",
            Scenario::HighEntropy => "s5-high-entropy",
            Scenario::Compressed => "s6-compressed",
        }
    }

    /// Short identifier (`s1` .. `s6`).
    pub fn code(self) -> &'static str {
        match self {
            Scenario::Repetitive => "s1",
            Scenario::Sequences => "s2",
            Scenario::ChatJson => "s3",
            Scenario::Duplicates => "s4",
            Scenario::HighEntropy => "s5",
            Scenario::Compressed => "s6",
        }
    }

    /// Accepts the short identifier or the full name, case-insensitively.
    pub fn parse(s: &str) -> Option<Scenario> {
        let s = s.trim().to_ascii_lowercase();
        Scenario::ALL
            .into_iter()
            .find(|sc| sc.code() == s || sc.name() == s)
    }

    /// What the generator produces.
    pub fn description(self) -> &'static str {
        match self {
            Scenario::Repetitive => {
                "Even i: value_size zero bytes. Odd i: a random motif of 2..=64 bytes repeated up to \
                 value_size (the last repetition is truncated)."
            }
            Scenario::Sequences => {
                "Consecutive little-endian u64 terms of an arithmetic progression with a per-record \
                 start < 2^48 and step in 1..=65536; a trailing partial term (value_size not a \
                 multiple of 8) is the little-endian prefix of the next term."
            }
            Scenario::ChatJson => {
                "Discord-like message JSON {id, channel_id, author {id, username}, content, \
                 timestamp, mentions [], pinned false}. id is a snowflake (Discord epoch, 42-bit ms \
                 timestamp << 22 | worker << 17 | process << 12 | increment); messages are sent \
                 100 ms apart on average, so snowflakes increase with i. channel: 1000 channels, \
                 Zipf (exponent 1) over channel popularity. author: 10000 users, skewed. content: \
                 words of a fixed 256-word list, padded so that the value is exactly value_size \
                 bytes whenever value_size is at least the fixed template (about 200 bytes)."
            }
            Scenario::Duplicates => {
                "Exactly 3 of every 10 consecutive records (never record 0) are exact copies of the \
                 value of an earlier record chosen uniformly in [0, i); every other value is \
                 BLAKE3-XOF output (incompressible)."
            }
            Scenario::HighEntropy => {
                "BLAKE3 XOF output derived from (seed, i); the planner does not know the seed."
            }
            Scenario::Compressed => {
                "One zstd level-3 frame per record of S3-like JSON-lines chat text; the text length \
                 is searched (secant method, at most 8 compressions, stopping within 2 %) and the \
                 frame closest to value_size is kept, so sizes vary a few percent around value_size \
                 (see min/max_value_len in the manifest)."
            }
        }
    }

    /// The hypothesis the scenario stands for (spec §11: synthetic scenarios
    /// are hypotheses until a real corpus exists).
    pub fn hypothesis(self) -> &'static str {
        match self {
            Scenario::Repetitive => {
                "Runs and exact repetitions (padding, fills, sparse blobs) are represented by \
                 RepeatV1 or a compressor at a tiny fraction of their size; BabelPure stays at Raw \
                 size by construction."
            }
            Scenario::Sequences => {
                "Counters, offset tables and timestamps are recognized exactly by ArithmeticU64V1 \
                 (constant-size body) when the unit length is a multiple of 8."
            }
            Scenario::ChatJson => {
                "Chat-scale workload: many small records sharing a fixed JSON template and a small \
                 vocabulary; a reverse prefix scan of a channel returns its latest messages."
            }
            Scenario::Duplicates => {
                "Byte-verified dedupe removes exact duplicates. Only values stored as objects \
                 (length > inline_max) can be deduplicated, and compression cannot help because the \
                 base values are high entropy."
            }
            Scenario::HighEntropy => {
                "Incompressible data: every Adaptive candidate falls back to Raw and the \
                 envelope/manifest overhead is pure cost."
            }
            Scenario::Compressed => {
                "Already-compressed payloads (attachments, .zst files): recompression gains nothing \
                 and costs CPU; the planner must fall back to Raw."
            }
        }
    }

    /// Key layout (Rust format syntax).
    pub fn key_format(self) -> &'static str {
        match self {
            Scenario::ChatJson => "ch/{channel:08}/msg/{snowflake:020}",
            Scenario::Repetitive => "s1-repetitive/{i:012}",
            Scenario::Sequences => "s2-sequences/{i:012}",
            Scenario::Duplicates => "s4-duplicates/{i:012}",
            Scenario::HighEntropy => "s5-high-entropy/{i:012}",
            Scenario::Compressed => "s6-compressed/{i:012}",
        }
    }
}

/// The i-th (key, value) of a scenario for a target value size. Deterministic
/// for (scenario, seed, i, value_size) on every platform.
pub fn record(scenario: Scenario, seed: u64, i: u64, value_size: usize) -> (Vec<u8>, Vec<u8>) {
    (key(scenario, seed, i), value(scenario, seed, i, value_size))
}

/// Key of record `i` (cheap: never builds the value).
pub fn key(scenario: Scenario, seed: u64, i: u64) -> Vec<u8> {
    match scenario {
        Scenario::ChatJson => format!(
            "ch/{:08}/msg/{:020}",
            channel_of(i, seed),
            s3_snowflake(seed, i)
        )
        .into_bytes(),
        _ => format!("{}/{i:012}", scenario.name()).into_bytes(),
    }
}

/// A key that is absent from the dataset for every record count: the key of
/// record `i` followed by `~`. Every real key has a fixed-width numeric tail,
/// so this never collides, and a lookup lands next to a real key (a "near miss").
pub fn miss_key(scenario: Scenario, seed: u64, i: u64) -> Vec<u8> {
    let mut k = key(scenario, seed, i);
    k.push(b'~');
    k
}

/// Value of record `i`.
pub fn value(scenario: Scenario, seed: u64, i: u64, value_size: usize) -> Vec<u8> {
    match scenario {
        Scenario::Repetitive => repetitive(seed, i, value_size),
        Scenario::Sequences => sequences(seed, i, value_size),
        Scenario::ChatJson => chat_json(seed, i, value_size),
        Scenario::Duplicates => xof(S4_CONTEXT, seed, duplicate_root(seed, i), value_size),
        Scenario::HighEntropy => xof(S5_CONTEXT, seed, i, value_size),
        Scenario::Compressed => compressed(seed, i, value_size),
    }
}

// ---------------------------------------------------------------------------
// Deterministic mixing and the splitmix64 generator
// ---------------------------------------------------------------------------

const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// splitmix64 output function applied to `x + 0x9E3779B97F4A7C15`
/// (Steele, Lea, Flood 2014): a bijective 64-bit mixer.
pub fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(GOLDEN_GAMMA);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Hash of `(seed, domain, i)`; `domain` separates independent streams.
fn mix3(seed: u64, domain: u64, i: u64) -> u64 {
    splitmix64(splitmix64(seed ^ domain) ^ i)
}

/// Stream separators for [`mix3`] (arbitrary distinct constants).
mod domain {
    pub const CHANNEL: u64 = 0x6368_616E_6E65_6C31;
    pub const TIME: u64 = 0x7469_6D65_0000_0031;
    pub const NODE: u64 = 0x6E6F_6465_0000_0031;
    pub const AUTHOR: u64 = 0x6175_7468_6F72_0031;
    pub const CONTENT: u64 = 0x636F_6E74_656E_7431;
    pub const USER: u64 = 0x7573_6572_0000_0031;
    pub const USERNAME: u64 = 0x756E_616D_6500_0031;
    pub const S1: u64 = 0x7331_0000_0000_0031;
    pub const S2: u64 = 0x7332_0000_0000_0031;
    pub const S4_GROUP: u64 = 0x7334_6772_6F75_7031;
    pub const S4_SOURCE: u64 = 0x7334_7372_6300_0031;
    pub const S6_ARCHIVE: u64 = 0x7336_6172_6368_0031;
}

const S4_CONTEXT: &str = "babeldb datasets v1 s4-duplicates base value";
const S5_CONTEXT: &str = "babeldb datasets v1 s5-high-entropy value";

/// splitmix64 sequence generator (identical output on every platform).
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> SplitMix64 {
        SplitMix64 { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        let out = splitmix64(self.state);
        self.state = self.state.wrapping_add(GOLDEN_GAMMA);
        out
    }

    /// Uniform in `[0, n)` by multiply-shift (bias below `n / 2^64`). `n` must be > 0.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }

    /// Uniform in `[0, 1)` with 53 random bits.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// `len` bytes of BLAKE3 XOF output for `(context, seed, i)`.
fn xof(context: &str, seed: u64, i: u64, len: usize) -> Vec<u8> {
    let mut h = blake3::Hasher::new_derive_key(context);
    h.update(&seed.to_le_bytes());
    h.update(&i.to_le_bytes());
    let mut out = vec![0u8; len];
    h.finalize_xof().fill(&mut out);
    out
}

// ---------------------------------------------------------------------------
// S1, S2
// ---------------------------------------------------------------------------

fn repetitive(seed: u64, i: u64, size: usize) -> Vec<u8> {
    if i.is_multiple_of(2) {
        return vec![0u8; size];
    }
    let mut rng = SplitMix64::new(mix3(seed, domain::S1, i));
    let period = 2 + rng.below(63) as usize;
    let motif: Vec<u8> = (0..period).map(|_| rng.next_u64() as u8).collect();
    motif.iter().copied().cycle().take(size).collect()
}

fn sequences(seed: u64, i: u64, size: usize) -> Vec<u8> {
    let mut rng = SplitMix64::new(mix3(seed, domain::S2, i));
    let start = rng.next_u64() >> 16;
    let step = 1 + rng.below(1 << 16);
    let mut out = Vec::with_capacity(size + 8);
    let mut term = start;
    while out.len() < size {
        out.extend_from_slice(&term.to_le_bytes());
        // start < 2^48 and step <= 2^16: no wrap below 2^47 terms.
        term = term.wrapping_add(step);
    }
    out.truncate(size);
    out
}

// ---------------------------------------------------------------------------
// S3: Discord-like chat messages
// ---------------------------------------------------------------------------

/// 256 lowercase ASCII words, roughly most frequent first (JSON-safe, no escaping).
const WORDS: [&str; 256] = [
    "the",
    "be",
    "to",
    "of",
    "and",
    "a",
    "in",
    "that",
    "have",
    "it",
    "for",
    "not",
    "on",
    "with",
    "he",
    "as", //
    "you",
    "do",
    "at",
    "this",
    "but",
    "his",
    "by",
    "from",
    "they",
    "we",
    "say",
    "her",
    "she",
    "or",
    "an",
    "will", //
    "my",
    "one",
    "all",
    "would",
    "there",
    "their",
    "what",
    "so",
    "up",
    "out",
    "if",
    "about",
    "who",
    "get",
    "which",
    "go", //
    "me",
    "when",
    "make",
    "can",
    "like",
    "time",
    "no",
    "just",
    "him",
    "know",
    "take",
    "people",
    "into",
    "year",
    "your",
    "good", //
    "some",
    "could",
    "them",
    "see",
    "other",
    "than",
    "then",
    "now",
    "look",
    "only",
    "come",
    "its",
    "over",
    "think",
    "also",
    "back", //
    "after",
    "use",
    "two",
    "how",
    "our",
    "work",
    "first",
    "well",
    "way",
    "even",
    "new",
    "want",
    "because",
    "any",
    "these",
    "give", //
    "day",
    "most",
    "us",
    "is",
    "are",
    "was",
    "were",
    "been",
    "has",
    "had",
    "did",
    "said",
    "got",
    "going",
    "really",
    "very", //
    "lol",
    "lmao",
    "gg",
    "brb",
    "afk",
    "idk",
    "tbh",
    "imo",
    "omg",
    "nice",
    "yeah",
    "yes",
    "ok",
    "okay",
    "thanks",
    "pls", //
    "please",
    "sure",
    "cool",
    "wow",
    "haha",
    "hey",
    "hi",
    "hello",
    "bye",
    "night",
    "morning",
    "game",
    "play",
    "server",
    "channel",
    "voice", //
    "chat",
    "message",
    "bot",
    "role",
    "admin",
    "mod",
    "ping",
    "link",
    "stream",
    "live",
    "today",
    "tomorrow",
    "tonight",
    "later",
    "soon",
    "again", //
    "never",
    "always",
    "maybe",
    "much",
    "more",
    "less",
    "still",
    "already",
    "here",
    "where",
    "why",
    "yet",
    "something",
    "anything",
    "everything",
    "nothing", //
    "someone",
    "everyone",
    "thing",
    "things",
    "stuff",
    "great",
    "bad",
    "best",
    "better",
    "worse",
    "fun",
    "funny",
    "weird",
    "crazy",
    "wild",
    "lucky", //
    "team",
    "match",
    "win",
    "lose",
    "lost",
    "won",
    "rank",
    "ranked",
    "queue",
    "lobby",
    "party",
    "squad",
    "map",
    "build",
    "patch",
    "update", //
    "bug",
    "fix",
    "issue",
    "problem",
    "error",
    "crash",
    "lag",
    "delay",
    "fps",
    "settings",
    "mic",
    "audio",
    "video",
    "screen",
    "share",
    "music", //
    "song",
    "listen",
    "watch",
    "movie",
    "show",
    "episode",
    "season",
    "anime",
    "meme",
    "emoji",
    "sticker",
    "gif",
    "image",
    "photo",
    "post",
    "reply", //
    "thread",
    "react",
    "vote",
    "poll",
    "event",
    "week",
    "weekend",
    "month",
    "hour",
    "minute",
    "second",
    "free",
    "busy",
    "tired",
    "hungry",
    "home", //
];

/// Integer Zipf weights `2^32 / (rank + 1)` over the channel popularity ranks.
static CHANNEL_CDF: [u64; S3_CHANNELS as usize] = channel_cdf();

const fn channel_cdf() -> [u64; S3_CHANNELS as usize] {
    let mut cdf = [0u64; S3_CHANNELS as usize];
    let mut acc = 0u64;
    let mut k = 0usize;
    while k < S3_CHANNELS as usize {
        acc += (1u64 << 32) / (k as u64 + 1);
        cdf[k] = acc;
        k += 1;
    }
    cdf
}

/// Popularity rank (0 = most active) of a channel for a uniform 64-bit draw:
/// Zipf with exponent 1 over [`S3_CHANNELS`] ranks, integer arithmetic only.
pub fn channel_rank(draw: u64) -> u64 {
    let total = CHANNEL_CDF[CHANNEL_CDF.len() - 1];
    let t = draw % total;
    CHANNEL_CDF.partition_point(|&c| c <= t) as u64
}

/// Channel id of a popularity rank: a fixed permutation of `[0, S3_CHANNELS)`
/// (337 is coprime with 1000) that spreads hot channels over the key space.
pub fn channel_for_rank(rank: u64) -> u64 {
    (rank % S3_CHANNELS * 337 + 211) % S3_CHANNELS
}

/// S3: channel of message `i`.
pub fn channel_of(i: u64, seed: u64) -> u64 {
    channel_for_rank(channel_rank(mix3(seed, domain::CHANNEL, i)))
}

/// S3: common prefix of every message key of `channel`. A reverse prefix
/// scan returns the latest messages of the channel first.
pub fn channel_prefix(channel: u64) -> Vec<u8> {
    format!("ch/{channel:08}/msg/").into_bytes()
}

/// S3: send time of message `i` (Unix ms), strictly increasing with `i`.
pub fn s3_time_ms(seed: u64, i: u64) -> u64 {
    S3_BASE_MS + i * S3_GAP_MS + mix3(seed, domain::TIME, i) % S3_GAP_MS
}

/// S3: snowflake id of message `i`.
pub fn s3_snowflake(seed: u64, i: u64) -> u64 {
    snowflake(s3_time_ms(seed, i), mix3(seed, domain::NODE, i), i)
}

/// Discord snowflake: `(ms since Discord epoch) << 22 | worker << 17 | process << 12 | increment`.
/// `node` carries the 10 worker/process bits.
fn snowflake(unix_ms: u64, node: u64, increment: u64) -> u64 {
    ((unix_ms - DISCORD_EPOCH_MS) << 22) | ((node & 0x3FF) << 12) | (increment & 0xFFF)
}

const S3_CHANNEL_BASE_MS: u64 = 1_483_228_800_000; // 2017-01-01T00:00:00Z
const S3_USER_BASE_MS: u64 = 1_464_739_200_000; // 2016-06-01T00:00:00Z
const S6_BASE_MS: u64 = 1_704_067_200_000; // 2024-01-01T00:00:00Z

fn channel_snowflake(channel: u64) -> u64 {
    snowflake(S3_CHANNEL_BASE_MS + channel * 86_400_000, channel, channel)
}

fn user_snowflake(user: u64) -> u64 {
    let h = splitmix64(user ^ domain::USER);
    snowflake(
        S3_USER_BASE_MS + user * 3_600_000 + h % 3_600_000,
        h >> 20,
        h >> 40,
    )
}

/// Skewed author choice: product of two uniforms, favours low user ids.
fn skewed_user(draw: u64) -> u64 {
    let a = draw % S3_USERS;
    let b = (draw >> 32) % S3_USERS;
    a * (b + 1) / S3_USERS
}

fn push_username(out: &mut Vec<u8>, user: u64) {
    let h = splitmix64(user ^ domain::USERNAME);
    out.extend_from_slice(WORDS[(h & 0xFF) as usize].as_bytes());
    out.push(b'_');
    out.extend_from_slice(WORDS[((h >> 8) & 0xFF) as usize].as_bytes());
    let _ = write!(out, "{}", user % 100);
}

/// Exactly `len` bytes of space-separated words (the last word may be cut).
fn push_words(out: &mut Vec<u8>, seed: u64, len: usize) {
    let start = out.len();
    let mut rng = SplitMix64::new(seed);
    while out.len() - start < len {
        if out.len() > start {
            out.push(b' ');
        }
        let r = rng.next_u64();
        // Product of two uniform bytes: frequent (low-index) words dominate.
        let idx = ((r & 0xFF) * (((r >> 8) & 0xFF) + 1)) >> 8;
        out.extend_from_slice(WORDS[idx as usize].as_bytes());
    }
    out.truncate(start + len);
}

/// `YYYY-MM-DDTHH:MM:SS.mmm000+00:00` (the Discord API format).
fn push_iso8601(out: &mut Vec<u8>, unix_ms: u64) {
    let secs = unix_ms / 1000;
    let (y, m, d) = civil_from_days((secs / 86_400) as i64);
    let sod = secs % 86_400;
    let _ = write!(
        out,
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}000+00:00",
        sod / 3600,
        sod / 60 % 60,
        sod % 60,
        unix_ms % 1000
    );
}

/// Proleptic Gregorian (year, month, day) of a day count since 1970-01-01
/// (H. Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

struct ChatMessage {
    id: u64,
    channel: u64,
    user: u64,
    unix_ms: u64,
    content_seed: u64,
}

impl ChatMessage {
    /// Everything up to and including `"content":"`.
    fn write_head(&self, out: &mut Vec<u8>) {
        let _ = write!(
            out,
            "{{\"id\":\"{}\",\"channel_id\":\"{}\",\"author\":{{\"id\":\"{}\",\"username\":\"",
            self.id,
            channel_snowflake(self.channel),
            user_snowflake(self.user)
        );
        push_username(out, self.user);
        out.extend_from_slice(b"\"},\"content\":\"");
    }

    /// Everything after the content.
    fn write_tail(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"\",\"timestamp\":\"");
        push_iso8601(out, self.unix_ms);
        out.extend_from_slice(b"\",\"mentions\":[],\"pinned\":false}");
    }
}

fn s3_message(seed: u64, i: u64) -> ChatMessage {
    ChatMessage {
        id: s3_snowflake(seed, i),
        channel: channel_of(i, seed),
        user: skewed_user(mix3(seed, domain::AUTHOR, i)),
        unix_ms: s3_time_ms(seed, i),
        content_seed: mix3(seed, domain::CONTENT, i),
    }
}

fn chat_json(seed: u64, i: u64, size: usize) -> Vec<u8> {
    let m = s3_message(seed, i);
    let mut out = Vec::with_capacity(size.max(256));
    m.write_head(&mut out);
    let mut tail = Vec::with_capacity(64);
    m.write_tail(&mut tail);
    let content_len = size.saturating_sub(out.len() + tail.len());
    push_words(&mut out, m.content_seed, content_len);
    out.extend_from_slice(&tail);
    out
}

/// S3: size of the JSON of message `i` with empty content, i.e. the smallest
/// value S3 can produce for that record.
pub fn s3_template_len(seed: u64, i: u64) -> usize {
    chat_json(seed, i, 0).len()
}

// ---------------------------------------------------------------------------
// S4: exact duplicates
// ---------------------------------------------------------------------------

fn is_duplicate(seed: u64, i: u64) -> bool {
    let group = i / 10;
    let pos = (i % 10) as u8;
    // Record 0 has no earlier record to copy.
    let first = usize::from(group == 0);
    let mut slots = [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let mut rng = SplitMix64::new(mix3(seed, domain::S4_GROUP, group));
    let picks = S4_DUPLICATES_PER_10 as usize;
    for k in 0..picks {
        let lo = first + k;
        let j = lo + rng.below((10 - lo) as u64) as usize;
        slots.swap(lo, j);
    }
    slots[first..first + picks].contains(&pos)
}

/// S4: the earlier record whose value record `i` copies, or `None` when
/// record `i` is an original.
pub fn duplicate_source(seed: u64, i: u64) -> Option<u64> {
    if is_duplicate(seed, i) {
        Some(mix3(seed, domain::S4_SOURCE, i) % i)
    } else {
        None
    }
}

/// S4: the original record whose value record `i` carries.
pub fn duplicate_root(seed: u64, i: u64) -> u64 {
    let mut i = i;
    while let Some(j) = duplicate_source(seed, i) {
        i = j;
    }
    i
}

// ---------------------------------------------------------------------------
// S6: zstd frames of chat text
// ---------------------------------------------------------------------------

const S6_MAX_COMPRESSIONS: usize = 8;

/// First guess of the text length whose level-3 frame is `size` bytes: the
/// measured text/frame ratio of this text grows from ~1.9 at 256 B to ~4.7 at
/// 64 KiB, fitted by `4.7 * size / (size + 400)`.
fn s6_initial_text_len(size: usize) -> usize {
    let s = size as u128;
    ((47 * s * s) / (10 * (s + 400))) as usize
}

/// JSON-lines chat log of one channel, generated on demand (prefix-stable).
struct ChatLog {
    buf: Vec<u8>,
    rng: SplitMix64,
    channel: u64,
    start_ms: u64,
    next: u64,
}

impl ChatLog {
    fn new(seed: u64, i: u64) -> ChatLog {
        let mut rng = SplitMix64::new(mix3(seed, domain::S6_ARCHIVE, i));
        let channel = channel_for_rank(channel_rank(rng.next_u64()));
        ChatLog {
            buf: Vec::new(),
            rng,
            channel,
            start_ms: S6_BASE_MS + i * 60_000,
            next: 0,
        }
    }

    fn prefix(&mut self, len: usize) -> &[u8] {
        while self.buf.len() < len {
            self.push_line();
        }
        &self.buf[..len]
    }

    fn push_line(&mut self) {
        let j = self.next;
        self.next += 1;
        let unix_ms = self.start_ms + j * 1000 + self.rng.below(1000);
        let node = self.rng.next_u64();
        let user = skewed_user(self.rng.next_u64());
        let r = self.rng.next_u64();
        // Mostly short messages: product of two uniforms in [0, 200).
        let content_len = ((r % 200) * ((r >> 32) % 200) / 200) as usize;
        let m = ChatMessage {
            id: snowflake(unix_ms, node, j),
            channel: self.channel,
            user,
            unix_ms,
            content_seed: self.rng.next_u64(),
        };
        m.write_head(&mut self.buf);
        push_words(&mut self.buf, m.content_seed, content_len);
        m.write_tail(&mut self.buf);
        self.buf.push(b'\n');
    }
}

fn compressed(seed: u64, i: u64, size: usize) -> Vec<u8> {
    let mut log = ChatLog::new(seed, i);
    let mut text_len = s6_initial_text_len(size).max(16);
    let mut best: Option<Vec<u8>> = None;
    for _ in 0..S6_MAX_COMPRESSIONS {
        let frame = zstd::bulk::compress(log.prefix(text_len), S6_ZSTD_LEVEL)
            .expect("zstd compression of an in-memory buffer");
        let diff = frame.len().abs_diff(size);
        let improves = best.as_ref().is_none_or(|b| diff < b.len().abs_diff(size));
        let frame_len = frame.len();
        if improves {
            best = Some(frame);
        }
        if diff * 50 <= size {
            break;
        }
        let next = (text_len as u128 * size as u128 / frame_len.max(1) as u128) as usize;
        if next == text_len {
            break;
        }
        text_len = next;
    }
    best.unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Digests
// ---------------------------------------------------------------------------

/// Incremental dataset digest: BLAKE3 over, for each record in order,
/// `u64 LE key length || key || u64 LE value length || value`.
#[derive(Clone, Default)]
pub struct DatasetHasher {
    h: blake3::Hasher,
    records: u64,
}

impl DatasetHasher {
    pub fn new() -> DatasetHasher {
        DatasetHasher::default()
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        self.h.update(&(key.len() as u64).to_le_bytes());
        self.h.update(key);
        self.h.update(&(value.len() as u64).to_le_bytes());
        self.h.update(value);
        self.records += 1;
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn finish(&self) -> [u8; 32] {
        *self.h.finalize().as_bytes()
    }
}

/// Digest of the first `n` records (see [`DatasetHasher`]).
pub fn dataset_digest(scenario: Scenario, seed: u64, n: u64, value_size: usize) -> [u8; 32] {
    let mut h = DatasetHasher::new();
    for i in 0..n {
        let (k, v) = record(scenario, seed, i, value_size);
        h.add(&k, &v);
    }
    h.finish()
}

// ---------------------------------------------------------------------------
// Access distributions for the harness
// ---------------------------------------------------------------------------

/// Zipfian ranks in `[0, n)` (0 = most popular) with the constant-time method
/// of Gray et al. ("Quickly generating billion-record synthetic databases",
/// SIGMOD 1994), as used by YCSB. Setup is O(n) (the zeta sum).
///
/// Floating point (`powf`) is used, so samples are reproducible on one
/// platform but not guaranteed bit-identical across platforms; datasets never
/// depend on it.
#[derive(Clone, Debug)]
pub struct Zipf {
    n: u64,
    theta: f64,
    zetan: f64,
    alpha: f64,
    eta: f64,
    half_pow_theta: f64,
}

impl Zipf {
    /// `n >= 1` items with exponent `theta` in (0, 1) (YCSB uses 0.99).
    pub fn new(n: u64, theta: f64) -> Zipf {
        assert!(n >= 1, "Zipf needs at least one item");
        assert!(theta > 0.0 && theta < 1.0, "Zipf theta must be in (0, 1)");
        let zetan = zeta(n, theta);
        let eta = if n > 2 {
            (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta(2, theta) / zetan)
        } else {
            0.0
        };
        Zipf {
            n,
            theta,
            zetan,
            alpha: 1.0 / (1.0 - theta),
            eta,
            half_pow_theta: 0.5f64.powf(theta),
        }
    }

    pub fn n(&self) -> u64 {
        self.n
    }

    pub fn theta(&self) -> f64 {
        self.theta
    }

    /// Rank for a uniform `u` in `[0, 1)`.
    pub fn sample(&self, u: f64) -> u64 {
        let uz = u * self.zetan;
        if self.n == 1 || uz < 1.0 {
            return 0;
        }
        if self.n == 2 || uz < 1.0 + self.half_pow_theta {
            return 1;
        }
        let r = (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        r.min(self.n - 1)
    }

    /// Exact Zipf probability of `rank`.
    pub fn probability(&self, rank: u64) -> f64 {
        ((rank + 1) as f64).powf(-self.theta) / self.zetan
    }
}

/// `sum_{i=1..n} i^-theta`.
pub fn zeta(n: u64, theta: f64) -> f64 {
    (1..=n).map(|i| (i as f64).powf(-theta)).sum()
}

const SCRAMBLE_PRIME: u64 = (1 << 61) - 1;

/// Bijection of `[0, n)` (`n < 2^61 - 1`) that spreads consecutive ranks over
/// the key space ("scrambled Zipfian"): `(rank * (2^61 - 1) + c) mod n`. The
/// multiplier is a prime larger than `n`, hence coprime with it.
pub fn scramble(rank: u64, n: u64) -> u64 {
    assert!(
        n > 0 && n < SCRAMBLE_PRIME,
        "scramble domain must be in 1..2^61-1"
    );
    ((rank as u128 * SCRAMBLE_PRIME as u128 + 0x5DEE_CE66D) % n as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    fn hex(d: &[u8]) -> String {
        d.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn records_are_deterministic_and_seed_dependent() {
        for sc in Scenario::ALL {
            for i in [0u64, 1, 2, 7, 1000] {
                assert_eq!(record(sc, 7, i, 300), record(sc, 7, i, 300), "{sc:?} {i}");
            }
            // Values that depend on the seed (S1 even records are all zeros).
            let i = if sc == Scenario::Duplicates { 0 } else { 1 };
            assert_ne!(value(sc, 1, i, 300), value(sc, 2, i, 300), "{sc:?}");
        }
    }

    /// Fixed digests: any change to a generator must bump DATASETS_VERSION and
    /// update these values and benchdata/manifest.json.
    #[test]
    fn fixed_digests() {
        let expected = [
            (
                Scenario::Repetitive,
                "7dd525790980002371ffdac8b347b067cc142f16821c889a83936edda0c2754b",
            ),
            (
                Scenario::Sequences,
                "626443c5c66530a72276ce6d9d2a101c7526a6dbb677e554e18a08bc81269e4c",
            ),
            (
                Scenario::ChatJson,
                "5cc6075d62b32f94525a9fbb6ad24b115d5f6f9a16c42118571d5ff65d7bbd48",
            ),
            (
                Scenario::Duplicates,
                "3e1762d9508c7ce56fbed953d9e999bb2d8e33e1ed0ad127b6281bd006bd6f48",
            ),
            (
                Scenario::HighEntropy,
                "e1cc0b7557fa3a45595caee3933f24120db8746a8347c23b1633e19e3bb24a0d",
            ),
            // Also depends on libzstd 1.5.7 (zstd-sys 2.1.0) level-3 output.
            (
                Scenario::Compressed,
                "a7c9aaaaaa9e1042130317b89adc01f5ef943ec843a6db6c5edfeae409b04868",
            ),
        ];
        for (sc, want) in expected {
            assert_eq!(
                hex(&dataset_digest(sc, DEFAULT_SEED, 20, 300)),
                want,
                "{sc:?}"
            );
        }
    }

    #[test]
    fn value_sizes() {
        for size in [0usize, 1, 7, 8, 64, 100, 256, 1000, 1024, 4096, 20_000] {
            for i in 0..12 {
                for sc in [
                    Scenario::Repetitive,
                    Scenario::Sequences,
                    Scenario::Duplicates,
                    Scenario::HighEntropy,
                ] {
                    assert_eq!(value(sc, 3, i, size).len(), size, "{sc:?} {i} {size}");
                }
                let chat = value(Scenario::ChatJson, 3, i, size);
                assert_eq!(chat.len(), size.max(s3_template_len(3, i)), "s3 {i} {size}");
            }
        }
        // The S3 template is about 200 bytes.
        let t = s3_template_len(DEFAULT_SEED, 0);
        assert!((150..=260).contains(&t), "template {t}");
        // S6 frames close to the requested size.
        for size in [256usize, 1024, 4096, 65_536] {
            for i in 0..4 {
                let v = value(Scenario::Compressed, 5, i, size);
                let ratio = v.len() as f64 / size as f64;
                assert!((0.9..=1.1).contains(&ratio), "s6 {i} {size} -> {}", v.len());
            }
        }
        for size in [0usize, 64] {
            let v = value(Scenario::Compressed, 5, 0, size);
            assert!(v.len() <= 128, "s6 small {size} -> {}", v.len());
        }
    }

    #[test]
    fn s1_is_zeros_or_short_period() {
        for i in 0..40u64 {
            let v = value(Scenario::Repetitive, 9, i, 1000);
            if i.is_multiple_of(2) {
                assert!(v.iter().all(|&b| b == 0));
            } else {
                let p = (1..=64)
                    .find(|&p| (p..v.len()).all(|k| v[k] == v[k - p]))
                    .expect("period <= 64");
                assert!(p <= 64);
            }
        }
    }

    #[test]
    fn s2_is_an_arithmetic_progression() {
        for i in 0..20u64 {
            let v = value(Scenario::Sequences, 9, i, 800);
            let terms: Vec<u64> = v
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect();
            assert_eq!(terms.len(), 100);
            let step = terms[1] - terms[0];
            assert!((1..=65_536).contains(&step));
            assert!(terms[0] < 1 << 48);
            assert!(terms.windows(2).all(|w| w[1] == w[0] + step));
        }
        // A trailing partial term is the prefix of the next term.
        let full = value(Scenario::Sequences, 9, 3, 16);
        let part = value(Scenario::Sequences, 9, 3, 13);
        assert_eq!(&full[..13], &part[..]);
    }

    #[test]
    fn s3_keys_sort_by_time_within_a_channel() {
        let seed = DEFAULT_SEED;
        let n = 5000u64;
        let mut map = BTreeMap::new();
        let mut per_channel: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for i in 0..n {
            let (k, v) = record(Scenario::ChatJson, seed, i, 256);
            let c = channel_of(i, seed);
            assert!(k.starts_with(&channel_prefix(c)));
            assert_eq!(k.len(), channel_prefix(c).len() + 20);
            // The JSON carries the same snowflake as the key.
            let sf = s3_snowflake(seed, i);
            let text = String::from_utf8(v).unwrap();
            assert!(text.starts_with(&format!("{{\"id\":\"{sf}\"")), "{text}");
            assert!(
                text.ends_with("\"mentions\":[],\"pinned\":false}"),
                "{text}"
            );
            assert_eq!((sf >> 22) + DISCORD_EPOCH_MS, s3_time_ms(seed, i));
            per_channel.entry(c).or_default().push(i);
            assert!(map.insert(k, i).is_none(), "duplicate key");
        }
        // Keys increase with i inside a channel.
        for idx in per_channel.values() {
            for w in idx.windows(2) {
                assert!(key(Scenario::ChatJson, seed, w[0]) < key(Scenario::ChatJson, seed, w[1]));
            }
        }
        // A reverse prefix scan returns the newest messages first.
        for (&c, idx) in per_channel.iter().take(50) {
            let p = channel_prefix(c);
            let latest: Vec<u64> = map
                .range(p.clone()..)
                .take_while(|(k, _)| k.starts_with(&p))
                .map(|(_, &i)| i)
                .collect();
            let rev: Vec<u64> = latest.iter().rev().take(50).copied().collect();
            let want: Vec<u64> = idx.iter().rev().take(50).copied().collect();
            assert_eq!(rev, want);
        }
        // Zipf skew: the most popular channel holds far more than 1/1000 of the messages.
        let top = per_channel.values().map(Vec::len).max().unwrap();
        assert!(top as u64 > n / 20, "top channel {top}");
    }

    #[test]
    fn s3_timestamp_format() {
        let mut out = Vec::new();
        push_iso8601(&mut out, S3_BASE_MS + 3_723_456);
        assert_eq!(out, b"2025-01-01T01:02:03.456000+00:00");
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(20_089), (2025, 1, 1));
    }

    #[test]
    fn s4_duplicate_ratio_and_copies() {
        let seed = 11;
        let n = 1000u64;
        let dups: Vec<u64> = (0..n)
            .filter(|&i| duplicate_source(seed, i).is_some())
            .collect();
        assert_eq!(dups.len() as u64, n * S4_DUPLICATES_PER_10 / 10);
        assert!(duplicate_source(seed, 0).is_none());
        for &i in &dups {
            let j = duplicate_source(seed, i).unwrap();
            assert!(j < i);
            assert_eq!(
                value(Scenario::Duplicates, seed, i, 2048),
                value(Scenario::Duplicates, seed, j, 2048)
            );
        }
        let originals: HashSet<Vec<u8>> = (0..n)
            .filter(|&i| duplicate_source(seed, i).is_none())
            .map(|i| value(Scenario::Duplicates, seed, i, 64))
            .collect();
        assert_eq!(originals.len() as u64, n - dups.len() as u64);
        let distinct: HashSet<Vec<u8>> = (0..n)
            .map(|i| value(Scenario::Duplicates, seed, i, 64))
            .collect();
        assert_eq!(distinct.len(), originals.len());
    }

    #[test]
    fn s5_is_high_entropy() {
        let v = value(Scenario::HighEntropy, 1, 1, 1 << 16);
        let mut counts = [0u32; 256];
        for &b in &v {
            counts[b as usize] += 1;
        }
        // 65536 bytes: 256 expected per symbol.
        assert!(counts.iter().all(|&c| (150..=370).contains(&c)));
    }

    #[test]
    fn s6_frames_decode_to_chat_lines() {
        let v = value(Scenario::Compressed, 2, 4, 4096);
        assert_eq!(&v[..4], &[0x28, 0xB5, 0x2F, 0xFD]);
        let text = zstd::bulk::decompress(&v, 1 << 20).unwrap();
        assert!(text.starts_with(b"{\"id\":\""));
        assert!(text.len() > v.len());
        assert!(text.split(|&b| b == b'\n').count() > 10);
    }

    #[test]
    fn keys_and_miss_keys() {
        for sc in Scenario::ALL {
            let keys: HashSet<Vec<u8>> = (0..500).map(|i| key(sc, 5, i)).collect();
            assert_eq!(keys.len(), 500, "{sc:?} keys must be unique");
            for i in 0..500 {
                assert!(!keys.contains(&miss_key(sc, 5, i)), "{sc:?}");
                assert_eq!(record(sc, 5, i, 64).0, key(sc, 5, i));
            }
            assert_eq!(key(sc, 5, 42).len(), key(sc, 5, 43).len());
        }
        assert_eq!(
            key(Scenario::Repetitive, 0, 42),
            b"s1-repetitive/000000000042"
        );
        assert_eq!(channel_prefix(7), b"ch/00000007/msg/");
    }

    #[test]
    fn scenario_names_parse() {
        for sc in Scenario::ALL {
            assert_eq!(Scenario::parse(sc.code()), Some(sc));
            assert_eq!(Scenario::parse(sc.name()), Some(sc));
            assert_eq!(Scenario::parse(&sc.code().to_uppercase()), Some(sc));
        }
        assert_eq!(Scenario::parse("s7"), None);
    }

    #[test]
    fn word_list_is_json_safe_and_unique() {
        let set: HashSet<&str> = WORDS.iter().copied().collect();
        assert_eq!(set.len(), WORDS.len());
        assert!(
            WORDS
                .iter()
                .all(|w| !w.is_empty() && w.bytes().all(|b| b.is_ascii_lowercase()))
        );
    }

    #[test]
    fn channel_distribution() {
        let mut seen = HashSet::new();
        for r in 0..S3_CHANNELS {
            assert!(seen.insert(channel_for_rank(r)));
        }
        assert_eq!(channel_rank(0), 0);
        assert!(channel_rank(u64::MAX) < S3_CHANNELS);
        let mut rng = SplitMix64::new(1);
        let mut top = 0u64;
        let draws = 100_000u64;
        for _ in 0..draws {
            if channel_rank(rng.next_u64()) == 0 {
                top += 1;
            }
        }
        // P(rank 0) = 1 / H_1000 = 0.1336.
        let p = top as f64 / draws as f64;
        assert!((0.125..0.142).contains(&p), "{p}");
    }

    #[test]
    fn splitmix_reference_values() {
        // Reference outputs of splitmix64 seeded with 0 (Vigna's C implementation).
        let mut g = SplitMix64::new(0);
        assert_eq!(g.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(g.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(g.next_u64(), 0x06C4_5D18_8009_454F);
        let mut g = SplitMix64::new(3);
        for _ in 0..1000 {
            assert!(g.below(10) < 10);
            let f = g.next_f64();
            assert!((0.0..1.0).contains(&f));
        }
    }

    #[test]
    fn zipf_matches_the_law() {
        let z = Zipf::new(1000, 0.99);
        let mut rng = SplitMix64::new(42);
        let draws = 200_000u64;
        let mut counts = vec![0u64; 1000];
        for _ in 0..draws {
            let r = z.sample(rng.next_f64());
            assert!(r < 1000);
            counts[r as usize] += 1;
        }
        for rank in [0u64, 1] {
            let want = z.probability(rank);
            let got = counts[rank as usize] as f64 / draws as f64;
            assert!(
                (got - want).abs() / want < 0.05,
                "rank {rank}: {got} vs {want}"
            );
        }
        assert!(counts[0] > counts[1] && counts[1] > counts[9] && counts[9] > counts[500]);
        // Degenerate sizes.
        assert_eq!(Zipf::new(1, 0.99).sample(0.999), 0);
        let z2 = Zipf::new(2, 0.99);
        assert_eq!(z2.sample(0.0), 0);
        assert_eq!(z2.sample(0.999), 1);
        assert!((zeta(3, 0.5) - (1.0 + 2f64.powf(-0.5) + 3f64.powf(-0.5))).abs() < 1e-12);
    }

    #[test]
    fn scramble_is_a_bijection() {
        for n in [1u64, 2, 3, 10, 97, 1000, 4096] {
            let set: HashSet<u64> = (0..n).map(|r| scramble(r, n)).collect();
            assert_eq!(set.len() as u64, n);
            assert!(set.iter().all(|&x| x < n));
        }
    }

    #[test]
    fn digest_matches_incremental_hasher() {
        let mut h = DatasetHasher::new();
        for i in 0..10 {
            let (k, v) = record(Scenario::ChatJson, 1, i, 300);
            h.add(&k, &v);
        }
        assert_eq!(h.records(), 10);
        assert_eq!(h.finish(), dataset_digest(Scenario::ChatJson, 1, 10, 300));
        assert_ne!(
            dataset_digest(Scenario::ChatJson, 1, 10, 300),
            dataset_digest(Scenario::ChatJson, 1, 11, 300)
        );
    }
}
