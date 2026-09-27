//! Deterministic synthetic datasets (scenarios S1–S6 of `benchdata/manifest.json`).
//! Explicit hypotheses stand in for the production corpus, which is not available.
//! SKELETON — the bench agent implements the generators.

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
}

/// The i-th (key, value) of a scenario for a target value size. Deterministic
/// for (scenario, seed, i, value_size) on every platform.
pub fn record(scenario: Scenario, seed: u64, i: u64, value_size: usize) -> (Vec<u8>, Vec<u8>) {
    let _ = (scenario, seed, i, value_size);
    todo!("datasets::record")
}
