//! BLAKE3 content identity. Used for dedupe candidates and integrity checks,
//! never as proof of equality: candidates are always compared byte by byte.

pub type Digest = [u8; 32];

pub fn digest(bytes: &[u8]) -> Digest {
    *blake3::hash(bytes).as_bytes()
}

/// Incremental hashing for streamed values (imports, generated records).
#[derive(Default)]
pub struct StreamHasher(blake3::Hasher);

impl StreamHasher {
    pub fn new() -> Self {
        StreamHasher(blake3::Hasher::new())
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    pub fn finalize(&self) -> Digest {
        *self.0.finalize().as_bytes()
    }
}

pub fn to_hex(d: &[u8]) -> String {
    d.iter().map(|b| format!("{b:02x}")).collect()
}
