//! Registered, versioned, deterministic generators for `put_generated`.
//! A generator is code shipped in the binary: its size counts in the space
//! accounting. Output never depends on time, platform or unspecified PRNG state.
//! SKELETON — the util agent implements the built-in generators.

use crate::error::{Error, Result};

pub mod ids {
    /// params: start u64 LE | step u64 LE | count u64 LE -> count u64 LE values
    pub const ARITH_U64: u16 = 1;
    /// params: key[32] | len u64 LE -> BLAKE3 keyed XOF stream (random access)
    pub const BLAKE3_XOF: u16 = 2;
    /// params: total_len u64 LE | motif bytes -> motif repeated to total_len
    pub const REPEAT: u16 = 3;
}

pub trait Generator: Send + Sync {
    fn id(&self) -> u16;
    fn version(&self) -> u16;
    fn name(&self) -> &'static str;
    /// Validate params and return the exact output length.
    fn output_len(&self, params: &[u8]) -> Result<u64>;
    /// Write bytes `[offset, offset + out.len())` of the output into `out`.
    fn generate(&self, params: &[u8], offset: u64, out: &mut [u8]) -> Result<()>;
}

pub struct Registry {
    gens: Vec<Box<dyn Generator>>,
}

impl Registry {
    pub fn empty() -> Registry {
        Registry { gens: Vec::new() }
    }

    /// Built-in generators (ARITH_U64, BLAKE3_XOF, REPEAT; version 1).
    pub fn builtin() -> Registry {
        Registry::empty()
    }

    pub fn register(&mut self, g: Box<dyn Generator>) {
        self.gens.push(g);
    }

    pub fn get(&self, id: u16, version: u16) -> Result<&dyn Generator> {
        self.gens
            .iter()
            .find(|g| g.id() == id && g.version() == version)
            .map(|g| g.as_ref())
            .ok_or(Error::UnknownGenerator { id, version })
    }

    pub fn list(&self) -> Vec<(u16, u16, &'static str)> {
        self.gens.iter().map(|g| (g.id(), g.version(), g.name())).collect()
    }
}

pub fn arith_params(start: u64, step: u64, count: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(24);
    p.extend_from_slice(&start.to_le_bytes());
    p.extend_from_slice(&step.to_le_bytes());
    p.extend_from_slice(&count.to_le_bytes());
    p
}

pub fn blake3_xof_params(key: &[u8; 32], len: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(40);
    p.extend_from_slice(key);
    p.extend_from_slice(&len.to_le_bytes());
    p
}

pub fn repeat_params(total_len: u64, motif: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(8 + motif.len());
    p.extend_from_slice(&total_len.to_le_bytes());
    p.extend_from_slice(motif);
    p
}
