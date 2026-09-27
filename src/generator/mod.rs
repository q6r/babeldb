//! Registered, versioned, deterministic generators for `put_generated`.
//!
//! A generator expands a small parameter blob into an exact byte stream of
//! known length. A generated record persists only
//! `(generator id, version, params, digest)`; its bytes are recomputed on read,
//! and any byte range can be produced directly.
//!
//! # Rules every generator follows
//!
//! - **The code is part of the stored representation.** A generator is code
//!   shipped in this binary. Reading a generated record back needs exactly that
//!   code, so its size counts in the space accounting (spec §11: binary size
//!   attributable to generators and codecs).
//! - **Deterministic and platform independent.** The output depends only on
//!   `(params, offset, length)`. It never depends on the time, the locale, the
//!   platform (endianness, word size, SIMD level), thread scheduling or
//!   unspecified PRNG state. Integers are little-endian and arithmetic is
//!   checked `u64`.
//! - **Frozen per version.** Once an `(id, version)` pair ships, its output is
//!   fixed for every params. Changing any output byte, even to fix a bug,
//!   requires a new version number. The old version stays registered so
//!   existing records keep reading back exactly.
//! - **Random access.** `generate(params, offset, out)` produces bytes
//!   `[offset, offset + out.len())` without computing the bytes before
//!   `offset`. The cost is O(out.len()) plus O(1) setup.
//! - **Untrusted params never panic.** Every entry point validates params and
//!   the requested range with checked arithmetic and returns
//!   [`Error::InvalidArgument`]. On error the built-ins leave `out` untouched.
//!
//! Built-ins, all version 1: [`ArithU64`] (`ids::ARITH_U64`), [`Blake3Xof`]
//! (`ids::BLAKE3_XOF`) and [`Repeat`] (`ids::REPEAT`).

mod arith;
mod repeat;
mod xof;

pub use arith::{ARITH_U64_PARAMS_LEN, ArithU64};
pub use repeat::{REPEAT_MAX_MOTIF_LEN, Repeat};
pub use xof::{BLAKE3_XOF_PARAMS_LEN, Blake3Xof};

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
    /// Fails with `InvalidArgument` if the params are invalid or the range is
    /// not inside `[0, output_len]`, checked without overflow.
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
        let mut r = Registry::empty();
        r.register(Box::new(ArithU64));
        r.register(Box::new(Blake3Xof));
        r.register(Box::new(Repeat));
        r
    }

    /// Register a generator. The first registration of an `(id, version)` pair
    /// wins and later duplicates are ignored, so a built-in can never be
    /// shadowed and existing records keep decoding with the code that wrote
    /// them.
    pub fn register(&mut self, g: Box<dyn Generator>) {
        let (id, version) = (g.id(), g.version());
        if self.get(id, version).is_err() {
            self.gens.push(g);
        }
    }

    pub fn get(&self, id: u16, version: u16) -> Result<&dyn Generator> {
        self.gens
            .iter()
            .find(|g| g.id() == id && g.version() == version)
            .map(|g| g.as_ref())
            .ok_or(Error::UnknownGenerator { id, version })
    }

    pub fn list(&self) -> Vec<(u16, u16, &'static str)> {
        self.gens
            .iter()
            .map(|g| (g.id(), g.version(), g.name()))
            .collect()
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

/// Check that `[offset, offset + len)` lies inside an output of `output_len`
/// bytes, without overflow. An empty range at `offset == output_len` is valid.
pub(crate) fn check_range(name: &str, output_len: u64, offset: u64, len: usize) -> Result<()> {
    let end = u64::try_from(len).ok().and_then(|l| offset.checked_add(l));
    match end {
        Some(end) if end <= output_len => Ok(()),
        _ => Err(Error::InvalidArgument(format!(
            "{name}: range of {len} bytes at offset {offset} exceeds output length {output_len}"
        ))),
    }
}

/// Split a little-endian `u64` off the front of `bytes`.
pub(crate) fn take_u64_le(bytes: &[u8]) -> Option<(u64, &[u8])> {
    bytes
        .split_first_chunk::<8>()
        .map(|(head, rest)| (u64::from_le_bytes(*head), rest))
}
