//! `TemplatePatchV1`: output rebuilt from an identified immutable template
//! (param) plus exact edit operations (copies from the template and literal
//! inserts). Decoding is copy-only; no reserialization, bytes are preserved.
//!
//! Body layout (all integers are canonical unsigned LEB128 varints, as written
//! by `format::put_varint` and read by `format::get_varint`):
//!
//! ```text
//! body := op*                  ops run until the body ends (no header, no count)
//! op   := varint(head) payload
//!   head even: LITERAL  len = head >> 1, len >= 1
//!              payload = len bytes, appended to the output
//!   head odd:  COPY     len = (head >> 1) + MIN_COPY
//!              payload = varint(zigzag(src - cursor))
//!              appends template[src .. src + len], then cursor = src + len
//! cursor: template offset, 0 before the first COPY
//! zigzag(d) = (d << 1) ^ (d >> 63) for a signed 64-bit d
//! ```
//!
//! The output must be exactly raw_len bytes; the empty body is the empty output.
//! Every op is validated against the rest of the body, the template bounds and
//! the remaining output before anything is copied.
//!
//! Encoding is a greedy parse. At each position the candidates are the
//! template position right after the previous copy (same alignment, cheapest
//! to encode) and up to `MAX_CHAIN` template positions sharing the 4-byte hash
//! of the input; the one saving the most bytes wins, extended backwards over
//! pending literals. A copy is only emitted when it is at least 2 bytes cheaper
//! than the literals it replaces. After a run of misses the scan advances in
//! growing steps (as LZ4 does), bounding the work on unrelated input.

use crate::error::{Error, Result};
use crate::format::{MAX_UNIT_LEN, get_varint, put_varint};

/// Shortest encodable copy.
pub const MIN_COPY: usize = 4;
/// Hash-chain candidates examined per position (besides the continuation).
pub const MAX_CHAIN: usize = 16;
/// Longest template `train` proposes.
pub const DEFAULT_MAX_TEMPLATE_LEN: usize = 64 * 1024;
/// Template candidates evaluated by `train`.
pub const TRAIN_CANDIDATES: usize = 16;
/// Samples each training candidate is evaluated on.
pub const TRAIN_EVAL_SAMPLES: usize = 256;

const HASH_LEN: usize = 4;
const NONE: u32 = u32::MAX;
/// Misses before the scan step grows by one byte.
const SKIP_SHIFT: u32 = 4;

/// Prepared template (bytes + match index), built once per param id (Send + Sync).
pub struct Template {
    pub id: u64,
    bytes: Vec<u8>,
    hash_shift: u32,
    /// Hash bucket -> last indexed position with that hash (NONE if empty).
    head: Vec<u32>,
    /// Position -> previous indexed position with the same hash (NONE ends).
    prev: Vec<u32>,
}

impl Template {
    pub fn new(id: u64, bytes: Vec<u8>) -> Template {
        // Positions are u32; bytes past u32::MAX - 1 can be copied but not searched.
        let indexed = bytes.len().saturating_sub(HASH_LEN - 1).min(NONE as usize);
        let bits = indexed
            .max(1)
            .next_power_of_two()
            .trailing_zeros()
            .clamp(8, 16);
        let hash_shift = 32 - bits;
        let mut head = vec![NONE; 1 << bits];
        let mut prev = vec![NONE; indexed];
        for (pos, p) in prev.iter_mut().enumerate() {
            let h = hash4(&bytes, pos, hash_shift);
            *p = head[h];
            head[h] = pos as u32;
        }
        Template {
            id,
            bytes,
            hash_shift,
            head,
            prev,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl std::fmt::Debug for Template {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Template")
            .field("id", &self.id)
            .field("len", &self.bytes.len())
            .finish()
    }
}

#[inline]
fn hash4(b: &[u8], pos: usize, shift: u32) -> usize {
    let w = u32::from_le_bytes(b[pos..pos + HASH_LEN].try_into().unwrap());
    (w.wrapping_mul(0x9E37_79B1) >> shift) as usize
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let max = a.len().min(b.len());
    let mut i = 0;
    while i + 8 <= max {
        let x = u64::from_le_bytes(a[i..i + 8].try_into().unwrap());
        let y = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        let diff = x ^ y;
        if diff != 0 {
            return i + (diff.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }
    while i < max && a[i] == b[i] {
        i += 1;
    }
    i
}

fn varint_len(v: u64) -> usize {
    (64 - (v | 1).leading_zeros() as usize).div_ceil(7)
}

fn zigzag(d: i64) -> u64 {
    ((d << 1) ^ (d >> 63)) as u64
}

fn unzigzag(z: u64) -> i64 {
    ((z >> 1) as i64) ^ -((z & 1) as i64)
}

fn copy_head(len: usize) -> u64 {
    (((len - MIN_COPY) as u64) << 1) | 1
}

fn copy_cost(len: usize, src: usize, cursor: usize) -> usize {
    varint_len(copy_head(len)) + varint_len(zigzag(src as i64 - cursor as i64))
}

fn put_literals(body: &mut Vec<u8>, lit: &[u8]) {
    if !lit.is_empty() {
        put_varint(body, (lit.len() as u64) << 1);
        body.extend_from_slice(lit);
    }
}

/// Patch body of `data` against `template`, or None unless it is strictly
/// smaller than `data`.
pub fn encode(data: &[u8], template: &Template) -> Option<Vec<u8>> {
    let t = &template.bytes;
    let n = data.len();
    if n <= 2 || t.len() < MIN_COPY {
        return None;
    }
    let mut body = Vec::with_capacity(n / 2 + 16);
    let mut cursor = 0usize;
    let mut lit_start = 0usize;
    let mut i = 0usize;
    let mut misses = 0usize;
    while i + MIN_COPY <= n {
        // (saving, data position, template position, length)
        let mut best: Option<(usize, usize, usize, usize)> = None;
        let mut consider = |src: usize| {
            let mut len = common_prefix_len(&data[i..], &t[src..]);
            if len < MIN_COPY {
                return;
            }
            let (mut at, mut from) = (i, src);
            while at > lit_start && from > 0 && data[at - 1] == t[from - 1] {
                at -= 1;
                from -= 1;
                len += 1;
            }
            let cost = copy_cost(len, from, cursor);
            if len >= cost + 2 && best.is_none_or(|(s, ..)| len - cost > s) {
                best = Some((len - cost, at, from, len));
            }
        };
        if cursor < t.len() {
            consider(cursor);
        }
        if i + HASH_LEN <= n {
            let mut cand = template.head[hash4(data, i, template.hash_shift)];
            let mut steps = 0;
            while cand != NONE && steps < MAX_CHAIN {
                if cand as usize != cursor {
                    consider(cand as usize);
                }
                cand = template.prev[cand as usize];
                steps += 1;
            }
        }
        match best {
            Some((_, at, src, len)) => {
                put_literals(&mut body, &data[lit_start..at]);
                put_varint(&mut body, copy_head(len));
                put_varint(&mut body, zigzag(src as i64 - cursor as i64));
                cursor = src + len;
                i = at + len;
                lit_start = i;
                misses = 0;
                if body.len() >= n {
                    return None;
                }
            }
            None => {
                i += 1 + (misses >> SKIP_SHIFT);
                misses += 1;
            }
        }
    }
    put_literals(&mut body, &data[lit_start..]);
    (body.len() < n).then_some(body)
}

/// Append exactly `raw_len` bytes rebuilt from `body` and `template` to `out`.
pub fn decode(body: &[u8], raw_len: u32, template: &Template, out: &mut Vec<u8>) -> Result<()> {
    if raw_len > MAX_UNIT_LEN {
        return Err(Error::format(
            "TemplatePatchV1 raw_len exceeds MAX_UNIT_LEN",
        ));
    }
    let start = out.len();
    let res = apply(body, raw_len as usize, &template.bytes, out, start);
    if res.is_err() {
        out.truncate(start);
    }
    res
}

fn apply(body: &[u8], n: usize, t: &[u8], out: &mut Vec<u8>, start: usize) -> Result<()> {
    out.reserve(n);
    let mut pos = 0usize;
    let mut cursor = 0usize;
    while pos < body.len() {
        let head = get_varint(body, &mut pos)?;
        let remaining = (n - (out.len() - start)) as u64;
        if head & 1 == 0 {
            let len = head >> 1;
            if len == 0 {
                return Err(Error::format("TemplatePatchV1 empty literal"));
            }
            if len > (body.len() - pos) as u64 || len > remaining {
                return Err(Error::format("TemplatePatchV1 literal out of bounds"));
            }
            let len = len as usize;
            out.extend_from_slice(&body[pos..pos + len]);
            pos += len;
        } else {
            let len = (head >> 1)
                .checked_add(MIN_COPY as u64)
                .filter(|&l| l <= remaining)
                .ok_or_else(|| Error::format("TemplatePatchV1 copy longer than the output"))?;
            let src = cursor as i128 + unzigzag(get_varint(body, &mut pos)?) as i128;
            if src < 0 || src + len as i128 > t.len() as i128 {
                return Err(Error::format("TemplatePatchV1 copy outside the template"));
            }
            let (src, len) = (src as usize, len as usize);
            out.extend_from_slice(&t[src..src + len]);
            cursor = src + len;
        }
    }
    if out.len() - start != n {
        return Err(Error::format(format!(
            "TemplatePatchV1 produced {} bytes, expected {n}",
            out.len() - start
        )));
    }
    Ok(())
}

/// Choose a template from samples: among up to `TRAIN_CANDIDATES` evenly spaced
/// samples (of at most `DEFAULT_MAX_TEMPLATE_LEN` bytes), the one minimizing the
/// total patch size over up to `TRAIN_EVAL_SAMPLES` evenly spaced samples.
pub fn train<S: AsRef<[u8]>>(samples: &[S]) -> Option<Vec<u8>> {
    train_bounded(samples, DEFAULT_MAX_TEMPLATE_LEN)
}

/// `train` with candidates limited to `max_len` bytes. A sample that does not
/// patch counts with its own length. Ties keep the earlier candidate.
pub fn train_bounded<S: AsRef<[u8]>>(samples: &[S], max_len: usize) -> Option<Vec<u8>> {
    let eligible: Vec<usize> = (0..samples.len())
        .filter(|&i| (MIN_COPY..=max_len).contains(&samples[i].as_ref().len()))
        .collect();
    if eligible.is_empty() {
        return None;
    }
    let eval = evenly_spaced(samples.len(), TRAIN_EVAL_SAMPLES);
    let mut best: Option<(u64, usize)> = None;
    for c in evenly_spaced(eligible.len(), TRAIN_CANDIDATES) {
        let idx = eligible[c];
        let t = Template::new(0, samples[idx].as_ref().to_vec());
        let mut total = 0u64;
        for &e in &eval {
            let s = samples[e].as_ref();
            total += encode(s, &t).map_or(s.len(), |b| b.len()) as u64;
            if best.is_some_and(|(b, _)| total >= b) {
                break;
            }
        }
        if best.is_none_or(|(b, _)| total < b) {
            best = Some((total, idx));
        }
    }
    best.map(|(_, idx)| samples[idx].as_ref().to_vec())
}

/// Up to `k` distinct indices spread evenly over `0..n`.
fn evenly_spaced(n: usize, k: usize) -> Vec<usize> {
    let k = k.min(n);
    (0..k).map(|j| j * n / k).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(data: &[u8], t: &Template) -> Option<usize> {
        let body = encode(data, t)?;
        assert!(body.len() < data.len());
        let mut out = vec![9];
        decode(&body, data.len() as u32, t, &mut out).unwrap();
        assert_eq!(&out[1..], data);
        Some(body.len())
    }

    #[test]
    fn patches_similar_records() {
        let t = Template::new(
            1,
            br#"{"id":"1111222233334444","author":"alice","content":"hello there","pinned":false}"#
                .to_vec(),
        );
        let data = br#"{"id":"1111222233335555","author":"bob","content":"something else entirely","pinned":true}"#;
        let len = roundtrip(data, &t).unwrap();
        // 4 literals of 35 bytes in total ("5555", "bob", the new content,
        // "true}") with 1-byte heads, and 4 copies of 2 bytes each.
        assert_eq!(len, 35 + 4 + 8);
        assert_eq!(roundtrip(t.bytes(), &t), Some(3));
        assert!(encode(b"zzzzzzzzzzzzzzzzzzzzzzz", &t).is_none());
    }

    #[test]
    fn varints_and_zigzag() {
        for v in [0u64, 1, 127, 128, 16383, 16384, u64::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            assert_eq!(varint_len(v), b.len(), "{v}");
        }
        for d in [0i64, 1, -1, 63, -64, 1 << 40, i64::MIN, i64::MAX] {
            assert_eq!(unzigzag(zigzag(d)), d);
        }
    }

    #[test]
    fn rejects_bad_ops() {
        let t = Template::new(1, b"0123456789abcdef".to_vec());
        let mut out = Vec::new();
        // empty literal, literal past the body, copy past the template end,
        // copy before the template start, output too short, output too long
        for (body, raw_len) in [
            (vec![0u8], 0u32),
            (vec![4, b'a'], 2),
            (vec![1 | (13 << 1), 0], 17),
            (vec![1, 1], 4),
            (vec![1, 0], 5),
            (vec![1, 0], 3),
        ] {
            assert!(decode(&body, raw_len, &t, &mut out).is_err(), "{body:?}");
            assert!(out.is_empty());
        }
        decode(&[1, 0], 4, &t, &mut out).unwrap();
        assert_eq!(out, b"0123");
    }

    #[test]
    fn train_prefers_representative_sample() {
        let mut samples: Vec<Vec<u8>> = (0..40)
            .map(|i| {
                format!(r#"{{"id":{i},"kind":"message","body":"text number {i}","flags":[1,2,3]}}"#)
                    .into_bytes()
            })
            .collect();
        samples[0] = b"completely unrelated sample that should not be chosen".to_vec();
        let t = train(&samples).unwrap();
        assert_ne!(t, samples[0]);
    }
}
