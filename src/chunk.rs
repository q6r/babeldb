//! Fixed-size blocks and range location over chunk references.

use std::ops::Range;

use crate::format::ChunkRef;

/// Block ranges of a value of `len` bytes (last block may be shorter).
pub fn split(len: usize, block_size: usize) -> impl Iterator<Item = Range<usize>> {
    assert!(block_size > 0);
    (0..len.div_ceil(block_size)).map(move |i| {
        let start = i * block_size;
        start..(start + block_size).min(len)
    })
}

/// Logical start of chunk `idx`.
pub fn chunk_start(refs: &[ChunkRef], idx: usize) -> u64 {
    if idx == 0 { 0 } else { refs[idx - 1].logical_end }
}

/// Inclusive index range `(first, last)` of the chunks intersecting
/// `[offset, offset + len)`. `None` if `len == 0` or the range is outside.
/// Binary search over the logical ends: O(log n).
pub fn locate(refs: &[ChunkRef], offset: u64, len: u64) -> Option<(usize, usize)> {
    let total = refs.last()?.logical_end;
    if len == 0 || offset >= total {
        return None;
    }
    let end = offset.saturating_add(len).min(total);
    let first = refs.partition_point(|r| r.logical_end <= offset);
    let last = refs.partition_point(|r| r.logical_end < end);
    Some((first, last.min(refs.len() - 1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs(ends: &[u64]) -> Vec<ChunkRef> {
        ends.iter().enumerate().map(|(i, &e)| ChunkRef { logical_end: e, object_id: i as u64 + 1 }).collect()
    }

    #[test]
    fn split_blocks() {
        let v: Vec<_> = split(10, 4).collect();
        assert_eq!(v, vec![0..4, 4..8, 8..10]);
        assert_eq!(split(0, 4).count(), 0);
        assert_eq!(split(8, 4).count(), 2);
    }

    #[test]
    fn locate_ranges() {
        let r = refs(&[4, 8, 10]);
        assert_eq!(locate(&r, 0, 1), Some((0, 0)));
        assert_eq!(locate(&r, 3, 2), Some((0, 1)));
        assert_eq!(locate(&r, 4, 4), Some((1, 1)));
        assert_eq!(locate(&r, 7, 100), Some((1, 2)));
        assert_eq!(locate(&r, 10, 1), None);
        assert_eq!(locate(&r, 0, 0), None);
        assert_eq!(locate(&r, 0, 10), Some((0, 2)));
    }
}
