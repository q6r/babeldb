//! `ZstdV1`: exactly one Zstandard frame per unit, optionally with an
//! identified, immutable dictionary (envelope `aux_id` = param id). The frame
//! header must declare the content size, equal to raw_len (every frame this
//! module writes does).
//!
//! Compression and decompression contexts are reused per thread (one-shot
//! calls fully reset them); a context that reported an error is dropped.

use std::cell::RefCell;
use std::fmt;
use std::thread::LocalKey;

use zstd::zstd_safe::{self, CCtx, CDict, DCtx, DDict, SafeResult};

use crate::error::{Error, Result};
use crate::format::MAX_UNIT_LEN;

/// Smallest dictionary zstd's trainer accepts.
pub const MIN_DICT_LEN: usize = 256;
/// Largest dictionary this crate trains or accepts.
pub const MAX_DICT_LEN: usize = MAX_UNIT_LEN as usize;

/// Prepared dictionary, built once per param id and shared (Send + Sync).
pub struct ZstdDict {
    pub id: u64,
    level: i32,
    bytes: Vec<u8>,
    cdict: CDict<'static>,
    ddict: DDict<'static>,
}

const _: () = {
    const fn send_sync<T: Send + Sync>() {}
    send_sync::<ZstdDict>();
};

impl ZstdDict {
    /// Digest `bytes` for compression at `level` and for decompression.
    pub fn new(id: u64, bytes: Vec<u8>, level: i32) -> Result<ZstdDict> {
        if bytes.is_empty() || bytes.len() > MAX_DICT_LEN {
            return Err(Error::format(format!(
                "zstd dictionary of {} bytes",
                bytes.len()
            )));
        }
        let cdict = CDict::try_create(&bytes, level)
            .ok_or_else(|| Error::format("zstd rejected the dictionary"))?;
        let ddict = DDict::try_create(&bytes)
            .ok_or_else(|| Error::format("zstd rejected the dictionary"))?;
        Ok(ZstdDict {
            id,
            level,
            bytes,
            cdict,
            ddict,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Compression level the dictionary was prepared for.
    pub fn level(&self) -> i32 {
        self.level
    }
}

impl fmt::Debug for ZstdDict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdDict")
            .field("id", &self.id)
            .field("level", &self.level)
            .field("len", &self.bytes.len())
            .finish()
    }
}

thread_local! {
    static CCTX: RefCell<Option<CCtx<'static>>> = const { RefCell::new(None) };
    static DCTX: RefCell<Option<DCtx<'static>>> = const { RefCell::new(None) };
}

fn zstd_error(code: usize) -> Error {
    Error::format(format!("zstd: {}", zstd_safe::get_error_name(code)))
}

/// Run `f` with this thread's cached context (or a fresh one if the cache is
/// unavailable). A context that reported an error is not kept.
fn with_ctx<C>(
    key: &'static LocalKey<RefCell<Option<C>>>,
    create: fn() -> Option<C>,
    f: impl FnOnce(&mut C) -> SafeResult,
) -> Result<usize> {
    let mut f = Some(f);
    let mut res = None;
    let _ = key.try_with(|cell| {
        let Ok(mut slot) = cell.try_borrow_mut() else {
            return;
        };
        if slot.is_none() {
            *slot = create();
        }
        if let Some(ctx) = slot.as_mut()
            && let Some(f) = f.take()
        {
            let r = f(ctx);
            if r.is_err() {
                *slot = None;
            }
            res = Some(r);
        }
    });
    let r = match (res, f) {
        (Some(r), _) => r,
        (None, Some(f)) => {
            let mut ctx =
                create().ok_or_else(|| Error::format("zstd: cannot allocate a context"))?;
            f(&mut ctx)
        }
        (None, None) => return Err(Error::format("zstd: context unavailable")),
    };
    r.map_err(zstd_error)
}

/// One frame holding `data`. With a dictionary prepared for `level` the
/// digested dictionary is reused; another level re-digests it for this call.
pub fn encode(data: &[u8], level: i32, dict: Option<&ZstdDict>) -> Result<Vec<u8>> {
    let mut body = Vec::with_capacity(zstd_safe::compress_bound(data.len()));
    match dict {
        None => with_ctx(&CCTX, CCtx::try_create, |c| {
            c.compress(&mut body, data, level)
        })?,
        Some(d) if d.level == level => with_ctx(&CCTX, CCtx::try_create, |c| {
            c.compress_using_cdict(&mut body, data, &d.cdict)
        })?,
        Some(d) => with_ctx(&CCTX, CCtx::try_create, |c| {
            c.compress_using_dict(&mut body, data, &d.bytes, level)
        })?,
    };
    Ok(body)
}

/// Append exactly `raw_len` decoded bytes to `out`, or fail. The frame must be
/// the whole body and declare `raw_len` as its content size; decompression
/// writes into a buffer of exactly raw_len bytes.
pub fn decode(body: &[u8], raw_len: u32, dict: Option<&ZstdDict>, out: &mut Vec<u8>) -> Result<()> {
    if raw_len > MAX_UNIT_LEN {
        return Err(Error::format("ZstdV1 raw_len exceeds MAX_UNIT_LEN"));
    }
    match zstd_safe::get_frame_content_size(body) {
        Ok(Some(size)) if size == raw_len as u64 => {}
        Ok(Some(size)) => {
            return Err(Error::format(format!(
                "ZstdV1 frame declares {size} bytes, expected {raw_len}"
            )));
        }
        Ok(None) => return Err(Error::format("ZstdV1 frame without content size")),
        Err(_) => return Err(Error::format("ZstdV1 body is not a zstd frame")),
    }
    match zstd_safe::find_frame_compressed_size(body) {
        Ok(len) if len == body.len() => {}
        Ok(_) => return Err(Error::format("ZstdV1 body is not exactly one frame")),
        Err(code) => return Err(zstd_error(code)),
    }
    let n = raw_len as usize;
    let start = out.len();
    out.resize(start + n, 0);
    let dst = &mut out[start..];
    let res = match dict {
        None => with_ctx(&DCTX, DCtx::try_create, |d| d.decompress(dst, body)),
        Some(dict) => with_ctx(&DCTX, DCtx::try_create, |d| {
            d.decompress_using_ddict(dst, body, &dict.ddict)
        }),
    };
    match res {
        Ok(written) if written == n => Ok(()),
        Ok(written) => {
            out.truncate(start);
            Err(Error::format(format!(
                "ZstdV1 produced {written} bytes, expected {n}"
            )))
        }
        Err(e) => {
            out.truncate(start);
            Err(e)
        }
    }
}

/// Train a dictionary of at most `max_size` bytes from samples. Trainer
/// failures (e.g. too few or too small samples) are `InvalidArgument`.
pub fn train_dictionary<S: AsRef<[u8]>>(samples: &[S], max_size: usize) -> Result<Vec<u8>> {
    if samples.is_empty() {
        return Err(Error::InvalidArgument(
            "zstd dictionary training needs samples".into(),
        ));
    }
    if !(MIN_DICT_LEN..=MAX_DICT_LEN).contains(&max_size) {
        return Err(Error::InvalidArgument(format!(
            "zstd dictionary size {max_size} outside [{MIN_DICT_LEN}, {MAX_DICT_LEN}]"
        )));
    }
    let dict = zstd::dict::from_samples(samples, max_size)
        .map_err(|e| Error::InvalidArgument(format!("zstd dictionary training failed: {e}")))?;
    if dict.is_empty() {
        return Err(Error::InvalidArgument(
            "zstd dictionary training produced nothing".into(),
        ));
    }
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_with_and_without_dict() {
        let samples: Vec<Vec<u8>> = (0..200)
            .map(|i| {
                format!(
                    "{{\"id\":{i},\"user\":\"user{}\",\"text\":\"message number {i}\"}}",
                    i % 7
                )
                .into_bytes()
            })
            .collect();
        let dict_bytes = train_dictionary(&samples, 4096).unwrap();
        let dict = ZstdDict::new(9, dict_bytes, 3).unwrap();
        let data = br#"{"id":1234,"user":"user3","text":"message number 1234"}"#;
        for d in [None, Some(&dict)] {
            let body = encode(data, 3, d).unwrap();
            let mut out = Vec::new();
            decode(&body, data.len() as u32, d, &mut out).unwrap();
            assert_eq!(&out[..], &data[..]);
            assert!(decode(&body, data.len() as u32 + 1, d, &mut Vec::new()).is_err());
        }
        let other_level = encode(data, 1, Some(&dict)).unwrap();
        let mut out = Vec::new();
        decode(&other_level, data.len() as u32, Some(&dict), &mut out).unwrap();
        assert_eq!(&out[..], &data[..]);
        let with_dict = encode(data, 3, Some(&dict)).unwrap();
        assert!(decode(&with_dict, data.len() as u32, None, &mut Vec::new()).is_err());
    }
}
