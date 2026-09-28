//! Binary wire protocol of `babeldb serve` (stage 10).
//!
//! The server exists to measure the cost of a remote interface separately
//! from the engine: a benchmark times the same operation through
//! [`crate::cli::server::Client`] and through direct `Db` calls (and
//! [`crate::cli::server::handle_request`] isolates decode + dispatch from TCP).
//!
//! # Framing
//!
//! Every message is one frame: `len: u32 LE | body[len]` with
//! `1 <= len <= MAX_FRAME_LEN` (64 MiB). Receivers validate `len` before
//! allocating, and their buffers only grow with bytes actually received (at
//! most `max(1 MiB, 2 x received)` is allocated ahead of the data).
//!
//! - request body: `op: u8 | payload`
//! - response body: `status: u8 | payload`
//!
//! Fields (integers little-endian): `bytes` = `n: u32 | n bytes`; `u32`, `u64`
//! fixed width; `bool` = `u8` 0 or 1 (any other value is malformed); `bound` =
//! `u8` 0 (unbounded), or 1 (included) / 2 (excluded) followed by `key: bytes`
//! (any other tag is malformed).
//!
//! # Operations
//!
//! | op | name        | request payload                                          | OK payload |
//! |----|-------------|----------------------------------------------------------|------------|
//! | 1  | GET         | key: bytes                                               | value: bytes |
//! | 2  | PUT         | key: bytes, value: bytes                                 | revision: u64 |
//! | 3  | DELETE      | key: bytes                                               | (empty) |
//! | 4  | RANGE       | key: bytes, offset: u64, len: u64                        | data: bytes |
//! | 5  | SCAN_PREFIX | prefix: bytes, limit: u32 (0 = none), reverse: bool, with_values: bool | count: u32, count x item |
//! | 6  | PING        | (empty)                                                  | (empty) |
//! | 7  | PUT_BATCH   | count: u32, count x (key: bytes, value: bytes)           | count: u32, count x revision: u64 |
//! | 8  | SCAN_RANGE  | start: bound, end: bound, limit: u32 (0 = none), reverse: bool, with_values: bool | more: bool, count: u32, count x item |
//!
//! A scan `item` is `key: bytes, revision: u64, logical_len: u64, has_value:
//! bool, \[value: bytes\]`.
//!
//! Status: 0 OK; 1 NOT_FOUND (empty payload; GET, RANGE and DELETE of a
//! missing key); 2 ERROR (payload: UTF-8 message, not length-prefixed).
//!
//! Semantics: PUT and PUT_BATCH items are unconditional (last writer wins).
//! A PUT_BATCH is validated as a whole (one invalid item rejects it and
//! nothing is applied) and applied atomically in one commit. The server
//! group-commits: writes of every connection may share a commit, and a write
//! is acknowledged only after the commit carrying it; every acknowledged
//! write is durable, unless the server was started to acknowledge buffered
//! writes (`cli::server::ServerConfig::commit`, not the `serve` command's
//! default). RANGE follows `Db::get_range` (clamped; an offset past the end
//! is an ERROR). A response that would exceed `MAX_FRAME_LEN` is replaced by
//! an ERROR, except for SCAN_RANGE, which splits its result instead.
//!
//! SCAN_RANGE (version 2) is `Db::scan` over a key range: the live records
//! between `start` and `end`, in key order (descending with `reverse`), at
//! most `limit`. Its result comes in pages, so that no result is refused for
//! its size. A page is the next items of the range, as many as fit the
//! server's page size (`cli::server::ServerConfig::scan_page_bytes`, 16 MiB
//! by default; the first item of a page may exceed it up to `MAX_FRAME_LEN`)
//! and at most `cli::server::SCAN_PAGE_ITEMS`. `more` is 1 when the page
//! stopped before the end of the range and of the limit: the client then asks
//! again from just after the last key it received (`start` = that key,
//! excluded; `end` instead when reversed) with the rest of its limit, until a
//! page comes with `more` = 0 (an empty page always does;
//! [`crate::cli::server::Client::scan`] does all this). Each page is read in
//! one snapshot; a result of several pages is not. An item that alone cannot
//! fit in a frame gets an ERROR (read that value with RANGE). SCAN_PREFIX and
//! the other version-1 messages are unchanged.
//!
//! Transfer: values of at least [`SPLICE_MIN`] bytes are written from their
//! own buffers in one vectored write with the rest of the frame
//! ([`Request::encode_spliced`], [`Reply::encode_spliced`], [`write_spliced`])
//! and read straight into their final buffers ([`read_reply`]); the bytes on
//! the wire are the same.
//!
//! # Connections
//!
//! A connection carries any number of requests, executed and answered in
//! order (clients may pipeline: a request sees the effects of every earlier
//! request of its connection, and pipelined writes may share a commit). A
//! malformed request inside a well-delimited frame gets an ERROR and the
//! connection stays usable; a frame length of 0 or above `MAX_FRAME_LEN`
//! gets an ERROR and the connection is closed (the stream can no longer be
//! delimited); a truncated frame closes the connection.

use std::fmt;
use std::io::{self, IoSlice, Read, Write};
use std::ops::Bound;

use crate::engine::ScanItem;

/// Version of this protocol (documentation; not sent on the wire). Version 2
/// added SCAN_RANGE; every version-1 message is unchanged.
pub const PROTOCOL_VERSION: u8 = 2;
/// Maximum frame body length (op/status byte included).
pub const MAX_FRAME_LEN: u32 = 64 << 20;
/// Size of the frame length prefix.
pub const LEN_PREFIX: usize = 4;
/// Longest error message sent in an ERROR response.
pub const MAX_ERROR_MESSAGE: usize = 4096;
/// Initial allocation when reading a frame or a value; the buffer then grows
/// with the data (at most doubling what already arrived).
const READ_RESERVE: usize = 1 << 20;
/// Values of at least this many bytes are not copied into frame buffers by
/// [`Request::encode_spliced`] and [`Reply::encode_spliced`]: they are
/// written from their own buffers.
pub const SPLICE_MIN: usize = 4 << 10;
/// Most slices handed to one vectored write.
const MAX_WRITE_SLICES: usize = 1024;
/// Bytes of a SCAN_RANGE OK reply before its items: status, `more`, count.
pub const PAGE_HEADER_LEN: u64 = 1 + 1 + 4;

/// Request operation codes.
pub mod op {
    pub const GET: u8 = 1;
    pub const PUT: u8 = 2;
    pub const DELETE: u8 = 3;
    pub const RANGE: u8 = 4;
    pub const SCAN_PREFIX: u8 = 5;
    pub const PING: u8 = 6;
    pub const PUT_BATCH: u8 = 7;
    pub const SCAN_RANGE: u8 = 8;

    pub fn name(op: u8) -> &'static str {
        match op {
            GET => "GET",
            PUT => "PUT",
            DELETE => "DELETE",
            RANGE => "RANGE",
            SCAN_PREFIX => "SCAN_PREFIX",
            PING => "PING",
            PUT_BATCH => "PUT_BATCH",
            SCAN_RANGE => "SCAN_RANGE",
            _ => "UNKNOWN",
        }
    }
}

/// Response status codes.
pub mod status {
    pub const OK: u8 = 0;
    pub const NOT_FOUND: u8 = 1;
    pub const ERROR: u8 = 2;
}

/// Tags of a `bound` field.
mod bound_tag {
    pub const UNBOUNDED: u8 = 0;
    pub const INCLUDED: u8 = 1;
    pub const EXCLUDED: u8 = 2;
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ProtocolError {
    Io(io::Error),
    /// Declared (or produced) frame body longer than [`MAX_FRAME_LEN`].
    FrameTooLarge {
        len: u64,
    },
    /// Frame body of length 0 (no op/status byte).
    EmptyFrame,
    /// The input ended inside a frame.
    Truncated,
    UnknownOp(u8),
    UnknownStatus(u8),
    /// Bad field lengths, flags or trailing bytes inside a delimited frame.
    Malformed(String),
    /// ERROR response of the server (client side).
    Remote(String),
    /// The peer closed the connection before answering.
    Closed,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Io(e) => write!(f, "I/O error: {e}"),
            ProtocolError::FrameTooLarge { len } => {
                write!(
                    f,
                    "frame of {len} bytes exceeds the maximum of {MAX_FRAME_LEN} bytes"
                )
            }
            ProtocolError::EmptyFrame => write!(f, "empty frame (length 0)"),
            ProtocolError::Truncated => write!(f, "truncated frame"),
            ProtocolError::UnknownOp(op) => write!(f, "unknown op {op}"),
            ProtocolError::UnknownStatus(s) => write!(f, "unknown response status {s}"),
            ProtocolError::Malformed(m) => write!(f, "malformed message: {m}"),
            ProtocolError::Remote(m) => write!(f, "server error: {m}"),
            ProtocolError::Closed => write!(f, "connection closed by the peer"),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProtocolError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ProtocolError {
    fn from(e: io::Error) -> Self {
        ProtocolError::Io(e)
    }
}

type Result<T> = std::result::Result<T, ProtocolError>;

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

fn check_len(len: u32) -> Result<usize> {
    if len == 0 {
        return Err(ProtocolError::EmptyFrame);
    }
    if len > MAX_FRAME_LEN {
        return Err(ProtocolError::FrameTooLarge {
            len: u64::from(len),
        });
    }
    Ok(len as usize)
}

/// The validated length prefix of the next frame; `Ok(None)` on a clean end
/// of stream before its first byte.
fn read_len<R: Read + ?Sized>(r: &mut R) -> Result<Option<usize>> {
    let mut prefix = [0u8; LEN_PREFIX];
    let mut got = 0;
    while got < LEN_PREFIX {
        match r.read(&mut prefix[got..]) {
            Ok(0) if got == 0 => return Ok(None),
            Ok(0) => return Err(ProtocolError::Truncated),
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(ProtocolError::Io(e)),
        }
    }
    check_len(u32::from_le_bytes(prefix)).map(Some)
}

/// `read_exact`, an early end of stream being [`ProtocolError::Truncated`].
fn read_exact<R: Read + ?Sized>(r: &mut R, buf: &mut [u8]) -> Result<()> {
    r.read_exact(buf).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            ProtocolError::Truncated
        } else {
            ProtocolError::Io(e)
        }
    })
}

/// Read `len` bytes into `buf[..len]`, growing `buf` (never shrinking it)
/// with the data: at most `max(READ_RESERVE, 2 x received)` bytes ahead of
/// what arrived.
fn read_growing<R: Read + ?Sized>(r: &mut R, buf: &mut Vec<u8>, len: usize) -> Result<()> {
    let mut filled = 0;
    while filled < len {
        let upto = len.min(filled.saturating_mul(2).max(READ_RESERVE));
        if buf.len() < upto {
            // Small buffers grow amortized; large ones to the size needed
            // (a reused buffer then holds no more than its largest frame).
            if upto <= READ_RESERVE {
                buf.reserve(upto - buf.len());
            } else {
                buf.reserve_exact(upto - buf.len());
            }
            buf.resize(upto, 0);
        }
        read_exact(r, &mut buf[filled..upto])?;
        filled = upto;
    }
    Ok(())
}

/// Exactly `n` bytes in a new vector, allocated as [`read_growing`] does.
fn read_vec<R: Read + ?Sized>(r: &mut R, n: usize) -> Result<Vec<u8>> {
    // A zeroed allocation: fresh pages are not written twice.
    let mut v = vec![0u8; n.min(READ_RESERVE)];
    read_exact(r, &mut v)?;
    while v.len() < n {
        let filled = v.len();
        let upto = n.min(filled.saturating_mul(2));
        v.reserve_exact(upto - filled);
        v.resize(upto, 0);
        read_exact(r, &mut v[filled..])?;
    }
    Ok(v)
}

/// Read one frame into `body` (cleared first; the length prefix is not kept).
/// Returns `Ok(false)` on a clean end of stream before the first byte of a
/// frame. The declared length is validated before anything is allocated.
pub fn read_frame<R: Read + ?Sized>(r: &mut R, body: &mut Vec<u8>) -> Result<bool> {
    body.clear();
    match read_frame_reuse(r, body)? {
        Some(len) => {
            body.truncate(len);
            Ok(true)
        }
        None => Ok(false),
    }
}

/// [`read_frame`] into a reused buffer: the body is `buf[..len]` for the
/// returned `len`. `buf` keeps the length of the longest frame read into it,
/// so that the next frames neither allocate nor clear anything (bytes past
/// `len` are stale). `Ok(None)` on a clean end of stream.
pub fn read_frame_reuse<R: Read + ?Sized>(r: &mut R, buf: &mut Vec<u8>) -> Result<Option<usize>> {
    let Some(len) = read_len(r)? else {
        return Ok(None);
    };
    read_growing(r, buf, len)?;
    Ok(Some(len))
}

/// Split the first complete frame off `bytes`: `(body, bytes consumed)`.
pub fn decode_frame(bytes: &[u8]) -> Result<(&[u8], usize)> {
    let Some(prefix) = bytes.get(..LEN_PREFIX) else {
        return Err(ProtocolError::Truncated);
    };
    let len = check_len(u32::from_le_bytes([
        prefix[0], prefix[1], prefix[2], prefix[3],
    ]))?;
    let end = LEN_PREFIX + len;
    let body = bytes.get(LEN_PREFIX..end).ok_or(ProtocolError::Truncated)?;
    Ok((body, end))
}

/// Append a frame carrying `body` to `out`.
pub fn write_frame(out: &mut Vec<u8>, body: &[u8]) -> Result<()> {
    let len = check_len(
        u32::try_from(body.len()).map_err(|_| ProtocolError::FrameTooLarge {
            len: body.len() as u64,
        })?,
    )?;
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.extend_from_slice(body);
    Ok(())
}

/// Append a frame whose body is produced by `fill`; on error `out` is left
/// as it was.
fn framed(out: &mut Vec<u8>, fill: impl FnOnce(&mut Vec<u8>) -> Result<()>) -> Result<()> {
    framed_spliced::<&[u8]>(out, &mut Vec::new(), |out, _| fill(out))
}

/// Append a frame whose body is produced by `fill`, which may keep values
/// aside in `spliced` (see [`put_spliced`]): the frame length counts them. On
/// error `out` and `spliced` are left as they were.
fn framed_spliced<S: AsRef<[u8]>>(
    out: &mut Vec<u8>,
    spliced: &mut Vec<(usize, S)>,
    fill: impl FnOnce(&mut Vec<u8>, &mut Vec<(usize, S)>) -> Result<()>,
) -> Result<()> {
    let (at, first) = (out.len(), spliced.len());
    out.extend_from_slice(&[0u8; LEN_PREFIX]);
    let result = fill(out, spliced).and_then(|()| {
        let aside: usize = spliced[first..].iter().map(|(_, v)| v.as_ref().len()).sum();
        let len = out.len() - at - LEN_PREFIX + aside;
        match u32::try_from(len) {
            Ok(n) if n <= MAX_FRAME_LEN => {
                out[at..at + LEN_PREFIX].copy_from_slice(&n.to_le_bytes());
                Ok(())
            }
            _ => Err(ProtocolError::FrameTooLarge { len: len as u64 }),
        }
    });
    if result.is_err() {
        out.truncate(at);
        spliced.truncate(first);
    }
    result
}

/// The length field of a `bytes` field.
fn put_len(out: &mut Vec<u8>, len: usize) -> Result<()> {
    let n = u32::try_from(len)
        .ok()
        .filter(|&n| n < MAX_FRAME_LEN)
        .ok_or(ProtocolError::FrameTooLarge { len: len as u64 })?;
    out.extend_from_slice(&n.to_le_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) -> Result<()> {
    put_len(out, b.len())?;
    out.extend_from_slice(b);
    Ok(())
}

/// A `bytes` field whose content, from [`SPLICE_MIN`] bytes on, is not
/// copied: it is kept in `spliced` with the offset of `out` it belongs at.
fn put_spliced<S: AsRef<[u8]>>(
    out: &mut Vec<u8>,
    spliced: &mut Vec<(usize, S)>,
    value: S,
) -> Result<()> {
    let len = value.as_ref().len();
    if len < SPLICE_MIN {
        return put_bytes(out, value.as_ref());
    }
    put_len(out, len)?;
    spliced.push((out.len(), value));
    Ok(())
}

fn put_count(out: &mut Vec<u8>, n: usize) -> Result<()> {
    let n = u32::try_from(n)
        .map_err(|_| ProtocolError::Malformed(format!("{n} items do not fit a u32 count")))?;
    out.extend_from_slice(&n.to_le_bytes());
    Ok(())
}

fn put_bound(out: &mut Vec<u8>, bound: &Bound<&[u8]>) -> Result<()> {
    match bound {
        Bound::Unbounded => out.push(bound_tag::UNBOUNDED),
        Bound::Included(key) => {
            out.push(bound_tag::INCLUDED);
            put_bytes(out, key)?;
        }
        Bound::Excluded(key) => {
            out.push(bound_tag::EXCLUDED);
            put_bytes(out, key)?;
        }
    }
    Ok(())
}

/// A scan item up to its value: key, revision, logical length, value flag.
fn put_item_head(out: &mut Vec<u8>, item: &ScanItem, has_value: bool) -> Result<()> {
    put_bytes(out, &item.key)?;
    out.extend_from_slice(&item.revision.to_le_bytes());
    out.extend_from_slice(&item.logical_len.to_le_bytes());
    out.push(u8::from(has_value));
    Ok(())
}

fn put_items(out: &mut Vec<u8>, items: &[ScanItem]) -> Result<()> {
    put_count(out, items.len())?;
    for item in items {
        put_item_head(out, item, item.value.is_some())?;
        if let Some(v) = &item.value {
            put_bytes(out, v)?;
        }
    }
    Ok(())
}

fn put_items_spliced(
    out: &mut Vec<u8>,
    spliced: &mut Vec<(usize, Vec<u8>)>,
    items: Vec<ScanItem>,
) -> Result<()> {
    put_count(out, items.len())?;
    for mut item in items {
        let value = item.value.take();
        put_item_head(out, &item, value.is_some())?;
        if let Some(v) = value {
            put_spliced(out, spliced, v)?;
        }
    }
    Ok(())
}

/// Bytes of one scan item in a reply (`value_len`: the length of its value,
/// when the value is sent).
pub fn scan_item_wire_len(key_len: usize, value_len: Option<u64>) -> u64 {
    (4 + key_len as u64 + 8 + 8 + 1).saturating_add(value_len.map_or(0, |n| n.saturating_add(4)))
}

/// Write `out` with each `spliced` value inserted at its offset (what the
/// `encode_spliced` methods produce), in vectored writes.
pub fn write_spliced<W: Write + ?Sized, S: AsRef<[u8]>>(
    w: &mut W,
    out: &[u8],
    spliced: &[(usize, S)],
) -> io::Result<()> {
    if spliced.is_empty() {
        return w.write_all(out);
    }
    let mut slices = Vec::with_capacity(2 * spliced.len() + 1);
    let mut at = 0;
    for (pos, value) in spliced {
        if *pos > at {
            slices.push(IoSlice::new(&out[at..*pos]));
        }
        slices.push(IoSlice::new(value.as_ref()));
        at = *pos;
    }
    if at < out.len() {
        slices.push(IoSlice::new(&out[at..]));
    }
    let mut bufs = &mut slices[..];
    while !bufs.is_empty() {
        let n = bufs.len().min(MAX_WRITE_SLICES);
        match w.write_vectored(&bufs[..n]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write the whole frame",
                ));
            }
            Ok(written) => IoSlice::advance_slices(&mut bufs, written),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Bounds-checked reader over one frame body.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(ProtocolError::Malformed(format!(
                "{what} needs {n} bytes, {} left",
                self.remaining()
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self, what: &str) -> Result<u8> {
        Ok(self.take(1, what)?[0])
    }

    fn u32(&mut self, what: &str) -> Result<u32> {
        let b = self.take(4, what)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self, what: &str) -> Result<u64> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8, what)?);
        Ok(u64::from_le_bytes(a))
    }

    fn bool(&mut self, what: &str) -> Result<bool> {
        match self.u8(what)? {
            0 => Ok(false),
            1 => Ok(true),
            v => Err(ProtocolError::Malformed(format!(
                "{what} must be 0 or 1, got {v}"
            ))),
        }
    }

    fn bytes(&mut self, what: &str) -> Result<&'a [u8]> {
        let n = self.u32(what)? as usize;
        self.take(n, what)
    }

    fn bound(&mut self, what: &str) -> Result<Bound<&'a [u8]>> {
        match self.u8(what)? {
            bound_tag::UNBOUNDED => Ok(Bound::Unbounded),
            bound_tag::INCLUDED => Ok(Bound::Included(self.bytes(what)?)),
            bound_tag::EXCLUDED => Ok(Bound::Excluded(self.bytes(what)?)),
            v => Err(ProtocolError::Malformed(format!(
                "{what} must be 0 (unbounded), 1 (included) or 2 (excluded), got {v}"
            ))),
        }
    }

    /// A count of items needing at least `min_item` bytes each: validated
    /// against the bytes present before anything is allocated.
    fn count(&mut self, min_item: usize) -> Result<usize> {
        let n = self.u32("count")? as usize;
        if n > self.remaining() / min_item {
            return Err(ProtocolError::Malformed(format!(
                "count {n} cannot fit in the {} remaining bytes",
                self.remaining()
            )));
        }
        Ok(n)
    }

    fn finish(&self) -> Result<()> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(ProtocolError::Malformed(format!("{n} trailing bytes"))),
        }
    }
}

/// Bounds-checked reader of the `left` bytes of one frame body, from any
/// source (a decoded slice, or the stream itself): values are read straight
/// into their own buffers. Same checks and errors as [`Cursor`]; a source that
/// ends early is [`ProtocolError::Truncated`].
struct Fields<'r, R: Read + ?Sized> {
    r: &'r mut R,
    left: usize,
}

impl<R: Read + ?Sized> Fields<'_, R> {
    fn need(&self, n: usize, what: &str) -> Result<()> {
        if n > self.left {
            return Err(ProtocolError::Malformed(format!(
                "{what} needs {n} bytes, {} left",
                self.left
            )));
        }
        Ok(())
    }

    fn array<const N: usize>(&mut self, what: &str) -> Result<[u8; N]> {
        self.need(N, what)?;
        let mut a = [0u8; N];
        read_exact(self.r, &mut a)?;
        self.left -= N;
        Ok(a)
    }

    fn u8(&mut self, what: &str) -> Result<u8> {
        Ok(self.array::<1>(what)?[0])
    }

    fn u32(&mut self, what: &str) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array(what)?))
    }

    fn u64(&mut self, what: &str) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array(what)?))
    }

    fn bool(&mut self, what: &str) -> Result<bool> {
        match self.u8(what)? {
            0 => Ok(false),
            1 => Ok(true),
            v => Err(ProtocolError::Malformed(format!(
                "{what} must be 0 or 1, got {v}"
            ))),
        }
    }

    fn bytes(&mut self, what: &str) -> Result<Vec<u8>> {
        let n = self.u32(what)? as usize;
        self.need(n, what)?;
        let v = read_vec(self.r, n)?;
        self.left -= n;
        Ok(v)
    }

    /// See [`Cursor::count`].
    fn count(&mut self, min_item: usize) -> Result<usize> {
        let n = self.u32("count")? as usize;
        if n > self.left / min_item {
            return Err(ProtocolError::Malformed(format!(
                "count {n} cannot fit in the {} remaining bytes",
                self.left
            )));
        }
        Ok(n)
    }

    fn rest(&mut self) -> Result<Vec<u8>> {
        let v = read_vec(self.r, self.left)?;
        self.left = 0;
        Ok(v)
    }

    fn finish(&self) -> Result<()> {
        match self.left {
            0 => Ok(()),
            n => Err(ProtocolError::Malformed(format!("{n} trailing bytes"))),
        }
    }

    fn items(&mut self) -> Result<Vec<ScanItem>> {
        // Smallest item: key length (4) + revision (8) + length (8) + flag (1).
        let count = self.count(21)?;
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            let key = self.bytes("key")?;
            let revision = self.u64("revision")?;
            let logical_len = self.u64("logical_len")?;
            let value = if self.bool("has_value")? {
                Some(self.bytes("value")?)
            } else {
                None
            };
            items.push(ScanItem {
                key,
                revision,
                logical_len,
                value,
            });
        }
        Ok(items)
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// A request; byte fields borrow from the frame they were decoded from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request<'a> {
    Get {
        key: &'a [u8],
    },
    Put {
        key: &'a [u8],
        value: &'a [u8],
    },
    Delete {
        key: &'a [u8],
    },
    Range {
        key: &'a [u8],
        offset: u64,
        len: u64,
    },
    ScanPrefix {
        prefix: &'a [u8],
        limit: u32,
        reverse: bool,
        with_values: bool,
    },
    Ping,
    PutBatch {
        items: Vec<(&'a [u8], &'a [u8])>,
    },
    /// One page of `Db::scan` between two bounds (see the module docs).
    ScanRange {
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        limit: u32,
        reverse: bool,
        with_values: bool,
    },
}

impl<'a> Request<'a> {
    pub fn op(&self) -> u8 {
        match self {
            Request::Get { .. } => op::GET,
            Request::Put { .. } => op::PUT,
            Request::Delete { .. } => op::DELETE,
            Request::Range { .. } => op::RANGE,
            Request::ScanPrefix { .. } => op::SCAN_PREFIX,
            Request::Ping => op::PING,
            Request::PutBatch { .. } => op::PUT_BATCH,
            Request::ScanRange { .. } => op::SCAN_RANGE,
        }
    }

    /// Append the complete frame (length prefix included) to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        framed(out, |out| {
            out.push(self.op());
            match self {
                Request::Get { key } | Request::Delete { key } => put_bytes(out, key)?,
                Request::Put { key, value } => {
                    put_bytes(out, key)?;
                    put_bytes(out, value)?;
                }
                Request::Range { key, offset, len } => {
                    put_bytes(out, key)?;
                    out.extend_from_slice(&offset.to_le_bytes());
                    out.extend_from_slice(&len.to_le_bytes());
                }
                Request::ScanPrefix {
                    prefix,
                    limit,
                    reverse,
                    with_values,
                } => {
                    put_bytes(out, prefix)?;
                    out.extend_from_slice(&limit.to_le_bytes());
                    out.push(u8::from(*reverse));
                    out.push(u8::from(*with_values));
                }
                Request::Ping => {}
                Request::PutBatch { items } => {
                    put_count(out, items.len())?;
                    for (key, value) in items {
                        put_bytes(out, key)?;
                        put_bytes(out, value)?;
                    }
                }
                Request::ScanRange {
                    start,
                    end,
                    limit,
                    reverse,
                    with_values,
                } => {
                    put_bound(out, start)?;
                    put_bound(out, end)?;
                    out.extend_from_slice(&limit.to_le_bytes());
                    out.push(u8::from(*reverse));
                    out.push(u8::from(*with_values));
                }
            }
            Ok(())
        })
    }

    /// [`Request::encode`], except that values of at least [`SPLICE_MIN`]
    /// bytes are not copied: `spliced` gets them with their offset in `out`,
    /// and [`write_spliced`] sends the frame. On error `out` and `spliced` are
    /// left as they were.
    pub fn encode_spliced(
        &self,
        out: &mut Vec<u8>,
        spliced: &mut Vec<(usize, &'a [u8])>,
    ) -> Result<()> {
        match self {
            Request::Put { key, value } => framed_spliced(out, spliced, |out, spliced| {
                out.push(op::PUT);
                put_bytes(out, key)?;
                put_spliced(out, spliced, *value)
            }),
            Request::PutBatch { items } => framed_spliced(out, spliced, |out, spliced| {
                out.push(op::PUT_BATCH);
                put_count(out, items.len())?;
                for &(key, value) in items {
                    put_bytes(out, key)?;
                    put_spliced(out, spliced, value)?;
                }
                Ok(())
            }),
            _ => self.encode(out),
        }
    }

    /// Decode a request body (a frame without its length prefix).
    pub fn decode(body: &'a [u8]) -> Result<Request<'a>> {
        let mut c = Cursor::new(body);
        let op = c.u8("op").map_err(|_| ProtocolError::EmptyFrame)?;
        let request = match op {
            op::GET => Request::Get {
                key: c.bytes("key")?,
            },
            op::PUT => Request::Put {
                key: c.bytes("key")?,
                value: c.bytes("value")?,
            },
            op::DELETE => Request::Delete {
                key: c.bytes("key")?,
            },
            op::RANGE => Request::Range {
                key: c.bytes("key")?,
                offset: c.u64("offset")?,
                len: c.u64("len")?,
            },
            op::SCAN_PREFIX => Request::ScanPrefix {
                prefix: c.bytes("prefix")?,
                limit: c.u32("limit")?,
                reverse: c.bool("reverse")?,
                with_values: c.bool("with_values")?,
            },
            op::PING => Request::Ping,
            op::PUT_BATCH => {
                // Every item holds at least two 4-byte length fields.
                let count = c.count(8)?;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    items.push((c.bytes("key")?, c.bytes("value")?));
                }
                Request::PutBatch { items }
            }
            op::SCAN_RANGE => Request::ScanRange {
                start: c.bound("start")?,
                end: c.bound("end")?,
                limit: c.u32("limit")?,
                reverse: c.bool("reverse")?,
                with_values: c.bool("with_values")?,
            },
            other => return Err(ProtocolError::UnknownOp(other)),
        };
        c.finish()?;
        Ok(request)
    }
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// A response. Which OK variant is valid depends on the request's op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// OK, empty payload (PING, DELETE).
    Done,
    /// OK with bytes (GET, RANGE).
    Value(Vec<u8>),
    /// OK with the new revision (PUT).
    Revision(u64),
    /// OK with one revision per item (PUT_BATCH).
    Revisions(Vec<u64>),
    /// OK with scan items (SCAN_PREFIX).
    Items(Vec<ScanItem>),
    /// OK with one page of scan items (SCAN_RANGE); `more`: ask for the next
    /// page (see the module docs).
    Page {
        items: Vec<ScanItem>,
        more: bool,
    },
    NotFound,
    Error(String),
}

impl Reply {
    pub fn status(&self) -> u8 {
        match self {
            Reply::NotFound => status::NOT_FOUND,
            Reply::Error(_) => status::ERROR,
            _ => status::OK,
        }
    }

    /// Short name of the variant (diagnostics without dumping payloads).
    pub fn kind(&self) -> &'static str {
        match self {
            Reply::Done => "Done",
            Reply::Value(_) => "Value",
            Reply::Revision(_) => "Revision",
            Reply::Revisions(_) => "Revisions",
            Reply::Items(_) => "Items",
            Reply::Page { .. } => "Page",
            Reply::NotFound => "NotFound",
            Reply::Error(_) => "Error",
        }
    }

    /// Append the complete frame (length prefix included) to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        framed(out, |out| {
            out.push(self.status());
            match self {
                Reply::Done | Reply::NotFound => {}
                Reply::Value(v) => put_bytes(out, v)?,
                Reply::Revision(r) => out.extend_from_slice(&r.to_le_bytes()),
                Reply::Revisions(revs) => {
                    put_count(out, revs.len())?;
                    for r in revs {
                        out.extend_from_slice(&r.to_le_bytes());
                    }
                }
                Reply::Items(items) => put_items(out, items)?,
                Reply::Page { items, more } => {
                    out.push(u8::from(*more));
                    put_items(out, items)?;
                }
                Reply::Error(msg) => {
                    let mut end = msg.len().min(MAX_ERROR_MESSAGE);
                    while !msg.is_char_boundary(end) {
                        end -= 1;
                    }
                    out.extend_from_slice(&msg.as_bytes()[..end]);
                }
            }
            Ok(())
        })
    }

    /// [`Reply::encode`] without copying values of at least [`SPLICE_MIN`]
    /// bytes: they move to `spliced` with their offset in `out`, and
    /// [`write_spliced`] sends the frame. On error (the reply is then
    /// dropped) `out` and `spliced` are left as they were.
    pub fn encode_spliced(
        self,
        out: &mut Vec<u8>,
        spliced: &mut Vec<(usize, Vec<u8>)>,
    ) -> Result<()> {
        match self {
            Reply::Value(v) => framed_spliced(out, spliced, |out, spliced| {
                out.push(status::OK);
                put_spliced(out, spliced, v)
            }),
            Reply::Items(items) => framed_spliced(out, spliced, |out, spliced| {
                out.push(status::OK);
                put_items_spliced(out, spliced, items)
            }),
            Reply::Page { items, more } => framed_spliced(out, spliced, |out, spliced| {
                out.push(status::OK);
                out.push(u8::from(more));
                put_items_spliced(out, spliced, items)
            }),
            other => other.encode(out),
        }
    }

    /// Decode a response body (a frame without its length prefix) answering
    /// a request of kind `op`.
    pub fn decode(op: u8, body: &[u8]) -> Result<Reply> {
        let mut source = body;
        let mut f = Fields {
            r: &mut source,
            left: body.len(),
        };
        let reply = Reply::decode_fields(op, &mut f)?;
        f.finish()?;
        Ok(reply)
    }

    fn decode_fields<R: Read + ?Sized>(op: u8, f: &mut Fields<'_, R>) -> Result<Reply> {
        let status = f.u8("status").map_err(|e| match e {
            ProtocolError::Malformed(_) => ProtocolError::EmptyFrame,
            other => other,
        })?;
        Ok(match status {
            status::OK => match op {
                op::GET | op::RANGE => Reply::Value(f.bytes("value")?),
                op::PUT => Reply::Revision(f.u64("revision")?),
                op::DELETE | op::PING => Reply::Done,
                op::SCAN_PREFIX => Reply::Items(f.items()?),
                op::SCAN_RANGE => {
                    let more = f.bool("more")?;
                    Reply::Page {
                        items: f.items()?,
                        more,
                    }
                }
                op::PUT_BATCH => {
                    let count = f.count(8)?;
                    let mut revisions = Vec::with_capacity(count);
                    for _ in 0..count {
                        revisions.push(f.u64("revision")?);
                    }
                    Reply::Revisions(revisions)
                }
                other => return Err(ProtocolError::UnknownOp(other)),
            },
            status::NOT_FOUND => Reply::NotFound,
            status::ERROR => Reply::Error(match String::from_utf8(f.rest()?) {
                Ok(message) => message,
                Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
            }),
            other => return Err(ProtocolError::UnknownStatus(other)),
        })
    }
}

/// Read the next response frame from `r` and decode it as the reply to a
/// request of kind `op` (as [`Reply::decode`]), reading each value straight
/// into its own buffer: nothing is copied after the read. `Ok(None)` on a
/// clean end of stream before the frame. After an error the stream is out of
/// step.
pub fn read_reply<R: Read + ?Sized>(r: &mut R, op: u8) -> Result<Option<Reply>> {
    let Some(len) = read_len(r)? else {
        return Ok(None);
    };
    let mut f = Fields { r, left: len };
    let reply = Reply::decode_fields(op, &mut f)?;
    f.finish()?;
    Ok(Some(reply))
}
