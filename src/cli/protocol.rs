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
//! allocating, and their buffers only grow with bytes actually received.
//!
//! - request body: `op: u8 | payload`
//! - response body: `status: u8 | payload`
//!
//! Fields (integers little-endian): `bytes` = `n: u32 | n bytes`; `u32`, `u64`
//! fixed width; `bool` = `u8` 0 or 1 (any other value is malformed).
//!
//! # Operations
//!
//! | op | name        | request payload                                          | OK payload |
//! |----|-------------|----------------------------------------------------------|------------|
//! | 1  | GET         | key: bytes                                               | value: bytes |
//! | 2  | PUT         | key: bytes, value: bytes                                 | revision: u64 |
//! | 3  | DELETE      | key: bytes                                               | (empty) |
//! | 4  | RANGE       | key: bytes, offset: u64, len: u64                        | data: bytes |
//! | 5  | SCAN_PREFIX | prefix: bytes, limit: u32 (0 = none), reverse: bool, with_values: bool | count: u32, count x (key: bytes, revision: u64, logical_len: u64, has_value: bool, \[value: bytes\]) |
//! | 6  | PING        | (empty)                                                  | (empty) |
//! | 7  | PUT_BATCH   | count: u32, count x (key: bytes, value: bytes)           | count: u32, count x revision: u64 |
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
//! an ERROR.
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
use std::io::{self, Read};

use crate::engine::ScanItem;

/// Version of this protocol (documentation; not sent on the wire).
pub const PROTOCOL_VERSION: u8 = 1;
/// Maximum frame body length (op/status byte included).
pub const MAX_FRAME_LEN: u32 = 64 << 20;
/// Size of the frame length prefix.
pub const LEN_PREFIX: usize = 4;
/// Longest error message sent in an ERROR response.
pub const MAX_ERROR_MESSAGE: usize = 4096;
/// Initial reservation when reading a frame; the buffer then grows with the data.
const READ_RESERVE: usize = 1 << 20;

/// Request operation codes.
pub mod op {
    pub const GET: u8 = 1;
    pub const PUT: u8 = 2;
    pub const DELETE: u8 = 3;
    pub const RANGE: u8 = 4;
    pub const SCAN_PREFIX: u8 = 5;
    pub const PING: u8 = 6;
    pub const PUT_BATCH: u8 = 7;

    pub fn name(op: u8) -> &'static str {
        match op {
            GET => "GET",
            PUT => "PUT",
            DELETE => "DELETE",
            RANGE => "RANGE",
            SCAN_PREFIX => "SCAN_PREFIX",
            PING => "PING",
            PUT_BATCH => "PUT_BATCH",
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

/// Read one frame into `body` (cleared first; the length prefix is not kept).
/// Returns `Ok(false)` on a clean end of stream before the first byte of a
/// frame. The declared length is validated before anything is allocated.
pub fn read_frame<R: Read + ?Sized>(r: &mut R, body: &mut Vec<u8>) -> Result<bool> {
    let mut prefix = [0u8; LEN_PREFIX];
    let mut got = 0;
    while got < LEN_PREFIX {
        match r.read(&mut prefix[got..]) {
            Ok(0) if got == 0 => return Ok(false),
            Ok(0) => return Err(ProtocolError::Truncated),
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(ProtocolError::Io(e)),
        }
    }
    let len = check_len(u32::from_le_bytes(prefix))?;
    body.clear();
    body.reserve(len.min(READ_RESERVE));
    let read = Read::take(&mut *r, len as u64).read_to_end(body)?;
    if read < len {
        return Err(ProtocolError::Truncated);
    }
    Ok(true)
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
    let at = out.len();
    out.extend_from_slice(&[0u8; LEN_PREFIX]);
    let result = fill(out).and_then(|()| {
        let len = out.len() - at - LEN_PREFIX;
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
    }
    result
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) -> Result<()> {
    let n = u32::try_from(b.len())
        .ok()
        .filter(|&n| n < MAX_FRAME_LEN)
        .ok_or(ProtocolError::FrameTooLarge {
            len: b.len() as u64,
        })?;
    out.extend_from_slice(&n.to_le_bytes());
    out.extend_from_slice(b);
    Ok(())
}

fn put_count(out: &mut Vec<u8>, n: usize) -> Result<()> {
    let n = u32::try_from(n)
        .map_err(|_| ProtocolError::Malformed(format!("{n} items do not fit a u32 count")))?;
    out.extend_from_slice(&n.to_le_bytes());
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

    fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }

    fn finish(&self) -> Result<()> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(ProtocolError::Malformed(format!("{n} trailing bytes"))),
        }
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
            }
            Ok(())
        })
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
                Reply::Items(items) => {
                    put_count(out, items.len())?;
                    for item in items {
                        put_bytes(out, &item.key)?;
                        out.extend_from_slice(&item.revision.to_le_bytes());
                        out.extend_from_slice(&item.logical_len.to_le_bytes());
                        match &item.value {
                            Some(v) => {
                                out.push(1);
                                put_bytes(out, v)?;
                            }
                            None => out.push(0),
                        }
                    }
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

    /// Decode a response body (a frame without its length prefix) answering
    /// a request of kind `op`.
    pub fn decode(op: u8, body: &[u8]) -> Result<Reply> {
        let mut c = Cursor::new(body);
        let status = c.u8("status").map_err(|_| ProtocolError::EmptyFrame)?;
        let reply = match status {
            status::OK => match op {
                op::GET | op::RANGE => Reply::Value(c.bytes("value")?.to_vec()),
                op::PUT => Reply::Revision(c.u64("revision")?),
                op::DELETE | op::PING => Reply::Done,
                op::SCAN_PREFIX => {
                    // Smallest item: key length (4) + revision (8) + length (8) + flag (1).
                    let count = c.count(21)?;
                    let mut items = Vec::with_capacity(count);
                    for _ in 0..count {
                        let key = c.bytes("key")?.to_vec();
                        let revision = c.u64("revision")?;
                        let logical_len = c.u64("logical_len")?;
                        let value = if c.bool("has_value")? {
                            Some(c.bytes("value")?.to_vec())
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
                    Reply::Items(items)
                }
                op::PUT_BATCH => {
                    let count = c.count(8)?;
                    let mut revisions = Vec::with_capacity(count);
                    for _ in 0..count {
                        revisions.push(c.u64("revision")?);
                    }
                    Reply::Revisions(revisions)
                }
                other => return Err(ProtocolError::UnknownOp(other)),
            },
            status::NOT_FOUND => Reply::NotFound,
            status::ERROR => {
                return Ok(Reply::Error(String::from_utf8_lossy(c.rest()).into_owned()));
            }
            other => return Err(ProtocolError::UnknownStatus(other)),
        };
        c.finish()?;
        Ok(reply)
    }
}
