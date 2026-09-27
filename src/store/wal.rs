//! Write-ahead log in front of any [`Store`]: [`WalStore`].
//!
//! # Why
//!
//! redb makes a commit durable with one `FlushFileBuffers` (fsync), which on
//! Windows also flushes the whole volatile cache of the drive: ~1.8 ms per
//! commit on the reference NVMe. PostgreSQL's default on Windows
//! (`wal_sync_method = open_datasync`) instead writes its WAL through a handle
//! opened with `FILE_FLAG_WRITE_THROUGH`: ~0.1-0.25 ms on the same drive.
//! `WalStore` logs each durable commit with one write, and
//! [`WalSync`] chooses how that write is made durable:
//!
//! - `WriteThrough` (default): `FILE_FLAG_WRITE_THROUGH` on a cached handle,
//!   PostgreSQL's default mechanism on Windows and so its guarantee, which is
//!   weaker than redb's: Microsoft's `CreateFile` documentation promises that
//!   the OS asks the drive to write through its volatile cache only when
//!   `FILE_FLAG_NO_BUFFERING` is also set, and PostgreSQL's documentation says
//!   that `open_datasync` does not prevent drive write caching on Windows. On
//!   Linux it is `O_DSYNC`, which does flush the drive cache.
//! - `WriteThroughUnbuffered` (Windows): both flags and 4 KiB-aligned writes;
//!   the OS asks the drive to write through its cache (FUA).
//! - `Flush`: a cached write followed by `FlushFileBuffers` (`fdatasync`), the
//!   drive-cache flush of redb's `Immediate` commits (PostgreSQL's `fsync`),
//!   on a small sequential file instead of the database.
//!
//! Whether a given drive honors FUA or flushes was not verified here (there is
//! no power-loss test).
//!
//! # How
//!
//! - A write transaction wraps the inner one. Reads go to the inner
//!   transaction (read-your-writes is the inner store's), and every `put`, and
//!   every `remove` that removed something, is also appended to a redo record
//!   kept in a reused buffer.
//! - `commit(Immediate)`: the record, preceded by the records of earlier
//!   `Deferred` commits still in memory, is appended to the WAL file with ONE
//!   positional write, made durable as `WalSync` says (platforms without a
//!   write-through flag always follow it with `fdatasync`). Only after that
//!   write returned is the inner
//!   transaction committed, with `Durability::Deferred` (published, not
//!   synced), carrying the record's LSN in `meta` under [`WAL_LSN_KEY`].
//! - `commit(Deferred)`: the record stays in memory and the inner transaction
//!   is committed `Deferred`. The next `Immediate` commit (or an empty one,
//!   `Db::sync`) writes it together with its own record, so deferred commits
//!   become durable with the next immediate one, as the [`Durability`]
//!   contract says. More than `max_pending_bytes` of them are written early.
//! - Checkpoint: when the records (pending ones included) of a commit do not
//!   fit in what is left of the WAL file, when the transaction is larger than
//!   `max_record_bytes`, on [`WalStore::checkpoint`], on `compact` and when
//!   the store is dropped (a clean close, also recorded in [`WAL_CLEAN_KEY`]):
//!   the inner transaction is committed `Immediate`
//!   (fsync; every earlier deferred inner commit becomes durable with it) and
//!   the log restarts at the beginning of the file. Checkpoints run inside a
//!   write transaction, so under the single writer.
//! - Recovery, at [`WalStore::open`]: read the durable `wal_lsn` = L of the
//!   inner store, apply in order every record of the WAL chain with LSN > L,
//!   stop at the first record that is torn or corrupt (magic, length,
//!   checksum) or does not continue the LSN sequence, then checkpoint. The
//!   rest of the file is never read again (the next cycle overwrites it).
//!
//! # WAL file (normative layout: `docs/format.md` §19)
//!
//! A file of `segment_bytes` (default 16 MiB) created zero-filled and synced
//! before first use, so that writes never change its size or allocation:
//! a write that extends a file costs twice as much with write-through
//! (metadata update). Block 0 is a header (magic `BABELWAL`, version,
//! capacity, a random 32-byte salt, checksum); records start at 4096:
//!
//! ```text
//! 0   4  magic "BWR1"
//! 4   4  payload length (u32 LE)
//! 8   8  lsn (u64 LE); consecutive records have consecutive LSNs
//! 16  16 first 16 bytes of keyed BLAKE3(key, bytes 0..16 ++ payload),
//!        key = BLAKE3 derive_key("babeldb 2026-09 wal record checksum v1", salt)
//! 32  .. payload: ops, each
//!        put:    01, table index, key len u32 LE, value len u32 LE, key, value
//!        remove: 02, table index, key len u32 LE, key
//! ```
//!
//! The salt is secret to the database files, so a value stored through the
//! API that happens to contain bytes shaped like a record can never pass the
//! checksum. The inner store also records the file's id (derived from the
//! salt) under [`WAL_ID_KEY`]; the records of a WAL that belongs to another
//! database are refused, never replayed.
//!
//! # Durability argument
//!
//! Claim: once `commit(Immediate)` returned `Ok`, the commit survives a crash
//! of the process, and a crash of the machine as far as the OS and the drive
//! keep the promise of the chosen `WalSync` mode (see "Why").
//!
//! 1. `Ok` is returned only after the write holding the record was made
//!    durable (write-through, or write + flush), like every earlier write of
//!    the cycle before it. The file was allocated, zero-filled and synced when it was
//!    created, so reading the records back needs no metadata the writes
//!    changed.
//! 2. The inner store's durable state is always exactly the state after the
//!    commits with LSN <= its durable `wal_lsn` (the LSN is written in the same
//!    atomic inner transaction as the changes; the inner store only becomes
//!    durable at checkpoints, which also write it). This is the invariant that
//!    makes recovery exact: the inner durable state never holds a commit whose
//!    place in the history is unknown, and never misses one that the WAL does
//!    not hold (a checkpoint restarts the WAL only after the inner commit that
//!    made everything before it durable).
//! 3. A cycle starts at LSN D + 1 at offset 4096, where D is the `wal_lsn` made
//!    durable by the checkpoint that started it, and only appends. At
//!    recovery the durable `wal_lsn` L >= D, so the chain read from offset 4096
//!    holds every acknowledged record with LSN > L, contiguous from L + 1:
//!    replaying them onto the state after L gives the state after the last
//!    acknowledged commit.
//! 4. Deferred commits whose records were still in memory are lost by a
//!    crash, as the contract allows; the survivors are always a prefix of the
//!    commit order (the chain is contiguous, and a checkpoint makes every
//!    earlier deferred commit durable in the inner store).
//! 5. Torn writes: a crash in the middle of a write can persist any subset of
//!    its sectors. The first damaged record fails its checksum (or length) and
//!    ends the chain; the records after it in the same write were not
//!    acknowledged either. Earlier writes are never rewritten, except the
//!    partial last sector (buffered: the last page) that they share with the
//!    next write, which is rewritten with identical bytes: a sector write is
//!    atomic on NVMe (AWUPF >= 1 logical block), so the acknowledged bytes are
//!    intact whether the new write landed or not. PostgreSQL rewrites its
//!    partial WAL page on every commit under the same assumption.
//! 6. Stale records: the file keeps records of older cycles (LSN < D + 1) and,
//!    after a crash, possibly intact records written after a torn one. At
//!    every open the next LSN jumps above the largest LSN the file can hold
//!    (max(L, end of chain) + file size / 32 + 1, made durable before the first
//!    new write), so no leftover record can ever continue a new chain.
//!
//! # Failures
//!
//! If the WAL write fails, or the inner commit fails after the WAL write
//! succeeded, the outcome of that commit is uncertain (its record may be
//! durable and would then be applied by the next recovery; redb itself
//! documents the same for a failed commit). The store is then "broken": reads
//! keep working, every later write transaction fails, and reopening the store
//! runs recovery. This mirrors PostgreSQL, which PANICs on a WAL write
//! failure.
//!
//! # Limitations
//!
//! - After a crash, reopen the database with its WAL (`Db::open_wal`) before
//!   anything else. Opening the inner file alone (`Db::open`) shows the state
//!   of the last checkpoint, without the commits that only the WAL holds; if
//!   it then writes, a later `open_wal` replays those records over a state
//!   that no longer matches them. After a clean close (drop, which
//!   checkpoints) the inner file alone is complete.
//! - The WAL file is held exclusively while the store is open (Windows: no
//!   write or delete sharing; Unix: `flock`), and with `WalConfig::dir` its
//!   name carries a hash of the database's absolute path, so two databases
//!   never share one. A WAL of another database is refused when it holds
//!   records, and replaced by a fresh file (new salt) when it holds none.
//! - A missing WAL file is an error when the inner store was not closed
//!   cleanly ([`WAL_CLEAN_KEY`] is not 1): acknowledged commits may exist only
//!   in that file. `WalConfig::recreate_missing` accepts losing them.
//! - Killing the process is not a power-loss test. The durability argument
//!   relies on the drive honoring FUA / write-through like PostgreSQL does.
//! - A checkpoint is a durable commit of the inner store under the writer
//!   lock, and writers wait for it. With redb it costs bookkeeping per deferred
//!   commit since the previous checkpoint plus the flush of the dirty pages:
//!   ~140-400 ms per checkpoint under sustained load on the reference machine.
//!   `segment_bytes` sets how often they come (16 MiB, the default, gave the
//!   best sustained throughput there: fewer dirty pages pile up in between);
//!   calling [`WalStore::checkpoint`] when the application is idle avoids
//!   them for bursty workloads. (PostgreSQL checkpoints in the background;
//!   redb exposes no durable flush outside a write transaction.)
//! - The `meta` entries [`WAL_LSN_KEY`], [`WAL_ID_KEY`] and [`WAL_CLEAN_KEY`]
//!   are hidden from the store's users (the engine never sees them); writing
//!   them through a `WalStore` is an error. [`WalStore::inner`] is for reading:
//!   a commit made through it bypasses the WAL.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::{Durability, ReadTxn, ScanFn, Store, Table, WriteTxn};
use crate::config::{WalConfig, WalSync};
use crate::error::{Error, Result};

/// `meta` entry, u64 LE: LSN of the last WAL record whose changes the inner
/// store holds (written in the same inner transaction as those changes).
pub const WAL_LSN_KEY: &str = "wal_lsn";
/// `meta` entry, 16 bytes: id of the WAL file this inner store belongs to.
pub const WAL_ID_KEY: &str = "wal_id";
/// `meta` entry, 1 byte: 1 after a clean close (the WAL holds nothing the
/// inner store lacks), 0 while a session may have logged commits.
pub const WAL_CLEAN_KEY: &str = "wal_clean";

/// Magic of the WAL file header.
pub const WAL_FILE_MAGIC: [u8; 8] = *b"BABELWAL";
pub const WAL_FILE_VERSION: u32 = 1;
/// Offset of the first record; the block before it holds the file header.
pub const WAL_DATA_START: u64 = 4096;
/// Magic of a record.
pub const WAL_RECORD_MAGIC: [u8; 4] = *b"BWR1";
/// Bytes of a record header (magic, length, LSN, checksum).
pub const WAL_RECORD_HEADER: usize = 32;

/// Alignment unit of the file: header block, preallocation, unbuffered I/O.
const BLOCK: usize = 4096;
/// Encoded file header: magic 8, version 4, data start 4, capacity 8, salt 32, checksum 16.
const FILE_HEADER_LEN: usize = 72;
const OP_PUT: u8 = 1;
const OP_REMOVE: u8 = 2;
const PUT_HEADER: usize = 10;
const REMOVE_HEADER: usize = 6;
/// Replayed payload bytes per inner transaction during recovery.
const REPLAY_BATCH_BYTES: u64 = 32 << 20;
/// Capacity of the record buffer kept after a checkpoint.
const KEEP_BUFFER: usize = 1 << 20;
const ZERO_CHUNK: usize = 1 << 20;

const SALT_CONTEXT: &str = "babeldb 2026-09 wal salt v1";
const ID_CONTEXT: &str = "babeldb 2026-09 wal file id v1";
const KEY_CONTEXT: &str = "babeldb 2026-09 wal record checksum v1";

// ---------------------------------------------------------------------------
// Public reports
// ---------------------------------------------------------------------------

/// Why the chain of valid records read from the WAL file ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChainEnd {
    /// Not enough bytes left in the file for a record header.
    #[default]
    EndOfFile,
    /// No record magic: zero fill, or bytes of an older record.
    NoRecord,
    /// The length runs past the end of the file.
    BadLength,
    /// Torn or corrupt record (or one from another WAL file).
    BadChecksum,
    /// A valid record that does not continue the LSN sequence (an older cycle).
    LsnGap,
}

/// What [`WalStore::open`] found and did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalRecovery {
    /// Durable `wal_lsn` of the inner store at open.
    pub durable_lsn: u64,
    /// Valid records at the start of the WAL (the chain).
    pub records_scanned: u64,
    /// Records applied to the inner store (LSN > `durable_lsn`).
    pub records_replayed: u64,
    /// Payload bytes of the applied records.
    pub bytes_replayed: u64,
    /// LSN of the last record of the chain (0 = empty chain).
    pub chain_end_lsn: u64,
    /// File offset where the chain ended, and why.
    pub chain_end_offset: u64,
    pub chain_end: ChainEnd,
    /// The WAL file was created (or replaced) by this open.
    pub created: bool,
    /// LSN of the first commit of this session.
    pub next_lsn: u64,
}

/// Activity counters of a [`WalStore`] since it was opened.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalStats {
    /// Commits whose redo record went to the log (`Immediate` and `Deferred`).
    pub logged_commits: u64,
    /// Write-through writes (each makes every record before it durable).
    pub wal_writes: u64,
    /// Bytes written by them.
    pub wal_bytes: u64,
    /// Inner `Immediate` commits: WAL full, oversized transaction, explicit
    /// checkpoint, `compact`, close.
    pub checkpoints: u64,
    /// Commits larger than `max_record_bytes`: not logged, made durable by a
    /// checkpoint instead.
    pub unlogged_commits: u64,
    /// Bytes of `Deferred` records waiting in memory (not durable yet).
    pub pending_bytes: u64,
    /// LSN of the next commit.
    pub next_lsn: u64,
    /// Time spent in write-through writes, in `Deferred` commits of the inner
    /// store after them, and in checkpoint commits (nanoseconds, cumulative).
    pub write_nanos: u64,
    pub inner_commit_nanos: u64,
    pub checkpoint_nanos: u64,
}

/// One record of the chain, as reported by [`inspect`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecordInfo {
    pub offset: u64,
    pub lsn: u64,
    pub payload_len: u32,
    pub ops: u64,
}

/// The chain of a WAL file, read without applying it ([`inspect`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalInspection {
    pub capacity: u64,
    pub id: [u8; 16],
    pub records: Vec<WalRecordInfo>,
    pub end_offset: u64,
    pub end: ChainEnd,
}

// ---------------------------------------------------------------------------
// File header
// ---------------------------------------------------------------------------

struct FileHeader {
    capacity: u64,
    salt: [u8; 32],
}

impl FileHeader {
    fn encode(&self) -> [u8; FILE_HEADER_LEN] {
        let mut b = [0u8; FILE_HEADER_LEN];
        b[0..8].copy_from_slice(&WAL_FILE_MAGIC);
        b[8..12].copy_from_slice(&WAL_FILE_VERSION.to_le_bytes());
        b[12..16].copy_from_slice(&(WAL_DATA_START as u32).to_le_bytes());
        b[16..24].copy_from_slice(&self.capacity.to_le_bytes());
        b[24..56].copy_from_slice(&self.salt);
        let sum = blake3::hash(&b[0..56]);
        b[56..72].copy_from_slice(&sum.as_bytes()[..16]);
        b
    }

    fn decode(b: &[u8], path: &Path) -> Result<FileHeader> {
        let bad = |what: &str| {
            Error::format(format!(
                "WAL file {}: {what}; the file is not usable (if the database was closed cleanly it can be deleted)",
                path.display()
            ))
        };
        if b.len() < FILE_HEADER_LEN {
            return Err(bad("truncated header"));
        }
        if b[0..8] != WAL_FILE_MAGIC {
            return Err(bad("not a babeldb WAL file (magic)"));
        }
        let sum = blake3::hash(&b[0..56]);
        if b[56..72] != sum.as_bytes()[..16] {
            return Err(bad("header checksum mismatch"));
        }
        let version = u32::from_le_bytes(le4(&b[8..12]));
        if version != WAL_FILE_VERSION {
            return Err(Error::Unsupported(format!(
                "WAL file {}: version {version} (this build reads {WAL_FILE_VERSION})",
                path.display()
            )));
        }
        if u64::from(u32::from_le_bytes(le4(&b[12..16]))) != WAL_DATA_START {
            return Err(bad("unexpected data offset"));
        }
        let capacity = u64::from_le_bytes(le8(&b[16..24]));
        if capacity <= WAL_DATA_START {
            return Err(bad("capacity too small"));
        }
        let mut salt = [0u8; 32];
        salt.copy_from_slice(&b[24..56]);
        Ok(FileHeader { capacity, salt })
    }

    fn id(&self) -> [u8; 16] {
        let k = blake3::derive_key(ID_CONTEXT, &self.salt);
        let mut id = [0u8; 16];
        id.copy_from_slice(&k[..16]);
        id
    }

    fn record_key(&self) -> [u8; 32] {
        blake3::derive_key(KEY_CONTEXT, &self.salt)
    }
}

fn le4(b: &[u8]) -> [u8; 4] {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[..4]);
    a
}

fn le8(b: &[u8]) -> [u8; 8] {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    a
}

/// Unpredictable salt without an RNG dependency: std's `RandomState` keys
/// (seeded from the OS RNG), the time, the process id and the path.
fn random_salt(path: &Path) -> [u8; 32] {
    use std::hash::{BuildHasher, Hasher};
    let mut h = blake3::Hasher::new_derive_key(SALT_CONTEXT);
    for i in 0..4u64 {
        let mut s = std::collections::hash_map::RandomState::new().build_hasher();
        s.write_u64(i);
        h.update(&s.finish().to_le_bytes());
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    h.update(&now.to_le_bytes());
    h.update(&std::process::id().to_le_bytes());
    h.update(path.as_os_str().as_encoded_bytes());
    *h.finalize().as_bytes()
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Create a zero-filled WAL file of `capacity` bytes: written and synced
/// under a temporary name, then renamed, so the final name only ever holds a
/// complete file.
fn create_wal_file(path: &Path, capacity: u64) -> Result<FileHeader> {
    let header = FileHeader {
        capacity,
        salt: random_salt(path),
    };
    let tmp = tmp_path(path);
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        let len = usize::try_from(capacity).unwrap_or(usize::MAX).min(ZERO_CHUNK);
        let mut chunk = vec![0u8; len];
        chunk[..FILE_HEADER_LEN].copy_from_slice(&header.encode());
        let mut written = 0u64;
        while written < capacity {
            let n = (capacity - written).min(chunk.len() as u64) as usize;
            f.write_all(&chunk[..n])?;
            if written == 0 {
                chunk[..FILE_HEADER_LEN].fill(0);
            }
            written += n as u64;
        }
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    // Flush the file's metadata (and with it the rename) before any record
    // is acknowledged from it.
    OpenOptions::new().write(true).open(path)?.sync_all()?;
    sync_parent_dir(path)?;
    Ok(header)
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Windows cannot flush a directory handle opened without backup semantics;
/// NTFS journals the rename and the `sync_all` of the renamed file flushes
/// the log (PostgreSQL skips directory fsyncs on Windows as well).
#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> Result<()> {
    Ok(())
}

fn read_file_header(path: &Path) -> Result<FileHeader> {
    let mut f = File::open(path)?;
    let mut b = [0u8; FILE_HEADER_LEN];
    let mut got = 0;
    while got < b.len() {
        match f.read(&mut b[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    FileHeader::decode(&b[..got], path)
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

fn record_checksum(key: &[u8; 32], head: &[u8], payload: &[u8]) -> [u8; 16] {
    let mut h = blake3::Hasher::new_keyed(key);
    h.update(head);
    h.update(payload);
    let mut sum = [0u8; 16];
    sum.copy_from_slice(&h.finalize().as_bytes()[..16]);
    sum
}

/// Fill the header of `rec` (header placeholder + payload) for `lsn`.
fn seal_record(rec: &mut [u8], lsn: u64, key: &[u8; 32]) {
    let payload_len = (rec.len() - WAL_RECORD_HEADER) as u32;
    rec[0..4].copy_from_slice(&WAL_RECORD_MAGIC);
    rec[4..8].copy_from_slice(&payload_len.to_le_bytes());
    rec[8..16].copy_from_slice(&lsn.to_le_bytes());
    let (head, rest) = rec.split_at_mut(16);
    let (sum, payload) = rest.split_at_mut(16);
    sum.copy_from_slice(&record_checksum(key, head, payload));
}

fn truncated_op() -> Error {
    Error::format("WAL record: truncated op")
}

fn take_u32(p: &[u8]) -> Result<(usize, &[u8])> {
    if p.len() < 4 {
        return Err(truncated_op());
    }
    let (n, rest) = p.split_at(4);
    Ok((u32::from_le_bytes(le4(n)) as usize, rest))
}

fn take(p: &[u8], n: usize) -> Result<(&[u8], &[u8])> {
    if p.len() < n {
        return Err(truncated_op());
    }
    Ok(p.split_at(n))
}

/// Decode a payload, calling `f(op, table, key, value)` for each op.
fn for_each_op(mut p: &[u8], mut f: impl FnMut(u8, Table, &[u8], &[u8]) -> Result<()>) -> Result<u64> {
    let mut n = 0;
    while let Some((&op, rest)) = p.split_first() {
        let (&t, rest) = rest.split_first().ok_or_else(truncated_op)?;
        let table = *Table::ALL
            .get(usize::from(t))
            .ok_or_else(|| Error::format(format!("WAL record: unknown table {t}")))?;
        match op {
            OP_PUT => {
                let (klen, rest) = take_u32(rest)?;
                let (vlen, rest) = take_u32(rest)?;
                let (key, rest) = take(rest, klen)?;
                let (value, rest) = take(rest, vlen)?;
                f(op, table, key, value)?;
                p = rest;
            }
            OP_REMOVE => {
                let (klen, rest) = take_u32(rest)?;
                let (key, rest) = take(rest, klen)?;
                f(op, table, key, &[])?;
                p = rest;
            }
            other => return Err(Error::format(format!("WAL record: unknown op {other}"))),
        }
        n += 1;
    }
    Ok(n)
}

/// Apply the ops of one record. Puts and removes are blind writes, so
/// replaying a record over a state that already holds it changes nothing.
fn apply_payload<W: WriteTxn + ?Sized>(w: &mut W, payload: &[u8]) -> Result<u64> {
    for_each_op(payload, |op, table, key, value| {
        if op == OP_PUT {
            w.put(table, key, value)
        } else {
            w.remove(table, key).map(|_| ())
        }
    })
}

/// Sequential reader of the chain of valid records from `WAL_DATA_START`.
struct ChainReader {
    r: BufReader<File>,
    key: [u8; 32],
    file_len: u64,
    pos: u64,
    last: Option<u64>,
    end: Option<ChainEnd>,
    head: [u8; WAL_RECORD_HEADER],
    payload: Vec<u8>,
}

impl ChainReader {
    fn open(path: &Path, header: &FileHeader) -> Result<ChainReader> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let mut r = BufReader::with_capacity(1 << 20, file);
        r.seek(SeekFrom::Start(WAL_DATA_START))?;
        Ok(ChainReader {
            r,
            key: header.record_key(),
            file_len,
            pos: WAL_DATA_START,
            last: None,
            end: None,
            head: [0u8; WAL_RECORD_HEADER],
            payload: Vec::new(),
        })
    }

    /// Next valid record `(offset, lsn)`; its payload is in `self.payload`.
    fn next_record(&mut self) -> Result<Option<(u64, u64)>> {
        if self.end.is_some() {
            return Ok(None);
        }
        match self.read_one()? {
            Ok(found) => Ok(Some(found)),
            Err(end) => {
                self.end = Some(end);
                Ok(None)
            }
        }
    }

    fn read_one(&mut self) -> Result<std::result::Result<(u64, u64), ChainEnd>> {
        let left = self.file_len.saturating_sub(self.pos);
        if left < WAL_RECORD_HEADER as u64 {
            return Ok(Err(ChainEnd::EndOfFile));
        }
        self.r.read_exact(&mut self.head)?;
        if self.head[0..4] != WAL_RECORD_MAGIC {
            return Ok(Err(ChainEnd::NoRecord));
        }
        let len = u64::from(u32::from_le_bytes(le4(&self.head[4..8])));
        let lsn = u64::from_le_bytes(le8(&self.head[8..16]));
        if len > left - WAL_RECORD_HEADER as u64 {
            return Ok(Err(ChainEnd::BadLength));
        }
        self.payload.resize(len as usize, 0);
        self.r.read_exact(&mut self.payload)?;
        if record_checksum(&self.key, &self.head[..16], &self.payload) != self.head[16..32] {
            return Ok(Err(ChainEnd::BadChecksum));
        }
        if let Some(last) = self.last
            && Some(lsn) != last.checked_add(1)
        {
            return Ok(Err(ChainEnd::LsnGap));
        }
        let offset = self.pos;
        self.pos += WAL_RECORD_HEADER as u64 + len;
        self.last = Some(lsn);
        Ok(Ok((offset, lsn)))
    }

    fn end(&self) -> ChainEnd {
        self.end.unwrap_or_default()
    }
}

/// Read the chain of a WAL file without applying it (tools, tests).
pub fn inspect(path: &Path) -> Result<WalInspection> {
    let header = read_file_header(path)?;
    let mut rd = ChainReader::open(path, &header)?;
    let mut records = Vec::new();
    while let Some((offset, lsn)) = rd.next_record()? {
        let ops = for_each_op(&rd.payload, |_, _, _, _| Ok(()))?;
        records.push(WalRecordInfo {
            offset,
            lsn,
            payload_len: rd.payload.len() as u32,
            ops,
        });
    }
    Ok(WalInspection {
        capacity: header.capacity,
        id: header.id(),
        records,
        end_offset: rd.pos,
        end: rd.end(),
    })
}

/// Apply to `w`, in order, the ops of every chain record with LSN >
/// `after_lsn`, without the checks of [`WalStore::open`] (file id, LSN gap).
/// Returns the number of records applied. For tools and tests.
pub fn replay_into<W: WriteTxn + ?Sized>(path: &Path, w: &mut W, after_lsn: u64) -> Result<u64> {
    let header = read_file_header(path)?;
    let mut rd = ChainReader::open(path, &header)?;
    let mut applied = 0;
    while let Some((_, lsn)) = rd.next_record()? {
        if lsn > after_lsn {
            apply_payload(w, &rd.payload)?;
            applied += 1;
        }
    }
    Ok(applied)
}

// ---------------------------------------------------------------------------
// Write-through I/O
// ---------------------------------------------------------------------------

/// The write handle of the WAL file.
struct WalIo {
    file: File,
    /// `sync_data` after each write (`WalSync::Flush`, or no write-through flag).
    sync_data: bool,
    /// `FILE_FLAG_NO_BUFFERING` state (experimental, Windows).
    unbuffered: Option<Unbuffered>,
}

/// Unbuffered writes must be block-aligned in offset, length and memory: each
/// write starts at the block holding the current end of the log and rewrites
/// its first bytes (kept in `tail`) with identical content.
struct Unbuffered {
    buf: Vec<u8>,
    off: usize,
    tail: Vec<u8>,
}

/// Open the WAL for writing, exclusively: other handles may read it (recovery
/// does) but not write or delete it, so a second store on the same file fails
/// here instead of interleaving records. Returns the handle and whether every
/// write must be followed by `sync_data`.
#[cfg(windows)]
fn open_write_handle(path: &Path, sync: WalSync) -> Result<(File, bool)> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_NO_BUFFERING, FILE_FLAG_WRITE_THROUGH, FILE_SHARE_READ,
    };
    const ERROR_SHARING_VIOLATION: i32 = 32;
    let flags = match sync {
        WalSync::WriteThrough => FILE_FLAG_WRITE_THROUGH,
        WalSync::WriteThroughUnbuffered => FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING,
        WalSync::Flush => 0,
    };
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ)
        .custom_flags(flags)
        .open(path)
        .map_err(|e| {
            if e.raw_os_error() == Some(ERROR_SHARING_VIOLATION) {
                in_use_error(path)
            } else {
                e.into()
            }
        })?;
    Ok((file, sync == WalSync::Flush))
}

/// `O_DSYNC` has this value on the Linux architectures listed (asm-generic).
#[cfg(all(
    target_os = "linux",
    any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv64"
    )
))]
fn open_write_handle(path: &Path, sync: WalSync) -> Result<(File, bool)> {
    use std::os::unix::fs::OpenOptionsExt;
    const O_DSYNC: i32 = 0o10000;
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    match sync {
        WalSync::WriteThrough => {
            options.custom_flags(O_DSYNC);
        }
        WalSync::WriteThroughUnbuffered => return Err(unbuffered_unsupported()),
        WalSync::Flush => {}
    }
    let file = options.open(path)?;
    lock_exclusive(&file, path)?;
    Ok((file, sync == WalSync::Flush))
}

#[cfg(not(any(
    windows,
    all(
        target_os = "linux",
        any(
            target_arch = "x86",
            target_arch = "x86_64",
            target_arch = "arm",
            target_arch = "aarch64",
            target_arch = "riscv64"
        )
    )
)))]
fn open_write_handle(path: &Path, sync: WalSync) -> Result<(File, bool)> {
    if sync == WalSync::WriteThroughUnbuffered {
        return Err(unbuffered_unsupported());
    }
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    #[cfg(unix)]
    lock_exclusive(&file, path)?;
    Ok((file, true))
}

/// `flock(LOCK_EX | LOCK_NB)`: advisory, so this store's own readers still work.
#[cfg(unix)]
fn lock_exclusive(file: &File, path: &Path) -> Result<()> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(in_use_error(path)),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

#[cfg_attr(not(any(windows, unix)), allow(dead_code))]
fn in_use_error(path: &Path) -> Error {
    Error::Backend(format!(
        "WAL {} is in use by another open store",
        path.display()
    ))
}

#[cfg(not(windows))]
fn unbuffered_unsupported() -> Error {
    Error::Unsupported("WalConfig::unbuffered is only implemented on Windows".into())
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "WAL write wrote nothing")),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(not(any(windows, unix)))]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut f = file;
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(buf)
}

impl WalIo {
    fn open(path: &Path, sync: WalSync) -> Result<WalIo> {
        let (file, sync_data) = open_write_handle(path, sync)?;
        Ok(WalIo {
            file,
            sync_data,
            unbuffered: (sync == WalSync::WriteThroughUnbuffered).then(|| Unbuffered {
                buf: Vec::new(),
                off: 0,
                tail: Vec::new(),
            }),
        })
    }

    /// One durable write of `data` at `offset` (the end of the log).
    fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        match &mut self.unbuffered {
            None => write_all_at(&self.file, data, offset)?,
            Some(u) => write_unbuffered(&self.file, u, offset, data)?,
        }
        if self.sync_data {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// The log restarts at `WAL_DATA_START` (block aligned).
    fn restart(&mut self) {
        if let Some(u) = &mut self.unbuffered {
            u.tail.clear();
        }
    }
}

fn write_unbuffered(file: &File, u: &mut Unbuffered, offset: u64, data: &[u8]) -> io::Result<()> {
    let block = BLOCK as u64;
    let start = offset - offset % block;
    let prefix = (offset - start) as usize;
    if prefix != u.tail.len() {
        return Err(io::Error::other("WAL: unbuffered tail block out of step"));
    }
    let total = prefix + data.len();
    let padded = total.div_ceil(BLOCK) * BLOCK;
    if u.buf.len() < u.off + padded {
        let buf = vec![0u8; padded.max(ZERO_CHUNK) + BLOCK];
        let off = buf.as_ptr().align_offset(BLOCK);
        if off >= BLOCK {
            return Err(io::Error::other("WAL: cannot align the staging buffer"));
        }
        u.buf = buf;
        u.off = off;
    }
    let stage = &mut u.buf[u.off..u.off + padded];
    stage[..prefix].copy_from_slice(&u.tail);
    stage[prefix..total].copy_from_slice(data);
    stage[total..].fill(0);
    write_all_at(file, stage, start)?;
    let end = offset + data.len() as u64;
    let last_block = ((end - end % block) - start) as usize;
    u.tail.clear();
    u.tail.extend_from_slice(&stage[last_block..total]);
    Ok(())
}

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Counters {
    logged_commits: AtomicU64,
    wal_writes: AtomicU64,
    wal_bytes: AtomicU64,
    checkpoints: AtomicU64,
    unlogged_commits: AtomicU64,
    pending_bytes: AtomicU64,
    next_lsn: AtomicU64,
    write_nanos: AtomicU64,
    inner_commit_nanos: AtomicU64,
    checkpoint_nanos: AtomicU64,
}

fn bump(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

fn bump_since(c: &AtomicU64, t: Instant) {
    bump(c, u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX));
}

/// Everything a write transaction needs besides the locked state.
struct Shared {
    path: PathBuf,
    cfg: WalConfig,
    key: [u8; 32],
    counters: Counters,
    recovery: WalRecovery,
}

/// Log state, owned by the single writer (locked for the whole life of a
/// write transaction, which already excludes every other writer).
struct WalState {
    io: WalIo,
    capacity: u64,
    /// File offset where the next write starts.
    tail: u64,
    next_lsn: u64,
    /// Records of `Deferred` commits not written yet (`pending_len` bytes),
    /// followed by the record of the open write transaction.
    out: Vec<u8>,
    pending_len: usize,
    /// The inner store holds commits that are not durable in it.
    dirty: bool,
    /// Set around the I/O of a commit; still set when a panic escaped it.
    in_commit: bool,
    /// Why the store refuses writes.
    broken: Option<String>,
}

impl WalState {
    /// Write every record of `out` in one write-through write.
    fn write_out(&mut self, shared: &Shared) -> io::Result<()> {
        let n = self.out.len();
        let t = Instant::now();
        self.io.write_at(self.tail, &self.out)?;
        bump_since(&shared.counters.write_nanos, t);
        self.tail += n as u64;
        self.out.clear();
        self.pending_len = 0;
        bump(&shared.counters.wal_writes, 1);
        bump(&shared.counters.wal_bytes, n as u64);
        shared.counters.pending_bytes.store(0, Ordering::Relaxed);
        Ok(())
    }

    /// Commit `inner` durably (it carries `wal_lsn = lsn`) and restart the log.
    fn checkpoint_commit<W: WriteTxn>(&mut self, inner: W, lsn: u64, shared: &Shared) -> Result<()> {
        self.in_commit = true;
        let t = Instant::now();
        let r = inner.commit(Durability::Immediate);
        bump_since(&shared.counters.checkpoint_nanos, t);
        self.in_commit = false;
        if let Err(e) = r {
            return Err(self.fail("the checkpoint commit of the inner store", &e));
        }
        self.tail = WAL_DATA_START;
        self.out.clear();
        self.out.shrink_to(KEEP_BUFFER);
        self.pending_len = 0;
        self.next_lsn = lsn + 1;
        self.dirty = false;
        self.io.restart();
        let c = &shared.counters;
        bump(&c.checkpoints, 1);
        c.pending_bytes.store(0, Ordering::Relaxed);
        c.next_lsn.store(self.next_lsn, Ordering::Relaxed);
        Ok(())
    }

    /// Mark the store broken after a failure whose outcome is uncertain.
    fn fail(&mut self, what: &str, e: &dyn std::fmt::Display) -> Error {
        let reason = format!("{what} failed ({e})");
        self.broken = Some(reason.clone());
        Error::Backend(format!(
            "WAL: {reason}; the outcome of this commit is uncertain (a durable WAL record is \
             applied by the next recovery) and the store refuses writes until it is reopened"
        ))
    }
}

fn broken_error(reason: &str) -> Error {
    Error::Backend(format!(
        "WAL store is broken ({reason}); reopen it to run recovery"
    ))
}

fn is_reserved(table: Table, key: &[u8]) -> bool {
    table == Table::Meta && [WAL_LSN_KEY, WAL_ID_KEY, WAL_CLEAN_KEY].iter().any(|k| key == k.as_bytes())
}

fn reserved_error() -> Error {
    Error::InvalidArgument(format!(
        "meta entries {WAL_LSN_KEY:?}, {WAL_ID_KEY:?} and {WAL_CLEAN_KEY:?} are reserved by the WAL store"
    ))
}

fn visible_get<T: ReadTxn + ?Sized>(t: &T, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
    if is_reserved(table, key) {
        return Ok(None);
    }
    t.get(table, key)
}

fn visible_scan<T: ReadTxn + ?Sized>(
    t: &T,
    table: Table,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    reverse: bool,
    f: &mut ScanFn<'_>,
) -> Result<()> {
    if table != Table::Meta {
        return t.scan(table, start, end, reverse, f);
    }
    t.scan(table, start, end, reverse, &mut |k, v| {
        if is_reserved(table, k) {
            Ok(true)
        } else {
            f(k, v)
        }
    })
}

fn visible_len<T: ReadTxn + ?Sized>(t: &T, table: Table) -> Result<u64> {
    let n = t.len(table)?;
    if table != Table::Meta {
        return Ok(n);
    }
    let mut hidden = 0;
    for key in [WAL_LSN_KEY, WAL_ID_KEY, WAL_CLEAN_KEY] {
        hidden += u64::from(t.get(table, key.as_bytes())?.is_some());
    }
    Ok(n.saturating_sub(hidden))
}

/// A [`Store`] whose durable commits go through a write-through WAL; see the
/// module documentation.
pub struct WalStore<S: Store> {
    inner: S,
    state: Mutex<WalState>,
    shared: Shared,
}

impl<S: Store> WalStore<S> {
    /// Open the WAL at `path` in front of `inner`, recover, and checkpoint.
    /// The file is created when missing (an error after an unclean shutdown,
    /// unless `recreate_missing`) and held exclusively until the store is
    /// dropped. `inner` must not be written by anything else meanwhile.
    pub fn open(inner: S, path: impl Into<PathBuf>, cfg: WalConfig) -> Result<WalStore<S>> {
        cfg.validate()?;
        let path: PathBuf = path.into();
        let capacity = cfg.segment_capacity();
        let (durable_lsn, inner_id, clean) = {
            let r = inner.begin_read()?;
            let lsn = match r.get(Table::Meta, WAL_LSN_KEY.as_bytes())? {
                Some(v) => crate::format::decode_u64(&v)?,
                None => 0,
            };
            let clean = r.get(Table::Meta, WAL_CLEAN_KEY.as_bytes())?.as_deref() == Some(&[1u8][..]);
            (lsn, r.get(Table::Meta, WAL_ID_KEY.as_bytes())?, clean)
        };
        remove_if_exists(&tmp_path(&path))?;
        let mut rec = WalRecovery {
            durable_lsn,
            ..WalRecovery::default()
        };
        let exists = match fs::metadata(&path) {
            Ok(_) => true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => return Err(e.into()),
        };
        let (io, header, last_lsn) = if exists {
            // Take the file before reading it: nothing else may write it while
            // it is recovered and used.
            let io = WalIo::open(&path, cfg.sync)?;
            let file_len = fs::metadata(&path)?.len();
            let header = read_file_header(&path)?;
            let ours = inner_id.as_deref() == Some(&header.id()[..]);
            // A file of another size, or one without records that belongs to
            // another database (or to none yet), is replaced by a fresh one
            // (new salt) once the inner store holds everything durably.
            let replace = !ours || file_len != capacity || header.capacity != capacity;
            let last = replay(&inner, &path, &header, file_len, ours, replace, &mut rec)?;
            if replace {
                drop(io);
                fs::remove_file(&path)?;
                let fresh = create_wal_file(&path, capacity)?;
                let io = WalIo::open(&path, cfg.sync)?;
                set_marks(&inner, None, &fresh.id())?;
                rec.created = true;
                (io, fresh, last)
            } else {
                (io, header, last)
            }
        } else {
            if inner_id.is_some() && !clean && !cfg.recreate_missing {
                return Err(Error::integrity(
                    None,
                    format!(
                        "WAL file {} is missing and the database was not closed cleanly: commits acknowledged since its last checkpoint may exist only in that file. Restore it (check WalConfig::dir), or set WalConfig::recreate_missing to open without it and lose them",
                        path.display()
                    ),
                ));
            }
            let fresh = create_wal_file(&path, capacity)?;
            let io = WalIo::open(&path, cfg.sync)?;
            set_marks(&inner, Some(durable_lsn), &fresh.id())?;
            rec.created = true;
            (io, fresh, durable_lsn)
        };
        rec.next_lsn = last_lsn + 1;
        let counters = Counters::default();
        counters.next_lsn.store(rec.next_lsn, Ordering::Relaxed);
        let state = WalState {
            io,
            capacity: header.capacity,
            tail: WAL_DATA_START,
            next_lsn: rec.next_lsn,
            out: Vec::with_capacity(64 << 10),
            pending_len: 0,
            dirty: false,
            in_commit: false,
            broken: None,
        };
        Ok(WalStore {
            inner,
            state: Mutex::new(state),
            shared: Shared {
                path,
                key: header.record_key(),
                cfg,
                counters,
                recovery: rec,
            },
        })
    }

    /// The inner store, for reading: a commit made through it bypasses the WAL
    /// (and its `wal_lsn`), which breaks recovery.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn wal_path(&self) -> &Path {
        &self.shared.path
    }

    pub fn config(&self) -> &WalConfig {
        &self.shared.cfg
    }

    /// What recovery found when the store was opened.
    pub fn recovery(&self) -> &WalRecovery {
        &self.shared.recovery
    }

    pub fn stats(&self) -> WalStats {
        let c = &self.shared.counters;
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        WalStats {
            logged_commits: l(&c.logged_commits),
            wal_writes: l(&c.wal_writes),
            wal_bytes: l(&c.wal_bytes),
            checkpoints: l(&c.checkpoints),
            unlogged_commits: l(&c.unlogged_commits),
            pending_bytes: l(&c.pending_bytes),
            next_lsn: l(&c.next_lsn),
            write_nanos: l(&c.write_nanos),
            inner_commit_nanos: l(&c.inner_commit_nanos),
            checkpoint_nanos: l(&c.checkpoint_nanos),
        }
    }

    /// Make every commit durable in the inner store (one `Immediate` inner
    /// commit) and restart the log. Waits for the current writer. A no-op
    /// when nothing was committed since the last checkpoint.
    pub fn checkpoint(&self) -> Result<()> {
        self.begin_write()?.checkpoint(false)
    }
}

/// Record durably in the inner store the WAL file it belongs to (and its LSN,
/// when given), with `wal_clean` = 0: a session is open.
fn set_marks<S: Store>(inner: &S, lsn: Option<u64>, id: &[u8]) -> Result<()> {
    let mut w = inner.begin_write()?;
    if let Some(lsn) = lsn {
        w.put(Table::Meta, WAL_LSN_KEY.as_bytes(), &lsn.to_le_bytes())?;
    }
    w.put(Table::Meta, WAL_ID_KEY.as_bytes(), id)?;
    w.put(Table::Meta, WAL_CLEAN_KEY.as_bytes(), &[0])?;
    w.commit(Durability::Immediate)
}

/// Apply the chain of an existing WAL file to `inner` and make the result
/// durable, together with the LSN jump (module docs, point 6). `ours`: the file
/// id is the one recorded in `inner`; `replace`: the caller replaces the file
/// next. Returns the `wal_lsn` now durable in `inner`.
fn replay<S: Store>(
    inner: &S,
    path: &Path,
    header: &FileHeader,
    file_len: u64,
    ours: bool,
    replace: bool,
    rec: &mut WalRecovery,
) -> Result<u64> {
    let id = header.id();
    let durable_lsn = rec.durable_lsn;
    let mut rd = ChainReader::open(path, header)?;
    let mut txn = Some(inner.begin_write()?);
    let mut batch = 0u64;
    while let Some((_, lsn)) = rd.next_record()? {
        if !ours {
            return Err(Error::integrity(
                None,
                format!(
                    "WAL {} holds records but belongs to another database (its id is not the one \
                     recorded in the store); refusing to replay it",
                    path.display()
                ),
            ));
        }
        rec.records_scanned += 1;
        if lsn <= durable_lsn {
            continue;
        }
        if rec.records_replayed == 0 && lsn != durable_lsn + 1 {
            return Err(Error::integrity(
                None,
                format!(
                    "WAL {} continues at LSN {lsn} but the store holds up to LSN {durable_lsn}: \
                     records are missing (was the database file replaced?)",
                    path.display()
                ),
            ));
        }
        let w = txn
            .as_mut()
            .ok_or_else(|| Error::backend("WAL replay: no open transaction"))?;
        apply_payload(w, &rd.payload)?;
        rec.records_replayed += 1;
        rec.bytes_replayed += rd.payload.len() as u64;
        batch += rd.payload.len() as u64;
        if batch >= REPLAY_BATCH_BYTES {
            if let Some(mut w) = txn.take() {
                w.put(Table::Meta, WAL_LSN_KEY.as_bytes(), &lsn.to_le_bytes())?;
                // Not durable: a crash here leaves the inner store at
                // `durable_lsn` and the next open replays again.
                w.commit(Durability::Deferred)?;
            }
            txn = Some(inner.begin_write()?);
            batch = 0;
        }
    }
    rec.chain_end_lsn = rd.last.unwrap_or(0);
    rec.chain_end_offset = rd.pos;
    rec.chain_end = rd.end();
    // No record left in the file can have an LSN above this.
    let max_records = file_len.max(header.capacity) / WAL_RECORD_HEADER as u64;
    let jump = durable_lsn
        .max(rec.chain_end_lsn)
        .checked_add(max_records + 1)
        .ok_or(Error::IdExhausted("WAL LSN"))?;
    let mut w = txn.ok_or_else(|| Error::backend("WAL replay: no open transaction"))?;
    w.put(Table::Meta, WAL_LSN_KEY.as_bytes(), &jump.to_le_bytes())?;
    if replace {
        // Everything is durable in the inner store, so the file may go; a crash
        // before its successor is recorded must not look like a lost WAL.
        w.put(Table::Meta, WAL_CLEAN_KEY.as_bytes(), &[1])?;
    } else {
        w.put(Table::Meta, WAL_ID_KEY.as_bytes(), &id)?;
        w.put(Table::Meta, WAL_CLEAN_KEY.as_bytes(), &[0])?;
    }
    w.commit(Durability::Immediate)?;
    Ok(jump)
}

impl<S: Store> Drop for WalStore<S> {
    /// Checkpoints and records the clean close (`wal_clean` = 1): the inner
    /// store alone is then complete, and a missing WAL is no loss. Skipped while
    /// panicking (and when broken): the next open recovers from the WAL.
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = self.begin_write().and_then(|w| w.checkpoint(true));
        }
    }
}

/// Read transaction: the inner one, with the WAL's `meta` entries hidden.
pub struct WalRead<R> {
    inner: R,
}

impl<R: ReadTxn> ReadTxn for WalRead<R> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        visible_get(&self.inner, table, key)
    }

    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()> {
        visible_scan(&self.inner, table, start, end, reverse, f)
    }

    fn len(&self, table: Table) -> Result<u64> {
        visible_len(&self.inner, table)
    }
}

/// Write transaction: the inner one plus the redo record being built.
/// Dropping it without commit aborts the inner transaction; its partial
/// record is discarded by the next `begin_write`.
pub struct WalWrite<'a, S: Store + 'a> {
    inner: S::Write<'a>,
    st: MutexGuard<'a, WalState>,
    shared: &'a Shared,
    /// Offset of this transaction's record in `st.out`.
    start: usize,
    /// Puts and effective removes.
    ops: u64,
    /// The record outgrew `max_record_bytes`: it is not logged, the commit
    /// becomes a checkpoint.
    oversized: bool,
}

impl<'a, S: Store + 'a> WalWrite<'a, S> {
    fn log(&mut self, op: u8, table: Table, key: &[u8], value: &[u8]) {
        if self.oversized {
            return;
        }
        let head = if op == OP_PUT { PUT_HEADER } else { REMOVE_HEADER };
        let size = self.st.out.len() - self.start;
        let fits = key.len() <= u32::MAX as usize
            && value.len() <= u32::MAX as usize
            && size
                .saturating_add(head)
                .saturating_add(key.len())
                .saturating_add(value.len())
                <= self.shared.cfg.record_limit();
        if !fits {
            self.oversized = true;
            let start = self.start;
            self.st.out.truncate(start);
            return;
        }
        let out = &mut self.st.out;
        out.push(op);
        out.push(table.index() as u8);
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        if op == OP_PUT {
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        }
        out.extend_from_slice(key);
        out.extend_from_slice(value);
    }

    /// `WalStore::checkpoint`, or with `closing` the checkpoint of a clean
    /// close (always committed, records `wal_clean` = 1), inside the write
    /// transaction that serializes it.
    fn checkpoint(self, closing: bool) -> Result<()> {
        let WalWrite {
            mut inner,
            mut st,
            shared,
            ..
        } = self;
        let st = &mut *st;
        let keep = st.pending_len;
        st.out.truncate(keep);
        if !st.dirty && !closing {
            return Ok(());
        }
        let lsn = st.next_lsn - 1;
        inner.put(Table::Meta, WAL_LSN_KEY.as_bytes(), &lsn.to_le_bytes())?;
        if closing {
            inner.put(Table::Meta, WAL_CLEAN_KEY.as_bytes(), &[1])?;
        }
        st.checkpoint_commit(inner, lsn, shared)
    }
}

impl<'a, S: Store + 'a> ReadTxn for WalWrite<'a, S> {
    fn get(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        visible_get(&self.inner, table, key)
    }

    fn scan(
        &self,
        table: Table,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        f: &mut ScanFn<'_>,
    ) -> Result<()> {
        visible_scan(&self.inner, table, start, end, reverse, f)
    }

    fn len(&self, table: Table) -> Result<u64> {
        visible_len(&self.inner, table)
    }
}

impl<'a, S: Store + 'a> WriteTxn for WalWrite<'a, S> {
    fn put(&mut self, table: Table, key: &[u8], value: &[u8]) -> Result<()> {
        if is_reserved(table, key) {
            return Err(reserved_error());
        }
        self.inner.put(table, key, value)?;
        self.ops += 1;
        self.log(OP_PUT, table, key, value);
        Ok(())
    }

    /// A remove that found nothing changed nothing, so the redo record does
    /// not need it: replay reproduces the same sequence of states without it.
    fn remove(&mut self, table: Table, key: &[u8]) -> Result<bool> {
        if is_reserved(table, key) {
            return Err(reserved_error());
        }
        let existed = self.inner.remove(table, key)?;
        if existed {
            self.ops += 1;
            self.log(OP_REMOVE, table, key, &[]);
        }
        Ok(existed)
    }

    fn commit(self, durability: Durability) -> Result<()> {
        let WalWrite {
            mut inner,
            mut st,
            shared,
            start,
            ops,
            oversized,
        } = self;
        let st = &mut *st;
        if ops == 0 && !oversized {
            // Nothing changed: aborting the inner transaction is equivalent.
            drop(inner);
            let keep = st.pending_len;
            st.out.truncate(keep);
            if durability == Durability::Immediate && keep > 0 {
                st.in_commit = true;
                let r = st.write_out(shared);
                st.in_commit = false;
                if let Err(e) = r {
                    return Err(st.fail("the WAL write", &e));
                }
            }
            return Ok(());
        }
        let lsn = st.next_lsn;
        inner.put(Table::Meta, WAL_LSN_KEY.as_bytes(), &lsn.to_le_bytes())?;
        // `out` = pending records + this record (nothing of it when oversized).
        let end = st.tail + st.out.len() as u64;
        if oversized || end > st.capacity {
            if oversized {
                bump(&shared.counters.unlogged_commits, 1);
            }
            return st.checkpoint_commit(inner, lsn, shared);
        }
        seal_record(&mut st.out[start..], lsn, &shared.key);
        let write_now =
            durability == Durability::Immediate || st.out.len() > shared.cfg.max_pending_bytes;
        st.in_commit = true;
        if write_now && let Err(e) = st.write_out(shared) {
            st.in_commit = false;
            return Err(st.fail("the WAL write", &e));
        }
        // WAL first: the commit becomes visible only once its record is durable
        // (or, for Deferred, queued behind every earlier record).
        let t = Instant::now();
        let r = inner.commit(Durability::Deferred);
        bump_since(&shared.counters.inner_commit_nanos, t);
        st.in_commit = false;
        if let Err(e) = r {
            return Err(st.fail("the inner commit after the WAL write", &e));
        }
        st.next_lsn = lsn + 1;
        st.dirty = true;
        if !write_now {
            st.pending_len = st.out.len();
        }
        let c = &shared.counters;
        bump(&c.logged_commits, 1);
        c.pending_bytes.store(st.pending_len as u64, Ordering::Relaxed);
        c.next_lsn.store(st.next_lsn, Ordering::Relaxed);
        Ok(())
    }
}

impl<S: Store> Store for WalStore<S> {
    type Read<'a>
        = WalRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = WalWrite<'a, S>
    where
        Self: 'a;

    fn begin_read(&self) -> Result<Self::Read<'_>> {
        Ok(WalRead {
            inner: self.inner.begin_read()?,
        })
    }

    fn begin_write(&self) -> Result<Self::Write<'_>> {
        let inner = self.inner.begin_write()?;
        let mut st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if st.in_commit && st.broken.is_none() {
            st.broken = Some("a panic interrupted a commit".into());
        }
        if let Some(reason) = &st.broken {
            return Err(broken_error(reason));
        }
        let keep = st.pending_len;
        st.out.truncate(keep);
        let start = st.out.len();
        st.out.extend_from_slice(&[0u8; WAL_RECORD_HEADER]);
        Ok(WalWrite {
            inner,
            st,
            shared: &self.shared,
            start,
            ops: 0,
            oversized: false,
        })
    }

    /// Checkpoints, then compacts the inner store.
    fn compact(&mut self) -> Result<bool> {
        self.checkpoint()?;
        self.inner.compact()
    }

    /// The inner store's files and the WAL file.
    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.inner.files();
        files.push(self.shared.path.clone());
        files
    }

    fn backend_name(&self) -> &'static str {
        match self.inner.backend_name() {
            "redb" => "redb+wal",
            "mem" => "mem+wal",
            "lmdb" => "lmdb+wal",
            "fjall" => "fjall+wal",
            _ => "wal",
        }
    }
}
