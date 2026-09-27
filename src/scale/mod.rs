//! Discord-scale layer on top of [`Db`]: group commit, sharding, read
//! coalescing and a chat-message schema.
//!
//! # Pieces
//!
//! - [`GroupCommitter`] (`group_commit`): one writer thread per database owns a
//!   [`BatchSink`]; callers enqueue operations in a bounded queue and wait on a
//!   completion. The writer turns everything that queued up while the previous
//!   commit was running into ONE commit, so N concurrent writers pay one fsync
//!   instead of N.
//! - [`ShardedDb`] (`sharded`): N independent databases behind a stable
//!   [`Router`], each with its own committer: N commits (and fsyncs) in flight
//!   at once. Atomicity is per shard only.
//! - [`SingleFlight`], [`Coalescer`] and [`RecentCache`] (`coalesce`):
//!   concurrent identical reads share one execution, and a small write-through
//!   cache keeps the newest page of hot channels.
//! - [`ChatStore`] (`chat`): the Discord-like schema `channel_id BE ||
//!   message_id BE` with [`Snowflake`] ids; "latest messages" is one reverse
//!   prefix scan on one shard.
//!
//! Every component is written against the small traits [`BatchSink`] (write
//! side) and [`ReadSource`] (read side) so it can be tested with fakes; [`Db`]
//! implements both.
//!
//! # Durability
//!
//! [`WriteDurability::Immediate`]: a caller returns only after the commit that
//! carries its operations returned with `Durability::Immediate` (durable).
//! [`WriteDurability::Buffered`]: a caller returns once its operations are
//! committed and visible (`Durability::Deferred`), NOT durable. Acknowledged
//! writes that may be lost on a crash are bounded by `max_pending_bytes`
//! (key + value bytes) and by roughly `flush_interval` plus two commit
//! durations; with redb what survives a crash is a prefix of the commit order.
//! `flush()` makes everything submitted before it durable. This is the same
//! trade Discord's Read States service makes with its write-behind (commit
//! scheduled 30 s later): pick it only for data whose recent tail may be lost.
//!
//! # Lock order
//!
//! No code path in this module holds two locks at once: the committer's queue
//! lock, a completion's slot lock, a single-flight stripe lock, a flight's
//! result lock and a cache stripe lock are each taken and released on their
//! own, and none is held while calling a sink, a read source or a user
//! callback. (The only nesting is `GroupCommitter::shutdown`, which holds the
//! join-handle mutex while joining; the writer thread never takes it.) The
//! blocking waits are on condition variables whose mutex is released while
//! waiting. Callbacks passed to [`SingleFlight::run`] must not start a read of
//! the same key (reported as an error, never a deadlock), and sinks must not
//! call back into their own [`GroupCommitter`].
//!
//! # Backpressure
//!
//! The committer queue is bounded by operation count and by key + value
//! bytes; `submit` blocks while it is full, `try_submit` hands the operations
//! back. A single request larger than the bounds is admitted only when the
//! queue is empty. Memory in flight is therefore bounded by the queue limits
//! plus one batch (`max_batch_ops` / `max_batch_bytes`).

pub mod chat;
pub mod coalesce;
pub mod group_commit;
pub mod sharded;

use std::any::Any;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::engine::{BatchOp, Db, Expect, Revision, ScanItem, ScanOptions};
use crate::error::{Error, Result};
use crate::store::{Durability, Store};

pub use chat::{
    ChatOptions, ChatStats, ChatStore, DISCORD_EPOCH_MS, Snowflake, SnowflakeParts, channel_prefix,
    message_key, parse_message_key,
};
pub use coalesce::{
    CachedMessage, CoalesceStats, Coalescer, EpochScope, RecentCache, RecentCacheStats, ScanKey,
    SharedRead, SingleFlight, WriteEpochs,
};
pub use group_commit::{
    GroupCommitConfig, GroupCommitStats, GroupCommitter, Pending, Ticket, TrySubmitError,
    WriteDurability,
};
pub use sharded::{
    MAX_SHARDS, ROUTING_HASH, RouteFn, Router, SHARD_META_FILE, ScanRoute, ShardBackend, ShardMeta,
    ShardedDb, ShardedStats, ShardedTicket, merge_sorted_runs, shard_file_name, shard_index,
    stable_hash,
};

/// Outcome of one operation of a batch: `Ok(Some(rev))` for an applied put or
/// for a delete that removed a live record, `Ok(None)` for a delete that found
/// nothing, `Err` when the operation's expectation failed or it was invalid
/// (in the committer and sharded layers also when the commit carrying it
/// failed).
pub type OpResult = Result<Option<Revision>>;

/// An owned write operation: what crosses the committer queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        expect: Expect,
    },
    Delete {
        key: Vec<u8>,
        expect: Expect,
    },
}

impl OwnedOp {
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>, expect: Expect) -> OwnedOp {
        OwnedOp::Put {
            key: key.into(),
            value: value.into(),
            expect,
        }
    }

    pub fn delete(key: impl Into<Vec<u8>>, expect: Expect) -> OwnedOp {
        OwnedOp::Delete {
            key: key.into(),
            expect,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            OwnedOp::Put { key, .. } | OwnedOp::Delete { key, .. } => key,
        }
    }

    pub fn expect(&self) -> Expect {
        match self {
            OwnedOp::Put { expect, .. } | OwnedOp::Delete { expect, .. } => *expect,
        }
    }

    pub fn is_put(&self) -> bool {
        matches!(self, OwnedOp::Put { .. })
    }

    /// Key + value bytes: the unit of the queue and batch byte limits.
    pub fn payload_bytes(&self) -> usize {
        match self {
            OwnedOp::Put { key, value, .. } => key.len().saturating_add(value.len()),
            OwnedOp::Delete { key, .. } => key.len(),
        }
    }

    pub fn as_batch_op(&self) -> BatchOp<'_> {
        match self {
            OwnedOp::Put { key, value, expect } => BatchOp::Put {
                key,
                value,
                expect: *expect,
            },
            OwnedOp::Delete { key, expect } => BatchOp::Delete {
                key,
                expect: *expect,
            },
        }
    }

    pub fn from_batch_op(op: &BatchOp<'_>) -> OwnedOp {
        match *op {
            BatchOp::Put { key, value, expect } => OwnedOp::put(key, value, expect),
            BatchOp::Delete { key, expect } => OwnedOp::delete(key, expect),
        }
    }
}

/// Write side of a database, as seen by a [`GroupCommitter`].
pub trait BatchSink: Send + Sync {
    /// Apply, in order and in ONE commit with `durability`, every operation
    /// whose expectation holds; operations whose expectation fails are
    /// skipped and reported in their own slot. The result has exactly one
    /// entry per operation. An outer `Err` means nothing was applied.
    fn apply(&self, ops: &[OwnedOp], durability: Durability) -> Result<Vec<OpResult>>;

    /// Make every earlier `Durability::Deferred` commit durable.
    fn sync(&self) -> Result<()>;
}

/// Read side of a database, as seen by the coalescing layer.
pub trait ReadSource: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn scan(&self, opts: &ScanOptions) -> Result<Vec<ScanItem>>;
}

impl<T: BatchSink + ?Sized> BatchSink for Arc<T> {
    fn apply(&self, ops: &[OwnedOp], durability: Durability) -> Result<Vec<OpResult>> {
        (**self).apply(ops, durability)
    }

    fn sync(&self) -> Result<()> {
        (**self).sync()
    }
}

impl<T: ReadSource + ?Sized> ReadSource for Arc<T> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        (**self).get(key)
    }

    fn scan(&self, opts: &ScanOptions) -> Result<Vec<ScanItem>> {
        (**self).scan(opts)
    }
}

impl<S: Store> BatchSink for Db<S> {
    fn apply(&self, ops: &[OwnedOp], durability: Durability) -> Result<Vec<OpResult>> {
        let batch: Vec<BatchOp<'_>> = ops.iter().map(OwnedOp::as_batch_op).collect();
        Db::write_batch_each(self, &batch, durability)
    }

    fn sync(&self) -> Result<()> {
        Db::sync(self)
    }
}

impl<S: Store> ReadSource for Db<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Db::get(self, key)
    }

    fn scan(&self, opts: &ScanOptions) -> Result<Vec<ScanItem>> {
        Db::scan(self, opts)
    }
}

// Every public handle of this module is meant to be shared across threads.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Db<crate::store::mem::MemStore>>();
    assert_send_sync::<GroupCommitter>();
    assert_send_sync::<Ticket>();
    assert_send_sync::<ShardedDb<Db<crate::store::mem::MemStore>>>();
    assert_send_sync::<ChatStore<Db<crate::store::mem::MemStore>>>();
    assert_send_sync::<Coalescer<Db<crate::store::mem::MemStore>>>();
    assert_send_sync::<RecentCache>();
};

/// MurmurHash3 `fmix64` finalizer: a bijective 64-bit mixer. Part of the
/// persisted routing function (see [`stable_hash`]); never change it.
pub fn mix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

/// Rebuild an error for another waiter (`Error` is not `Clone`). The variant
/// and its data are preserved; an `Io` error keeps its kind and message.
pub(crate) fn replicate_error(e: &Error) -> Error {
    match e {
        Error::Io(io) => Error::Io(std::io::Error::new(io.kind(), io.to_string())),
        Error::Backend(s) => Error::Backend(s.clone()),
        Error::Format(s) => Error::Format(s.clone()),
        Error::Integrity { object_id, detail } => Error::Integrity {
            object_id: *object_id,
            detail: detail.clone(),
        },
        Error::RevisionConflict {
            key,
            expected,
            actual,
        } => Error::RevisionConflict {
            key: key.clone(),
            expected: expected.clone(),
            actual: *actual,
        },
        Error::UnknownCodec { id, version } => Error::UnknownCodec {
            id: *id,
            version: *version,
        },
        Error::UnknownGenerator { id, version } => Error::UnknownGenerator {
            id: *id,
            version: *version,
        },
        Error::MissingDependency { param_id } => Error::MissingDependency {
            param_id: *param_id,
        },
        Error::LimitExceeded(s) => Error::LimitExceeded(s.clone()),
        Error::IdExhausted(what) => Error::IdExhausted(what),
        Error::InvalidArgument(s) => Error::InvalidArgument(s.clone()),
        Error::Unsupported(s) => Error::Unsupported(s.clone()),
        // Variants added later degrade to their message.
        #[allow(unreachable_patterns)]
        _ => Error::Backend(e.to_string()),
    }
}

/// Human-readable message of a panic payload.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

// Poison-tolerant lock helpers: no code in this module panics while holding a
// lock, and a poisoned lock must never turn into a hang or a second panic.

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn wait<'a, T>(cv: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    cv.wait(guard).unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn wait_timeout<'a, T>(
    cv: &Condvar,
    guard: MutexGuard<'a, T>,
    timeout: Duration,
) -> MutexGuard<'a, T> {
    cv.wait_timeout(guard, timeout)
        .unwrap_or_else(PoisonError::into_inner)
        .0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_op_roundtrips_batch_op() {
        let put = OwnedOp::put(b"k".as_slice(), b"v".as_slice(), Expect::Absent);
        assert_eq!(OwnedOp::from_batch_op(&put.as_batch_op()), put);
        assert_eq!(put.payload_bytes(), 2);
        let del = OwnedOp::delete(b"key".as_slice(), Expect::Revision(7));
        assert_eq!(OwnedOp::from_batch_op(&del.as_batch_op()), del);
        assert_eq!(del.payload_bytes(), 3);
        assert_eq!(del.expect(), Expect::Revision(7));
        assert!(!del.is_put());
    }

    #[test]
    fn replicate_error_keeps_variant() {
        let e = Error::RevisionConflict {
            key: b"k".to_vec(),
            expected: "absent".into(),
            actual: Some(3),
        };
        assert!(matches!(
            replicate_error(&e),
            Error::RevisionConflict {
                actual: Some(3),
                ..
            }
        ));
        let io = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        match replicate_error(&io) {
            Error::Io(inner) => assert_eq!(inner.kind(), std::io::ErrorKind::NotFound),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn mix64_known_values() {
        // fmix64 fixes 0; distinct inputs stay distinct (bijection).
        assert_eq!(mix64(0), 0);
        assert_ne!(mix64(1), mix64(2));
    }
}
