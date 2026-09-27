//! Sharding: N independent databases behind a stable router.
//!
//! Each shard is its own database (for redb: `shard-000.redb` ... in one
//! directory) with its own [`GroupCommitter`], so N commits and N fsyncs run
//! in parallel. The router maps a key to a shard with a hash that is part of
//! the on-disk layout (see [`stable_hash`] and [`ShardMeta`]): changing the
//! shard count or the router would move keys, so both are recorded in
//! `shards.meta` and checked on open.
//!
//! Atomicity is per shard: a multi-shard [`ShardedDb::write_batch`] commits
//! each shard's part in one transaction, but there is no cross-shard
//! transaction; after a crash some shards may have applied their part and
//! others not. Design keys so that what must be atomic shares a route (the
//! chat schema routes by channel, so a channel's messages live in one shard).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::fs;
use std::io::Write;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::group_commit::{GroupCommitConfig, GroupCommitStats, GroupCommitter, Pending, Ticket};
use super::{BatchSink, OpResult, OwnedOp, ReadSource, mix64, replicate_error};
use crate::config::Config;
use crate::engine::{BatchOp, Db, Expect, Revision, ScanItem, ScanOptions, prefix_successor};
use crate::error::{Error, Result};
use crate::stats::{FileSize, Stats};
use crate::store::Store;
use crate::store::redb::RedbStore;

/// Name of the metadata file of a sharded directory.
pub const SHARD_META_FILE: &str = "shards.meta";
/// Version of the `shards.meta` format.
pub const SHARD_META_VERSION: u32 = 1;
/// Identifier of the routing function recorded in `shards.meta`.
pub const ROUTING_HASH: &str = "fnv1a64-fmix64-mulshift";
/// Upper bound on the shard count (one writer thread per shard).
pub const MAX_SHARDS: usize = 256;

const META_MAGIC: &str = "babeldb-shards";

/// What a shard must provide: the write side (for its committer) and reads.
pub trait ShardBackend: BatchSink + ReadSource {
    /// Current (revision, logical length), tombstones excluded.
    fn head(&self, key: &[u8]) -> Result<Option<(Revision, u64)>>;
    fn get_range(&self, key: &[u8], offset: u64, len: u64) -> Result<Option<Vec<u8>>>;
}

impl<S: Store> ShardBackend for Db<S> {
    fn head(&self, key: &[u8]) -> Result<Option<(Revision, u64)>> {
        Db::head(self, key)
    }

    fn get_range(&self, key: &[u8], offset: u64, len: u64) -> Result<Option<Vec<u8>>> {
        Db::get_range(self, key, offset, len)
    }
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Routing hash of a byte string: 64-bit FNV-1a finalized with MurmurHash3's
/// `fmix64`. Deterministic on every platform and run (no random seed); it is
/// part of the on-disk shard layout and must never change.
pub fn stable_hash(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    mix64(h)
}

/// Reduce a routing hash to `0..shards` (Lemire's multiply-shift: uses the
/// high bits, no division). Part of the on-disk layout.
pub fn shard_index(hash: u64, shards: usize) -> usize {
    ((u128::from(hash) * shards as u128) >> 64) as usize
}

/// Caller-defined routing function of [`Router::Custom`].
pub type RouteFn = Arc<dyn Fn(&[u8]) -> u64 + Send + Sync>;

/// How a key is mapped to a shard.
#[derive(Clone)]
pub enum Router {
    /// Hash of the first `n` bytes of the key (the whole key when shorter),
    /// e.g. 8 for keys that start with a big-endian channel id.
    FirstBytes(usize),
    /// Hash of the key up to (excluding) its `count`-th `sep` byte (the whole
    /// key when it has fewer), e.g. `{ sep: b'/', count: 2 }` routes
    /// `ch/<id>/msg/<n>` and `ch/<id>` by `ch/<id>`.
    UntilSeparator { sep: u8, count: usize },
    /// Caller-defined routing value, mixed with `fmix64` and reduced like the
    /// built-in hashes (so raw ids spread well). `name` is recorded in
    /// `shards.meta`: the same name must always denote the same function.
    /// Prefix scans cannot be routed and always fan out.
    Custom { name: String, route: RouteFn },
}

impl fmt::Debug for Router {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.describe())
    }
}

fn nth_separator(bytes: &[u8], sep: u8, count: usize) -> Option<usize> {
    let skip = count.checked_sub(1)?;
    bytes
        .iter()
        .enumerate()
        .filter(|(_, b)| **b == sep)
        .nth(skip)
        .map(|(i, _)| i)
}

impl Router {
    pub fn custom(
        name: impl Into<String>,
        route: impl Fn(&[u8]) -> u64 + Send + Sync + 'static,
    ) -> Router {
        Router::Custom {
            name: name.into(),
            route: Arc::new(route),
        }
    }

    /// Stable description recorded in `shards.meta`.
    pub fn describe(&self) -> String {
        match self {
            Router::FirstBytes(n) => format!("first-bytes:{n}"),
            Router::UntilSeparator { sep, count } => format!("until-separator:0x{sep:02x}:{count}"),
            Router::Custom { name, .. } => format!("custom:{name}"),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Router::FirstBytes(0) => Err(Error::InvalidArgument(
                "router: FirstBytes needs n >= 1".into(),
            )),
            Router::UntilSeparator { count: 0, .. } => Err(Error::InvalidArgument(
                "router: UntilSeparator needs count >= 1".into(),
            )),
            Router::Custom { name, .. }
                if name.is_empty()
                    || name.len() > 128
                    || !name.chars().all(|c| c.is_ascii_graphic()) =>
            {
                Err(Error::InvalidArgument(
                    "router: custom name must be 1..=128 printable ASCII characters".into(),
                ))
            }
            _ => Ok(()),
        }
    }

    /// Bytes of `key` that decide its shard (`None` for custom routers).
    pub fn routing_bytes<'k>(&self, key: &'k [u8]) -> Option<&'k [u8]> {
        match self {
            Router::FirstBytes(n) => Some(&key[..key.len().min(*n)]),
            Router::UntilSeparator { sep, count } => Some(match nth_separator(key, *sep, *count) {
                Some(i) => &key[..i],
                None => key,
            }),
            Router::Custom { .. } => None,
        }
    }

    /// Routing hash of a key.
    pub fn key_hash(&self, key: &[u8]) -> u64 {
        match self {
            Router::Custom { route, .. } => mix64(route(key)),
            _ => stable_hash(self.routing_bytes(key).unwrap_or(key)),
        }
    }

    /// Length of the shortest prefix of `bytes` that fixes the route of every
    /// key starting with it, with the routing bytes of that route.
    fn fixing_prefix<'k>(&self, bytes: &'k [u8]) -> Option<(usize, &'k [u8])> {
        match self {
            Router::FirstBytes(n) => (bytes.len() >= *n).then(|| (*n, &bytes[..*n])),
            Router::UntilSeparator { sep, count } => {
                nth_separator(bytes, *sep, *count).map(|i| (i + 1, &bytes[..i]))
            }
            Router::Custom { .. } => None,
        }
    }

    /// Routing hash shared by every key that starts with `prefix`, when the
    /// prefix determines it.
    pub fn prefix_hash(&self, prefix: &[u8]) -> Option<u64> {
        self.fixing_prefix(prefix)
            .map(|(_, routing)| stable_hash(routing))
    }

    /// Routing hash shared by every key inside the range, when the bounds
    /// determine it: the range must start inside a route-fixing prefix `p` of
    /// its start key and end at or before the first key after `p`.
    pub fn range_hash(&self, start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> Option<u64> {
        let s = match start {
            Bound::Included(s) | Bound::Excluded(s) => s.as_slice(),
            Bound::Unbounded => return None,
        };
        let (len, routing) = self.fixing_prefix(s)?;
        let within = match prefix_successor(&s[..len]) {
            // Every key >= an all-0xFF prefix starts with it.
            None => true,
            Some(next) => match end {
                Bound::Unbounded => false,
                Bound::Included(e) => e.as_slice() < next.as_slice(),
                Bound::Excluded(e) => e.as_slice() <= next.as_slice(),
            },
        };
        within.then(|| stable_hash(routing))
    }

    /// Shard of `key` among `shards`.
    pub fn shard_of(&self, key: &[u8], shards: usize) -> usize {
        shard_index(self.key_hash(key), shards)
    }
}

// ---------------------------------------------------------------------------
// shards.meta
// ---------------------------------------------------------------------------

/// File name of shard `index`.
pub fn shard_file_name(index: usize) -> String {
    format!("shard-{index:03}.redb")
}

/// Contents of `shards.meta` (text, `key=value` lines after a header line).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardMeta {
    pub version: u32,
    pub shards: usize,
    /// [`Router::describe`] of the router the keys were placed with.
    pub router: String,
    /// [`ROUTING_HASH`] of the build that created the directory.
    pub hash: String,
    /// False while the directory is being created (`state=creating`): shard
    /// files may still be missing and are created on the next open.
    pub complete: bool,
}

impl ShardMeta {
    pub fn new(shards: usize, router: &Router) -> ShardMeta {
        ShardMeta {
            version: SHARD_META_VERSION,
            shards,
            router: router.describe(),
            hash: ROUTING_HASH.to_string(),
            complete: true,
        }
    }

    pub fn path(dir: &Path) -> PathBuf {
        dir.join(SHARD_META_FILE)
    }

    pub fn encode(&self) -> String {
        format!(
            "{META_MAGIC}\n# Do not edit: the shard count and router decide where every key lives.\nversion={}\nshards={}\nrouter={}\nhash={}\nstate={}\n",
            self.version,
            self.shards,
            self.router,
            self.hash,
            if self.complete { "ready" } else { "creating" }
        )
    }

    pub fn decode(text: &str) -> Result<ShardMeta> {
        let bad = |msg: String| Error::Format(format!("{SHARD_META_FILE}: {msg}"));
        let mut lines = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'));
        if lines.next() != Some(META_MAGIC) {
            return Err(bad("missing header".into()));
        }
        let mut fields = BTreeMap::new();
        for line in lines {
            let (k, v) = line
                .split_once('=')
                .ok_or_else(|| bad(format!("malformed line {line:?}")))?;
            if fields.insert(k.to_string(), v.to_string()).is_some() {
                return Err(bad(format!("duplicate key {k:?}")));
            }
        }
        let version: u32 = fields
            .get("version")
            .ok_or_else(|| bad("missing version".into()))?
            .parse()
            .map_err(|_| bad("bad version".into()))?;
        if version != SHARD_META_VERSION {
            return Err(Error::Unsupported(format!(
                "{SHARD_META_FILE} version {version} (this build reads {SHARD_META_VERSION})"
            )));
        }
        for k in fields.keys() {
            if !["version", "shards", "router", "hash", "state"].contains(&k.as_str()) {
                return Err(bad(format!("unknown key {k:?}")));
            }
        }
        let field = |k: &str| {
            fields
                .get(k)
                .cloned()
                .ok_or_else(|| bad(format!("missing {k}")))
        };
        let shards: usize = field("shards")?
            .parse()
            .map_err(|_| bad("bad shard count".into()))?;
        if !(1..=MAX_SHARDS).contains(&shards) {
            return Err(bad(format!(
                "shard count {shards} outside 1..={MAX_SHARDS}"
            )));
        }
        let complete = match field("state")?.as_str() {
            "ready" => true,
            "creating" => false,
            other => return Err(bad(format!("unknown state {other:?}"))),
        };
        Ok(ShardMeta {
            version,
            shards,
            router: field("router")?,
            hash: field("hash")?,
            complete,
        })
    }

    pub fn read(dir: &Path) -> Result<Option<ShardMeta>> {
        match fs::read_to_string(Self::path(dir)) {
            Ok(text) => ShardMeta::decode(&text).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Replace `shards.meta` atomically (temp file + fsync + rename).
    pub fn write(&self, dir: &Path) -> Result<()> {
        let tmp = dir.join(format!("{SHARD_META_FILE}.tmp"));
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(self.encode().as_bytes())?;
            f.sync_all()?;
        }
        fs::rename(&tmp, Self::path(dir))?;
        sync_dir(dir);
        Ok(())
    }

    /// Error unless the directory was created with `shards` shards and the
    /// same router and routing hash.
    pub fn check(&self, shards: usize, router: &Router) -> Result<()> {
        if self.hash != ROUTING_HASH {
            return Err(Error::Unsupported(format!(
                "{SHARD_META_FILE}: routing hash {:?} (this build routes with {ROUTING_HASH:?})",
                self.hash
            )));
        }
        if self.shards != shards {
            return Err(Error::InvalidArgument(format!(
                "sharded database has {} shards but was opened with {shards}: changing the count moves keys between shards",
                self.shards
            )));
        }
        let desc = router.describe();
        if self.router != desc {
            return Err(Error::InvalidArgument(format!(
                "sharded database was created with router {:?} but was opened with {desc:?}",
                self.router
            )));
        }
        Ok(())
    }
}

#[cfg(unix)]
fn sync_dir(dir: &Path) {
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) {}

/// Indices of `shard-NNN.redb` files present in `dir`.
fn shard_files_in(dir: &Path) -> Result<Vec<usize>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(i) = name
            .strip_prefix("shard-")
            .and_then(|r| r.strip_suffix(".redb"))
            && let Ok(i) = i.parse::<usize>()
        {
            found.push(i);
        }
    }
    found.sort_unstable();
    Ok(found)
}

fn check_shard_count(shards: usize) -> Result<()> {
    if (1..=MAX_SHARDS).contains(&shards) {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "shard count {shards} outside 1..={MAX_SHARDS}"
        )))
    }
}

// ---------------------------------------------------------------------------
// Sharded database
// ---------------------------------------------------------------------------

struct ShardSlot<B> {
    backend: Arc<B>,
    committer: GroupCommitter,
}

/// Where a scan runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanRoute {
    /// Every key of the range lives in this shard.
    Single(usize),
    /// Every shard is scanned and the results are merged.
    FanOut,
}

/// N shards, each with its own group committer. Writes go through the
/// committers; reads go straight to the shard.
pub struct ShardedDb<B = Db<RedbStore>> {
    shards: Vec<ShardSlot<B>>,
    router: Router,
    dir: Option<PathBuf>,
}

impl<B> fmt::Debug for ShardedDb<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedDb")
            .field("shards", &self.shards.len())
            .field("router", &self.router)
            .field("dir", &self.dir)
            .finish()
    }
}

impl ShardedDb<Db<RedbStore>> {
    /// Open or create `dir` with `shards` redb files (`shard-000.redb`, ...).
    /// `cfg` is used as is for every shard, so caches are per shard (total =
    /// `shards` x `cfg.cache_bytes` + `shards` x `cfg.backend_cache_bytes`).
    /// The shard count and router must match `shards.meta` of an existing
    /// directory.
    pub fn open(
        dir: impl AsRef<Path>,
        shards: usize,
        cfg: Config,
        router: Router,
        commit: impl Into<GroupCommitConfig>,
    ) -> Result<Self> {
        let dir = dir.as_ref();
        check_shard_count(shards)?;
        router.validate()?;
        let commit = commit.into();
        commit.validate()?;
        fs::create_dir_all(dir)?;
        let complete = match ShardMeta::read(dir)? {
            Some(meta) => {
                meta.check(shards, &router)?;
                meta.complete
            }
            None => {
                if let Some(i) = shard_files_in(dir)?.first() {
                    return Err(Error::Format(format!(
                        "{} exists in {} but {SHARD_META_FILE} is missing: refusing to guess the shard layout",
                        shard_file_name(*i),
                        dir.display()
                    )));
                }
                ShardMeta {
                    complete: false,
                    ..ShardMeta::new(shards, &router)
                }
                .write(dir)?;
                false
            }
        };
        if let Some(extra) = shard_files_in(dir)?.into_iter().find(|&i| i >= shards) {
            return Err(Error::Format(format!(
                "{} exists but the database has {shards} shards",
                shard_file_name(extra)
            )));
        }
        let mut dbs = Vec::with_capacity(shards);
        for i in 0..shards {
            let path = dir.join(shard_file_name(i));
            if complete && !path.exists() {
                return Err(Error::Format(format!(
                    "shard file {} is missing",
                    path.display()
                )));
            }
            dbs.push(Db::open(&path, cfg.clone())?);
        }
        if !complete {
            ShardMeta::new(shards, &router).write(dir)?;
        }
        let mut db = ShardedDb::new(dbs, router, commit)?;
        db.dir = Some(dir.to_path_buf());
        Ok(db)
    }
}

impl<S: Store> ShardedDb<Db<S>> {
    /// One engine per store (in memory, tests, other backends); no metadata
    /// file is involved.
    pub fn with_stores(
        stores: Vec<S>,
        cfg: Config,
        router: Router,
        commit: impl Into<GroupCommitConfig>,
    ) -> Result<Self> {
        let dbs = stores
            .into_iter()
            .map(|s| Db::with_store(s, cfg.clone()))
            .collect::<Result<Vec<_>>>()?;
        ShardedDb::new(dbs, router, commit)
    }

    /// Per-shard engine statistics plus totals. The totals include
    /// `shards.meta`: every persisted byte is accounted for.
    pub fn stats(&self) -> Result<ShardedStats> {
        let mut shards = Vec::with_capacity(self.shards.len());
        for s in &self.shards {
            shards.push(s.backend.stats()?);
        }
        let meta_file = match &self.dir {
            Some(dir) => Some(crate::sys::file_size(&ShardMeta::path(dir))?),
            None => None,
        };
        let mut t = ShardedStats {
            commit: self.commit_stats(),
            meta_file,
            records: 0,
            tombstones: 0,
            logical_bytes: 0,
            payload_bytes: 0,
            file_apparent_bytes: 0,
            file_allocated_bytes: Some(0),
            shards: Vec::new(),
        };
        for s in &shards {
            t.records += s.records;
            t.tombstones += s.tombstones;
            t.logical_bytes += s.logical_bytes;
            t.payload_bytes += s.payload_bytes();
            t.file_apparent_bytes += s.file_apparent_bytes();
            t.file_allocated_bytes = t
                .file_allocated_bytes
                .zip(s.file_allocated_bytes())
                .map(|(a, b)| a + b);
        }
        if let Some(m) = &t.meta_file {
            t.file_apparent_bytes += m.apparent_bytes;
            t.file_allocated_bytes = t
                .file_allocated_bytes
                .zip(m.allocated_bytes)
                .map(|(a, b)| a + b);
        }
        t.shards = shards;
        Ok(t)
    }
}

/// Statistics of a sharded database.
#[derive(Clone, Debug)]
pub struct ShardedStats {
    pub shards: Vec<Stats>,
    pub commit: Vec<GroupCommitStats>,
    pub meta_file: Option<FileSize>,
    pub records: u64,
    pub tombstones: u64,
    pub logical_bytes: u64,
    pub payload_bytes: u64,
    pub file_apparent_bytes: u64,
    /// `None` when any file's allocation is unknown.
    pub file_allocated_bytes: Option<u64>,
}

impl<B: ShardBackend + 'static> ShardedDb<B> {
    /// Wrap existing backends (shard `i` = `backends[i]`).
    pub fn new(
        backends: Vec<B>,
        router: Router,
        commit: impl Into<GroupCommitConfig>,
    ) -> Result<Self> {
        Self::from_shared(backends.into_iter().map(Arc::new).collect(), router, commit)
    }

    /// Like [`ShardedDb::new`] for backends the caller keeps handles on.
    pub fn from_shared(
        backends: Vec<Arc<B>>,
        router: Router,
        commit: impl Into<GroupCommitConfig>,
    ) -> Result<Self> {
        let commit = commit.into();
        commit.validate()?;
        router.validate()?;
        check_shard_count(backends.len())?;
        let mut shards = Vec::with_capacity(backends.len());
        for (i, backend) in backends.into_iter().enumerate() {
            let cfg = GroupCommitConfig {
                thread_name: format!("{}-{i:03}", commit.thread_name),
                ..commit.clone()
            };
            let committer = GroupCommitter::new(backend.clone(), cfg)?;
            shards.push(ShardSlot { backend, committer });
        }
        Ok(ShardedDb {
            shards,
            router,
            dir: None,
        })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn router(&self) -> &Router {
        &self.router
    }

    /// Directory of a redb-backed sharded database.
    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    pub fn shard_for_key(&self, key: &[u8]) -> usize {
        if self.shards.len() == 1 {
            0
        } else {
            self.router.shard_of(key, self.shards.len())
        }
    }

    /// Direct access to one shard's backend (reads bypass nothing; writes
    /// made here bypass its committer and are not ordered with it).
    pub fn shard(&self, index: usize) -> Option<&B> {
        self.shards.get(index).map(|s| s.backend.as_ref())
    }

    pub fn committer(&self, index: usize) -> Option<&GroupCommitter> {
        self.shards.get(index).map(|s| &s.committer)
    }

    fn slot(&self, key: &[u8]) -> &ShardSlot<B> {
        &self.shards[self.shard_for_key(key)]
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.slot(key).backend.get(key)
    }

    pub fn get_range(&self, key: &[u8], offset: u64, len: u64) -> Result<Option<Vec<u8>>> {
        self.slot(key).backend.get_range(key, offset, len)
    }

    pub fn head(&self, key: &[u8]) -> Result<Option<(Revision, u64)>> {
        self.slot(key).backend.head(key)
    }

    /// Group-committed put on the key's shard; returns the new revision.
    pub fn put(&self, key: &[u8], value: &[u8], expect: Expect) -> Result<Revision> {
        self.slot(key).committer.put(key, value, expect)
    }

    /// Group-committed delete; returns whether a live record was deleted.
    pub fn delete(&self, key: &[u8], expect: Expect) -> Result<bool> {
        self.slot(key).committer.delete(key, expect)
    }

    /// One operation with its raw result (`Some(rev)` / `None` / `Err`).
    pub fn apply_one(&self, op: OwnedOp) -> OpResult {
        self.slot(op.key()).committer.apply_one(op)
    }

    /// Split `ops` by shard and enqueue each part on its shard's committer
    /// (blocking only on backpressure). Each shard's part is one request:
    /// committed in one transaction, in order.
    pub fn submit(&self, ops: Vec<OwnedOp>) -> ShardedTicket {
        let total = ops.len();
        let mut groups: Vec<(Vec<OwnedOp>, Vec<usize>)> =
            (0..self.shards.len()).map(|_| Default::default()).collect();
        for (pos, op) in ops.into_iter().enumerate() {
            let group = &mut groups[self.shard_for_key(op.key())];
            group.0.push(op);
            group.1.push(pos);
        }
        let mut parts = Vec::new();
        for (shard, (ops, positions)) in groups.into_iter().enumerate() {
            if !ops.is_empty() {
                parts.push(Part {
                    positions,
                    ticket: self.shards[shard].committer.submit(ops),
                });
            }
        }
        ShardedTicket { parts, total }
    }

    /// Write owned operations across shards and wait. Results are per
    /// operation, in input order: a failing expectation fails only its
    /// operation, a failed commit fails the operations of that shard only.
    /// Atomic per shard, never across shards.
    pub fn write_owned(&self, ops: Vec<OwnedOp>) -> Vec<OpResult> {
        self.submit(ops).wait()
    }

    /// [`ShardedDb::write_owned`] for borrowed operations (copied once).
    pub fn write_batch(&self, ops: &[BatchOp<'_>]) -> Vec<OpResult> {
        self.write_owned(ops.iter().map(OwnedOp::from_batch_op).collect())
    }

    /// Where `opts` would be executed.
    pub fn scan_route(&self, opts: &ScanOptions) -> ScanRoute {
        if self.shards.len() == 1 {
            return ScanRoute::Single(0);
        }
        match self.router.range_hash(&opts.start, &opts.end) {
            Some(h) => ScanRoute::Single(shard_index(h, self.shards.len())),
            None => ScanRoute::FanOut,
        }
    }

    /// Ordered scan. Runs on one shard when the range determines it (e.g. a
    /// prefix at least as long as the routing bytes); otherwise every shard
    /// returns up to `limit` items and the runs are merged by key (respecting
    /// `reverse`) and cut to `limit`. Fan-out shards are scanned one after
    /// another and each shard's scan is its own snapshot.
    pub fn scan(&self, opts: &ScanOptions) -> Result<Vec<ScanItem>> {
        match self.scan_route(opts) {
            ScanRoute::Single(i) => self.shards[i].backend.scan(opts),
            ScanRoute::FanOut => {
                let mut runs = Vec::with_capacity(self.shards.len());
                for s in &self.shards {
                    runs.push(s.backend.scan(opts)?);
                }
                Ok(merge_sorted_runs(runs, opts.reverse, opts.limit))
            }
        }
    }

    /// Keys starting with `prefix` (routed to one shard when possible).
    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        reverse: bool,
        limit: usize,
        with_values: bool,
    ) -> Result<Vec<ScanItem>> {
        self.scan(
            &ScanOptions::prefix(prefix)
                .reverse(reverse)
                .limit(limit)
                .with_values(with_values),
        )
    }

    /// Wait until everything submitted before this call is durable on every
    /// shard (the shards' fsyncs overlap). Returns the first error.
    pub fn flush(&self) -> Result<()> {
        let pending: Vec<Result<Pending<()>>> = self
            .shards
            .iter()
            .map(|s| s.committer.flush_async())
            .collect();
        first_error(pending.into_iter().map(|p| p.and_then(Pending::wait)))
    }

    /// Drain and stop every committer (in parallel). Idempotent; returns the
    /// first error. Reads keep working afterwards; writes fail.
    pub fn shutdown(&self) -> Result<()> {
        for s in &self.shards {
            s.committer.close();
        }
        first_error(self.shards.iter().map(|s| s.committer.shutdown()))
    }

    pub fn commit_stats(&self) -> Vec<GroupCommitStats> {
        self.shards.iter().map(|s| s.committer.stats()).collect()
    }
}

fn first_error(results: impl Iterator<Item = Result<()>>) -> Result<()> {
    let mut first = None;
    for r in results {
        if let Err(e) = r {
            first.get_or_insert(e);
        }
    }
    first.map_or(Ok(()), Err)
}

impl<B: ShardBackend + 'static> ReadSource for ShardedDb<B> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        ShardedDb::get(self, key)
    }

    fn scan(&self, opts: &ScanOptions) -> Result<Vec<ScanItem>> {
        ShardedDb::scan(self, opts)
    }
}

struct Part {
    positions: Vec<usize>,
    ticket: Result<Ticket>,
}

/// Pending results of a multi-shard write.
pub struct ShardedTicket {
    parts: Vec<Part>,
    total: usize,
}

impl ShardedTicket {
    /// One result per operation, in input order.
    pub fn wait(self) -> Vec<OpResult> {
        let mut out: Vec<Option<OpResult>> =
            std::iter::repeat_with(|| None).take(self.total).collect();
        for part in self.parts {
            match part.ticket.and_then(Pending::wait) {
                Ok(results) if results.len() == part.positions.len() => {
                    for (pos, r) in part.positions.into_iter().zip(results) {
                        out[pos] = Some(r);
                    }
                }
                Ok(results) => {
                    let e = Error::Backend(format!(
                        "sharded write: {} results for {} operations",
                        results.len(),
                        part.positions.len()
                    ));
                    for pos in part.positions {
                        out[pos] = Some(Err(replicate_error(&e)));
                    }
                }
                Err(e) => {
                    for pos in part.positions {
                        out[pos] = Some(Err(replicate_error(&e)));
                    }
                }
            }
        }
        out.into_iter()
            .map(|r| {
                r.unwrap_or_else(|| Err(Error::Backend("sharded write: missing result".into())))
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Fan-out merge
// ---------------------------------------------------------------------------

struct HeapEntry {
    item: ScanItem,
    run: usize,
    descending: bool,
}

impl Ord for HeapEntry {
    /// `BinaryHeap` pops the greatest entry: the next key in output order,
    /// ties broken by the lower run (shard) index.
    fn cmp(&self, other: &Self) -> Ordering {
        let by_key = self.item.key.cmp(&other.item.key);
        let by_key = if self.descending {
            by_key
        } else {
            by_key.reverse()
        };
        by_key.then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

/// K-way merge of runs that are each sorted by key (descending when
/// `reverse`), cut to `limit` items (0 = unlimited).
pub fn merge_sorted_runs(runs: Vec<Vec<ScanItem>>, reverse: bool, limit: usize) -> Vec<ScanItem> {
    let total: usize = runs.iter().map(Vec::len).sum();
    let want = if limit == 0 { total } else { limit.min(total) };
    let mut out = Vec::with_capacity(want);
    let mut iters: Vec<std::vec::IntoIter<ScanItem>> =
        runs.into_iter().map(Vec::into_iter).collect();
    let mut heap = BinaryHeap::with_capacity(iters.len());
    for (run, it) in iters.iter_mut().enumerate() {
        if let Some(item) = it.next() {
            heap.push(HeapEntry {
                item,
                run,
                descending: reverse,
            });
        }
    }
    while out.len() < want {
        let Some(HeapEntry { item, run, .. }) = heap.pop() else {
            break;
        };
        if let Some(next) = iters[run].next() {
            heap.push(HeapEntry {
                item: next,
                run,
                descending: reverse,
            });
        }
        out.push(item);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(key: &[u8]) -> ScanItem {
        ScanItem {
            key: key.to_vec(),
            revision: 1,
            logical_len: 0,
            value: None,
        }
    }

    #[test]
    fn stable_hash_golden_values() {
        // Computed with an independent Python implementation of
        // FNV-1a 64 + fmix64 + multiply-shift.
        assert_eq!(stable_hash(b""), 0xefd0_1f60_ba99_2926);
        assert_eq!(stable_hash(b"a"), 0x82a2_a958_a9be_ce5b);
        assert_eq!(stable_hash(b"ch/42"), 0x1c3e_1496_41e4_cf03);
        assert_eq!(stable_hash(&0u64.to_be_bytes()), 0x7bd3_144f_29c0_cc9e);
        assert_eq!(stable_hash(&1u64.to_be_bytes()), 0x0d4a_d0eb_39c5_0357);
        assert_eq!(shard_index(stable_hash(b""), 8), 7);
        assert_eq!(shard_index(stable_hash(b"a"), 16), 8);
        assert_eq!(
            shard_index(stable_hash(&175_928_847_299_117_063u64.to_be_bytes()), 8),
            4
        );
        assert_eq!(mix64(1), 0xb456_bcfc_34c2_cb2c);
    }

    #[test]
    fn range_hash_routes_prefixes_and_chat_pages() {
        let r = Router::FirstBytes(8);
        let ch = 42u64.to_be_bytes();
        let opts = ScanOptions::prefix(&ch);
        assert_eq!(r.range_hash(&opts.start, &opts.end), Some(stable_hash(&ch)));
        // "before" page: [ch || 0, ch || id)
        let mut start = ch.to_vec();
        start.extend_from_slice(&0u64.to_be_bytes());
        let mut end = ch.to_vec();
        end.extend_from_slice(&99u64.to_be_bytes());
        assert_eq!(
            r.range_hash(&Bound::Included(start.clone()), &Bound::Excluded(end)),
            Some(stable_hash(&ch))
        );
        // A range that crosses into the next channel cannot be routed.
        let next = 43u64.to_be_bytes().to_vec();
        assert_eq!(
            r.range_hash(
                &Bound::Included(start.clone()),
                &Bound::Included(next.clone())
            ),
            None
        );
        assert!(
            r.range_hash(&Bound::Included(start), &Bound::Excluded(next))
                .is_some()
        );
        // Short prefixes and unbounded ranges fan out.
        let short = ScanOptions::prefix(&ch[..4]);
        assert_eq!(r.range_hash(&short.start, &short.end), None);
        assert_eq!(r.range_hash(&Bound::Unbounded, &Bound::Unbounded), None);
        // All-0xFF prefix: no successor, still routable.
        let ff = ScanOptions::prefix(&[0xFF; 8]);
        assert_eq!(
            r.range_hash(&ff.start, &ff.end),
            Some(stable_hash(&[0xFF; 8]))
        );
    }

    #[test]
    fn until_separator_routing() {
        let r = Router::UntilSeparator {
            sep: b'/',
            count: 2,
        };
        assert_eq!(r.routing_bytes(b"ch/42/msg/7"), Some(b"ch/42".as_slice()));
        assert_eq!(r.routing_bytes(b"ch/42"), Some(b"ch/42".as_slice()));
        assert_eq!(r.key_hash(b"ch/42/msg/7"), r.key_hash(b"ch/42"));
        assert_eq!(r.prefix_hash(b"ch/42/"), Some(r.key_hash(b"ch/42/msg/1")));
        assert_eq!(r.prefix_hash(b"ch/42/msg/"), Some(r.key_hash(b"ch/42")));
        assert_eq!(r.prefix_hash(b"ch/4"), None);
        assert_eq!(r.describe(), "until-separator:0x2f:2");
        assert!(
            Router::UntilSeparator {
                sep: b'/',
                count: 0
            }
            .validate()
            .is_err()
        );
        assert!(Router::FirstBytes(0).validate().is_err());
    }

    #[test]
    fn custom_router_mixes_and_never_routes_prefixes() {
        let r = Router::custom("by-len", |k: &[u8]| k.len() as u64);
        assert_eq!(r.key_hash(b"abc"), mix64(3));
        assert_eq!(r.prefix_hash(b"abcdefgh"), None);
        assert_eq!(r.describe(), "custom:by-len");
        assert!(
            Router::custom("has space", |_: &[u8]| 0)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn meta_roundtrip_and_checks() {
        let r = Router::FirstBytes(8);
        let meta = ShardMeta::new(4, &r);
        let decoded = ShardMeta::decode(&meta.encode()).unwrap();
        assert_eq!(decoded, meta);
        assert!(decoded.check(4, &r).is_ok());
        assert!(matches!(
            decoded.check(8, &r),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            decoded.check(4, &Router::FirstBytes(4)),
            Err(Error::InvalidArgument(_))
        ));
        let creating = ShardMeta {
            complete: false,
            ..meta.clone()
        };
        assert!(!ShardMeta::decode(&creating.encode()).unwrap().complete);
        let future = meta.encode().replace("version=1", "version=2");
        assert!(matches!(
            ShardMeta::decode(&future),
            Err(Error::Unsupported(_))
        ));
        assert!(ShardMeta::decode("nope\nversion=1\n").is_err());
        let unknown = format!("{}extra=1\n", meta.encode());
        assert!(ShardMeta::decode(&unknown).is_err());
        let other_hash = meta.encode().replace(ROUTING_HASH, "sip");
        assert!(matches!(
            ShardMeta::decode(&other_hash).unwrap().check(4, &r),
            Err(Error::Unsupported(_))
        ));
    }

    #[test]
    fn merge_respects_order_and_limit() {
        let runs = vec![
            vec![item(b"a"), item(b"d"), item(b"g")],
            vec![item(b"b"), item(b"e")],
            vec![],
            vec![item(b"c"), item(b"f"), item(b"h")],
        ];
        let keys = |v: Vec<ScanItem>| v.into_iter().map(|i| i.key).collect::<Vec<_>>();
        let all = keys(merge_sorted_runs(runs.clone(), false, 0));
        assert_eq!(
            all,
            [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"].map(|k| k.to_vec())
        );
        assert_eq!(
            keys(merge_sorted_runs(runs, false, 3)),
            [b"a", b"b", b"c"].map(|k| k.to_vec())
        );
        let desc = vec![
            vec![item(b"g"), item(b"d"), item(b"a")],
            vec![item(b"h"), item(b"b")],
        ];
        assert_eq!(
            keys(merge_sorted_runs(desc, true, 4)),
            [b"h", b"g", b"d", b"b"].map(|k| k.to_vec())
        );
    }
}
