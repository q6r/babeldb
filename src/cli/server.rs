//! Minimal synchronous TCP server of the wire protocol (stage 10) and a
//! blocking [`Client`].
//!
//! # Threads
//!
//! A `std::net::TcpListener` hands accepted connections to a fixed pool of
//! worker threads; each worker serves one connection at a time and any
//! number of requests on it, with `TCP_NODELAY`. All workers share one
//! `Arc<Db<S>>` (the engine is `Send + Sync`; `gc` and `compact` need
//! `&mut Db` and are not exposed remotely) and ONE [`GroupCommitter`]: every
//! write (PUT, DELETE, PUT_BATCH) of every connection is queued to its writer
//! thread, which turns everything that queued up while the previous commit
//! was running into one commit. N concurrent writers pay one commit (one
//! fsync) instead of N. Reads run on the workers, in parallel with commits.
//! The protocol is documented in [`crate::cli::protocol`].
//!
//! Every write of every connection waits for that one writer thread, and it
//! is busy all the time once a few connections write concurrently (measured
//! on Windows 11, localhost, durable WAL puts). Each commit wakes up the
//! workers and clients of its batch, which the scheduler boosts on wake-up:
//! at normal priority they preempt the writer and stretch the next commit
//! (16 clients: typically 20-30% fewer durable puts per second, more on a
//! busy machine), so the writer runs at a raised priority
//! ([`ServerConfig::raise_commit_priority`]). For the same reason the writer
//! thread only runs transactions: each worker validates and encodes its own
//! request before queueing it ([`BatchSink::prepare`]; large requests, such
//! as bulk loads, are encoded by the writer, overlapped with their
//! transaction), and the writer wakes two waiters of a finished commit, which
//! wake the others (see [`crate::scale::group_commit`]).
//!
//! # Acknowledgements
//!
//! A write is answered only after the commit carrying it returned. With
//! [`WriteDurability::Immediate`] (the default, [`ServerConfig::commit`])
//! that commit is durable; with [`WriteDurability::Buffered`] it is committed
//! and visible, and becomes durable within the configured bound. A PUT_BATCH
//! is one request of the committer, never split across commits: its items
//! are validated first (one invalid item rejects the whole batch, nothing is
//! applied), then committed together, possibly in a commit shared with other
//! connections.
//!
//! # Pipelining
//!
//! Requests of one connection run in order. Consecutive writes are submitted
//! without waiting for each other, so a client that pipelines N writes shares
//! commits even on a single connection; a read (or any reply produced by the
//! worker itself) first waits for the connection's earlier writes, so a
//! connection always reads its own writes. Replies go out in request order,
//! coalesced into one `send` while more complete requests are already
//! buffered. [`Client::pipeline`] sends many requests before reading their
//! replies.
//!
//! # Stopping
//!
//! [`ServerHandle::stop`] closes every connection, joins every thread and
//! then drains the committer: every write accepted before the stop is
//! committed (in buffered mode, made durable) before `stop` returns, and the
//! server no longer holds the database afterwards.
//!
//! The server exists to measure the cost of a remote interface separately
//! from the engine: [`Client`] issues the operations a benchmark can also call
//! directly on `Db`, and [`handle_request`] isolates decode + dispatch +
//! engine from the TCP transport. No authentication: bind to loopback.

use std::collections::{HashMap, VecDeque};
use std::io::{self, BufReader, Read, Write};
use std::net::{Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::panic_message;
use super::protocol::{self, ProtocolError, Reply, Request};
use crate::engine::{BatchOp, Db, Expect, Revision, ScanItem, ScanOptions};
use crate::scale::group_commit::single_result;
use crate::scale::{
    BatchSink, GroupCommitConfig, GroupCommitStats, GroupCommitter, OpResult, OwnedOp, PreparedOp,
    SinkOps, Ticket, WriteDurability,
};
use crate::store::{Durability, Store};

/// Capacity of the per-connection read buffer.
const READ_BUFFER: usize = 64 * 1024;
/// How often a connection waiting for data checks whether the server stops.
const STOP_POLL: Duration = Duration::from_millis(50);
/// A client that does not accept its response for this long is disconnected.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-connection buffers larger than this are released after use.
const KEEP_BUFFER: usize = 1 << 20;
/// Pending replies are sent once they reach this size, even while more
/// complete requests are buffered.
const FLUSH_BYTES: usize = 256 * 1024;
/// Default bound of [`ServerConfig::max_inflight_writes`].
pub const DEFAULT_MAX_INFLIGHT_WRITES: usize = 1024;
/// [`Client::pipeline`] sends at most this many request bytes (or one larger
/// request) before reading their replies: they always fit the socket buffers,
/// so the client never blocks sending while the server blocks sending
/// replies nobody reads yet.
pub const PIPELINE_WINDOW: usize = 32 * 1024;

/// Default size of the worker pool: the available parallelism, at most 64.
pub fn default_threads() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, 64)
}

/// Configuration of a server started with [`serve_with`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    /// Worker threads (at least 1); each serves one connection at a time, so
    /// this is also the number of connections served at once (the others wait).
    pub threads: usize,
    /// The group committer shared by every connection. `commit.durability`
    /// decides when a write is acknowledged (default
    /// [`WriteDurability::Immediate`]: after a durable commit).
    pub commit: GroupCommitConfig,
    /// Most write requests of one connection submitted and not yet answered
    /// (pipelined writes); at the bound the worker waits for them before
    /// submitting more.
    pub max_inflight_writes: usize,
    /// Run the committer's writer thread at a raised priority (Windows:
    /// `THREAD_PRIORITY_HIGHEST`; no effect elsewhere). Default `true`.
    pub raise_commit_priority: bool,
}

impl ServerConfig {
    /// `threads` workers, durable acknowledgements, default group commit.
    pub fn new(threads: usize) -> ServerConfig {
        ServerConfig {
            threads,
            commit: GroupCommitConfig {
                thread_name: "babeldb-server-commit".to_string(),
                ..GroupCommitConfig::default()
            },
            max_inflight_writes: DEFAULT_MAX_INFLIGHT_WRITES,
            raise_commit_priority: true,
        }
    }

    /// Same configuration with another acknowledgement durability.
    pub fn with_durability(mut self, durability: WriteDurability) -> ServerConfig {
        self.commit.durability = durability;
        self
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig::new(default_threads())
    }
}

/// Activity of a running server (monotonic, except the committer's gauges).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServerStats {
    pub connections: u64,
    pub requests: u64,
    pub error_replies: u64,
    /// Request frames received, length prefixes included.
    pub bytes_in: u64,
    /// Response frames sent, length prefixes included.
    pub bytes_out: u64,
    /// The shared group committer: `commit.batches` commits carried
    /// `commit.ops` write operations.
    pub commit: GroupCommitStats,
}

#[derive(Default)]
struct Counters {
    connections: AtomicU64,
    requests: AtomicU64,
    error_replies: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}

struct Shared {
    stop: AtomicBool,
    /// Clones of the open connections, shut down by `stop` to unblock workers.
    active: Mutex<HashMap<u64, TcpStream>>,
    next_id: AtomicU64,
    counters: Counters,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a worker needs to serve a connection.
struct Env<S: Store> {
    db: Arc<Db<S>>,
    committer: Arc<GroupCommitter>,
    shared: Arc<Shared>,
    max_inflight: usize,
}

/// A running server. `stop` (or dropping the handle) shuts it down: open
/// connections are closed, every thread is joined and the committer drained.
pub struct ServerHandle {
    addr: SocketAddr,
    shared: Arc<Shared>,
    committer: Arc<GroupCommitter>,
    acceptor: Option<JoinHandle<()>>,
    workers: Vec<JoinHandle<()>>,
}

/// Bind a listener (the CLI binds before opening the database, so an address
/// already in use never touches the database).
pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<TcpListener> {
    TcpListener::bind(addr)
}

/// Bind `addr` and serve `db` with `threads` workers (default configuration).
pub fn serve<S: Store, A: ToSocketAddrs>(
    db: Arc<Db<S>>,
    addr: A,
    threads: usize,
) -> io::Result<ServerHandle> {
    serve_listener(db, bind(addr)?, threads)
}

/// Serve `db` on an already bound listener with `threads` workers (at least
/// 1) and the default configuration ([`ServerConfig::new`]).
pub fn serve_listener<S: Store>(
    db: Arc<Db<S>>,
    listener: TcpListener,
    threads: usize,
) -> io::Result<ServerHandle> {
    serve_with(db, listener, ServerConfig::new(threads))
}

/// Serve `db` on an already bound listener with `cfg`.
pub fn serve_with<S: Store>(
    db: Arc<Db<S>>,
    listener: TcpListener,
    cfg: ServerConfig,
) -> io::Result<ServerHandle> {
    let threads = cfg.threads.max(1);
    let addr = listener.local_addr()?;
    let sink = CommitSink {
        db: Arc::clone(&db),
        raise: AtomicBool::new(cfg.raise_commit_priority),
    };
    let committer = GroupCommitter::new(sink, cfg.commit.clone()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("cannot start the group committer: {e}"),
        )
    })?;
    let committer = Arc::new(committer);
    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        active: Mutex::new(HashMap::new()),
        next_id: AtomicU64::new(1),
        counters: Counters::default(),
    });
    // Declared before the channel: on an early return the sender is dropped
    // first, the idle workers see a closed channel, and the handle's `Drop`
    // joins them (then drains the committer).
    let mut handle = ServerHandle {
        addr,
        shared: Arc::clone(&shared),
        committer: Arc::clone(&committer),
        acceptor: None,
        workers: Vec::new(),
    };
    let (tx, rx) = mpsc::sync_channel::<TcpStream>(0);
    let rx = Arc::new(Mutex::new(rx));
    for i in 0..threads {
        let env = Env {
            db: Arc::clone(&db),
            committer: Arc::clone(&committer),
            shared: Arc::clone(&shared),
            max_inflight: cfg.max_inflight_writes.max(1),
        };
        let rx = Arc::clone(&rx);
        let worker = thread::Builder::new()
            .name(format!("babeldb-worker-{i}"))
            .spawn(move || worker_loop(&env, &rx))?;
        handle.workers.push(worker);
    }
    let acceptor = thread::Builder::new()
        .name("babeldb-acceptor".into())
        .spawn(move || accept_loop(&listener, &tx, &shared))?;
    handle.acceptor = Some(acceptor);
    Ok(handle)
}

impl ServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn stats(&self) -> ServerStats {
        let c = &self.shared.counters;
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        ServerStats {
            connections: l(&c.connections),
            requests: l(&c.requests),
            error_replies: l(&c.error_replies),
            bytes_in: l(&c.bytes_in),
            bytes_out: l(&c.bytes_out),
            commit: self.committer.stats(),
        }
    }

    /// Configuration of the shared group committer.
    pub fn commit_config(&self) -> &GroupCommitConfig {
        self.committer.config()
    }

    /// Wait until every write acknowledged so far is durable (buffered mode:
    /// forces the sync; immediate mode: they already are).
    pub fn flush(&self) -> crate::Result<()> {
        self.committer.flush()
    }

    /// Close every connection, stop accepting, join all threads, drain the
    /// committer and return the final statistics.
    pub fn stop(mut self) -> ServerStats {
        let _ = self.halt();
        self.stats()
    }

    /// [`ServerHandle::stop`], reporting the committer's failure: the error
    /// of the final durability step (buffered mode), or a dead writer thread.
    pub fn shutdown(mut self) -> crate::Result<ServerStats> {
        self.halt()?;
        Ok(self.stats())
    }

    /// Block until the server stops (the CLI `serve` command: until the
    /// process is killed), then drain the committer.
    pub fn wait(mut self) {
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        let _ = self.committer.shutdown();
    }

    fn halt(&mut self) -> crate::Result<()> {
        if self.acceptor.is_some() || !self.workers.is_empty() {
            self.shared.stop.store(true, Ordering::SeqCst);
            for stream in lock(&self.shared.active).values() {
                let _ = stream.shutdown(Shutdown::Both);
            }
            if let Some(acceptor) = self.acceptor.take() {
                // Wake the acceptor blocked in `accept`: it sees `stop` and
                // exits, closing the channel, so the idle workers exit too.
                let _ = TcpStream::connect_timeout(&wake_addr(self.addr), Duration::from_secs(2));
                let _ = acceptor.join();
            }
            // Workers finish the request at hand (waiting for its commit):
            // the committer keeps running until they are joined.
            for worker in self.workers.drain(..) {
                let _ = worker.join();
            }
        }
        // Commit whatever was accepted and not yet committed (idempotent).
        self.committer.shutdown()
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.halt();
    }
}

/// The committer's sink: the database, and on the first commit (made by the
/// writer thread, the only thread that commits) the priority raise of that
/// thread ([`ServerConfig::raise_commit_priority`]). Requests are prepared
/// (validated and encoded) by the database in the workers that submit them
/// ([`BatchSink::prepare`]), so the writer thread only runs transactions.
struct CommitSink<S: Store> {
    db: Arc<Db<S>>,
    /// The raise is still to be done.
    raise: AtomicBool,
}

impl<S: Store> CommitSink<S> {
    /// Called by the writer thread before each commit.
    fn raise_once(&self) {
        if self.raise.load(Ordering::Relaxed) && self.raise.swap(false, Ordering::Relaxed) {
            raise_current_thread_priority();
        }
    }
}

impl<S: Store> BatchSink for CommitSink<S> {
    fn apply(&self, ops: &[OwnedOp], durability: Durability) -> crate::Result<Vec<OpResult>> {
        self.raise_once();
        BatchSink::apply(&*self.db, ops, durability)
    }

    fn sync(&self) -> crate::Result<()> {
        self.raise_once();
        BatchSink::sync(&*self.db)
    }

    /// Runs in the submitting worker (its priority is left alone).
    fn prepare(&self, ops: Vec<OwnedOp>) -> SinkOps {
        BatchSink::prepare(&*self.db, ops)
    }

    fn apply_prepared(
        &self,
        ops: Vec<PreparedOp>,
        durability: Durability,
    ) -> crate::Result<Vec<OpResult>> {
        self.raise_once();
        BatchSink::apply_prepared(&*self.db, ops, durability)
    }
}

#[cfg(windows)]
fn raise_current_thread_priority() {
    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, SetThreadPriority, THREAD_PRIORITY_HIGHEST,
    };
    // SAFETY: the pseudo handle of the calling thread is always valid; a
    // failure leaves the priority as it was.
    unsafe {
        SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
    }
}

#[cfg(not(windows))]
fn raise_current_thread_priority() {}

/// Address to connect to in order to reach a listener bound to `addr`.
fn wake_addr(mut addr: SocketAddr) -> SocketAddr {
    if addr.ip().is_unspecified() {
        match addr {
            SocketAddr::V4(_) => addr.set_ip(Ipv4Addr::LOCALHOST.into()),
            SocketAddr::V6(_) => addr.set_ip(Ipv6Addr::LOCALHOST.into()),
        }
    }
    addr
}

fn accept_loop(listener: &TcpListener, tx: &mpsc::SyncSender<TcpStream>, shared: &Shared) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                if shared.stop.load(Ordering::SeqCst) {
                    break;
                }
                shared.counters.connections.fetch_add(1, Ordering::Relaxed);
                if tx.send(stream).is_err() {
                    break;
                }
            }
            Err(e) => {
                if shared.stop.load(Ordering::SeqCst) {
                    break;
                }
                // Transient failures (aborted handshakes, descriptor
                // exhaustion): report and keep serving.
                eprintln!("babeldb serve: accept failed: {e}");
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

fn worker_loop<S: Store>(env: &Env<S>, rx: &Mutex<mpsc::Receiver<TcpStream>>) {
    loop {
        // The guard is a temporary of this statement: held only while waiting.
        let next = lock(rx).recv();
        let Ok(stream) = next else { return };
        serve_connection(env, stream);
    }
}

/// Removes a connection from the registry when its worker is done with it.
struct Registration<'a> {
    shared: &'a Shared,
    id: u64,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        lock(&self.shared.active).remove(&self.id);
    }
}

/// Read half of a server connection, interruptible by `stop`: the socket has
/// a read timeout ([`STOP_POLL`]) and every timed-out wait checks the stop
/// flag.
///
/// On Windows, `shutdown` does not wake a `recv` already blocked in another
/// thread (checked on Windows 11: neither `Read`, `Write` nor `Both`), and
/// after a *timed-out* `recv` Winsock declares the socket state indeterminate
/// (data may be lost). So there the reader waits for data with `peek`:
/// peeking consumes nothing, so a timed-out wait cannot lose bytes, and it
/// only calls `read` once bytes are available, which then returns at once and
/// never times out. The wait costs one extra system call per received burst
/// (measured on the benchmark machine: about 5-8 us of a 35-60 us loopback
/// round trip); requests already buffered never reach it. Elsewhere a
/// timed-out `recv` consumes nothing, so the reader reads directly.
struct ConnReader<'a> {
    stream: TcpStream,
    stop: &'a AtomicBool,
}

impl ConnReader<'_> {
    /// One wait for data: `Ok(None)` when it timed out.
    fn read_once(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        let waited = if cfg!(windows) {
            let mut probe = [0u8; 1];
            self.stream.peek(&mut probe)
        } else {
            self.stream.read(buf)
        };
        match waited {
            Ok(0) => Ok(Some(0)),
            // Windows: bytes are available, so this read returns at once.
            Ok(_) if cfg!(windows) => self.stream.read(buf).map(Some),
            Ok(n) => Ok(Some(n)),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

impl Read for ConnReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            if let Some(n) = self.read_once(buf)? {
                return Ok(n);
            }
            if self.stop.load(Ordering::SeqCst) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "server stopping",
                ));
            }
        }
    }
}

/// Whether `buf` starts with a complete frame, or with a length prefix that
/// `read_frame` rejects: reading the next frame then never touches the socket.
fn frame_buffered(buf: &[u8]) -> bool {
    let Some(prefix) = buf.get(..protocol::LEN_PREFIX) else {
        return false;
    };
    let len = u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
    len == 0 || len > protocol::MAX_FRAME_LEN || buf.len() - protocol::LEN_PREFIX >= len as usize
}

fn serve_connection<S: Store>(env: &Env<S>, stream: TcpStream) {
    let shared = &*env.shared;
    let _ = stream.set_nodelay(true);
    let (Ok(registered), Ok(read_half)) = (stream.try_clone(), stream.try_clone()) else {
        return;
    };
    let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
    {
        // Checking `stop` under the registry lock orders this against
        // `shutdown`: either the stream is registered before `shutdown` walks
        // the registry, or this worker sees `stop` and closes it.
        let mut active = lock(&shared.active);
        if shared.stop.load(Ordering::SeqCst) {
            let _ = stream.shutdown(Shutdown::Both);
            return;
        }
        active.insert(id, registered);
    }
    let _registration = Registration { shared, id };
    if read_half.set_read_timeout(Some(STOP_POLL)).is_err()
        || stream.set_write_timeout(Some(WRITE_TIMEOUT)).is_err()
    {
        return;
    }
    let mut reader = BufReader::with_capacity(
        READ_BUFFER,
        ConnReader {
            stream: read_half,
            stop: &shared.stop,
        },
    );
    let mut conn = Connection {
        env,
        writer: stream,
        out: Vec::new(),
        inflight: VecDeque::new(),
    };
    let mut body = Vec::new();
    loop {
        // When the next read may block, everything received so far is
        // answered first.
        if !frame_buffered(reader.buffer()) && conn.flush().is_err() {
            break;
        }
        if shared.stop.load(Ordering::Relaxed) {
            break;
        }
        match protocol::read_frame(&mut reader, &mut body) {
            Ok(true) => {
                let c = &shared.counters;
                c.requests.fetch_add(1, Ordering::Relaxed);
                c.bytes_in.fetch_add(
                    (protocol::LEN_PREFIX + body.len()) as u64,
                    Ordering::Relaxed,
                );
                conn.handle(&body);
            }
            Ok(false) => break,
            Err(e @ (ProtocolError::FrameTooLarge { .. } | ProtocolError::EmptyFrame)) => {
                // The stream can no longer be delimited: answer, then close.
                conn.reply(&Reply::Error(format!("{e}; closing the connection")));
                let _ = conn.flush();
                break;
            }
            Err(_) => break,
        }
        if conn.out.len() >= FLUSH_BYTES && conn.flush().is_err() {
            break;
        }
        if body.capacity() > KEEP_BUFFER {
            body = Vec::new();
        }
    }
    // Writes still in flight are committed by the committer whether or not
    // anybody waits for them; their replies can no longer be delivered.
    let _ = conn.writer.shutdown(Shutdown::Both);
}

/// How the results of a submitted write turn into its reply.
#[derive(Clone, Copy)]
enum WriteKind {
    Put,
    Delete,
    PutBatch,
}

/// One connection's reply pipeline: every request whose reply is not in
/// `out` yet is in `inflight`, and those come after everything in `out`.
struct Connection<'a, S: Store> {
    env: &'a Env<S>,
    writer: TcpStream,
    /// Encoded replies not sent yet, in request order.
    out: Vec<u8>,
    /// Submitted writes, oldest first; their replies follow `out`.
    inflight: VecDeque<(WriteKind, Ticket)>,
}

impl<S: Store> Connection<'_, S> {
    fn handle(&mut self, body: &[u8]) {
        let request = match Request::decode(body) {
            Ok(r) => r,
            Err(e) => return self.reply(&Reply::Error(format!("bad request: {e}"))),
        };
        let env = self.env;
        let db = &*env.db;
        match request {
            Request::Put { key, value } => match validate_put(db, key, value) {
                Ok(()) => self.submit(WriteKind::Put, vec![OwnedOp::put(key, value, Expect::Any)]),
                Err(e) => self.reply(&Reply::Error(e.to_string())),
            },
            Request::Delete { key } => {
                self.submit(WriteKind::Delete, vec![OwnedOp::delete(key, Expect::Any)]);
            }
            Request::PutBatch { items } if items.is_empty() => {
                self.reply(&Reply::Revisions(Vec::new()));
            }
            Request::PutBatch { items } => {
                // All or nothing: one invalid item rejects the batch before
                // anything is submitted.
                match items.iter().try_for_each(|&(k, v)| validate_put(db, k, v)) {
                    Ok(()) => {
                        let ops = items
                            .iter()
                            .map(|&(key, value)| OwnedOp::put(key, value, Expect::Any))
                            .collect();
                        self.submit(WriteKind::PutBatch, ops);
                    }
                    Err(e) => self.reply(&Reply::Error(e.to_string())),
                }
            }
            read => {
                // Read-your-writes: the connection's earlier writes first.
                self.settle();
                let reply = guarded(|| execute(db, read));
                self.push(&reply);
            }
        }
    }

    /// Queue a write without waiting for its commit.
    fn submit(&mut self, kind: WriteKind, ops: Vec<OwnedOp>) {
        if self.inflight.len() >= self.env.max_inflight {
            self.settle();
        }
        match self.env.committer.submit(ops) {
            Ok(ticket) => self.inflight.push_back((kind, ticket)),
            Err(e) => self.reply(&Reply::Error(e.to_string())),
        }
    }

    /// Queue a reply produced now: after the replies of the earlier writes.
    fn reply(&mut self, reply: &Reply) {
        self.settle();
        self.push(reply);
    }

    /// Wait for every in-flight write and queue its reply, in order.
    fn settle(&mut self) {
        while let Some((kind, ticket)) = self.inflight.pop_front() {
            let reply = write_reply(kind, ticket.wait());
            self.push(&reply);
        }
    }

    fn push(&mut self, reply: &Reply) {
        push_reply(&mut self.out, reply, &self.env.shared.counters);
    }

    /// Settle, then send every queued reply in one write.
    fn flush(&mut self) -> io::Result<()> {
        self.settle();
        if self.out.is_empty() {
            return Ok(());
        }
        let sent = self.writer.write_all(&self.out);
        self.out.clear();
        if self.out.capacity() > KEEP_BUFFER {
            self.out = Vec::new();
        }
        sent
    }
}

/// The checks `Db::put` makes before its transaction (same errors).
fn validate_put<S: Store>(db: &Db<S>, key: &[u8], value: &[u8]) -> crate::Result<()> {
    db.check_key(key)?;
    db.check_value_len(value.len() as u64)
}

/// Reply of a committed (or failed) write request.
fn write_reply(kind: WriteKind, outcome: crate::Result<Vec<OpResult>>) -> Reply {
    let results = match outcome {
        Ok(results) => results,
        Err(e) => return Reply::Error(e.to_string()),
    };
    match kind {
        WriteKind::Put => match single_result(results) {
            Ok(Some(revision)) => Reply::Revision(revision),
            Ok(None) => Reply::Error("internal error: put committed without a revision".into()),
            Err(e) => Reply::Error(e.to_string()),
        },
        WriteKind::Delete => match single_result(results) {
            Ok(Some(_)) => Reply::Done,
            Ok(None) => Reply::NotFound,
            Err(e) => Reply::Error(e.to_string()),
        },
        WriteKind::PutBatch => {
            let n = results.len();
            let mut revisions = Vec::with_capacity(n);
            for (i, r) in results.into_iter().enumerate() {
                match r {
                    Ok(revision) => revisions.push(revision.unwrap_or(0)),
                    // Only a corrupt stored manifest fails a validated,
                    // unconditional put; the other items were committed.
                    Err(e) => {
                        return Reply::Error(format!(
                            "item {i} of {n} failed ({e}); the other items were committed"
                        ));
                    }
                }
            }
            Reply::Revisions(revisions)
        }
    }
}

/// Append the frame of `reply` to `out` (an ERROR instead when it cannot be
/// encoded) and account for it.
fn push_reply(out: &mut Vec<u8>, reply: &Reply, counters: &Counters) {
    let start = out.len();
    let mut is_error = matches!(reply, Reply::Error(_));
    if let Err(e) = reply.encode(out) {
        // Typically a response above MAX_FRAME_LEN; `encode` left `out` as it
        // was. An ERROR (at most MAX_ERROR_MESSAGE bytes) always fits.
        let _ = Reply::Error(format!("response not sent: {e}")).encode(out);
        is_error = true;
    }
    if is_error {
        counters.error_replies.fetch_add(1, Ordering::Relaxed);
    }
    counters
        .bytes_out
        .fetch_add((out.len() - start) as u64, Ordering::Relaxed);
}

/// Panics turned into ERROR replies, so one failing request never takes a
/// worker (and its connection) down.
fn guarded(f: impl FnOnce() -> Reply) -> Reply {
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(reply) => reply,
        Err(payload) => Reply::Error(format!(
            "internal error: {}",
            panic_message(payload.as_ref())
        )),
    }
}

/// Decode one request body, run it against `db` and build the reply:
/// everything the server does per request except the socket I/O. Writes run
/// here as direct `Db` calls with their own commit (the server itself routes
/// them through its group committer).
pub fn handle_request<S: Store>(db: &Db<S>, body: &[u8]) -> Reply {
    match Request::decode(body) {
        Ok(request) => execute(db, request),
        Err(e) => Reply::Error(format!("bad request: {e}")),
    }
}

/// Run one decoded request directly against `db`.
fn execute<S: Store>(db: &Db<S>, request: Request<'_>) -> Reply {
    let result = match request {
        Request::Ping => Ok(Reply::Done),
        Request::Get { key } => db.get(key).map(|v| v.map_or(Reply::NotFound, Reply::Value)),
        Request::Put { key, value } => db.put(key, value, Expect::Any).map(Reply::Revision),
        Request::Delete { key } => db.delete(key, Expect::Any).map(|deleted| {
            if deleted {
                Reply::Done
            } else {
                Reply::NotFound
            }
        }),
        Request::Range { key, offset, len } => db
            .get_range(key, offset, len)
            .map(|v| v.map_or(Reply::NotFound, Reply::Value)),
        Request::ScanPrefix {
            prefix,
            limit,
            reverse,
            with_values,
        } => {
            let opts = ScanOptions::prefix(prefix)
                .limit(limit as usize)
                .reverse(reverse)
                .with_values(with_values);
            db.scan(&opts).map(Reply::Items)
        }
        Request::PutBatch { items } if items.is_empty() => Ok(Reply::Revisions(Vec::new())),
        Request::PutBatch { items } => {
            let ops: Vec<BatchOp<'_>> = items
                .iter()
                .map(|&(key, value)| BatchOp::Put {
                    key,
                    value,
                    expect: Expect::Any,
                })
                .collect();
            db.write_batch(&ops)
                .map(|revs| Reply::Revisions(revs.into_iter().map(|r| r.unwrap_or(0)).collect()))
        }
    };
    result.unwrap_or_else(|e| Reply::Error(e.to_string()))
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Blocking client of the wire protocol: one connection. [`Client::call`]
/// (and the typed helpers) keep one request in flight; [`Client::queue`],
/// [`Client::send`], [`Client::recv`] and [`Client::pipeline`] pipeline many.
/// ERROR replies surface as [`ProtocolError::Remote`] from `call`, and as
/// [`Reply::Error`] from the pipelining methods; the connection stays usable
/// after them. After a transport error (I/O, truncated or unexpected frame)
/// the connection is out of step and must be dropped.
pub struct Client {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
    /// Encoded requests not sent yet.
    out: Vec<u8>,
    body: Vec<u8>,
    /// Ops of the requests queued or sent and not answered yet, oldest first.
    pending: VecDeque<u8>,
}

fn unexpected(reply: &Reply) -> ProtocolError {
    ProtocolError::Malformed(format!("unexpected {} reply", reply.kind()))
}

fn misuse(message: String) -> ProtocolError {
    ProtocolError::Io(io::Error::new(io::ErrorKind::InvalidInput, message))
}

impl Client {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Client, ProtocolError> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let reader = BufReader::with_capacity(READ_BUFFER, stream.try_clone()?);
        Ok(Client {
            writer: stream,
            reader,
            out: Vec::new(),
            body: Vec::new(),
            pending: VecDeque::new(),
        })
    }

    /// Read/write timeout of the connection (`None`: block forever).
    pub fn set_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.writer.set_read_timeout(timeout)?;
        self.writer.set_write_timeout(timeout)
    }

    /// Send one request and wait for its reply (no pipelined request may be
    /// awaiting its reply).
    pub fn call(&mut self, request: &Request<'_>) -> Result<Reply, ProtocolError> {
        if !self.pending.is_empty() {
            return Err(misuse(format!(
                "call with {} pipelined requests awaiting their replies (read them with recv)",
                self.pending.len()
            )));
        }
        match self.exchange(request)? {
            Reply::Error(message) => Err(ProtocolError::Remote(message)),
            reply => Ok(reply),
        }
    }

    /// One request, one reply: the request and its frame in one write.
    fn exchange(&mut self, request: &Request<'_>) -> Result<Reply, ProtocolError> {
        self.out.clear();
        request.encode(&mut self.out)?;
        let sent = self.writer.write_all(&self.out);
        self.release_out();
        sent?;
        if !protocol::read_frame(&mut self.reader, &mut self.body)? {
            return Err(ProtocolError::Closed);
        }
        Reply::decode(request.op(), &self.body)
    }

    fn release_out(&mut self) {
        self.out.clear();
        if self.out.capacity() > KEEP_BUFFER {
            self.out = Vec::new();
        }
    }

    /// Encode `request` behind the queued ones without sending it.
    pub fn queue(&mut self, request: &Request<'_>) -> Result<(), ProtocolError> {
        request.encode(&mut self.out)?;
        self.pending.push_back(request.op());
        Ok(())
    }

    /// Send every queued request in one write.
    pub fn send(&mut self) -> Result<(), ProtocolError> {
        if self.out.is_empty() {
            return Ok(());
        }
        let sent = self.writer.write_all(&self.out);
        self.release_out();
        Ok(sent?)
    }

    /// Requests queued or sent whose replies were not read yet.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Read the reply of the oldest request not answered yet (queued
    /// requests are sent first). An ERROR reply is `Ok(Reply::Error(_))`.
    pub fn recv(&mut self) -> Result<Reply, ProtocolError> {
        let Some(&op) = self.pending.front() else {
            return Err(misuse("recv without a request awaiting its reply".into()));
        };
        self.send()?;
        if !protocol::read_frame(&mut self.reader, &mut self.body)? {
            return Err(ProtocolError::Closed);
        }
        self.pending.pop_front();
        Reply::decode(op, &self.body)
    }

    /// Send `requests` and read their replies, in order, keeping at most
    /// [`PIPELINE_WINDOW`] request bytes (or one larger request) in flight.
    /// The server runs them in order; consecutive writes can share commits.
    /// ERROR replies are returned in their slot (`Reply::Error`). If a
    /// request cannot be encoded, the windows before it were sent and
    /// answered, and nothing after them is sent.
    pub fn pipeline(&mut self, requests: &[Request<'_>]) -> Result<Vec<Reply>, ProtocolError> {
        if !self.pending.is_empty() {
            return Err(misuse(format!(
                "pipeline with {} requests awaiting their replies (read them with recv)",
                self.pending.len()
            )));
        }
        let mut replies = Vec::with_capacity(requests.len());
        let mut rest = requests;
        while !rest.is_empty() {
            // A window: requests up to PIPELINE_WINDOW bytes, or one larger
            // request on its own.
            let mut n = 0;
            while n < rest.len() {
                let at = self.out.len();
                if let Err(e) = self.queue(&rest[n]) {
                    // Nothing of this window was sent: forget it.
                    self.release_out();
                    self.pending.clear();
                    return Err(e);
                }
                if n > 0 && self.out.len() > PIPELINE_WINDOW {
                    self.out.truncate(at);
                    self.pending.pop_back();
                    break;
                }
                n += 1;
            }
            self.send()?;
            for _ in 0..n {
                replies.push(self.recv()?);
            }
            rest = &rest[n..];
        }
        Ok(replies)
    }

    /// Send arbitrary bytes (tests of malformed input) and read one response
    /// body; `Ok(None)` when the server closed the connection instead.
    pub fn send_raw(&mut self, bytes: &[u8]) -> Result<Option<Vec<u8>>, ProtocolError> {
        self.writer.write_all(bytes)?;
        if protocol::read_frame(&mut self.reader, &mut self.body)? {
            Ok(Some(self.body.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn ping(&mut self) -> Result<(), ProtocolError> {
        match self.call(&Request::Ping)? {
            Reply::Done => Ok(()),
            other => Err(unexpected(&other)),
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ProtocolError> {
        match self.call(&Request::Get { key })? {
            Reply::Value(v) => Ok(Some(v)),
            Reply::NotFound => Ok(None),
            other => Err(unexpected(&other)),
        }
    }

    /// Pipelined GETs, one reply per key in order. The first ERROR reply is
    /// returned as [`ProtocolError::Remote`] after every reply was read.
    pub fn get_many(&mut self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, ProtocolError> {
        let requests: Vec<Request<'_>> = keys.iter().map(|&key| Request::Get { key }).collect();
        let mut values = Vec::with_capacity(keys.len());
        let mut failure = None;
        for reply in self.pipeline(&requests)? {
            match reply {
                Reply::Value(v) => values.push(Some(v)),
                Reply::NotFound => values.push(None),
                Reply::Error(message) => {
                    failure.get_or_insert(ProtocolError::Remote(message));
                }
                other => {
                    failure.get_or_insert(unexpected(&other));
                }
            }
        }
        failure.map_or(Ok(values), Err)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Revision, ProtocolError> {
        match self.call(&Request::Put { key, value })? {
            Reply::Revision(r) => Ok(r),
            other => Err(unexpected(&other)),
        }
    }

    /// Pipelined PUTs: each item is its own request, acknowledged on its own
    /// (unlike [`Client::put_batch`]), but consecutive ones can share
    /// commits. The first ERROR reply is returned as
    /// [`ProtocolError::Remote`] after every reply was read.
    pub fn put_many(&mut self, items: &[(&[u8], &[u8])]) -> Result<Vec<Revision>, ProtocolError> {
        let requests: Vec<Request<'_>> = items
            .iter()
            .map(|&(key, value)| Request::Put { key, value })
            .collect();
        let mut revisions = Vec::with_capacity(items.len());
        let mut failure = None;
        for reply in self.pipeline(&requests)? {
            match reply {
                Reply::Revision(r) => revisions.push(r),
                Reply::Error(message) => {
                    failure.get_or_insert(ProtocolError::Remote(message));
                }
                other => {
                    failure.get_or_insert(unexpected(&other));
                }
            }
        }
        failure.map_or(Ok(revisions), Err)
    }

    /// All items in one atomic commit (durable before the reply, unless the
    /// server acknowledges buffered writes); one revision per item.
    pub fn put_batch(&mut self, items: &[(&[u8], &[u8])]) -> Result<Vec<Revision>, ProtocolError> {
        match self.call(&Request::PutBatch {
            items: items.to_vec(),
        })? {
            Reply::Revisions(r) => Ok(r),
            other => Err(unexpected(&other)),
        }
    }

    /// Whether a live record was deleted.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, ProtocolError> {
        match self.call(&Request::Delete { key })? {
            Reply::Done => Ok(true),
            Reply::NotFound => Ok(false),
            other => Err(unexpected(&other)),
        }
    }

    pub fn range(
        &mut self,
        key: &[u8],
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, ProtocolError> {
        match self.call(&Request::Range { key, offset, len })? {
            Reply::Value(v) => Ok(Some(v)),
            Reply::NotFound => Ok(None),
            other => Err(unexpected(&other)),
        }
    }

    /// `limit == 0` means no limit.
    pub fn scan_prefix(
        &mut self,
        prefix: &[u8],
        limit: u32,
        reverse: bool,
        with_values: bool,
    ) -> Result<Vec<ScanItem>, ProtocolError> {
        match self.call(&Request::ScanPrefix {
            prefix,
            limit,
            reverse,
            with_values,
        })? {
            Reply::Items(items) => Ok(items),
            other => Err(unexpected(&other)),
        }
    }
}
