//! Minimal synchronous TCP server of the wire protocol (stage 10) and a
//! blocking [`Client`].
//!
//! A `std::net::TcpListener` hands accepted connections to a fixed pool of
//! worker threads; each worker serves one connection at a time and any
//! number of requests on it, with `TCP_NODELAY`. All workers share one
//! `Arc<Db<S>>` (the engine is `Send + Sync`; `gc` and `compact` need
//! `&mut Db` and are not exposed remotely). The protocol is documented in
//! [`crate::cli::protocol`].
//!
//! The server exists to measure the cost of a remote interface separately
//! from the engine: [`Client`] issues the operations a benchmark can also call
//! directly on `Db`, and [`handle_request`] isolates decode + dispatch +
//! engine from the TCP transport. No authentication: bind to loopback.

use std::collections::HashMap;
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
use crate::store::Store;

/// Capacity of the per-connection read buffer.
const READ_BUFFER: usize = 64 * 1024;
/// How often a connection waiting for data checks whether the server stops.
const STOP_POLL: Duration = Duration::from_millis(50);
/// A client that does not accept its response for this long is disconnected.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-connection buffers larger than this are released after each request.
const KEEP_BUFFER: usize = 1 << 20;

/// Default size of the worker pool: the available parallelism, at most 64.
pub fn default_threads() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, 64)
}

/// Activity of a running server (monotonic).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServerStats {
    pub connections: u64,
    pub requests: u64,
    pub error_replies: u64,
    /// Request frames received, length prefixes included.
    pub bytes_in: u64,
    /// Response frames sent, length prefixes included.
    pub bytes_out: u64,
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

/// A running server. `stop` (or dropping the handle) shuts it down: open
/// connections are closed and every thread is joined.
pub struct ServerHandle {
    addr: SocketAddr,
    shared: Arc<Shared>,
    acceptor: Option<JoinHandle<()>>,
    workers: Vec<JoinHandle<()>>,
}

/// Bind a listener (the CLI binds before opening the database, so an address
/// already in use never touches the database).
pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<TcpListener> {
    TcpListener::bind(addr)
}

/// Bind `addr` and serve `db` with `threads` workers.
pub fn serve<S: Store, A: ToSocketAddrs>(
    db: Arc<Db<S>>,
    addr: A,
    threads: usize,
) -> io::Result<ServerHandle> {
    serve_listener(db, bind(addr)?, threads)
}

/// Serve `db` on an already bound listener with `threads` workers (at least 1).
pub fn serve_listener<S: Store>(
    db: Arc<Db<S>>,
    listener: TcpListener,
    threads: usize,
) -> io::Result<ServerHandle> {
    let threads = threads.max(1);
    let addr = listener.local_addr()?;
    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        active: Mutex::new(HashMap::new()),
        next_id: AtomicU64::new(1),
        counters: Counters::default(),
    });
    // Declared before the channel: on an early return the sender is dropped
    // first, the idle workers see a closed channel, and the handle's `Drop`
    // joins them.
    let mut handle = ServerHandle {
        addr,
        shared: Arc::clone(&shared),
        acceptor: None,
        workers: Vec::new(),
    };
    let (tx, rx) = mpsc::sync_channel::<TcpStream>(0);
    let rx = Arc::new(Mutex::new(rx));
    for i in 0..threads {
        let (db, shared, rx) = (Arc::clone(&db), Arc::clone(&shared), Arc::clone(&rx));
        let worker = thread::Builder::new()
            .name(format!("babeldb-worker-{i}"))
            .spawn(move || worker_loop(&db, &shared, &rx))?;
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
        }
    }

    /// Close every connection, stop accepting, join all threads and return
    /// the final statistics.
    pub fn stop(mut self) -> ServerStats {
        self.shutdown();
        self.stats()
    }

    /// Block until the server stops (the CLI `serve` command: until the
    /// process is killed).
    pub fn wait(mut self) {
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }

    fn shutdown(&mut self) {
        if self.acceptor.is_none() && self.workers.is_empty() {
            return;
        }
        self.shared.stop.store(true, Ordering::SeqCst);
        for stream in lock(&self.shared.active).values() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        if let Some(acceptor) = self.acceptor.take() {
            // Wake the acceptor blocked in `accept`: it sees `stop` and exits,
            // closing the channel, so the idle workers exit too.
            let _ = TcpStream::connect_timeout(&wake_addr(self.addr), Duration::from_secs(2));
            let _ = acceptor.join();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

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

fn worker_loop<S: Store>(db: &Db<S>, shared: &Shared, rx: &Mutex<mpsc::Receiver<TcpStream>>) {
    loop {
        // The guard is a temporary of this statement: held only while waiting.
        let next = lock(rx).recv();
        let Ok(stream) = next else { return };
        serve_connection(db, shared, stream);
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

/// Read half of a server connection, interruptible by `stop`.
///
/// On Windows, `shutdown` does not wake a `recv` already blocked in another
/// thread, and after a *timed-out* `recv` Winsock declares the socket state
/// indeterminate (data may be lost). So the reader waits for data with `peek`
/// under the socket's read timeout ([`STOP_POLL`]): peeking consumes nothing,
/// so a timed-out wait cannot lose bytes. Between waits it checks the stop
/// flag, and it only calls `read` once bytes are available, which then
/// returns at once and never times out.
struct ConnReader<'a> {
    stream: TcpStream,
    stop: &'a AtomicBool,
}

impl Read for ConnReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut probe = [0u8; 1];
        loop {
            match self.stream.peek(&mut probe) {
                Ok(0) => return Ok(0),
                Ok(_) => return self.stream.read(buf),
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                    ) =>
                {
                    if self.stop.load(Ordering::SeqCst) {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "server stopping",
                        ));
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn serve_connection<S: Store>(db: &Db<S>, shared: &Shared, stream: TcpStream) {
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
    let mut writer = stream;
    let mut body = Vec::new();
    let mut out = Vec::new();
    loop {
        let reply = match protocol::read_frame(&mut reader, &mut body) {
            Ok(true) => {
                let c = &shared.counters;
                c.requests.fetch_add(1, Ordering::Relaxed);
                c.bytes_in.fetch_add(
                    (protocol::LEN_PREFIX + body.len()) as u64,
                    Ordering::Relaxed,
                );
                handle_guarded(db, &body)
            }
            Ok(false) => break,
            Err(e @ (ProtocolError::FrameTooLarge { .. } | ProtocolError::EmptyFrame)) => {
                // The stream can no longer be delimited: answer, then close.
                let reply = Reply::Error(format!("{e}; closing the connection"));
                let _ = send(&mut writer, &mut out, &reply, &shared.counters);
                break;
            }
            Err(_) => break,
        };
        if send(&mut writer, &mut out, &reply, &shared.counters).is_err() {
            break;
        }
        if body.capacity() > KEEP_BUFFER {
            body = Vec::new();
        }
        if out.capacity() > KEEP_BUFFER {
            out = Vec::new();
        }
    }
    let _ = writer.shutdown(Shutdown::Both);
}

fn send(
    writer: &mut TcpStream,
    out: &mut Vec<u8>,
    reply: &Reply,
    counters: &Counters,
) -> io::Result<()> {
    out.clear();
    let mut is_error = matches!(reply, Reply::Error(_));
    if let Err(e) = reply.encode(out) {
        // Typically a response above MAX_FRAME_LEN: report it instead.
        out.clear();
        Reply::Error(format!("response not sent: {e}"))
            .encode(out)
            .map_err(io::Error::other)?;
        is_error = true;
    }
    if is_error {
        counters.error_replies.fetch_add(1, Ordering::Relaxed);
    }
    counters
        .bytes_out
        .fetch_add(out.len() as u64, Ordering::Relaxed);
    writer.write_all(out)
}

/// [`handle_request`] with panics turned into ERROR replies, so one failing
/// request never takes a worker (and its connection) down.
fn handle_guarded<S: Store>(db: &Db<S>, body: &[u8]) -> Reply {
    match panic::catch_unwind(AssertUnwindSafe(|| handle_request(db, body))) {
        Ok(reply) => reply,
        Err(payload) => Reply::Error(format!(
            "internal error: {}",
            panic_message(payload.as_ref())
        )),
    }
}

/// Decode one request body, run it against `db` and build the reply:
/// everything the server does per request except the socket I/O.
pub fn handle_request<S: Store>(db: &Db<S>, body: &[u8]) -> Reply {
    let request = match Request::decode(body) {
        Ok(r) => r,
        Err(e) => return Reply::Error(format!("bad request: {e}")),
    };
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

/// Blocking client of the wire protocol: one connection, one request in
/// flight. ERROR replies surface as [`ProtocolError::Remote`]; the connection
/// stays usable after them.
pub struct Client {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
    out: Vec<u8>,
    body: Vec<u8>,
}

fn unexpected(reply: &Reply) -> ProtocolError {
    ProtocolError::Malformed(format!("unexpected {} reply", reply.kind()))
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
        })
    }

    /// Read/write timeout of the connection (`None`: block forever).
    pub fn set_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.writer.set_read_timeout(timeout)?;
        self.writer.set_write_timeout(timeout)
    }

    /// Send one request and wait for its reply.
    pub fn call(&mut self, request: &Request<'_>) -> Result<Reply, ProtocolError> {
        self.out.clear();
        request.encode(&mut self.out)?;
        self.writer.write_all(&self.out)?;
        if !protocol::read_frame(&mut self.reader, &mut self.body)? {
            return Err(ProtocolError::Closed);
        }
        match Reply::decode(request.op(), &self.body)? {
            Reply::Error(message) => Err(ProtocolError::Remote(message)),
            reply => Ok(reply),
        }
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

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Revision, ProtocolError> {
        match self.call(&Request::Put { key, value })? {
            Reply::Revision(r) => Ok(r),
            other => Err(unexpected(&other)),
        }
    }

    /// All items in one atomic, durable commit; one revision per item.
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
