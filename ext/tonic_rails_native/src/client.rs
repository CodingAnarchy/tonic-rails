//! `TonicRails::Native::Channel`: outgoing gRPC calls over a tonic
//! [`Channel`], with message bytes passed through untouched.  Unary calls
//! return their result; streaming calls return a
//! `TonicRails::Native::ResponseStream` to read messages from (and, for
//! client and bidirectional streaming, to write requests to).
//!
//! Fork safety: each process gets its own tokio runtime and its own
//! connection, created on first use and keyed by pid.  After `fork` the
//! child sees a different pid and builds fresh ones.  It never touches
//! what it inherited: the runtime's threads don't exist in the child, so
//! dropping the runtime (or a channel whose tasks ran on it) could hang
//! or wake the parent's event loop.  Those are leaked instead.
//!
//! The system's root certificates are loaded once and kept, so children
//! inherit them.  On macOS loading them goes through Security.framework,
//! which aborts in the child of a multi-threaded process.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine;
use bytes::{Buf, BufMut, Bytes};
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue};
use magnus::{Error, RArray, RHash, RString, Ruby, r_hash::ForEach};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder, Streaming};
use tonic::metadata::MetadataMap;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request, Status, TimeoutExpired};

use crate::gvl::{Cancel, Wait, block_on, recv_blocking, send_with_interrupts, without_gvl};
use crate::server::{CONNECTION_WINDOW, STREAM_WINDOW};
use crate::sync::Mutex;

// ─── Per-process runtime ──────────────────────────────────────────

struct ProcessRuntime {
    pid: u32,
    runtime: Runtime,
}

static RUNTIME: Mutex<Option<ProcessRuntime>> = Mutex::new(None);

/// I/O for outgoing calls is light: two threads serve every client in
/// the process.
const RUNTIME_THREADS: usize = 2;

/// The current process's runtime, created on first use.
fn runtime() -> io::Result<Handle> {
    let pid = std::process::id();
    if let Some(current) = RUNTIME.lock().as_ref().filter(|r| r.pid == pid) {
        return Ok(current.runtime.handle().clone());
    }
    // Built outside the lock: if another thread forked while this one
    // held it, the child's copy of the lock would never be released.
    let runtime = Builder::new_multi_thread()
        .worker_threads(RUNTIME_THREADS)
        .thread_name("tonic-rails-client")
        .enable_all()
        .build()?;
    let mut slot = RUNTIME.lock();
    if let Some(current) = slot.as_ref().filter(|r| r.pid == pid) {
        // Another thread got there first.
        runtime.shutdown_background();
        return Ok(current.runtime.handle().clone());
    }
    let handle = runtime.handle().clone();
    if let Some(inherited) = slot.replace(ProcessRuntime { pid, runtime }) {
        std::mem::forget(inherited);
    }
    Ok(handle)
}

// ─── Raw-bytes codec ──────────────────────────────────────────────

/// Passes encoded protobuf messages through: Ruby encodes requests and
/// decodes responses.
#[derive(Clone, Copy, Default)]
struct RawCodec;

impl Codec for RawCodec {
    type Encode = Outgoing;
    type Decode = Bytes;
    type Encoder = RawCodec;
    type Decoder = RawCodec;

    fn encoder(&mut self) -> Self::Encoder {
        *self
    }

    fn decoder(&mut self) -> Self::Decoder {
        *self
    }
}

/// A request message, or `None` to reset the stream: tonic's request
/// stream can't fail, but its encoder can, and hyper resets the stream when
/// the body fails, so the server sees the call cancelled.
type Outgoing = Option<Bytes>;

impl Encoder for RawCodec {
    type Item = Outgoing;
    type Error = Status;

    fn encode(&mut self, item: Outgoing, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        let Some(message) = item else {
            return Err(Status::cancelled("call cancelled by the client"));
        };
        dst.put(message);
        Ok(())
    }
}

impl Decoder for RawCodec {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}

// ─── Calls ────────────────────────────────────────────────────────

/// How a call ended.  `code` 0 means OK and `response` is set.
struct Outcome {
    code: Code,
    message: String,
    /// The failure happened here (a broken connection, a timeout), not
    /// on the server.
    local: bool,
    /// Response headers arrived, so the server has started on the call
    /// and retrying it could repeat its work.
    committed: bool,
    details: Bytes,
    response: Option<Bytes>,
    headers: HeaderMap,
    trailers: HeaderMap,
}

impl Outcome {
    fn ok(response: Option<Bytes>, headers: HeaderMap, trailers: HeaderMap) -> Self {
        Self {
            code: Code::Ok,
            message: String::new(),
            local: false,
            committed: true,
            details: Bytes::new(),
            response,
            headers,
            trailers,
        }
    }

    fn error(code: Code, message: impl Into<String>) -> Self {
        Self {
            local: true,
            ..Self::status(Status::new(code, message), None)
        }
    }

    /// `headers` are the response headers, if they arrived before the
    /// status.
    fn status(status: Status, headers: Option<HeaderMap>) -> Self {
        // A status with a source is a local transport failure, not one the
        // server sent.  The source holds the cause (e.g. "Connection
        // refused"), and tonic reports broken connections as UNKNOWN or
        // CANCELLED where gRPC clients report UNAVAILABLE.
        let (code, message, local) = match std::error::Error::source(&status) {
            Some(source) => (
                match status.code() {
                    Code::Unknown | Code::Cancelled => Code::Unavailable,
                    code => code,
                },
                append_causes(status.message().to_owned(), Some(source)),
                true,
            ),
            None => (status.code(), status.message().to_owned(), false),
        };
        Self {
            code,
            message,
            local,
            committed: headers.is_some(),
            details: Bytes::copy_from_slice(status.details()),
            response: None,
            headers: headers.unwrap_or_default(),
            trailers: status.metadata().clone().into_headers(),
        }
    }

    /// `[code, message, response, headers, trailers, details, committed]`,
    /// with metadata as `[name, value]` pairs in wire form (`-bin` values
    /// base64-encoded).
    fn into_ruby(self, ruby: &Ruby) -> Result<RArray, Error> {
        let result = ruby.ary_new_capa(7);
        result.push(self.code as u32)?;
        result.push(ruby.str_new(&self.message))?;
        result.push(self.response.map(|bytes| ruby.str_from_slice(&bytes)))?;
        result.push(metadata_pairs(ruby, &self.headers)?)?;
        result.push(metadata_pairs(ruby, &self.trailers)?)?;
        result.push((!self.details.is_empty()).then(|| ruby.str_from_slice(&self.details)))?;
        result.push(self.committed)?;
        Ok(result)
    }
}

fn metadata_pairs(ruby: &Ruby, headers: &HeaderMap) -> Result<RArray, Error> {
    let pairs = ruby.ary_new_capa(headers.len());
    for (name, value) in headers {
        let value = String::from_utf8_lossy(value.as_bytes());
        pairs.push((ruby.str_new(name.as_str()), ruby.str_new(&value)))?;
    }
    Ok(pairs)
}

/// Bound a unary call by `timeout`, if there is one.  (Streaming calls
/// watch their deadline in [`read_responses`].)
async fn with_timeout(call: impl Future<Output = Outcome>, timeout: Option<Duration>) -> Outcome {
    let Some(timeout) = timeout else {
        return call.await;
    };
    let started = tokio::time::Instant::now();
    match tokio::time::timeout(timeout, call).await {
        Ok(outcome) => deadline_checked(outcome, started, Some(timeout)),
        Err(_) => deadline_exceeded(),
    }
}

fn deadline_exceeded() -> Outcome {
    Outcome::error(Code::DeadlineExceeded, "deadline exceeded")
}

/// `outcome`, unless it's the deadline passing in disguise: a local
/// failure (a broken connection) once the time is up, or tonic's own
/// timeout.
fn deadline_checked(outcome: Outcome, started: tokio::time::Instant, timeout: Option<Duration>) -> Outcome {
    let Some(timeout) = timeout else {
        return outcome;
    };
    if (outcome.local && started.elapsed() >= timeout) || timeout_expired(&outcome) {
        deadline_exceeded()
    } else {
        outcome
    }
}

/// tonic's channel enforces the grpc-timeout header too, and when its timer
/// fires before ours it ends the call with CANCELLED "Timeout expired", with
/// no source to mark it local.  (A tonic server enforcing the same deadline
/// sends that status too.)  Coarse timers, like Windows' 15 ms ticks, let
/// it fire first even though ours started earlier (#48).
fn timeout_expired(outcome: &Outcome) -> bool {
    outcome.code == Code::Cancelled && outcome.message == TimeoutExpired(()).to_string()
}

/// The request messages of a call.
type RequestBody = Pin<Box<dyn Stream<Item = Outgoing> + Send>>;

/// What every call needs: the method, its request messages and metadata.
struct CallArgs {
    channel: Channel,
    path: PathAndQuery,
    request: RequestBody,
    metadata: HeaderMap,
    timeout: Option<Duration>,
    max_message_size: usize,
}

/// Send the request and wait for the response headers.
async fn start(args: CallArgs) -> Result<(HeaderMap, Streaming<Bytes>), Box<Outcome>> {
    let mut grpc = tonic::client::Grpc::new(args.channel)
        .max_decoding_message_size(args.max_message_size)
        .max_encoding_message_size(args.max_message_size);
    if let Err(e) = grpc.ready().await {
        let message = format!("channel not ready: {e}");
        return Err(Box::new(Outcome::error(Code::Unavailable, message)));
    }

    // The streaming call keeps response headers and trailers apart, which
    // tonic's own unary call merges.
    let mut request = Request::from_parts(
        MetadataMap::from_headers(args.metadata),
        Default::default(),
        args.request,
    );
    if let Some(timeout) = args.timeout {
        request.set_timeout(timeout);
    }
    match grpc.streaming(request, args.path, RawCodec).await {
        Ok(response) => {
            let (headers, stream, _) = response.into_parts();
            Ok((headers.into_headers(), stream))
        }
        // Includes trailers-only responses, whose status comes in the
        // headers.  They don't commit the call: gRPC retries treat a server
        // that answers with only a status as not having started on it.
        Err(status) => Err(Box::new(Outcome::status(status, None))),
    }
}

/// The trailers that end a successful call.
async fn finish(stream: &mut Streaming<Bytes>, headers: &HeaderMap) -> Result<HeaderMap, Box<Outcome>> {
    match stream.trailers().await {
        Ok(trailers) => Ok(trailers.map(MetadataMap::into_headers).unwrap_or_default()),
        Err(status) => Err(Box::new(Outcome::status(status, Some(headers.clone())))),
    }
}

async fn unary(args: CallArgs) -> Outcome {
    let (headers, mut stream) = match start(args).await {
        Ok(started) => started,
        Err(outcome) => return *outcome,
    };
    let message = match stream.message().await {
        Ok(message) => message,
        Err(status) => return Outcome::status(status, Some(headers)),
    };
    let trailers = match finish(&mut stream, &headers).await {
        Ok(trailers) => trailers,
        Err(outcome) => return *outcome,
    };
    match message {
        Some(message) => Outcome::ok(Some(message), headers, trailers),
        None => Outcome::error(Code::Internal, "server sent no response message"),
    }
}

/// Response bytes read ahead of Ruby, beyond room for one whole message.
const READ_AHEAD: usize = 64 * 1024;

/// What a message read ahead counts against `READ_AHEAD`: its bytes and
/// gRPC frame header, so even empty messages add up.
fn read_ahead_cost(message: &Bytes) -> usize {
    message.len() + 5
}

/// Forward each response message to `tx`, until the call ends, its
/// deadline passes, or `cancel` fires (or is dropped).  The deadline covers
/// every message, including time spent waiting on a slow reader.
///
/// Messages are read as they arrive, into a buffer of their own while Ruby
/// catches up: h2 limits how many small DATA frames a connection may leave
/// unread, and closes it when a server sends many tiny messages faster than
/// Ruby reads them.  Once the buffer holds a message plus `READ_AHEAD`,
/// reading stops and HTTP/2 flow control holds the server back.
async fn read_responses(
    args: CallArgs,
    tx: mpsc::Sender<Bytes>,
    cancel: oneshot::Receiver<()>,
    requests: Option<RequestControl>,
) -> Outcome {
    let started = tokio::time::Instant::now();
    let timeout = args.timeout;
    let limit = args.max_message_size.saturating_add(5 + READ_AHEAD);
    let stopped = stopped(cancel, &tx, timeout.map(|t| started + t));
    tokio::pin!(stopped);

    // Ending before the response headers resets the stream: hyper does that
    // when its response future is dropped.
    let (headers, mut stream) = tokio::select! {
        biased;
        outcome = &mut stopped => return outcome,
        result = start(args) => match result {
            Ok(started) => started,
            Err(outcome) => return deadline_checked(*outcome, started, timeout),
        },
    };
    let mut buffered = VecDeque::new();
    let mut buffered_bytes = 0;
    // How the call ended, once it has: Ruby still gets every message read
    // before that.
    let mut ended = None;
    let outcome = loop {
        if buffered.is_empty()
            && let Some(outcome) = ended.take()
        {
            break outcome;
        }
        let read = ended.is_none() && buffered_bytes < limit;
        tokio::select! {
            biased;
            outcome = &mut stopped => break outcome,
            permit = tx.reserve(), if !buffered.is_empty() => match permit {
                Ok(permit) => {
                    let message = buffered.pop_front().expect("checked above");
                    buffered_bytes -= read_ahead_cost(&message);
                    permit.send(message);
                }
                Err(_) => break closed_by_client(),
            },
            message = stream.message(), if read => match message {
                Ok(Some(message)) => {
                    buffered_bytes += read_ahead_cost(&message);
                    buffered.push_back(message);
                }
                Ok(None) => {
                    ended = Some(match finish(&mut stream, &headers).await {
                        Ok(trailers) => Outcome::ok(None, headers.clone(), trailers),
                        Err(outcome) => *outcome,
                    });
                }
                Err(status) => ended = Some(Outcome::status(status, Some(headers.clone()))),
            },
        }
    };
    release(stream, requests);
    deadline_checked(outcome, started, timeout)
}

fn closed_by_client() -> Outcome {
    Outcome::error(Code::Cancelled, "stream closed by the client")
}

/// Resolves when a streaming call must stop early: Ruby closed it (or let
/// go of it), or its deadline passed.
async fn stopped(
    cancel: oneshot::Receiver<()>,
    tx: &mpsc::Sender<Bytes>,
    deadline: Option<tokio::time::Instant>,
) -> Outcome {
    let deadline = async {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        _ = cancel => closed_by_client(),
        _ = tx.closed() => closed_by_client(),
        _ = deadline => deadline_exceeded(),
    }
}

/// How long a call that ended early keeps reading responses so its request
/// body can finish; see [`release`].
const RELEASE_GRACE: Duration = Duration::from_secs(5);

/// The request side of a call, as its task sees it.
struct RequestControl {
    /// Ends the request stream, dropping what's buffered.
    abort: oneshot::Sender<()>,
    /// Resolves once the request stream has ended or been dropped.
    done: oneshot::Receiver<()>,
}

/// Let go of a call's response stream.  h2 resets a stream once nothing
/// holds it, but hyper's request body task holds it until the body ends.
/// If that task is waiting for send window from a server that's blocked
/// sending responses nobody reads any more, the window never comes and
/// neither does the reset: the server's handler would wait forever.  So
/// end the request stream, and keep reading (and discarding) responses
/// until it has, for `RELEASE_GRACE` at most.
fn release(stream: Streaming<Bytes>, requests: Option<RequestControl>) {
    let Some(RequestControl { abort, done }) = requests else {
        return;
    };
    let _ = abort.send(());
    tokio::spawn(async move {
        let mut stream = stream;
        let drain = async { while let Ok(Some(_)) = stream.message().await {} };
        let _ = tokio::time::timeout(RELEASE_GRACE, async {
            tokio::select! {
                _ = done => {}
                _ = drain => {}
            }
        })
        .await;
    });
}

/// Request messages written from Ruby (`ResponseStream#push`).  The first
/// poll means the connection is up and the call has started, so it's
/// announced on `ready`: Ruby waits for that before reading its requests,
/// and a call that fails before then has consumed none and can be retried.
struct RequestStream {
    rx: mpsc::Receiver<Bytes>,
    ready: Option<mpsc::Sender<()>>,
    /// See [`RequestControl`].
    abort: Option<oneshot::Receiver<()>>,
    done: Option<oneshot::Sender<()>>,
    /// Set by `ResponseStream#close` before it drops its sender, so the
    /// wake-up that causes isn't taken for the end of the requests.
    closed: Arc<AtomicBool>,
}

impl Stream for RequestStream {
    type Item = Outgoing;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Outgoing>> {
        if let Some(ready) = self.ready.take() {
            let _ = ready.try_send(());
        }
        if self.done.is_none() {
            return Poll::Ready(None);
        }
        // Sent, or dropped with the task: either way the call is over, so
        // reset the stream rather than end it, or the server would take
        // the requests it has as complete.
        let aborted = self.closed.load(Ordering::Acquire)
            || self
                .abort
                .as_mut()
                .is_some_and(|abort| Pin::new(abort).poll(cx).is_ready());
        if aborted {
            self.abort = None;
            self.rx.close();
            self.done = None;
            return Poll::Ready(Some(None));
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(message)) => Poll::Ready(Some(Some(message))),
            Poll::Ready(None) => {
                self.done = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Aborts the task if the waiting Ruby thread gives up on it, which
/// resets the HTTP/2 stream so the server sees the call cancelled.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Wait for `task` without the GVL.  Interrupts (`Thread#raise`,
/// `Timeout`, signals) are handled by Ruby; if one raises, the task is
/// aborted.
fn wait<T: Send + 'static>(ruby: &Ruby, mut task: AbortOnDrop<T>) -> Result<T, Error> {
    loop {
        let cancel = Cancel::default();
        let done = without_gvl(&cancel, || {
            block_on(async {
                tokio::select! {
                    biased;
                    result = &mut task.0 => Some(result),
                    _ = cancel.cancelled() => None,
                }
            })
        });
        match done {
            Some(Some(Ok(value))) => return Ok(value),
            Some(Some(Err(e))) => {
                return Err(Error::new(
                    ruby.exception_runtime_error(),
                    format!("tonic-rails client call failed: {e}"),
                ));
            }
            _ => ruby.thread_check_ints()?,
        }
    }
}

// ─── Ruby class ───────────────────────────────────────────────────

/// `TonicRails::Native::Channel` — one HTTP/2 connection (per process) to
/// a gRPC server, shared by every thread.
#[derive(magnus::TypedData)]
#[magnus(class = "TonicRails::Native::Channel", free_immediately)]
pub struct NativeChannel {
    endpoint: Endpoint,
    max_message_size: usize,
    /// The connection for the process that created it.
    channel: Mutex<Option<(u32, Channel)>>,
}

impl magnus::typed_data::DataTypeFunctions for NativeChannel {}

impl Drop for NativeChannel {
    fn drop(&mut self) {
        if let Some((pid, channel)) = self.channel.get_mut().take()
            && pid != std::process::id()
        {
            // Its tasks ran on the parent's runtime: see the module docs.
            std::mem::forget(channel);
        }
    }
}

/// `[ca_pem, cert_pem, key_pem, server_name]`, each optional.
type TlsOptions = (Option<String>, Option<String>, Option<String>, Option<String>);

impl NativeChannel {
    /// `uri` is `http://host:port` or `https://host:port`.  With `tls`
    /// and no CA, the system's root certificates are trusted.
    pub fn new(
        ruby: &Ruby,
        uri: String,
        max_message_size: usize,
        connect_timeout_ms: u64,
        tls: Option<TlsOptions>,
    ) -> Result<Self, Error> {
        let argument_error = |message: String| Error::new(ruby.exception_arg_error(), message);
        let mut endpoint = Endpoint::from_shared(uri.clone())
            .map_err(|e| argument_error(format!("invalid gRPC address {uri}: {e}")))?
            .connect_timeout(Duration::from_millis(connect_timeout_ms))
            .tcp_nodelay(true)
            .initial_stream_window_size(STREAM_WINDOW)
            .initial_connection_window_size(CONNECTION_WINDOW)
            .user_agent(concat!("tonic-rails/", env!("CARGO_PKG_VERSION")))
            .map_err(|e| argument_error(e.to_string()))?;
        if let Some((ca, cert, key, server_name)) = tls {
            let mut config = ClientTlsConfig::new();
            config = match ca {
                Some(ca) => config.ca_certificate(Certificate::from_pem(ca)),
                None => config.ca_certificate(Certificate::from_pem(system_roots().map_err(argument_error)?)),
            };
            if let (Some(cert), Some(key)) = (cert, key) {
                config = config.identity(Identity::from_pem(cert, key));
            }
            if let Some(server_name) = server_name {
                config = config.domain_name(server_name);
            }
            endpoint = endpoint
                .tls_config(config)
                .map_err(|e| argument_error(format!("invalid TLS configuration: {}", source_chain(&e))))?;
        }
        Ok(Self {
            endpoint,
            max_message_size,
            channel: Mutex::new(None),
        })
    }

    /// This process's connection, created (lazily connecting) on first use.
    fn channel(&self, runtime: &Handle) -> Channel {
        let pid = std::process::id();
        let mut slot = self.channel.lock();
        if let Some((owner, channel)) = slot.as_ref()
            && *owner == pid
        {
            return channel.clone();
        }
        let _entered = runtime.enter();
        let channel = self.endpoint.connect_lazy();
        if let Some((_, inherited)) = slot.replace((pid, channel.clone())) {
            std::mem::forget(inherited);
        }
        channel
    }

    /// Everything a call needs, from its Ruby arguments.
    fn call_args(
        ruby: &Ruby,
        this: &Self,
        path: String,
        request: RequestBody,
        metadata: RHash,
        timeout_ms: Option<u64>,
    ) -> Result<(Handle, CallArgs), Error> {
        let path = PathAndQuery::try_from(path).map_err(|e| {
            Error::new(
                ruby.exception_arg_error(),
                format!("invalid gRPC method path: {e}"),
            )
        })?;
        let metadata = header_map(ruby, metadata)?;
        let runtime = runtime().map_err(|e| {
            Error::new(
                ruby.exception_runtime_error(),
                format!("tonic-rails client: can't start a tokio runtime: {e}"),
            )
        })?;
        let args = CallArgs {
            channel: this.channel(&runtime),
            path,
            request,
            metadata,
            timeout: timeout_ms.map(Duration::from_millis),
            max_message_size: this.max_message_size,
        };
        Ok((runtime, args))
    }

    /// Make a unary call.  Returns the array described in
    /// [`Outcome::into_ruby`]; errors come back as a status, not raised.
    pub fn unary(
        ruby: &Ruby,
        this: &Self,
        path: String,
        request: RString,
        metadata: RHash,
        timeout_ms: Option<u64>,
    ) -> Result<RArray, Error> {
        let (runtime, args) = Self::call_args(ruby, this, path, single(request), metadata, timeout_ms)?;
        let timeout = args.timeout;
        let task = AbortOnDrop(runtime.spawn(with_timeout(unary(args), timeout)));
        wait(ruby, task)?.into_ruby(ruby)
    }

    /// Start a server-streaming call.  It runs in the background, a few
    /// messages ahead of the reader.
    pub fn server_streaming(
        ruby: &Ruby,
        this: &Self,
        path: String,
        request: RString,
        metadata: RHash,
        timeout_ms: Option<u64>,
    ) -> Result<ResponseStream, Error> {
        let (runtime, args) = Self::call_args(ruby, this, path, single(request), metadata, timeout_ms)?;
        Ok(ResponseStream::start(&runtime, args, None))
    }

    /// Start a client- or bidirectional-streaming call.  Write requests
    /// with `#push` once `#ready` returns true, then `#close_send`; read
    /// responses as for a server stream.
    pub fn streaming(
        ruby: &Ruby,
        this: &Self,
        path: String,
        metadata: RHash,
        timeout_ms: Option<u64>,
    ) -> Result<ResponseStream, Error> {
        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        let (ready_tx, ready_rx) = mpsc::channel(1);
        let (abort_tx, abort_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let closed = Arc::new(AtomicBool::new(false));
        let request = Box::pin(RequestStream {
            rx,
            ready: Some(ready_tx),
            abort: Some(abort_rx),
            done: Some(done_tx),
            closed: closed.clone(),
        });
        let (runtime, args) = Self::call_args(ruby, this, path, request, metadata, timeout_ms)?;
        let control = RequestControl {
            abort: abort_tx,
            done: done_rx,
        };
        Ok(ResponseStream::start(
            &runtime,
            args,
            Some((
                Requests {
                    tx,
                    ready: ready_rx,
                    closed,
                },
                control,
            )),
        ))
    }
}

/// A single request message, as a request body.
fn single(request: RString) -> RequestBody {
    Box::pin(tokio_stream::once(Some(Bytes::copy_from_slice(unsafe {
        request.as_slice()
    }))))
}

// ─── Response stream ──────────────────────────────────────────────

/// Messages buffered between Ruby and the connection, in either
/// direction.  Past this, HTTP/2 flow control holds back the sender.
const STREAM_BUFFER: usize = 8;

/// The request side of a client- or bidirectional-streaming call.
struct Requests {
    tx: mpsc::Sender<Bytes>,
    ready: mpsc::Receiver<()>,
    closed: Arc<AtomicBool>,
}

/// `TonicRails::Native::ResponseStream` — the messages of a streaming
/// call, then its status; for client and bidirectional streaming, also
/// where its requests are written.
#[derive(magnus::TypedData)]
#[magnus(class = "TonicRails::Native::ResponseStream", free_immediately)]
pub struct ResponseStream {
    /// The process that started the call.
    pid: u32,
    rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
    requests: Mutex<Option<mpsc::Sender<Bytes>>>,
    /// See [`RequestStream`].
    closed: Option<Arc<AtomicBool>>,
    ready: Mutex<Option<mpsc::Receiver<()>>>,
    outcome: Arc<Mutex<Option<Outcome>>>,
    /// Stops the call's task, sent or dropped.
    cancel: Mutex<Option<oneshot::Sender<()>>>,
}

impl magnus::typed_data::DataTypeFunctions for ResponseStream {}

impl ResponseStream {
    /// The task runs on its own until the call ends; `#close` (or
    /// dropping this) stops it early.
    fn start(runtime: &Handle, args: CallArgs, requests: Option<(Requests, RequestControl)>) -> Self {
        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let (requests, control) = match requests {
            Some((requests, control)) => (Some(requests), Some(control)),
            None => (None, None),
        };
        let outcome = Arc::new(Mutex::new(None));
        let slot = outcome.clone();
        runtime.spawn(async move {
            let result = read_responses(args, tx.clone(), cancel_rx, control).await;
            // Set before `tx` is dropped, so the reader finds it at the end
            // of the stream.
            *slot.lock() = Some(result);
            drop(tx);
        });
        let (requests, ready, closed) = match requests {
            Some(Requests { tx, ready, closed }) => (Some(tx), Some(ready), Some(closed)),
            None => (None, None, None),
        };
        Self {
            pid: std::process::id(),
            rx: Mutex::new(Some(rx)),
            requests: Mutex::new(requests),
            closed,
            ready: Mutex::new(ready),
            outcome,
            cancel: Mutex::new(Some(cancel_tx)),
        }
    }

    /// Wait until requests can be written: true once the call has
    /// started, false if it ended first (see `#result`).
    pub fn ready(ruby: &Ruby, this: &Self) -> Result<bool, Error> {
        let Some(mut ready) = this.ready.lock().take() else {
            return Ok(false);
        };
        loop {
            match recv_blocking(&mut ready) {
                Wait::Ready(()) => return Ok(true),
                Wait::Closed => return Ok(false),
                Wait::Interrupted => {
                    if let Err(e) = ruby.thread_check_ints() {
                        this.ready.lock().replace(ready);
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Write a request message, waiting while the connection is behind.
    /// False if the call has ended, so the rest won't be sent.
    pub fn push(ruby: &Ruby, this: &Self, request: RString) -> Result<bool, Error> {
        // A clone, so `#close_send` from another thread doesn't wait on
        // this one.
        let Some(tx) = this.requests.lock().clone() else {
            return Ok(false);
        };
        let bytes = Bytes::copy_from_slice(unsafe { request.as_slice() });
        send_with_interrupts(ruby, &tx, bytes)
    }

    /// Send end-of-stream once the requests written so far have gone.
    pub fn close_send(&self) {
        drop(self.requests.lock().take());
    }
    /// Next raw message, or nil once the stream has ended (see `#result`).
    pub fn next(ruby: &Ruby, this: &Self) -> Result<Option<RString>, Error> {
        // Taken out of the mutex while blocked so `#close` from another
        // thread doesn't wait on it.
        let Some(mut rx) = this.rx.lock().take() else {
            return Ok(None);
        };
        loop {
            match recv_blocking(&mut rx) {
                Wait::Ready(bytes) => {
                    this.rx.lock().replace(rx);
                    return Ok(Some(ruby.str_from_slice(&bytes)));
                }
                Wait::Closed => return Ok(None),
                Wait::Interrupted => {
                    if let Err(e) = ruby.thread_check_ints() {
                        this.rx.lock().replace(rx);
                        return Err(e);
                    }
                }
            }
        }
    }

    /// How the call ended, as described in [`Outcome::into_ruby`], once
    /// `#next` has returned nil.
    pub fn result(ruby: &Ruby, this: &Self) -> Result<RArray, Error> {
        let outcome = this
            .outcome
            .lock()
            .take()
            .unwrap_or_else(|| Outcome::error(Code::Cancelled, "the stream was closed before it ended"));
        outcome.into_ruby(ruby)
    }

    /// Stop reading.  A call still running is cancelled on the server.
    pub fn close(&self) {
        if self.pid != std::process::id() {
            // Started by the parent: see the module docs.
            return;
        }
        if let Some(closed) = &self.closed {
            closed.store(true, Ordering::Release);
        }
        drop(self.cancel.lock().take());
        drop(self.rx.lock().take());
        drop(self.requests.lock().take());
    }
}

impl Drop for ResponseStream {
    fn drop(&mut self) {
        if self.pid != std::process::id() {
            // The task and channel belong to the parent's runtime.
            std::mem::forget(self.cancel.get_mut().take());
            std::mem::forget(self.rx.get_mut().take());
            std::mem::forget(self.requests.get_mut().take());
            std::mem::forget(self.ready.get_mut().take());
        }
    }
}

/// The system's root certificates as a PEM bundle, loaded on first use.
fn system_roots() -> Result<&'static [u8], String> {
    static ROOTS: OnceLock<Result<Vec<u8>, String>> = OnceLock::new();
    let roots = ROOTS.get_or_init(|| {
        let loaded = rustls_native_certs::load_native_certs();
        if loaded.certs.is_empty() {
            let cause = loaded.errors.first().map(|e| format!(" ({e})")).unwrap_or_default();
            return Err(format!(
                "no system root certificates found{cause}; install them (e.g. the ca-certificates package) or pass tls_ca_path"
            ));
        }
        let mut pem = Vec::new();
        for cert in loaded.certs {
            pem.extend_from_slice(b"-----BEGIN CERTIFICATE-----\n");
            let encoded = base64::engine::general_purpose::STANDARD.encode(cert.as_ref());
            for line in encoded.as_bytes().chunks(64) {
                pem.extend_from_slice(line);
                pem.push(b'\n');
            }
            pem.extend_from_slice(b"-----END CERTIFICATE-----\n");
        }
        Ok(pem)
    });
    roots.as_deref().map_err(Clone::clone)
}

/// Request metadata from a Hash of wire-form names and values (see
/// `Tonic::Rails::Metadata.encode`).
fn header_map(ruby: &Ruby, metadata: RHash) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::with_capacity(metadata.len());
    metadata.foreach(|name: String, value: String| {
        let invalid = || {
            Error::new(
                ruby.exception_arg_error(),
                format!("invalid gRPC metadata {name:?}"),
            )
        };
        let header = HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid())?;
        let value = HeaderValue::from_str(&value).map_err(|_| invalid())?;
        headers.append(header, value);
        Ok(ForEach::Continue)
    })?;
    Ok(headers)
}

/// tonic's errors wrap the useful message (e.g. "no valid private key")
/// a level or two down.
fn source_chain(error: &dyn std::error::Error) -> String {
    append_causes(error.to_string(), error.source())
}

/// Append each cause in the chain that the message doesn't already include.
fn append_causes(mut message: String, mut source: Option<&dyn std::error::Error>) -> String {
    while let Some(e) = source {
        let cause = e.to_string();
        if !message.contains(&cause) {
            message = format!("{message}: {cause}");
        }
        source = e.source();
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(call: impl Future<Output = Outcome>, timeout: Option<Duration>) -> Outcome {
        let runtime = Builder::new_current_thread().enable_time().build().unwrap();
        runtime.block_on(with_timeout(call, timeout))
    }

    async fn status(status: Status) -> Outcome {
        Outcome::status(status, None)
    }

    #[test]
    fn tonics_timeout_is_a_deadline_however_early_it_fires() {
        let outcome = run(
            status(Status::cancelled("Timeout expired")),
            Some(Duration::from_secs(60)),
        );
        assert_eq!(outcome.code, Code::DeadlineExceeded);
    }

    #[test]
    fn other_cancellations_stay_cancelled() {
        let outcome = run(
            status(Status::cancelled("client went away")),
            Some(Duration::from_secs(60)),
        );
        assert_eq!(outcome.code, Code::Cancelled);
        assert_eq!(outcome.message, "client went away");
    }

    struct Requests {
        tx: mpsc::Sender<Bytes>,
        abort: oneshot::Sender<()>,
        closed: Arc<AtomicBool>,
        done: oneshot::Receiver<()>,
        stream: RequestStream,
    }

    fn requests() -> Requests {
        let (tx, rx) = mpsc::channel(4);
        let (abort, abort_rx) = oneshot::channel();
        let (done_tx, done) = oneshot::channel();
        let closed = Arc::new(AtomicBool::new(false));
        let stream = RequestStream {
            rx,
            ready: None,
            abort: Some(abort_rx),
            done: Some(done_tx),
            closed: closed.clone(),
        };
        Requests {
            tx,
            abort,
            closed,
            done,
            stream,
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        Builder::new_current_thread().build().unwrap().block_on(future)
    }

    #[test]
    fn request_stream_ends_when_its_sender_closes() {
        use tokio_stream::StreamExt;
        let Requests {
            tx,
            abort: _abort,
            done,
            mut stream,
            ..
        } = requests();
        tx.try_send(Bytes::from_static(b"a")).unwrap();
        drop(tx);
        block_on(async {
            assert_eq!(stream.next().await, Some(Some(Bytes::from_static(b"a"))));
            assert_eq!(stream.next().await, None);
            assert!(done.await.is_err(), "done fires when the stream ends");
        });
    }

    #[test]
    fn request_stream_resets_when_aborted_or_closed() {
        use tokio_stream::StreamExt;
        let Requests {
            tx,
            abort,
            mut stream,
            ..
        } = requests();
        tx.try_send(Bytes::from_static(b"dropped")).unwrap();
        abort.send(()).unwrap();
        block_on(async {
            assert_eq!(
                stream.next().await,
                Some(None),
                "a reset, not the buffered message"
            );
            assert_eq!(stream.next().await, None);
        });
        assert!(
            tx.try_send(Bytes::new()).is_err(),
            "writers see the call has ended"
        );

        // `#close` drops the sender too; that must not read as a clean end.
        let Requests {
            tx,
            abort: _abort,
            closed,
            mut stream,
            ..
        } = requests();
        closed.store(true, Ordering::Release);
        drop(tx);
        block_on(async { assert_eq!(stream.next().await, Some(None)) });
    }

    #[test]
    fn our_timer_is_a_deadline() {
        let outcome = run(std::future::pending(), Some(Duration::from_millis(10)));
        assert_eq!(outcome.code, Code::DeadlineExceeded);
    }
}
