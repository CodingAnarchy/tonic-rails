//! `TonicRails::Native::Channel`: outgoing gRPC calls over a tonic
//! [`Channel`], with message bytes passed through untouched.  Unary calls
//! return their result; server-streaming calls return a
//! `TonicRails::Native::ResponseStream` to read messages from.
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

use std::future::Future;
use std::io;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use base64::Engine;
use bytes::{Buf, BufMut, Bytes};
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue};
use magnus::{Error, RArray, RHash, RString, Ruby, r_hash::ForEach};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder, Streaming};
use tonic::metadata::MetadataMap;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request, Status, TimeoutExpired};

use crate::gvl::{Cancel, Wait, block_on, recv_blocking, without_gvl};
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
    type Encode = Bytes;
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

impl Encoder for RawCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
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

/// Bound a call by `timeout`, if there is one.  For a stream, that
/// covers every message, including time spent waiting on a slow reader.
async fn with_timeout(call: impl Future<Output = Outcome>, timeout: Option<Duration>) -> Outcome {
    let Some(timeout) = timeout else {
        return call.await;
    };
    let started = tokio::time::Instant::now();
    let outcome = tokio::time::timeout(timeout, call).await;
    match outcome {
        Ok(outcome) if !(outcome.local && started.elapsed() >= timeout) && !timeout_expired(&outcome) => {
            outcome
        }
        _ => Outcome::error(Code::DeadlineExceeded, "deadline exceeded"),
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

/// What every call needs: the method, one request message and its metadata.
struct CallArgs {
    channel: Channel,
    path: PathAndQuery,
    request: Bytes,
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
        tokio_stream::once(args.request),
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

/// Forward each response message to `tx` as it arrives.  A full `tx`
/// stops reading from the connection, so HTTP/2 flow control holds the
/// server back.
async fn server_stream(args: CallArgs, tx: mpsc::Sender<Bytes>) -> Outcome {
    let (headers, mut stream) = match start(args).await {
        Ok(started) => started,
        Err(outcome) => return *outcome,
    };
    loop {
        match stream.message().await {
            Ok(Some(message)) => {
                if tx.send(message).await.is_err() {
                    return Outcome::error(Code::Cancelled, "stream closed by the client");
                }
            }
            Ok(None) => break,
            Err(status) => return Outcome::status(status, Some(headers)),
        }
    }
    match finish(&mut stream, &headers).await {
        Ok(trailers) => Outcome::ok(None, headers, trailers),
        Err(outcome) => *outcome,
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
        request: RString,
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
        let request = Bytes::copy_from_slice(unsafe { request.as_slice() });
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
        let (runtime, args) = Self::call_args(ruby, this, path, request, metadata, timeout_ms)?;
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
        let (runtime, args) = Self::call_args(ruby, this, path, request, metadata, timeout_ms)?;
        let timeout = args.timeout;
        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        let outcome = Arc::new(Mutex::new(None));
        let slot = outcome.clone();
        let task = runtime.spawn(async move {
            let result = with_timeout(server_stream(args, tx.clone()), timeout).await;
            // Set before `tx` is dropped, so the reader finds it at the end
            // of the stream.
            *slot.lock() = Some(result);
            drop(tx);
        });
        Ok(ResponseStream {
            pid: std::process::id(),
            rx: Mutex::new(Some(rx)),
            outcome,
            task: Mutex::new(Some(AbortOnDrop(task))),
        })
    }
}

// ─── Response stream ──────────────────────────────────────────────

/// Messages read ahead of a slow reader.  Past this, HTTP/2 flow control
/// holds the server back.
const STREAM_BUFFER: usize = 8;

/// `TonicRails::Native::ResponseStream` — the messages of a
/// server-streaming call, then its status.
#[derive(magnus::TypedData)]
#[magnus(class = "TonicRails::Native::ResponseStream", free_immediately)]
pub struct ResponseStream {
    /// The process that started the call.
    pid: u32,
    rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
    outcome: Arc<Mutex<Option<Outcome>>>,
    task: Mutex<Option<AbortOnDrop<()>>>,
}

impl magnus::typed_data::DataTypeFunctions for ResponseStream {}

impl ResponseStream {
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
        drop(self.task.lock().take());
        drop(self.rx.lock().take());
    }
}

impl Drop for ResponseStream {
    fn drop(&mut self) {
        if self.pid != std::process::id() {
            // The task and channel belong to the parent's runtime.
            std::mem::forget(self.task.get_mut().take());
            std::mem::forget(self.rx.get_mut().take());
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

    #[test]
    fn our_timer_is_a_deadline() {
        let outcome = run(std::future::pending(), Some(Duration::from_millis(10)));
        assert_eq!(outcome.code, Code::DeadlineExceeded);
    }
}
