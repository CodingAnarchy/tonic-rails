//! GVL bridge: releasing the GVL while blocked, re-acquiring it to run
//! Ruby handlers, and the Ruby-visible stream objects.
//!
//! Every blocking wait goes through [`without_gvl`], which uses
//! `rb_nogvl(..., RB_NOGVL_INTR_FAIL)`.  Unlike `rb_thread_call_without_gvl`
//! this never raises (longjmps) through Rust frames: if the thread is
//! interrupted we return [`Wait::Interrupted`], the caller runs
//! `thread_check_ints` (which may run a trap handler or raise a proper
//! Ruby exception through Magnus), and then retries.

use std::ffi::c_void;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use crate::log::{Level, LogSink};
use crate::sync::{Condvar, Mutex};
use magnus::{
    Error, RHash, RString, Ruby, Value, prelude::*, r_hash::ForEach, typed_data::DataTypeFunctions,
    value::Opaque,
};
use rb_sys::{RB_NOGVL_INTR_FAIL, rb_nogvl, rb_thread_call_with_gvl};
use tokio::sync::{Notify, mpsc};

// ─── Blocking primitives ──────────────────────────────────────────

/// Cancellation token tripped by Ruby's unblocking function when the
/// waiting thread is interrupted (signal, `Thread#raise`, `Thread#kill`).
#[derive(Default)]
pub struct Cancel {
    flag: AtomicBool,
    notify: Notify,
}

impl Cancel {
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
        // notify_one stores a permit if nobody is waiting yet, so a
        // cancel that races ahead of `cancelled()` is never lost.
        self.notify.notify_one();
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            self.notify.notified().await;
        }
    }
}

unsafe extern "C" fn cancel_ubf(data: *mut c_void) {
    let cancel = unsafe { &*(data as *const Cancel) };
    cancel.cancel();
}

/// Run `f` with the GVL released.  Returns `None` without running `f` if
/// an interrupt was already pending.  `f` must return promptly once
/// `cancel` is tripped.
pub fn without_gvl<F, R>(cancel: &Cancel, f: F) -> Option<R>
where
    F: FnOnce() -> R,
{
    struct Call<F, R> {
        f: Option<F>,
        result: Option<std::thread::Result<R>>,
    }

    unsafe extern "C" fn trampoline<F: FnOnce() -> R, R>(data: *mut c_void) -> *mut c_void {
        let call = unsafe { &mut *(data as *mut Call<F, R>) };
        if let Some(f) = call.f.take() {
            call.result = Some(catch_unwind(AssertUnwindSafe(f)));
        }
        std::ptr::null_mut()
    }

    let mut call = Call {
        f: Some(f),
        result: None,
    };
    unsafe {
        rb_nogvl(
            Some(trampoline::<F, R>),
            &mut call as *mut Call<F, R> as *mut c_void,
            Some(cancel_ubf),
            cancel as *const Cancel as *mut c_void,
            RB_NOGVL_INTR_FAIL as i32,
        );
    }
    match call.result {
        Some(Ok(value)) => Some(value),
        Some(Err(panic)) => resume_unwind(panic),
        None => None,
    }
}

/// Minimal single-future executor that parks the current (non-tokio)
/// thread.  tokio's channel and `Notify` futures are runtime-agnostic.
///
/// It parks on a condition variable rather than
/// `std::thread::park`, which on macOS uses libdispatch: libdispatch
/// traps in the child of a multi-threaded process, and the client blocks
/// here in processes forked after it made calls.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    #[derive(Default)]
    struct Signal {
        woken: Mutex<bool>,
        condvar: Condvar,
    }
    impl Wake for Signal {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            *self.woken.lock() = true;
            self.condvar.notify_one();
        }
    }

    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        if let Poll::Ready(value) = fut.as_mut().poll(&mut cx) {
            return value;
        }
        let mut woken = signal.woken.lock();
        while !*woken {
            woken = signal.condvar.wait(woken);
        }
        *woken = false;
    }
}

pub enum Wait<T> {
    Ready(T),
    Closed,
    Interrupted,
}

/// Receive from a tokio channel, releasing the GVL only if we must block.
pub fn recv_blocking<T>(rx: &mut mpsc::Receiver<T>) -> Wait<T> {
    match rx.try_recv() {
        Ok(value) => return Wait::Ready(value),
        Err(mpsc::error::TryRecvError::Disconnected) => return Wait::Closed,
        Err(mpsc::error::TryRecvError::Empty) => {}
    }
    let cancel = Cancel::default();
    let received = without_gvl(&cancel, || {
        block_on(async {
            tokio::select! {
                biased;
                value = rx.recv() => Some(value),
                _ = cancel.cancelled() => None,
            }
        })
    });
    match received {
        Some(Some(Some(value))) => Wait::Ready(value),
        Some(Some(None)) => Wait::Closed,
        _ => Wait::Interrupted,
    }
}

/// Send into a tokio channel, releasing the GVL only if we must block
/// (channel full — the client is consuming slowly).  On `Interrupted`
/// the message is handed back for a retry.
pub fn send_blocking<T>(tx: &mpsc::Sender<T>, msg: T) -> Result<(), Option<T>> {
    let msg = match tx.try_send(msg) {
        Ok(()) => return Ok(()),
        Err(mpsc::error::TrySendError::Closed(_)) => return Err(None),
        Err(mpsc::error::TrySendError::Full(msg)) => msg,
    };
    let cancel = Cancel::default();
    let mut pending = Some(msg);
    let sent = without_gvl(&cancel, || {
        block_on(async {
            let permit = tokio::select! {
                biased;
                permit = tx.reserve() => permit.ok(),
                _ = cancel.cancelled() => return None,
            };
            Some(permit.map(|p| p.send(pending.take().expect("message present"))))
        })
    });
    match sent {
        Some(Some(Some(()))) => Ok(()),
        Some(Some(None)) => Err(None),
        _ => Err(pending),
    }
}

/// `send_blocking`, retrying after Ruby has processed any interrupt.
/// Returns `Ok(false)` if the receiver is gone.
pub fn send_with_interrupts<T>(ruby: &Ruby, tx: &mpsc::Sender<T>, mut msg: T) -> Result<bool, Error> {
    loop {
        match send_blocking(tx, msg) {
            Ok(()) => return Ok(true),
            Err(None) => return Ok(false),
            Err(Some(back)) => {
                ruby.thread_check_ints()?;
                msg = back;
            }
        }
    }
}

// ─── Response stream ──────────────────────────────────────────────

/// gRPC metadata as (lower-case name, value) pairs; `-bin` values are
/// already base64-encoded.
pub type Metadata = Vec<(String, String)>;

/// Messages from the Ruby handler to the tokio response body.
pub enum StreamMessage {
    /// Response headers (initial metadata).  Only ever the first message.
    Headers(Metadata),
    /// One encoded protobuf response message.
    Data(Vec<u8>),
    /// End of stream: `status` None = OK, `Some((code, message))` = error,
    /// plus trailing metadata.
    End {
        status: Option<(u32, String)>,
        trailers: Metadata,
    },
}

impl StreamMessage {
    pub fn error(code: tonic::Code, message: &str) -> Self {
        StreamMessage::End {
            status: Some((code as u32, message.to_owned())),
            trailers: Vec::new(),
        }
    }
}

/// Convert a Ruby Hash of String => String into metadata pairs.
fn metadata_from_hash(hash: RHash) -> Result<Metadata, Error> {
    let mut pairs = Vec::with_capacity(hash.len());
    hash.foreach(|name: String, value: String| {
        pairs.push((name, value));
        Ok(ForEach::Continue)
    })?;
    Ok(pairs)
}

/// `TonicRails::Native::StreamSender` — pushes encoded response messages
/// (and response metadata) to the client with backpressure.
#[derive(magnus::TypedData)]
#[magnus(class = "TonicRails::Native::StreamSender", free_immediately)]
pub struct StreamSender {
    tx: mpsc::Sender<StreamMessage>,
    trailers: Arc<Mutex<Metadata>>,
    started: AtomicBool,
}

impl StreamSender {
    fn closed(ruby: &Ruby) -> Error {
        Error::new(ruby.exception_io_error(), "response stream closed by peer")
    }

    /// Raises `IOError` if the client has gone away.
    pub fn push(ruby: &Ruby, this: &Self, data: RString) -> Result<(), Error> {
        this.started.store(true, Ordering::Release);
        let bytes = unsafe { data.as_slice() }.to_vec();
        if send_with_interrupts(ruby, &this.tx, StreamMessage::Data(bytes))? {
            Ok(())
        } else {
            Err(Self::closed(ruby))
        }
    }

    /// Send response headers.  Must come before any message.
    pub fn send_headers(ruby: &Ruby, this: &Self, headers: RHash) -> Result<(), Error> {
        if this.started.swap(true, Ordering::AcqRel) {
            return Err(Error::new(
                ruby.exception_runtime_error(),
                "response headers must be sent before any response message",
            ));
        }
        let headers = metadata_from_hash(headers)?;
        if send_with_interrupts(ruby, &this.tx, StreamMessage::Headers(headers))? {
            Ok(())
        } else {
            Err(Self::closed(ruby))
        }
    }

    /// Add trailing metadata, sent with the final status.
    pub fn add_trailers(&self, trailers: RHash) -> Result<(), Error> {
        let trailers = metadata_from_hash(trailers)?;
        self.trailers.lock().extend(trailers);
        Ok(())
    }
}

impl DataTypeFunctions for StreamSender {}

// ─── Request stream ───────────────────────────────────────────────

/// `TonicRails::Native::RequestReader` — yields raw request messages.
#[derive(magnus::TypedData)]
#[magnus(class = "TonicRails::Native::RequestReader", free_immediately)]
pub struct RequestReader {
    /// Shared with the dispatcher, which closes it once the handler
    /// returns so the body-reading task never outlives the call.
    rx: Arc<Mutex<Option<mpsc::Receiver<Vec<u8>>>>>,
    error: Arc<RequestError>,
}

/// Set by the body-reading task when the request stream is invalid, so
/// the reader can tell an error-closed channel from a clean end of stream.
#[derive(Default)]
pub struct RequestError {
    code: AtomicU32,
    message: Mutex<&'static str>,
}

impl RequestError {
    pub fn set(&self, code: tonic::Code, message: &'static str) {
        *self.message.lock() = message;
        self.code.store(code as u32, Ordering::Release);
    }

    fn get(&self) -> Option<(u32, &'static str)> {
        match self.code.load(Ordering::Acquire) {
            0 => None,
            code => Some((code, *self.message.lock())),
        }
    }
}

impl RequestReader {
    fn new(rx: Arc<Mutex<Option<mpsc::Receiver<Vec<u8>>>>>, error: Arc<RequestError>) -> Self {
        Self { rx, error }
    }

    /// Next raw message, or nil at end of stream.  Check `#error` after nil.
    pub fn next(ruby: &Ruby, this: &Self) -> Result<Option<RString>, Error> {
        // Taken out of the mutex while blocked so a concurrent caller
        // (misuse) sees end-of-stream rather than deadlocking.
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

    /// `[code, message]` if the request stream ended with an error.
    pub fn error(&self) -> Option<(u32, &'static str)> {
        self.error.get()
    }
}

impl DataTypeFunctions for RequestReader {}

// ─── Dispatch into Ruby ───────────────────────────────────────────

/// One RPC handed from tokio to a Ruby dispatch thread.
pub struct DispatchContext {
    pub callback: Opaque<Value>,
    pub path: String,
    pub metadata: Vec<(String, String)>,
    pub timeout_ms: Option<u128>,
    pub response_tx: mpsc::Sender<StreamMessage>,
    pub request_rx: mpsc::Receiver<Vec<u8>>,
    pub request_error: Arc<RequestError>,
    pub log: LogSink,
    pub peer: Peer,
}

/// The client connection: remote address and, with mutual TLS, its
/// certificate chain (DER, leaf first).
#[derive(Default)]
pub struct Peer {
    pub address: Option<String>,
    pub certificates: Vec<Vec<u8>>,
}

/// Bounds on how long one thread keeps the GVL to drain a busy queue.
/// The time bound keeps CPU-heavy handlers from hurting tail latency:
/// only cheap requests get batched.
const MAX_BATCH: usize = 16;
const MAX_BATCH_TIME: std::time::Duration = std::time::Duration::from_micros(500);

/// Re-acquire the GVL and run the Ruby handler for `first`, then keep
/// the GVL to run requests already waiting in `queue` (within
/// `MAX_BATCH` / `MAX_BATCH_TIME`).  Each GVL handoff costs a thread wake-up, so draining a
/// busy queue under one acquisition raises throughput.  Handlers that
/// block on I/O still release the GVL internally, so this does not
/// serialise I/O-bound work.  Called from a thread that has released
/// the GVL.
pub fn dispatch(first: Box<DispatchContext>, queue: &crossbeam_channel::Receiver<Box<DispatchContext>>) {
    struct Batch<'a> {
        first: Option<Box<DispatchContext>>,
        queue: &'a crossbeam_channel::Receiver<Box<DispatchContext>>,
    }

    unsafe extern "C" fn with_gvl(data: *mut c_void) -> *mut c_void {
        let batch = unsafe { &mut *(data as *mut Batch<'_>) };
        let start = std::time::Instant::now();
        let mut next = batch.first.take();
        let mut handled = 0;
        while let Some(ctx) = next {
            run_guarded(*ctx);
            handled += 1;
            next = if handled < MAX_BATCH && start.elapsed() < MAX_BATCH_TIME {
                batch.queue.try_recv().ok()
            } else {
                None
            };
        }
        std::ptr::null_mut()
    }

    let mut batch = Batch {
        first: Some(first),
        queue,
    };
    unsafe {
        rb_thread_call_with_gvl(Some(with_gvl), &mut batch as *mut Batch<'_> as *mut c_void);
    }
}

fn run_guarded(ctx: DispatchContext) {
    let response_tx = ctx.response_tx.clone();
    let log = ctx.log.clone();
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| run_handler(ctx))) {
        let msg = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("unknown panic");
        log.log(Level::Error, &format!("panic in dispatch: {msg}"));
        let _ = response_tx.try_send(StreamMessage::error(tonic::Code::Internal, "internal error"));
    }
}

fn run_handler(ctx: DispatchContext) {
    let ruby = Ruby::get().expect("dispatch runs on a Ruby thread with the GVL");
    let DispatchContext {
        callback,
        path,
        metadata,
        timeout_ms,
        response_tx,
        request_rx,
        request_error,
        log,
        peer,
    } = ctx;

    let trailers = Arc::new(Mutex::new(Metadata::new()));
    let result = call_handler(
        &ruby,
        callback,
        &path,
        metadata,
        timeout_ms,
        &response_tx,
        request_rx,
        request_error,
        trailers.clone(),
        &peer,
    );
    let end = StreamMessage::End {
        status: result.err().map(|e| error_status(&e, &log)),
        trailers: std::mem::take(&mut *trailers.lock()),
    };
    // Ignore failures: the client may already be gone.
    let _ = send_with_interrupts(&ruby, &response_tx, end);
}

#[allow(clippy::too_many_arguments)]
fn call_handler(
    ruby: &Ruby,
    callback: Opaque<Value>,
    path: &str,
    metadata: Vec<(String, String)>,
    timeout_ms: Option<u128>,
    response_tx: &mpsc::Sender<StreamMessage>,
    request_rx: mpsc::Receiver<Vec<u8>>,
    request_error: Arc<RequestError>,
    trailers: Arc<Mutex<Metadata>>,
    peer: &Peer,
) -> Result<(), Error> {
    let md = ruby.hash_new_capa(metadata.len());
    for (k, v) in metadata {
        md.aset(ruby.str_new(&k), ruby.str_new(&v))?;
    }
    let request_rx = Arc::new(Mutex::new(Some(request_rx)));
    let call = ruby.hash_new_capa(3);
    call.aset(
        ruby.to_symbol("reader"),
        RequestReader::new(request_rx.clone(), request_error),
    )?;
    call.aset(
        ruby.to_symbol("sender"),
        StreamSender {
            tx: response_tx.clone(),
            trailers,
            started: AtomicBool::new(false),
        },
    )?;
    if let Some(ms) = timeout_ms {
        call.aset(ruby.to_symbol("timeout_ms"), ms as u64)?;
    }
    if let Some(address) = &peer.address {
        call.aset(ruby.to_symbol("peer_address"), ruby.str_new(address))?;
    }
    if !peer.certificates.is_empty() {
        let certificates = ruby.ary_new_capa(peer.certificates.len());
        for der in &peer.certificates {
            certificates.push(ruby.str_from_slice(der))?;
        }
        call.aset(ruby.to_symbol("peer_certificates"), certificates)?;
    }

    let callback = ruby.get_inner(callback);
    let result = callback.funcall::<_, _, Value>("call", (ruby.str_new(path), md, call));
    // Unread request messages are no longer wanted: closing the channel
    // ends the body-reading task now instead of whenever Ruby's GC frees
    // the reader.
    drop(request_rx.lock().take());
    let result = result?;

    // A String return is a unary response; anything else means the
    // handler already streamed its messages through the sender.
    if let Some(bytes) = RString::from_value(result) {
        let data = unsafe { bytes.as_slice() }.to_vec();
        send_with_interrupts(ruby, response_tx, StreamMessage::Data(data))?;
    }
    Ok(())
}

/// Map a Ruby exception to a gRPC status.  Only exceptions carrying an
/// Integer `@code` (i.e. `Tonic::Rails::RpcError`) expose their message;
/// anything else becomes a generic INTERNAL so details never leak.
fn error_status(e: &Error, log: &LogSink) -> (u32, String) {
    if let Some(exc) = e.value() {
        let code = exc
            .funcall::<_, _, Option<i64>>("instance_variable_get", ("@code",))
            .ok()
            .flatten();
        if let Some(code) = code.filter(|c| (1..=16).contains(c)) {
            let message = exc.funcall::<_, _, String>("message", ()).unwrap_or_default();
            return (code as u32, message);
        }
        let description = exc
            .funcall::<_, _, String>("inspect", ())
            .unwrap_or_else(|_| "unknown exception".into());
        log.log(
            Level::Error,
            &format!("unhandled exception in dispatch: {description}"),
        );
    }
    (tonic::Code::Internal as u32, "internal error".into())
}
