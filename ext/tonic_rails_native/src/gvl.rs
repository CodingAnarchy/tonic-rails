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

use magnus::{Error, RString, Ruby, Value, prelude::*, typed_data::DataTypeFunctions, value::Opaque};
use parking_lot::Mutex;
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
pub fn block_on<F: Future>(fut: F) -> F::Output {
    struct ThreadWaker(std::thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        if let Poll::Ready(value) = fut.as_mut().poll(&mut cx) {
            return value;
        }
        std::thread::park();
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

/// Messages from the Ruby handler to the tokio response body.
pub enum StreamMessage {
    /// One encoded protobuf response message.
    Data(Vec<u8>),
    /// End of stream: `None` = OK, `Some((code, message))` = error.
    End(Option<(u32, String)>),
}

/// `TonicRails::Native::StreamSender` — pushes encoded response messages
/// to the client with backpressure.
#[derive(magnus::TypedData)]
#[magnus(class = "TonicRails::Native::StreamSender", free_immediately)]
pub struct StreamSender {
    tx: mpsc::Sender<StreamMessage>,
}

impl StreamSender {
    /// Raises `IOError` if the client has gone away.
    pub fn push(ruby: &Ruby, this: &Self, data: RString) -> Result<(), Error> {
        let bytes = unsafe { data.as_slice() }.to_vec();
        if send_with_interrupts(ruby, &this.tx, StreamMessage::Data(bytes))? {
            Ok(())
        } else {
            Err(Error::new(
                ruby.exception_io_error(),
                "response stream closed by peer",
            ))
        }
    }
}

impl DataTypeFunctions for StreamSender {}

// ─── Request stream ───────────────────────────────────────────────

/// `TonicRails::Native::RequestReader` — yields raw request messages.
#[derive(magnus::TypedData)]
#[magnus(class = "TonicRails::Native::RequestReader", free_immediately)]
pub struct RequestReader {
    rx: Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
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
    pub fn new(rx: mpsc::Receiver<Vec<u8>>, error: Arc<RequestError>) -> Self {
        Self {
            rx: Mutex::new(Some(rx)),
            error,
        }
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
}

/// Re-acquire the GVL and run the Ruby handler for `ctx`.
/// Called from a Ruby thread that has released the GVL.
pub fn dispatch(ctx: Box<DispatchContext>) {
    unsafe extern "C" fn with_gvl(data: *mut c_void) -> *mut c_void {
        let ctx = unsafe { Box::from_raw(data as *mut DispatchContext) };
        let response_tx = ctx.response_tx.clone();
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| run_handler(*ctx))) {
            let msg = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            eprintln!("tonic-rails: panic in dispatch: {msg}");
            let _ = response_tx.try_send(StreamMessage::End(Some((
                tonic::Code::Internal as u32,
                "internal error".into(),
            ))));
        }
        std::ptr::null_mut()
    }

    unsafe {
        rb_thread_call_with_gvl(Some(with_gvl), Box::into_raw(ctx) as *mut c_void);
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
    } = ctx;

    let end = match call_handler(
        &ruby,
        callback,
        &path,
        metadata,
        timeout_ms,
        &response_tx,
        request_rx,
        request_error,
    ) {
        Ok(()) => StreamMessage::End(None),
        Err(e) => StreamMessage::End(Some(error_status(&e))),
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
) -> Result<(), Error> {
    let md = ruby.hash_new_capa(metadata.len());
    for (k, v) in metadata {
        md.aset(ruby.str_new(&k), ruby.str_new(&v))?;
    }
    let call = ruby.hash_new_capa(3);
    call.aset(
        ruby.to_symbol("reader"),
        RequestReader::new(request_rx, request_error),
    )?;
    call.aset(
        ruby.to_symbol("sender"),
        StreamSender {
            tx: response_tx.clone(),
        },
    )?;
    if let Some(ms) = timeout_ms {
        call.aset(ruby.to_symbol("timeout_ms"), ms as u64)?;
    }

    let callback = ruby.get_inner(callback);
    let result: Value = callback.funcall("call", (ruby.str_new(path), md, call))?;

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
fn error_status(e: &Error) -> (u32, String) {
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
        eprintln!(
            "tonic-rails: unhandled exception in dispatch: {}",
            crate::framing::sanitize_for_log(&description)
        );
    }
    (tonic::Code::Internal as u32, "internal error".into())
}
