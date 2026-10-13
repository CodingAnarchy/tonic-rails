//! Raw-byte gRPC dispatch service.
//!
//! Every request that is not for the built-in health or reflection
//! services lands here.  Requests are validated, the handler is queued
//! for a Ruby dispatch thread *before* the body is read (so bidi RPCs can
//! interleave), and a spawned task feeds request messages to the handler
//! through a bounded channel — giving HTTP/2 flow-control backpressure.

use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use http::{HeaderValue, Request, Response};
use http_body_util::BodyExt;
use magnus::{Value, value::Opaque};
use tokio::sync::mpsc;
use tonic::Code;
use tonic::body::Body as TonicBody;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};

use crate::body::{ResponseBody, ServerStats};
use crate::framing::{MessageReader, append_metadata, parse_grpc_timeout, status_trailers};
use crate::gvl::{CancelRequests, DispatchContext, Peer, RequestError, StreamMessage};
use crate::log::Level;

/// Messages buffered per direction before backpressure applies.
const STREAM_BUFFER: usize = 32;

/// Request metadata larger than this (sum of names + values) is rejected.
const MAX_METADATA_BYTES: usize = 8 * 1024;

/// Headers consumed by the transport and not exposed to handlers.
fn is_reserved_header(name: &str) -> bool {
    matches!(
        name,
        "content-type" | "te" | "grpc-timeout" | "grpc-encoding" | "grpc-accept-encoding"
    )
}

pub struct DispatchInner {
    pub callback: Opaque<Value>,
    pub routes: HashSet<String>,
    pub request_tx: crossbeam_channel::Sender<Box<DispatchContext>>,
    pub stats: Arc<ServerStats>,
    pub max_message_size: usize,
    pub compression: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct DispatchService(pub Arc<DispatchInner>);

impl tower::Service<Request<axum::body::Body>> for DispatchService {
    type Response = Response<TonicBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<axum::body::Body>) -> Self::Future {
        let inner = self.0.clone();
        Box::pin(async move { Ok(inner.handle(req).await) })
    }
}

fn grpc_response(body: TonicBody, compressed: bool) -> Response<TonicBody> {
    let mut resp = Response::new(body);
    let headers = resp.headers_mut();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc"),
    );
    headers.insert("grpc-accept-encoding", HeaderValue::from_static("gzip,identity"));
    if compressed {
        headers.insert("grpc-encoding", HeaderValue::from_static("gzip"));
    }
    resp
}

impl DispatchInner {
    /// A Trailers-Only response (the status in the headers, no body) for a
    /// call rejected before it reached a handler.  Clients may retry those
    /// safely, and gRPC retries count on the form to tell them apart.
    fn reject(&self, path: &str, start: Instant, code: Code, message: &str) -> Response<TonicBody> {
        self.stats.record(path, code as u32, message, start, true);
        let mut response = grpc_response(TonicBody::empty(), false);
        response
            .headers_mut()
            .extend(status_trailers(code as u32, message));
        response
    }

    async fn handle(&self, req: Request<axum::body::Body>) -> Response<TonicBody> {
        let start = Instant::now();
        self.stats.total.fetch_add(1, Ordering::Relaxed);
        let path = req.uri().path().to_owned();
        let headers = req.headers();

        if req.method() != http::Method::POST {
            return self.reject(&path, start, Code::Unimplemented, "gRPC requires HTTP POST");
        }

        // application/grpc, optionally with a +proto / ;params suffix.
        // gRPC-Web and other framings are not supported.
        let content_type = headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let base = content_type.split(';').next().unwrap_or("").trim();
        if base != "application/grpc" && base != "application/grpc+proto" {
            return self.reject(&path, start, Code::Unimplemented, "unsupported content-type");
        }

        if !self.routes.contains(&path) {
            return self.reject(&path, start, Code::Unimplemented, "unknown method");
        }

        let gzip_request = match headers.get("grpc-encoding").map(|v| v.to_str().unwrap_or("")) {
            None | Some("identity") => false,
            Some("gzip") => true,
            Some(_) => return self.reject(&path, start, Code::Unimplemented, "unsupported grpc-encoding"),
        };

        let accepts_gzip = headers
            .get("grpc-accept-encoding")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(',').any(|e| e.trim() == "gzip"));
        let compress = accepts_gzip && self.compression.load(Ordering::Relaxed);

        let timeout = headers
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_grpc_timeout);

        let mut metadata_bytes = 0;
        let mut metadata = Vec::with_capacity(headers.len());
        for (name, value) in headers {
            let name = name.as_str();
            if is_reserved_header(name) {
                continue;
            }
            metadata_bytes += name.len() + value.len();
            // Non-UTF-8 values cannot be represented as Ruby Strings
            // faithfully; binary metadata uses `-bin` + base64 per spec.
            if let Ok(value) = value.to_str() {
                metadata.push((name.to_owned(), value.to_owned()));
            }
        }
        if metadata_bytes > MAX_METADATA_BYTES {
            return self.reject(
                &path,
                start,
                Code::ResourceExhausted,
                "request metadata too large",
            );
        }

        let (request_tx, request_rx) = mpsc::channel::<Vec<u8>>(STREAM_BUFFER);
        let (response_tx, response_rx) = mpsc::channel::<StreamMessage>(STREAM_BUFFER);
        let request_error = Arc::new(RequestError::default());
        // Dropped with this future if the client resets the stream before
        // the response headers; then moved into the response body.
        let cancel_requests = CancelRequests::new(request_error.clone());

        let ctx = Box::new(DispatchContext {
            callback: self.callback,
            path: path.clone(),
            metadata,
            timeout_ms: timeout.map(|t| t.as_millis()),
            response_tx,
            request_rx,
            request_error: request_error.clone(),
            log: self.stats.log.clone(),
            peer: peer_of(&req),
        });

        match self.request_tx.try_send(ctx) {
            Ok(()) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                return self.reject(
                    &path,
                    start,
                    Code::ResourceExhausted,
                    "server busy: dispatch queue full",
                );
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                return self.reject(&path, start, Code::Unavailable, "server shutting down");
            }
        }

        let max_message_size = self.max_message_size;
        tokio::spawn(feed_request(
            req.into_body(),
            request_tx,
            request_error,
            max_message_size,
            gzip_request,
        ));

        // Response headers wait for the handler's first output, so it can
        // set initial metadata; unary calls send headers with the response
        // as usual.  The client's deadline applies while we wait.
        let mut response_rx = response_rx;
        let mut deadline = timeout.map(|t| Box::pin(tokio::time::sleep(t)));
        let first = match deadline.as_mut() {
            Some(deadline) => tokio::select! {
                biased;
                message = response_rx.recv() => Some(message),
                _ = deadline.as_mut() => None,
            },
            None => Some(response_rx.recv().await),
        };
        let (headers, pending) = match first {
            None => return self.reject(&path, start, Code::DeadlineExceeded, "deadline exceeded"),
            Some(None) => {
                return self.reject(&path, start, Code::Internal, "handler ended without a response");
            }
            Some(Some(StreamMessage::Headers(headers))) => (headers, None),
            Some(Some(message)) => (Vec::new(), Some(message)),
        };

        let body = ResponseBody {
            pending,
            receiver: Some(response_rx),
            stats: self.stats.clone(),
            path,
            start,
            compress,
            deadline,
            max_message_size,
            finished: false,
            cancel_requests,
        };
        let path = body.path.clone();
        let mut response = grpc_response(TonicBody::new(body), compress);
        let skipped = append_metadata(response.headers_mut(), &headers);
        if !skipped.is_empty() {
            self.stats.log.log(
                Level::Warn,
                &format!(
                    "gRPC {path} dropped invalid or reserved response headers: {}",
                    skipped.join(", ")
                ),
            );
        }
        response
    }
}

/// The client's address and, if it presented one over TLS, its
/// certificate chain.  tonic records both on the request.
fn peer_of<B>(req: &Request<B>) -> Peer {
    let address = req
        .extensions()
        .get::<TcpConnectInfo>()
        .and_then(TcpConnectInfo::remote_addr)
        .map(|addr| addr.to_string());
    let certificates = req
        .extensions()
        .get::<TlsConnectInfo<TcpConnectInfo>>()
        .and_then(TlsConnectInfo::peer_certs)
        .map(|chain| chain.iter().map(|der| der.to_vec()).collect())
        .unwrap_or_default();
    Peer {
        address,
        certificates,
    }
}

/// Request bytes read ahead of the handler, beyond room for one whole
/// message.
const READ_AHEAD: usize = 64 * 1024;

/// Read the request body and forward each unframed message to the
/// handler.  Dropping `tx` signals end of stream; errors are recorded
/// in `error` first so the reader can distinguish them.  Stops as soon as
/// the call is cancelled, even while waiting on the handler.
///
/// Frames are read as they arrive, into one buffer, while the handler
/// catches up: h2 limits how many small DATA frames a connection may leave
/// unread, and closes it when a client sends many tiny messages faster
/// than the handler reads them (#56).  Once the buffer holds a message
/// plus `READ_AHEAD`, reading stops and HTTP/2 flow control holds the
/// client back.
async fn feed_request(
    body: axum::body::Body,
    tx: mpsc::Sender<Vec<u8>>,
    error: Arc<RequestError>,
    max_message_size: usize,
    gzip: bool,
) {
    // A full buffer always holds a whole message (or an invalid one), so
    // there's always something to hand over.
    let limit = max_message_size.saturating_add(5 + READ_AHEAD);
    let mut reader = MessageReader::new(max_message_size, gzip);
    let mut body = body;
    let mut ended = false;
    // Unframed, waiting for room in the handler's channel.  The next one
    // is unframed only after this one is taken, so an invalid message is
    // reported after every message before it.
    let mut next = None;
    loop {
        if next.is_none() {
            match reader.try_next() {
                Ok(message) => next = message,
                Err(e) => {
                    error.set(e.code(), e.message());
                    return;
                }
            }
        }
        if ended && next.is_none() {
            break;
        }
        let read = !ended && reader.buffered() < limit;
        tokio::select! {
            biased;
            _ = error.cancelled() => return,
            permit = tx.reserve(), if next.is_some() => match permit {
                Ok(permit) => permit.send(next.take().expect("checked above")),
                Err(_) => return, // handler finished without reading everything
            },
            frame = body.frame(), if read => match frame {
                None => ended = true,
                Some(Ok(frame)) => {
                    // Request trailers carry nothing for us.
                    if let Ok(data) = frame.into_data() {
                        reader.push(&data);
                    }
                }
                Some(Err(_)) => {
                    error.set(Code::Cancelled, "request stream reset by client");
                    return;
                }
            },
        }
    }
    if let Err(e) = reader.finish() {
        error.set(e.code(), e.message());
    }
}
