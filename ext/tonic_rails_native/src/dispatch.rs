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

use crate::body::{ErrorBody, ResponseBody, ServerStats};
use crate::framing::{MessageReader, parse_grpc_timeout};
use crate::gvl::{DispatchContext, RequestError, StreamMessage};

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
        let response = self.0.handle(req);
        Box::pin(async move { Ok(response) })
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
    fn reject(&self, path: &str, start: Instant, code: Code, message: &str) -> Response<TonicBody> {
        self.stats.record(path, code as u32, message, start, true);
        grpc_response(TonicBody::new(ErrorBody::new(code, message)), false)
    }

    fn handle(&self, req: Request<axum::body::Body>) -> Response<TonicBody> {
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

        let ctx = Box::new(DispatchContext {
            callback: self.callback,
            path: path.clone(),
            metadata,
            timeout_ms: timeout.map(|t| t.as_millis()),
            response_tx,
            request_rx,
            request_error: request_error.clone(),
            log: self.stats.log.clone(),
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

        let body = ResponseBody {
            receiver: Some(response_rx),
            stats: self.stats.clone(),
            path,
            start,
            compress,
            deadline: timeout.map(|t| Box::pin(tokio::time::sleep(t))),
            max_message_size,
            finished: false,
        };
        grpc_response(TonicBody::new(body), compress)
    }
}

/// Read the request body and forward each unframed message to the
/// handler.  Dropping `tx` signals end of stream; errors are recorded
/// in `error` first so the reader can distinguish them.
async fn feed_request(
    body: axum::body::Body,
    tx: mpsc::Sender<Vec<u8>>,
    error: Arc<RequestError>,
    max_message_size: usize,
    gzip: bool,
) {
    let mut reader = MessageReader::new(max_message_size, gzip);
    let mut body = body;
    loop {
        let frame = match body.frame().await {
            None => break,
            Some(Ok(frame)) => frame,
            Some(Err(_)) => {
                error.set(Code::Cancelled, "request stream reset by client");
                return;
            }
        };
        let Ok(data) = frame.into_data() else {
            continue; // request trailers carry nothing for us
        };
        reader.push(&data);
        loop {
            match reader.try_next() {
                Ok(Some(message)) => {
                    if tx.send(message).await.is_err() {
                        return; // handler finished without reading everything
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    error.set(e.code(), e.message());
                    return;
                }
            }
        }
    }
    if let Err(e) = reader.finish() {
        error.set(e.code(), e.message());
    }
}
