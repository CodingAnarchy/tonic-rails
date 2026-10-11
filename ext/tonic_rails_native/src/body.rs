//! gRPC response bodies and request accounting.
//!
//! gRPC carries `grpc-status` in trailing HEADERS, so responses use
//! custom `http_body::Body` implementations that yield data frames
//! followed by a trailers frame.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use http_body::{Body, Frame};
use tokio::sync::mpsc;
use tonic::{Code, Status};

use crate::framing::{COMPRESSION_THRESHOLD, append_metadata, frame, status_trailers};
use crate::gvl::StreamMessage;
use crate::log::{Level, LogSink, status_name};

/// Request counters, exposed to Ruby via `Server#stats`.
#[derive(Default)]
pub struct ServerStats {
    pub total: AtomicUsize,
    pub successful: AtomicUsize,
    pub errors: AtomicUsize,
    pub cancelled: AtomicUsize,
    pub deadline_exceeded: AtomicUsize,
    pub resource_exhausted: AtomicUsize,
    pub unimplemented: AtomicUsize,
    pub log: LogSink,
}

impl ServerStats {
    /// Count a finished call.  `transport` marks outcomes decided here in
    /// Rust (rejections, deadlines, limits) rather than by the Ruby handler;
    /// only those are logged, since Ruby instruments and logs its own calls.
    pub fn record(&self, path: &str, code: u32, message: &str, start: Instant, transport: bool) {
        let counter = match Code::from_i32(code as i32) {
            Code::Ok => &self.successful,
            Code::Cancelled => &self.cancelled,
            _ => &self.errors,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        match Code::from_i32(code as i32) {
            Code::DeadlineExceeded => {
                self.deadline_exceeded.fetch_add(1, Ordering::Relaxed);
            }
            Code::ResourceExhausted => {
                self.resource_exhausted.fetch_add(1, Ordering::Relaxed);
            }
            Code::Unimplemented => {
                self.unimplemented.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }

        if transport && code != Code::Ok as u32 {
            let ms = start.elapsed().as_secs_f64() * 1000.0;
            let status = status_name(code);
            let line = if message.is_empty() {
                format!("gRPC {path} {status} in {ms:.1}ms")
            } else {
                format!("gRPC {path} {status} in {ms:.1}ms: {message}")
            };
            self.log.log(Level::for_code(code), &line);
        }
    }
}

/// Streaming response body fed by the Ruby handler through an mpsc
/// channel.  Each `Data` message becomes a gRPC frame; `End` (or the
/// deadline) produces the trailers.
pub struct ResponseBody {
    /// A message already received while waiting for response headers.
    pub pending: Option<StreamMessage>,
    pub receiver: Option<mpsc::Receiver<StreamMessage>>,
    pub stats: Arc<ServerStats>,
    pub path: String,
    pub start: Instant,
    pub compress: bool,
    pub deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    pub max_message_size: usize,
    pub finished: bool,
}

type Polled = Poll<Option<Result<Frame<Bytes>, Status>>>;

impl ResponseBody {
    fn finish(&mut self, code: Code, message: &str, transport: bool) -> Polled {
        self.finish_with(code, message, transport, &[])
    }

    fn finish_with(
        &mut self,
        code: Code,
        message: &str,
        transport: bool,
        metadata: &[(String, String)],
    ) -> Polled {
        self.receiver = None;
        self.finished = true;
        self.stats
            .record(&self.path, code as u32, message, self.start, transport);
        let mut trailers = status_trailers(code as u32, message);
        let skipped = append_metadata(&mut trailers, metadata);
        if !skipped.is_empty() {
            self.stats.log.log(
                Level::Warn,
                &format!(
                    "gRPC {} dropped invalid or reserved trailers: {}",
                    self.path,
                    skipped.join(", ")
                ),
            );
        }
        Poll::Ready(Some(Ok(Frame::trailers(trailers))))
    }

    /// Turn one handler message into a frame (None: nothing to emit).
    fn handle(&mut self, message: StreamMessage) -> Option<Polled> {
        match message {
            StreamMessage::Data(bytes) => {
                if bytes.len() > self.max_message_size {
                    return Some(self.finish(
                        Code::ResourceExhausted,
                        "response message exceeds max_message_size",
                        true,
                    ));
                }
                let compress = self.compress && bytes.len() > COMPRESSION_THRESHOLD;
                Some(Poll::Ready(Some(Ok(Frame::data(frame(&bytes, compress))))))
            }
            StreamMessage::End { status, trailers } => Some(match status {
                None => self.finish_with(Code::Ok, "", false, &trailers),
                Some((code, message)) => {
                    self.finish_with(Code::from_i32(code as i32), &message, false, &trailers)
                }
            }),
            // Headers are only valid first and are consumed by the
            // dispatcher; a late one has nowhere to go.
            StreamMessage::Headers(_) => None,
        }
    }
}

impl Body for ResponseBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Polled {
        if let Some(message) = self.pending.take()
            && let Some(poll) = self.handle(message)
        {
            return poll;
        }

        // Drain ready messages before checking the deadline so a response
        // that made it in time is never discarded.
        loop {
            let Some(receiver) = self.receiver.as_mut() else {
                return Poll::Ready(None);
            };
            match receiver.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    if let Some(poll) = self.handle(message) {
                        return poll;
                    }
                }
                Poll::Ready(None) => {
                    return self.finish(Code::Internal, "handler ended without a response", true);
                }
                Poll::Pending => break,
            }
        }

        if let Some(deadline) = self.deadline.as_mut()
            && deadline.as_mut().poll(cx).is_ready()
        {
            return self.finish(Code::DeadlineExceeded, "deadline exceeded", true);
        }
        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        self.receiver.is_none()
    }
}

impl Drop for ResponseBody {
    fn drop(&mut self) {
        // Dropped before trailers were produced: the client went away
        // (RST_STREAM / disconnect) — count it as cancelled.
        if !self.finished {
            self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::task::Waker;

    type PolledFrame = Poll<Option<Result<Frame<Bytes>, Status>>>;

    fn poll<B: Body<Data = Bytes, Error = Status> + Unpin>(body: &mut B) -> PolledFrame {
        let mut cx = Context::from_waker(Waker::noop());
        Pin::new(body).poll_frame(&mut cx)
    }

    fn response_body(rx: mpsc::Receiver<StreamMessage>, stats: Arc<ServerStats>) -> ResponseBody {
        ResponseBody {
            pending: None,
            receiver: Some(rx),
            stats,
            path: "/test.Svc/Method".into(),
            start: Instant::now(),
            compress: false,
            deadline: None,
            max_message_size: 16,
            finished: false,
        }
    }

    fn trailers(p: PolledFrame) -> HeaderMap {
        match p {
            Poll::Ready(Some(Ok(frame))) => frame.into_trailers().expect("trailers frame"),
            _ => panic!("expected trailers"),
        }
    }

    #[test]
    fn response_body_streams_data_then_ok() {
        let (tx, rx) = mpsc::channel(4);
        let stats = Arc::new(ServerStats::default());
        let mut body = response_body(rx, stats.clone());
        tx.try_send(StreamMessage::Data(b"abc".to_vec())).ok();
        tx.try_send(StreamMessage::End {
            status: None,
            trailers: vec![("x-done".into(), "yes".into())],
        })
        .ok();

        match poll(&mut body) {
            Poll::Ready(Some(Ok(frame))) => {
                assert_eq!(
                    frame.into_data().unwrap().as_ref(),
                    &[0, 0, 0, 0, 3, b'a', b'b', b'c']
                );
            }
            _ => panic!("expected data"),
        }
        let t = trailers(poll(&mut body));
        assert_eq!(t["grpc-status"], "0");
        assert_eq!(t["x-done"], "yes");
        assert!(matches!(poll(&mut body), Poll::Ready(None)));
        drop(body);
        assert_eq!(stats.successful.load(Ordering::Relaxed), 1);
        assert_eq!(stats.cancelled.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn response_body_rejects_oversized_message() {
        let (tx, rx) = mpsc::channel(4);
        let stats = Arc::new(ServerStats::default());
        let mut body = response_body(rx, stats.clone());
        tx.try_send(StreamMessage::Data(vec![0; 17])).ok();
        assert_eq!(trailers(poll(&mut body))["grpc-status"], "8");
        assert_eq!(stats.resource_exhausted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn response_body_reports_handler_error() {
        let (tx, rx) = mpsc::channel(4);
        let mut body = response_body(rx, Arc::new(ServerStats::default()));
        tx.try_send(StreamMessage::error(Code::PermissionDenied, "denied"))
            .ok();
        let t = trailers(poll(&mut body));
        assert_eq!(t["grpc-status"], "7");
        assert_eq!(t["grpc-message"], "denied");
    }

    #[test]
    fn response_body_starts_with_a_pending_message() {
        let (tx, rx) = mpsc::channel(4);
        let mut body = response_body(rx, Arc::new(ServerStats::default()));
        body.pending = Some(StreamMessage::Data(b"x".to_vec()));
        tx.try_send(StreamMessage::error(Code::Aborted, "stop")).ok();
        assert!(matches!(poll(&mut body), Poll::Ready(Some(Ok(f))) if f.is_data()));
        assert_eq!(trailers(poll(&mut body))["grpc-status"], "10");
    }

    #[test]
    fn response_body_closed_channel_is_internal() {
        let (tx, rx) = mpsc::channel(4);
        let mut body = response_body(rx, Arc::new(ServerStats::default()));
        drop(tx);
        assert_eq!(trailers(poll(&mut body))["grpc-status"], "13");
    }

    #[test]
    fn dropped_body_counts_as_cancelled() {
        let (_tx, rx) = mpsc::channel(4);
        let stats = Arc::new(ServerStats::default());
        let mut body = response_body(rx, stats.clone());
        assert!(poll(&mut body).is_pending());
        drop(body);
        assert_eq!(stats.cancelled.load(Ordering::Relaxed), 1);
    }
}
