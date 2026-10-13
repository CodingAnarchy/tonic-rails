//! Transport log events, delivered to Ruby's logger.
//!
//! tokio threads cannot call Ruby, so events go through a bounded channel
//! that a Ruby thread drains (`Server#next_log_events`).  If nobody is
//! draining, or the channel is full, events fall back to stderr.
//!
//! tonic reports a connection it failed to serve (one h2 closed for a
//! protocol violation, say) only as a `tracing` event, so
//! [`log_connection_errors`] forwards those to the server's log.

use std::cell::RefCell;
use std::fmt::Write as _;

use crossbeam_channel::{Receiver, Sender, TrySendError};
use tonic::Code;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

use crate::framing::sanitize_for_log;

/// Events buffered before falling back to stderr.
const CAPACITY: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    /// Client-caused outcomes are informational; overload is a warning;
    /// anything that suggests a server fault is an error.
    pub fn for_code(code: u32) -> Level {
        match Code::from_i32(code as i32) {
            Code::Internal | Code::Unknown | Code::DataLoss => Level::Error,
            Code::ResourceExhausted | Code::Unavailable => Level::Warn,
            _ => Level::Info,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}

const STATUS_NAMES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

/// The canonical gRPC status name, e.g. `NOT_FOUND`.
pub fn status_name(code: u32) -> &'static str {
    STATUS_NAMES.get(code as usize).copied().unwrap_or("UNKNOWN")
}

pub struct LogEvent {
    pub level: Level,
    pub message: String,
}

#[derive(Clone)]
pub struct LogSink {
    tx: Sender<LogEvent>,
}

impl LogSink {
    pub fn new() -> (Self, Receiver<LogEvent>) {
        let (tx, rx) = crossbeam_channel::bounded(CAPACITY);
        (Self { tx }, rx)
    }

    pub fn log(&self, level: Level, message: &str) {
        let event = LogEvent {
            level,
            message: sanitize_for_log(message),
        };
        match self.tx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(event) | TrySendError::Disconnected(event)) => {
                eprintln!("tonic-rails: [{}] {}", event.level.as_str(), event.message);
            }
        }
    }
}

impl Default for LogSink {
    /// A sink nobody drains: every event goes to stderr.
    fn default() -> Self {
        Self::new().0
    }
}

thread_local! {
    /// The log of the server whose runtime this thread belongs to.
    static THREAD_SINK: RefCell<Option<LogSink>> = const { RefCell::new(None) };
}

/// Send this thread's connection errors to `sink`: called as each of a
/// server's runtime threads starts.
pub fn set_thread_sink(sink: LogSink) {
    THREAD_SINK.with(|slot| *slot.borrow_mut() = Some(sink));
}

/// tonic's event for a connection it stopped serving with an error.
const CONNECTION_FAILED: &str = "failed serving connection: ";

/// Forward tonic's connection errors to the log of the server they belong
/// to (see [`set_thread_sink`]).  Installs a process-wide `tracing`
/// subscriber that ignores everything else, unless one is installed
/// already.
pub fn log_connection_errors() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let _ = tracing::subscriber::set_global_default(ConnectionErrors);
    });
}

struct ConnectionErrors;

/// How loudly to log a connection error, or `None` not to.
fn connection_error_level(error: &str) -> Option<Level> {
    if error.contains("before reading preface") {
        // Connected and left without a word: TCP health checks do this.
        None
    } else if error.contains("connection error detected") {
        // h2 closed the connection for a protocol violation.
        Some(Level::Warn)
    } else {
        // The client reset or dropped the connection, or keepalive timed out.
        Some(Level::Info)
    }
}

impl Subscriber for ConnectionErrors {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event()
            && *metadata.level() <= tracing::Level::DEBUG
            && metadata.target().starts_with("tonic::transport::server")
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::DEBUG)
    }

    fn event(&self, event: &Event<'_>) {
        let mut message = Message(String::new());
        event.record(&mut message);
        let Some(level) = message
            .0
            .strip_prefix(CONNECTION_FAILED)
            .and_then(connection_error_level)
        else {
            return;
        };
        let error = &message.0[CONNECTION_FAILED.len()..];
        THREAD_SINK.with(|slot| {
            if let Some(sink) = slot.borrow().as_ref() {
                sink.log(level, &format!("connection closed: {error}"));
            }
        });
    }

    // No spans: only events matter here.
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

/// An event's `message` field.
struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.0, "{value:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_by_status() {
        assert_eq!(Level::for_code(Code::Internal as u32), Level::Error);
        assert_eq!(Level::for_code(Code::ResourceExhausted as u32), Level::Warn);
        assert_eq!(Level::for_code(Code::Unimplemented as u32), Level::Info);
        assert_eq!(Level::for_code(Code::DeadlineExceeded as u32), Level::Info);
    }

    #[test]
    fn status_names() {
        assert_eq!(status_name(0), "OK");
        assert_eq!(status_name(12), "UNIMPLEMENTED");
        assert_eq!(status_name(99), "UNKNOWN");
    }

    #[test]
    fn connection_errors_by_kind() {
        assert_eq!(
            connection_error_level("connection error: connection closed before reading preface"),
            None
        );
        assert_eq!(
            connection_error_level(
                "http2 error: connection error detected: detected excessive load generating behavior"
            ),
            Some(Level::Warn)
        );
        assert_eq!(
            connection_error_level("connection error: Connection reset by peer (os error 54)"),
            Some(Level::Info)
        );
    }

    #[test]
    fn events_reach_the_receiver_sanitized() {
        let (sink, rx) = LogSink::new();
        sink.log(Level::Warn, "queue full\nforged line");
        let event = rx.try_recv().expect("event");
        assert_eq!(event.level, Level::Warn);
        assert_eq!(event.message, "queue full forged line");
    }
}
