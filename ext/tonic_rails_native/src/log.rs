//! Transport log events, delivered to Ruby's logger.
//!
//! tokio threads cannot call Ruby, so events go through a bounded channel
//! that a Ruby thread drains (`Server#next_log_events`).  If nobody is
//! draining, or the channel is full, events fall back to stderr.

use crossbeam_channel::{Receiver, Sender, TrySendError};
use tonic::Code;

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
    fn events_reach_the_receiver_sanitized() {
        let (sink, rx) = LogSink::new();
        sink.log(Level::Warn, "queue full\nforged line");
        let event = rx.try_recv().expect("event");
        assert_eq!(event.level, Level::Warn);
        assert_eq!(event.message, "queue full forged line");
    }
}
