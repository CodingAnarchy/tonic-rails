//! gRPC wire format: length-prefixed message framing, gzip, `grpc-timeout`
//! parsing, and status trailers.

use std::io::{Read, Write};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{HeaderMap, HeaderValue};

/// Responses smaller than this are never compressed — gzip overhead
/// outweighs the savings.
pub const COMPRESSION_THRESHOLD: usize = 1024;

const HEADER_LEN: usize = 5;

/// Why an incoming message could not be unframed.
#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    /// Declared or decompressed size exceeds `max_message_size`.
    TooLarge,
    /// Compressed flag set without a negotiated `grpc-encoding`, or the
    /// gzip payload is corrupt.
    Compression,
    /// The stream ended partway through a frame.
    Truncated,
}

impl FrameError {
    pub fn code(&self) -> tonic::Code {
        match self {
            FrameError::TooLarge => tonic::Code::ResourceExhausted,
            FrameError::Compression | FrameError::Truncated => tonic::Code::Internal,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            FrameError::TooLarge => "request message exceeds max_message_size",
            FrameError::Compression => "invalid compressed request message",
            FrameError::Truncated => "truncated request message",
        }
    }
}

/// Incremental gRPC message reader for request bodies.
///
/// Buffers incoming bytes and extracts complete messages one at a time.
/// Size is checked against the declared length before the payload
/// arrives, and decompressed output is capped (zip-bomb guard).
pub struct MessageReader {
    buf: BytesMut,
    max_message_size: usize,
    gzip: bool,
}

impl MessageReader {
    pub fn new(max_message_size: usize, gzip: bool) -> Self {
        Self {
            buf: BytesMut::new(),
            max_message_size,
            gzip,
        }
    }

    pub fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// `Ok(None)` means more data is needed.
    pub fn try_next(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        if self.buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let compressed = self.buf[0] != 0;
        let length = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;

        if length > self.max_message_size {
            return Err(FrameError::TooLarge);
        }
        if self.buf.len() < HEADER_LEN + length {
            return Ok(None);
        }

        let payload = &self.buf[HEADER_LEN..HEADER_LEN + length];
        let message = if compressed {
            if !self.gzip {
                return Err(FrameError::Compression);
            }
            gunzip(payload, self.max_message_size)?
        } else {
            payload.to_vec()
        };

        self.buf.advance(HEADER_LEN + length);
        Ok(Some(message))
    }

    /// Call once the body has ended: leftover bytes mean a truncated frame.
    pub fn finish(&self) -> Result<(), FrameError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(FrameError::Truncated)
        }
    }
}

fn gunzip(payload: &[u8], max_size: usize) -> Result<Vec<u8>, FrameError> {
    let mut decoder = flate2::read::GzDecoder::new(payload);
    let mut out = Vec::with_capacity(payload.len().saturating_mul(2).min(max_size));
    let mut buf = [0u8; 8192];
    loop {
        let n = decoder.read(&mut buf).map_err(|_| FrameError::Compression)?;
        if n == 0 {
            return Ok(out);
        }
        if out.len() + n > max_size {
            return Err(FrameError::TooLarge);
        }
        out.extend_from_slice(&buf[..n]);
    }
}

/// Frame a protobuf message, gzip-compressing it when `compress` is set.
pub fn frame(message: &[u8], compress: bool) -> Bytes {
    if compress {
        let mut encoder =
            flate2::write::GzEncoder::new(Vec::with_capacity(message.len() / 2), flate2::Compression::fast());
        // Writing into a Vec cannot fail.
        if encoder.write_all(message).is_ok()
            && let Ok(compressed) = encoder.finish()
        {
            return frame_raw(&compressed, true);
        }
    }
    frame_raw(message, false)
}

fn frame_raw(payload: &[u8], compressed: bool) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_LEN + payload.len());
    buf.put_u8(u8::from(compressed));
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

/// Parse a `grpc-timeout` header value (`<digits><unit>`, at most 8
/// digits per the spec, though longer values are tolerated if they fit).
pub fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    if value.len() < 2 || !value.is_char_boundary(value.len() - 1) {
        return None;
    }
    let (num, unit) = value.split_at(value.len() - 1);
    if !num.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let num: u64 = num.parse().ok()?;
    // checked_mul: a huge value must not wrap to a tiny deadline.
    Some(match unit {
        "H" => Duration::from_secs(num.checked_mul(3600)?),
        "M" => Duration::from_secs(num.checked_mul(60)?),
        "S" => Duration::from_secs(num),
        "m" => Duration::from_millis(num),
        "u" => Duration::from_micros(num),
        "n" => Duration::from_nanos(num),
        _ => return None,
    })
}

/// Percent-encode a status message for the `grpc-message` trailer as
/// required by the gRPC HTTP/2 spec: bytes outside printable ASCII, and
/// `%` itself, become `%XX`.  This also neutralises control characters.
pub fn encode_grpc_message(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    for &b in message.as_bytes() {
        if (0x20..=0x7e).contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Replace control characters so untrusted text cannot inject log lines.
pub fn sanitize_for_log(message: &str) -> String {
    message
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// Headers a handler may not set: they belong to HTTP/2 or the gRPC
/// protocol itself.
fn is_reserved_metadata(name: &str) -> bool {
    matches!(
        name,
        "content-type" | "te" | "user-agent" | "content-length" | "connection" | "transfer-encoding"
    ) || (name.starts_with("grpc-") && name != "grpc-status-details-bin")
}

/// Append handler-supplied metadata to `headers`, skipping names or values
/// that are invalid HTTP/2 headers or reserved by gRPC.  Returns the names
/// that were skipped so the caller can report them.
pub fn append_metadata(headers: &mut HeaderMap, metadata: &[(String, String)]) -> Vec<String> {
    let mut skipped = Vec::new();
    for (name, value) in metadata {
        let header = (!is_reserved_metadata(name))
            .then(|| http::HeaderName::from_bytes(name.as_bytes()).ok())
            .flatten()
            .filter(|n| n.as_str() == name.as_str()); // must already be lower-case
        match (header, HeaderValue::from_str(value)) {
            (Some(header), Ok(value)) => {
                headers.append(header, value);
            }
            _ => skipped.push(name.clone()),
        }
    }
    skipped
}

pub fn status_trailers(code: u32, message: &str) -> HeaderMap {
    let mut trailers = HeaderMap::with_capacity(2);
    trailers.insert("grpc-status", HeaderValue::from(code));
    if !message.is_empty()
        && let Ok(value) = HeaderValue::from_str(&encode_grpc_message(message))
    {
        trailers.insert("grpc-message", value);
    }
    trailers
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(payload: &[u8]) -> Vec<u8> {
        frame(payload, false).to_vec()
    }

    #[test]
    fn parse_timeout_units() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("100m"), Some(Duration::from_millis(100)));
        assert_eq!(parse_grpc_timeout("2H"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_grpc_timeout("3M"), Some(Duration::from_secs(180)));
        assert_eq!(parse_grpc_timeout("7u"), Some(Duration::from_micros(7)));
        assert_eq!(parse_grpc_timeout("9n"), Some(Duration::from_nanos(9)));
    }

    #[test]
    fn parse_timeout_overflow_returns_none() {
        let huge = u64::MAX.to_string();
        assert_eq!(parse_grpc_timeout(&format!("{huge}H")), None);
        assert_eq!(parse_grpc_timeout(&format!("{huge}M")), None);
    }

    #[test]
    fn parse_timeout_malformed_returns_none() {
        for value in ["", "S", "5", "5x", "abcS", "-5S", "+5S", "5é"] {
            assert_eq!(parse_grpc_timeout(value), None, "{value:?}");
        }
    }

    #[test]
    fn frame_uncompressed_header() {
        assert_eq!(framed(b"abc"), vec![0, 0, 0, 0, 3, b'a', b'b', b'c']);
        assert_eq!(framed(b""), vec![0, 0, 0, 0, 0]);
    }

    #[test]
    fn frame_compressed_round_trips() {
        let payload = vec![b'x'; 4096];
        let bytes = frame(&payload, true);
        assert_eq!(bytes[0], 1);
        let mut reader = MessageReader::new(8192, true);
        reader.push(&bytes);
        assert_eq!(reader.try_next(), Ok(Some(payload)));
    }

    #[test]
    fn reader_single_message() {
        let mut reader = MessageReader::new(1024, false);
        reader.push(&framed(b"abc"));
        assert_eq!(reader.try_next(), Ok(Some(b"abc".to_vec())));
        assert_eq!(reader.try_next(), Ok(None));
        assert_eq!(reader.finish(), Ok(()));
    }

    #[test]
    fn reader_message_split_across_chunks() {
        let mut reader = MessageReader::new(1024, false);
        let bytes = framed(b"hello");
        reader.push(&bytes[..3]);
        assert_eq!(reader.try_next(), Ok(None));
        reader.push(&bytes[3..7]);
        assert_eq!(reader.try_next(), Ok(None));
        reader.push(&bytes[7..]);
        assert_eq!(reader.try_next(), Ok(Some(b"hello".to_vec())));
    }

    #[test]
    fn reader_multiple_messages() {
        let mut reader = MessageReader::new(1024, false);
        let mut data = framed(b"ab");
        data.extend(framed(b"c"));
        data.extend(framed(b""));
        reader.push(&data);
        assert_eq!(reader.try_next(), Ok(Some(b"ab".to_vec())));
        assert_eq!(reader.try_next(), Ok(Some(b"c".to_vec())));
        assert_eq!(reader.try_next(), Ok(Some(Vec::new())));
        assert_eq!(reader.try_next(), Ok(None));
    }

    #[test]
    fn reader_rejects_oversized_before_payload_arrives() {
        let mut reader = MessageReader::new(100, false);
        reader.push(&[0, 0, 0, 0, 200]);
        assert_eq!(reader.try_next(), Err(FrameError::TooLarge));
    }

    #[test]
    fn reader_zip_bomb_guard() {
        let payload = vec![0u8; 10_000];
        let mut reader = MessageReader::new(1000, true);
        reader.push(&frame(&payload, true));
        assert_eq!(reader.try_next(), Err(FrameError::TooLarge));
    }

    #[test]
    fn reader_rejects_compressed_without_encoding() {
        let mut reader = MessageReader::new(8192, false);
        reader.push(&frame(&[b'x'; 2048], true));
        assert_eq!(reader.try_next(), Err(FrameError::Compression));
    }

    #[test]
    fn reader_rejects_corrupt_gzip() {
        let mut reader = MessageReader::new(1024, true);
        reader.push(&[1, 0, 0, 0, 3, 1, 2, 3]);
        assert_eq!(reader.try_next(), Err(FrameError::Compression));
    }

    #[test]
    fn reader_detects_truncated_stream() {
        let mut reader = MessageReader::new(1024, false);
        reader.push(&[0, 0, 0, 0, 10, b'a']);
        assert_eq!(reader.try_next(), Ok(None));
        assert_eq!(reader.finish(), Err(FrameError::Truncated));
    }

    #[test]
    fn grpc_message_percent_encoding() {
        assert_eq!(encode_grpc_message("not found"), "not found");
        assert_eq!(encode_grpc_message("100%"), "100%25");
        assert_eq!(encode_grpc_message("a\nb\0"), "a%0Ab%00");
        assert_eq!(encode_grpc_message("café"), "caf%C3%A9");
    }

    #[test]
    fn status_trailers_include_message() {
        let t = status_trailers(5, "missing\r\nX-Injected: 1");
        assert_eq!(t["grpc-status"], "5");
        assert_eq!(t["grpc-message"], "missing%0D%0AX-Injected: 1");
    }

    #[test]
    fn status_trailers_omit_empty_message() {
        let t = status_trailers(0, "");
        assert_eq!(t["grpc-status"], "0");
        assert!(t.get("grpc-message").is_none());
    }

    #[test]
    fn append_metadata_skips_reserved_and_invalid() {
        let mut headers = HeaderMap::new();
        let skipped = append_metadata(
            &mut headers,
            &[
                ("x-request-id".into(), "abc".into()),
                ("x-trace-bin".into(), "AAE=".into()),
                ("grpc-status-details-bin".into(), "CAU=".into()),
                ("grpc-status".into(), "0".into()),
                ("content-type".into(), "text/plain".into()),
                ("Upper".into(), "x".into()),
                ("x-bad".into(), "line\nbreak".into()),
            ],
        );
        assert_eq!(headers["x-request-id"], "abc");
        assert_eq!(headers["x-trace-bin"], "AAE=");
        assert_eq!(headers["grpc-status-details-bin"], "CAU=");
        assert_eq!(skipped, ["grpc-status", "content-type", "Upper", "x-bad"]);
    }

    #[test]
    fn sanitize_for_log_strips_controls() {
        assert_eq!(sanitize_for_log("a\nb\tc"), "a b c");
    }
}
