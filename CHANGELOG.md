# Changelog

## 0.1.0 (2024)

Initial release.

### Security hardening

- **Zip-bomb guard**: gzip-compressed request payloads are decompressed with
  a cap at `max_message_size`; decompressed output exceeding the limit returns
  `RESOURCE_EXHAUSTED` instead of unbounded memory allocation.
- **Reflection disabled by default**: `grpc.reflection.v1.ServerReflection` is
  now off unless explicitly enabled (`config.reflection = true`). Previously
  it was on by default, exposing the full API schema to any client.
- **Internal error messages sanitized**: unhandled Ruby exceptions are logged
  server-side with full detail; clients receive `"internal error"` instead of
  raw exception messages that could leak file paths, SQL fragments, or
  internal hostnames.
- **`grpc-message` trailer sanitized**: control characters (newlines, NULs)
  in error messages are replaced with spaces so the trailer is never silently
  dropped by `HeaderValue::from_str`.

### Operational improvements

- **`max_connections` enforced**: the concurrent TCP connection cap is now
  enforced via a semaphore-wrapped incoming stream. Connections exceeding the
  limit are dropped immediately. Default lowered from 10,000 to 64 to match
  the actual concurrency ceiling of a Rails handler (dispatch threads × GVL).
  Set to 0 for unlimited. Previously the config was accepted but had no
  effect (dead code).
- **`worker_threads` wired through**: `config.worker_threads` is now passed to
  the tokio runtime builder. Previously it was accepted but silently ignored.
- **TLS warning**: the server logs a prominent warning when starting without
  TLS, so plaintext mode is never accidental in production.
- **Malformed protobuf → `:invalid_argument`**: decode errors from
  `Google::Protobuf#decode` now map to gRPC code `INVALID_ARGUMENT` (3) instead
  of `INTERNAL` (13), correctly attributing the error to the client.

### Second-pass hardening

- **Per-connection stream limit**: `config.concurrency_limit_per_connection`
  (default 32) caps concurrent HTTP/2 streams per connection, preventing a
  single client from exhausting the dispatch queue with unlimited concurrent
  requests. Routed through tonic's `concurrency_limit_per_connection`.
- **`grpc-timeout` overflow guard**: timeout parsing uses `checked_mul` for
  hours/minutes, preventing `u64` overflow that would wrap to a tiny duration
  and defeat the deadline.
- **Body-read error sanitized**: `failed to read request body` no longer
  appends the internal hyper/tonic error text to the gRPC message trailer.
- **HPACK header size limit**: `max_header_list_size(16KB)` set on the tonic
  server to reject oversized headers at the HPACK decoder level, before the
  application-layer 8KB metadata check.
- **TCP keepalive**: `tcp_keepalive(60s)` added alongside the existing
  HTTP/2 PING for OS-level dead-peer detection on half-open connections.
- **Keepalive configurable**: `config.keepalive_interval` (default 15s) and
  `config.keepalive_timeout` (default 5s) control HTTP/2 PING interval and
  timeout. Previously these were hardcoded.

### Third-pass hardening

- **`active_connections` counter fixed**: the counter was incremented on each
  accepted connection but never decremented on disconnect, growing monotonically
  and making the "over limit" log message incorrect. Now decremented via
  `PermitIo::Drop`. Also exposed in `Server#stats` as `:active_connections`.
- **`SO_REUSEADDR` set**: alongside `SO_REUSEPORT`, ensuring rapid restarts
  during deploy don't fail to bind when the previous socket is in TIME_WAIT.
- **HTTP/2 flow control windows sized to `max_message_size`**: `initial_stream_window_size`
  and `initial_window_size` set to `max_message_size` (default 4MB), eliminating
  unnecessary window-update rounds for full-size messages (default was 64KB).

### Fourth-pass hardening

- **gRPC frame length field validated**: `unframe` now reads the 5-byte gRPC
  frame header's declared length and rejects truncated frames (declared length
  exceeds available data). Previously the length field was ignored entirely,
  meaning a frame declaring `length=3` with 1000 trailing bytes would return
  all 1000 bytes — a protocol confusion vector. Trailing bytes beyond the
  declared length are now discarded.
- **Uncompressed payload size checked in `unframe`**: the uncompressed code
  path now checks `payload.len() > max_size`, matching the existing check on
  the compressed path. Previously only the caller checked `body_bytes.len()`,
  leaving `unframe` itself as a defense-in-depth gap.
- **Log injection prevention**: the request path is sanitized (control
  characters replaced with spaces) before being used in log messages.
  Previously a path containing `\n` could inject fake log lines into stderr.
- **HTTP method validation**: requests with a method other than POST are
  rejected with `UNIMPLEMENTED` before any further processing. gRPC spec §4.1
  requires POST.
- **Unsupported `grpc-encoding` rejected**: requests with a `grpc-encoding`
  header other than `identity` or `gzip` are rejected with `UNIMPLEMENTED`
  instead of failing with a misleading `RESOURCE_EXHAUSTED` decompression
  error.

### Incremental streaming

- **Server-streaming responses no longer materialized**: previously, the
  entire streaming response was materialized in memory three times — Ruby's
  `result.map { encode }` array, Rust's `Vec<Vec<u8>>`, and the framed
  `BytesMut` buffer. Now, each encoded message is sent through a bounded
  mpsc channel (capacity 32) to a `GrpcStreamingBody` that frames and
  yields it as a gRPC data frame on demand. Peak memory is one message,
  not the full collection.
- **GVL released during backpressure**: when the client consumes slowly
  and the channel fills, `StreamSender#send` releases the GVL via
  `rb_thread_call_without_gvl` before blocking, so other Ruby workers can
  dispatch concurrently. The same GVL-released path is used for the
  terminal `End` send and the buffered-Array fallback, preventing GVL
  starvation in all code paths.
- **Bounded channel backpressure**: a 32-message channel cap provides
  natural backpressure — the handler is throttled to client consumption
  speed. A worker is pinned for the stream duration, but with the GVL
  released, the impact is limited to thread count, not GVL availability.
  Increase `dispatch_threads` if many concurrent slow streams are expected.
- **Deadline enforcement moved to streaming body**: `GrpcStreamingBody`
  checks the gRPC deadline in `poll_frame` and yields `DEADLINE_EXCEEDED`
  trailers when it expires, replacing the previous `tokio::time::timeout`
  wrapper around the oneshot receiver.
- **Stats moved to streaming body**: request counters (success, error,
  deadline exceeded, resource exhausted) are recorded when the body sees
  `End` or the channel closes, not when the oneshot fires. A `Drop` impl
  records an error if the body is dropped mid-stream (client disconnect).
- **Dispatch protocol unified**: all RPCs (unary and streaming) now use
  the same mpsc channel — unary sends one `Data` + `End`, streaming sends
  many `Data` + `End`. The `oneshot::channel<DispatchResult>` dispatch
  path is removed. `DispatchResult` is retained for tests only.
- **Buffered fallback preserved**: unit tests that call `call_rpc`
  directly (without the Rust dispatch layer) still use the buffered
  `result.map` path — no `_stream_sender` in metadata.

### Fifth-pass hardening

- **Response message size enforced in streaming body**: `GrpcStreamingBody`
  now checks each `Data` message against `max_message_size` in
  `poll_frame`, yielding `RESOURCE_EXHAUSTED` trailers if exceeded. This
  restores the per-message size check that was lost when the response
  path moved from buffered `handle_request` to the streaming body —
  without it, a handler could send arbitrarily large response messages
  through the channel.
- **Error message log injection prevention**: `GrpcStreamingBody::record_error`
  sanitizes the error message (control chars → spaces) before writing to
  stderr, matching the path sanitization already in place. Ruby error
  messages can contain newlines and other control characters that would
  inject fake log lines.

### Simplification pass (post-streaming refactor)

- **Removed `DispatchResult`**: the struct and its three constructors
  (`error`, `unary`, `streaming`) were dead code after the incremental
  streaming refactor replaced `oneshot<DispatchResult>` with
  `mpsc<StreamMessage>`. Three compiler warnings eliminated.
- **Removed `DispatchRequest` wrapper**: the single-field struct
  (`ctx: Box<DispatchContext>`) added an unnecessary indirection through
  the crossbeam channel. The channel now carries `Box<DispatchContext>`
  directly.
- **Extracted `build_error_trailers` free function**: the trailer-building
  logic (grpc-status + sanitized grpc-message) was duplicated between
  `GrpcBody::error` and `GrpcStreamingBody::error_trailers`. Now both use
  a shared free function, and the `error_trailers` method was removed.
- **Inlined `grpc_response` and `box_body`**: `grpc_response` was only
  called by `grpc_error_response` (always with `compress: false`), and
  `box_body` was only called by `grpc_response`. Both inlined; header-
  setting extracted into a shared `set_grpc_headers` helper used by both
  `grpc_error_response` and `handle_request`.
- **Marked `GrpcBody::ok` as `#[cfg(test)]`**: the success constructor
  was only used in unit tests after the streaming refactor moved all
  success responses to `GrpcStreamingBody`.
