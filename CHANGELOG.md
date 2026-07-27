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

### Compilation fixes (magnus 0.8.2 API alignment)

- **`Buf` trait imported**: `bytes::Buf` is now imported so `BytesMut::advance`
  compiles under the `bytes` version locked in `Cargo.lock`.
- **`ruby.nil()` → `ruby.qnil().as_value()`**: magnus 0.8.2 exposes `qnil()`
  (returning `Qnil`) rather than `nil()`. All nil-returning paths in
  `RequestReader::next` now use the correct API.
- **`RequestReader::next` parameter order**: the `RubyMethod0` trait expects
  `fn(&Ruby, &Self)`, not `fn(&Self, &Ruby)`. The parameter order was swapped
  so the `method!` macro resolves to the `RubyMethod0` variant.
- **`mpsc::Receiver` is not `Clone`**: `RequestReaderNextCtx.rx` changed from
  `*const` to `*mut`, and `dispatch_with_gvl_inner` now takes
  `&mut DispatchContext` and uses `request_rx.take()` to extract the receiver
  without cloning.
- **`dispatch_with_gvl` Box handling**: the crossbeam callback now passes
  `&mut ctx` (auto-derefed from `Box<DispatchContext>`) instead of `&mut *ctx`.

### Client-streaming and bidirectional-streaming support

- **Incremental request unframing**: `GrpcMessageReader` extracts gRPC
  messages incrementally from the HTTP/2 body stream, feeding each complete
  message through the mpsc channel to the Ruby handler. This replaces the
  previous `unframe_all` that required the entire body in memory before
  dispatch. Per-message size is enforced at unframe time.
- **Unary path uses `_request_reader`**: `call_rpc` now reads the request
  message from `_request_reader.next` when present (real dispatch), falling
  back to `request_bytes` for unit tests that call `call_rpc` directly.
  Previously the unary branch decoded `request_bytes` directly, which is
  empty in real dispatch — causing every unary RPC to decode an empty
  protobuf and fail.
- **Malformed protobuf in unary path**: decode errors now map to
  `:invalid_argument` with a server-side warning log, matching the
  streaming path's error handling.
- **`unframe` and `unframe_all` retained as test-only** (`#[cfg(test)]`):
  both functions are superseded by `GrpcMessageReader` in production but
  remain as test utilities with full coverage.

### Unsafe code audit

- **Eliminated `std::mem::transmute` for Ruby string creation**: the two
  `rb_str_new` + `transmute::<rb_sys::VALUE, Value>` blocks in
  `dispatch_with_gvl_inner` and `RequestReader::next` are replaced with
  `ruby.str_from_slice(&bytes).as_value()` — a safe magnus API that calls
  the same `rb_str_new` internally.
- **Eliminated `extract_ruby_string` unsafe block**: the function used
  `rb_string_value_ptr`, `rb_str_strlen`, and `slice::from_raw_parts` to
  extract bytes from a Ruby string. Replaced with
  `RString::from_value(val).map(|r| r.to_bytes().to_vec())` — fully safe,
  no raw pointer manipulation.
- **Removed `rb_str_new`, `rb_string_value_ptr`, `rb_str_strlen` from
  rb-sys imports**: these FFI symbols are no longer referenced directly.
- **Enabled magnus `bytes` feature**: required for `RString::to_bytes`.
  The `bytes` crate was already in the dependency tree via tonic; this
  only enables the magnus wrapper method.
- **Remaining unsafe code is irreducible Ruby C API FFI**: 13 `unsafe {}`
  blocks and 5 `unsafe extern "C" fn` callbacks (18 `unsafe` keywords total),
  all required by `rb_thread_call_without_gvl` / `rb_thread_call_with_gvl`
  trampolines, pointer dereferences from `void*` callback data, and
  `Box::from_raw` for ownership transfer. No safe alternative exists in
  magnus for the GVL-released blocking pattern.

### Second-pass unsafe reduction

- **MSRV bumped to 1.93** (from 1.75): enables `Waker::noop()` (stable
  since 1.85) and `std::ptr::without_provenance_mut` (stable since 1.84).
- **Eliminated test-only `Waker::from_raw`**: the `noop_waker()` helper
  with its `unsafe { Waker::from_raw(RawWaker::new(...)) }` block is
  replaced with `Waker::noop()` — a one-liner with no unsafe code.
- **Unified `stream_send_without_gvl` into `channel_send_without_gvl`**:
  `StreamSender::send_message` now calls the existing `gvl_released_send`
  helper instead of maintaining its own FFI call path. Eliminates 1
  `unsafe extern "C" fn`, 1 struct (`StreamSendCtx`), and 5 `unsafe {}`
  blocks (4 pointer dereferences + 1 FFI call site).
- **Unified `serve_unblock` into `cancel_unblock`** (renamed from
  `stream_send_unblock`): both callbacks set an `AtomicBool` — now a
  single shared callback. Eliminates 1 `unsafe extern "C" fn`, 1 struct
  (`UnblockContext`), and 1 `unsafe {}` block.
- **Removed `Arc<AtomicBool>` cancelled fields** from `StreamSender` and
  `RequestReader`: stack-local `AtomicBool` is sufficient for the call
  duration. Eliminates 2 `Arc` allocations and 2 `Arc::as_ptr` calls.
- **Replaced integer-to-pointer casts** (`N as *mut c_void`) with
  `std::ptr::without_provenance_mut::<c_void>(N)`: 5 occurrences in
  callback return codes, satisfying clippy 1.93's `manual_dangling_ptr`
  lint with provenance-safe pointer encoding.
- **Net reduction**: 27 → 18 `unsafe` keywords (−9), 7 → 5
  `unsafe extern "C" fn` (−2), 20 → 13 `unsafe {}` blocks (−7), 2
  structs removed, 2 functions removed.

### Edition 2024 migration

- **Bumped to Rust edition 2024**: from edition 2021. The edition change
  enforces `unsafe_op_in_unsafe_fn` (deny-by-default), requiring explicit
  `unsafe {}` blocks for all unsafe operations inside `unsafe fn` bodies.
  The one affected site (`Box::from_raw` in `dispatch_with_gvl`) was
  already the only unwrapped unsafe op — all other `unsafe extern "C" fn`
  callbacks already had explicit `unsafe {}` blocks from the prior audit.
- **Collapsed 3 `if let` chains** using Rust 1.88+ let-chain syntax
  (`if let X = a && let Y = b { ... }`): in `GrpcStreamingBody::poll_frame`
  (deadline check), `handle_request` (timeout header parsing), and
  `stop_server` (health reporter shutdown). Eliminates nested indentation.

### Edge-case hardening pass

- **Server double-start guard**: `start_server` now returns an error if
  the runtime is already running, preventing resource leaks (orphaned
  tokio threads, duplicate socket binds) from a second call without
  `stop_server`.
- **RequestReader EOS optimization**: after the request channel closes
  (end-of-stream), the receiver is dropped instead of being put back
  into the `Mutex`. Subsequent `.next` calls return nil immediately
  without a blocking `try_recv` on a known-disconnected channel.
- **Request stream error propagation**: when the body-reading tokio task
  encounters an invalid frame, oversized message, or body-read failure,
  it sets a shared `Arc<AtomicU8>` error flag (gRPC status code) before
  closing the request channel. `RequestReader::next` checks this flag
  when the channel closes — if set, it raises a Ruby exception instead
  of returning nil (end-of-stream). The handler's Enumerator catches
  the exception and propagates it as `RpcError` → error trailer →
  `GrpcStreamingBody` records the error. Previously, an invalid request
  stream was indistinguishable from legitimate EOS, so the handler
  committed a partial/invalid request as a successful response.
- **Streaming decode error → `:invalid_argument`**: decode errors in the
  client-streaming/bidi Enumerator now raise `RpcError` with
  `:invalid_argument` (matching the unary path), instead of falling
  through to the generic `:internal` handler.
- **RequestReader exception handling**: `request_reader.next` failures
  in both the streaming and unary paths are now caught and mapped to
  `RpcError` with `:unavailable` code, with a server-side warning log.
- **Handler nil return validation**: explicit `nil` checks before type
  validation give clearer error messages for handlers that return nil.
- **ProtoParser qualified type support**: the RPC regex now accepts
  qualified type names (e.g., `shop.v1.ProductMessage`) via `[\w.]+`
  instead of `\w+`.
- **`rpc_service` configuration guard**: raises a clear error when
  neither `proto_file` nor `proto_dir` is configured, instead of
  failing with a confusing `NoMethodError` inside `ProtoParser`.

### Streaming integration tests and send error mapping

- **`stream_sender.send` error → `:unavailable`**: when the channel
  closes mid-stream (client disconnect) or send is cancelled (server
  shutdown), the Ruby `send` loop now rescues the error and raises
  `RpcError` with `:unavailable` (code 14) instead of falling through
  to the generic `:internal` (code 13). Different retry semantics, and
  avoids spamming error logs on routine client disconnects.
- **Server-streaming integration tests**: added `StreamProducts` RPC
  to `shop.proto` with a handler emitting up to 50 messages (>32
  channel capacity). Integration tests verify: (1) 5 messages stream
  correctly, (2) 50 messages all arrive in order despite channel
  backpressure, (3) minimal stream (limit=1) works.
- **`stream_products` handler**: destructures the `SimulatedDB#all`
  tuple correctly and returns string-keyed product hashes directly.
