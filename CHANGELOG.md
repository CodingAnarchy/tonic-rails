# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/). Until 1.0,
minor versions may contain breaking changes.

## [Unreleased]

### Added

- Response metadata: handlers set `response_metadata` (headers) and
  `trailing_metadata` (trailers, sent on success or failure).
- Rich errors: `RpcError.new(..., metadata:, details:)` sends trailing
  metadata and a `google.rpc.Status` with packed details in
  `grpc-status-details-bin`.
- `Tonic::Rails::Testing.invoke` returns the response together with response
  headers and trailers.
- `process_rpc.tonic_rails` ActiveSupport::Notifications event for every RPC,
  with path, service, rpc, metadata, status and code (for APM agents,
  OpenTelemetry and custom subscribers).

### Changed

- Response headers are sent with the handler's first output instead of
  before the handler runs, so handlers can set them. The client deadline
  applies while waiting.
- `log_requests` now logs one line per RPC through the configured (Rails)
  logger instead of stderr.
- Transport events from the native extension (rejected calls, expired
  deadlines, refused connections, faults) now go through the configured
  (Rails) logger, with severities, instead of stderr. Handler outcomes such
  as a `NOT_FOUND` raised by a handler are no longer logged by the transport
  at all; use `log_requests` or the instrumentation event for those.

## [0.1.0] - 2026-10-10

First public release.

### Serving

- gRPC server for Rails built on tonic 0.14 (HTTP/2, TLS via rustls,
  keepalive, flow control), with Ruby handlers running on a pool of
  dispatch threads inside the Rails executor.
- Unary, server-streaming, client-streaming and bidirectional RPCs, with
  backpressure in both directions and incremental streaming.
- Handlers declared with `rpc_service` (read from the protobuf descriptor
  pool) or individual `rpc` calls; requests as Hashes or, with
  `request_format :message`, protobuf messages; responses as either.
- Routes registered at runtime: no service definitions are compiled into
  the native extension.
- Built-in `grpc.health.v1` (per service, flips to NOT_SERVING on
  shutdown) and opt-in `grpc.reflection.v1` generated from loaded
  descriptors.
- Interceptors, request metadata, client deadlines (`deadline_ms`,
  `deadline_exceeded?`) and shutdown awareness (`cancelled?`).
- `Tonic::Rails::Testing.call` for in-process handler tests.
- Railtie: `config.tonic_rails.*` settings, Rails logger and error
  reporter, code reloading support, rake tasks `tonic_rails:serve`,
  `tonic_rails:routes` and `tonic_rails:compile_protos`.

### Production behaviour

- Graceful shutdown on SIGTERM/SIGINT: health flips to NOT_SERVING, new
  connections are refused, in-flight calls drain for `drain_timeout`.
- Limits: `max_message_size` (also caps gzip-decompressed size),
  `max_connections`, `concurrency_limit_per_connection`, request metadata
  size, and `dispatch_queue_size` load shedding (`RESOURCE_EXHAUSTED`).
- TLS and mutual TLS (`tls_client_ca_path`). Invalid TLS configuration
  stops startup; the server never falls back to plaintext.
- Unexpected exceptions (in handlers or interceptors) reach clients only
  as `INTERNAL: internal error`; details are logged and reported.
- `grpc-message` is percent-encoded per the gRPC spec.
- Precompiled native gems for Linux (x86_64, aarch64; glibc and musl) and
  macOS (x86_64, arm64), Ruby 3.3 to 4.0.

[Unreleased]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/CodingAnarchy/tonic-rails/releases/tag/v0.1.0
