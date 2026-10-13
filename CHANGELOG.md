# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/). Until 1.0,
minor versions may contain breaking changes.

## [Unreleased]

### Fixed

- Client: a call whose deadline passed could raise `CANCELLED` ("Timeout
  expired") instead of `DEADLINE_EXCEEDED` (#48), mostly on Windows.
  tonic enforces the deadline too, and when its timer fired first the
  call ended with its status. Calls now raise `DEADLINE_EXCEEDED`
  whichever timer fires first.

## [0.8.0] - 2026-10-12

### Added

- Client: `method_retries:` sets retry policies per service
  (`"demo.v1.ItemService"`) or per method
  (`"demo.v1.ItemService/GetItem"`), like gRPC's service config. The most
  specific setting wins: the call's `retries:`, then the method, then the
  service, then the client's `retries:`.

### Fixed

- Windows: the process could crash with a segfault under load (#41). The
  extension's locks spun and then called `Sleep`, which the mingw linker
  bound to the `Sleep` that Ruby's DLL exports instead of the system's,
  and Ruby's version crashes on threads Ruby doesn't own. Locks now come
  from Rust's standard library, which doesn't spin through `Sleep`, the
  build binds `Sleep` to kernel32, and CI checks that the Windows build
  imports nothing but Ruby's C API from Ruby's DLL. 0.6.0 and 0.7.0 crashed in about 1 in 5 runs of the test suite;
  earlier versions could crash in the same way, but rarely.

## [0.7.0] - 2026-10-11

### Added

- Client: opt-in retries, per client or per call (`retries: true` or a
  Hash of `max_attempts`, `codes`, `initial_backoff`, `max_backoff` and
  `multiplier`), following gRPC's retry design. Only calls the server
  never started on are retried: a failed connection, or a status sent
  without response headers. Waits are random with exponential caps, stop
  before the deadline and follow `grpc-retry-pushback-ms`. Each retry is
  logged, and `request_rpc.tonic_rails` reports `:attempts`. Closes #39.

### Changed

- The server answers calls it turns away before a handler runs (shutting
  down, dispatch queue full, malformed requests) with a Trailers-Only
  response: the status in the headers and no body. Clients that follow
  gRPC's retry rules can tell those calls never started and retry them
  safely.

## [0.6.0] - 2026-10-11

### Added

- Client: `Tonic::Rails::Client` makes unary calls to other gRPC
  services without the grpc gem. Stubs come from the descriptors that
  `tonic_rails:compile_protos` generates. Calls take deadlines (and
  default to the deadline of the RPC being handled), send metadata,
  return headers and trailers with `return_call: true`, and raise
  `RpcError` with rich details. TLS uses the system's root certificates,
  a custom CA or mutual TLS. Clients keep working across `fork` with no
  hooks, and `Timeout` and `Thread#raise` interrupt calls. Each call
  publishes a `request_rpc.tonic_rails` event. Closes #37.
- Client: server-streaming calls. The stub method returns a stream that
  yields each message as it arrives. A slow reader holds the server back
  through HTTP/2 flow control, and stopping early (`break`, `first(n)`,
  an exception) cancels the call. The deadline covers the whole stream,
  an error partway through raises `RpcError` after the messages before
  it, and `headers` and `trailers` are available once the stream ends.
  Closes #38.

## [0.5.0] - 2026-10-11

### Added

- Multi-process mode: `config.processes = 4` runs four server processes
  on one port, so services that spend their time in Ruby use four cores
  instead of one. The app loads once and forks the workers. They share
  the listening socket, with new connections going to the least busy
  worker. Workers that die are replaced, and SIGTERM drains them all.
  `on_worker_boot` runs in each worker after fork. Works with
  `tonic_rails:serve` and the Puma plugin; Unix only. Closes #23.
- Benchmark: `PROCESSES` and `CONNECTIONS` for `bench/run_bench.sh`, and
  `-connections` for the Go client.

### Changed

- The Puma plugin waits `drain_timeout` + 10 seconds (was + 5) for the
  gRPC server to stop before killing it, which leaves a multi-process
  server time to stop its own workers.

## [0.4.0] - 2026-10-11

### Added

- gRPC-Web: `config.grpc_web = true` serves browser clients (unary and
  server-streaming RPCs, binary and text encodings) without a proxy, on the
  same port as native gRPC, with CORS via `grpc_web_origins`,
  `grpc_web_allowed_headers` and `grpc_web_exposed_headers`. Closes #22.

## [0.3.0] - 2026-10-11

### Added

- Generators: `rails g tonic_rails:install` (initializer, proto and
  generated-code directories, Puma plugin) and `rails g tonic_rails:service
  Item GetItem ListItems:server` (proto, handler with a stub per RPC,
  registration, and an RSpec or Minitest test).
- Controller-style callbacks for handlers: `before_rpc`, `after_rpc` and
  `around_rpc` with `only:` / `except:`, plus `current_rpc` and `request`
  accessors.
- `rescue_from` (ActiveSupport::Rescuable) with a `code:` shorthand that maps
  an exception class to a gRPC status.
- `peer` in handlers: the client's address and, with mutual TLS, its
  certificate (`peer.certificate`, an `OpenSSL::X509::Certificate`).
  `Testing.call`/`invoke` accept `peer:`.
- App-controlled health: `config.health_check` (polled every
  `health_check_interval` seconds inside the Rails executor) and
  `Tonic::Rails.server.serving!` / `not_serving!` for the whole server or
  individual services, so readiness probes reflect real dependency health.
- Test helpers: the `raise_grpc_error(code).with_message(...)` RSpec matcher
  (`require "tonic/rails/testing/rspec"`) and `assert_grpc_error` for Minitest
  (`Tonic::Rails::Testing::Assertions`). Both accept `Tonic::Rails::RpcError`
  and the grpc gem's `GRPC::BadStatus`.

### Changed

- `tonic_rails:compile_protos` requires protoc 22 or newer and says so,
  instead of generating code google-protobuf 4 can't load (older protoc,
  such as Ubuntu's 3.21 package, emits a Ruby DSL that google-protobuf 4
  removed).

## [0.2.0] - 2026-10-11

### Added

- Instrumentation: every RPC publishes a `process_rpc.tonic_rails`
  ActiveSupport::Notifications event (path, service, rpc, metadata, status,
  code) for APM agents, OpenTelemetry and custom subscribers. With no
  subscribers it costs nothing measurable.
- Response metadata: handlers set `response_metadata` (headers) and
  `trailing_metadata` (trailers, sent on success or failure).
- Rich errors: `RpcError.new(..., metadata:, details:)` sends trailing
  metadata and a `google.rpc.Status` with packed details in
  `grpc-status-details-bin`, which official gRPC clients decode.
- Puma plugin: `plugin :tonic_rails` in `config/puma.rb` runs the gRPC
  server as a child process of Puma, in single and cluster mode, with
  graceful stop and restart, and a shutdown if either side dies.
- Windows: precompiled `x64-mingw-ucrt` gems, tested in CI.
- `Tonic::Rails::Testing.invoke` returns the response together with its
  headers and trailers.

### Changed

- Response headers are sent with the handler's first output instead of
  before the handler runs, so handlers can set them. The client deadline
  applies while waiting.
- `log_requests` logs one line per RPC through the configured (Rails)
  logger instead of stderr.
- Transport events from the native extension (rejected calls, expired
  deadlines, refused connections, faults) go through the configured (Rails)
  logger with severities instead of stderr. Handler outcomes, such as a
  `NOT_FOUND` raised by a handler, are no longer logged by the transport;
  use `log_requests` or the instrumentation event for those.
- `Server#stop` signals shutdown through a queue instead of a pipe, so
  signal handling works the same on every platform.
- Dependencies: magnus 0.9, google-protobuf 4.36.

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

[Unreleased]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.8.0...HEAD
[0.8.0]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/CodingAnarchy/tonic-rails/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/CodingAnarchy/tonic-rails/releases/tag/v0.1.0
