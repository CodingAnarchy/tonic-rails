# tonic-rails

gRPC endpoints for Rails, powered by [tonic] via a Rust native extension.

tonic-rails brings gRPC to Rails applications. The HTTP/2 transport layer
(framing, connection management, flow control, TLS, health checks, reflection)
is implemented in Rust using [tonic] and exposed to Ruby through a native
extension built with [Magnus]. Protobuf (de)serialization and business logic
remain in Ruby, giving you Rails-grade controllers, routing, and middleware
for your gRPC services.

## Architecture

```
                     ┌─────────────────────────────────┐
                     │         Rust (tonic)             │
  gRPC client ──H2──▶│  HTTP/2 transport + framing      │
                     │  tonic-health (Rust-native)      │
                     │  tonic-reflection (Rust-native)  │
                     │  ┌─────────────────────────┐     │
                     │  │  GVL bridge (Magnus)    │     │
                     │  │  raw bytes ──▶ Ruby     │     │
                     │  │  raw bytes ◀── Ruby     │     │
                     │  └─────────────────────────┘     │
                     └────────────┬────────────────────┘
                                  │ GVL
                     ┌────────────▼────────────────────┐
                     │         Ruby (Rails)             │
                     │  google-protobuf decode (C ext)  │
                     │  handler (your business logic)   │
                     │  google-protobuf encode (C ext)  │
                     └─────────────────────────────────┘
```

The transport layer runs in Rust without the GVL. Application RPCs cross
the GVL boundary via `rb_thread_call_with_gvl` to execute Ruby handlers.
During blocking I/O (DB queries, HTTP calls), the GVL is released, allowing
other dispatch threads to run concurrently.

`grpc.health.v1.Health/Check` and `grpc.reflection.v1.ServerReflection/*`
are served entirely in Rust — zero GVL overhead.

## Installation

Add to your Gemfile:

```ruby
gem "tonic-rails"
```

Then install:

```sh
bundle install
rake compile   # builds the Rust native extension
```

The native extension requires Rust (via [rustup] or Homebrew) and
`protoc` (the Protocol Buffer compiler).

## Quick start

### 1. Define your proto

```protobuf
syntax = "proto3";
package shop;

service ProductService {
  rpc GetProduct(GetProductRequest) returns (Product);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}

message GetProductRequest { int64 id = 1; }
message Product { int64 id = 1; string title = 2; int64 price_cents = 3; }
message HealthCheckRequest {}
message HealthCheckResponse { bool healthy = 1; }
```

Generate Ruby protobuf classes:

```sh
protoc --ruby_out=app/grpc -Iprotos protos/shop.proto
```

### 2. Define a service handler

#### Auto-registration from proto (recommended)

Use `rpc_service` to auto-register all RPCs from the `.proto` file —
no need to declare each `rpc :Method, Req, Resp` individually:

```ruby
class ProductService
  include Tonic::Rails::Base
  rpc_service "shop.ProductService"  # scans config.proto_dir

  def get_product(request)
    product = Product.find(request["id"])
    raise Tonic::Rails::RpcError.new("not found", code: :not_found) unless product

    {
      "id" => product.id,
      "title" => product.title,
      "price_cents" => product.price_cents,
    }
  end

  def health_check(_request)
    { "healthy" => true }
  end
end
```

`rpc_service` sets the service name and registers all RPCs by parsing
the `.proto` file. The proto directory defaults to `"protos"` and can
be configured:

```ruby
Tonic::Rails.configure do |config|
  config.proto_dir = "app/protos"
end
```

Or specify the proto file explicitly:

```ruby
rpc_service "shop.v1.ProductService", proto_file: "protos/shop.proto"
```

#### Manual registration

For finer control, declare each RPC individually:

```ruby
class ProductService
  include Tonic::Rails::Base
  service "shop.ProductService"
  rpc :GetProduct, Shop::GetProductRequest, Shop::Product
  rpc :HealthCheck, Shop::HealthCheckRequest, Shop::HealthCheckResponse
end
```

Handlers receive a decoded Hash (via `google-protobuf`) and return a Hash
that gets encoded back to protobuf. Raise `Tonic::Rails::RpcError` for
gRPC error codes. The Ruby class name is irrelevant — routing is by the
`service` declaration.

### Server-streaming RPCs

Declare a server-streaming RPC with `stream: :server`:

```ruby
class ProductService
  include Tonic::Rails::Base
  service "shop.ProductService"

  rpc :ListProducts, Shop::ListProductsRequest, Shop::Product, stream: :server

  def list_products(request)
    Product.where("price_cents <= ?", request["max_price_cents"]).map do |p|
      {
        "id" => p.id,
        "title" => p.title,
        "price_cents" => p.price_cents,
      }
    end
  end
end
```

Streaming handlers return an Enumerable (Array, Enumerator, etc.) of
response Hashes. Each element is encoded as a separate gRPC message.

> **Buffered streaming**: The full Enumerable is materialized in Ruby
> before any bytes are sent on the wire. This is suitable for finite
> result sets (queries, listings, exports) but not for infinite/tail
> streams. True incremental streaming (flush per chunk) would require
> an mpsc channel from Ruby→Rust and a streaming HTTP/2 body — a
> future enhancement.

### 3. Configure and start

```ruby
# config/initializers/tonic_rails.rb
Tonic::Rails.configure do |config|
  config.bind_address = "0.0.0.0:50051"
  config.dispatch_threads = ENV.fetch("RAILS_MAX_THREADS", "4").to_i
  config.auto_start = true  # start alongside Rails server
end

# Register services
Tonic::Rails.server.register(ProductService)
```

Or start manually:

```ruby
Tonic::Rails.server.start
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `bind_address` | `0.0.0.0:50051` | Host and port to bind |
| `dispatch_threads` | `4` | Ruby dispatch threads (set to `RAILS_MAX_THREADS`) |
| `worker_threads` | num CPUs | Tokio worker threads for transport |
| `max_message_size` | 4 MB | Max request/response size |
| `tls_cert_path` | nil | PEM certificate path for TLS |
| `tls_key_path` | nil | PEM private key path for TLS |
| `drain_timeout` | 10s | Graceful shutdown drain timeout |
| `max_connections` | 10,000 | Max concurrent TCP connections |
| `auto_start` | false | Start alongside Rails server |
| `log_requests` | false | Log every request (not just errors) |
| `interceptors` | `[]` | Array of interceptor procs |
| `compression` | false | Enable gRPC gzip compression (responses >1KB) |

### Dispatch threads

`dispatch_threads` should match `RAILS_MAX_THREADS` so the dispatch thread
pool stays in sync with the ActiveRecord connection pool. Each dispatch
thread can run one Ruby handler at a time; during blocking I/O (DB queries),
the GVL is released and another thread runs concurrently.

For high-throughput I/O-bound workloads, 16 threads per process is the
sweet spot. Use SO_REUSEPORT with multiple processes for higher aggregate
throughput.

## Features

- **HTTP/2 transport**: tonic-based, with TLS, keepalive, and SO_REUSEPORT
- **Health checks**: `grpc.health.v1.Health/Check` served natively in Rust
- **Reflection**: `grpc.reflection.v1.ServerReflection` for `grpcurl` / client discovery
- **Graceful shutdown**: drain timeout with in-flight request tracking
- **Interceptors**: request middleware chain (auth, logging, tracing)
- **Metadata**: gRPC metadata headers passed to handlers
- **Deadlines**: per-request deadline propagation and enforcement
- **Cancellation**: `Tonic::Rails.cancelled?` for graceful shutdown
- **Compression**: gRPC frame-level gzip (responses >1KB when enabled)
- **Server streaming**: buffered server-streaming RPCs via `stream: :server`
- **Message size limits**: configurable max message size
- **Stats**: connection count, request count, latency tracking

### Proto compilation

Generate Ruby protobuf classes from `.proto` files:

```sh
rake tonic_rails:compile_protos          # uses app/protos → app/grpc
PROTO_DIR=protos RUBY_OUT=app/grpc \
  rake tonic_rails:compile_protos        # custom paths
```

Requires `protoc` (`brew install protobuf`).

### Deadline propagation

When a client sends a `grpc-timeout` header, the Rust layer injects
`_timeout_ms` (remaining milliseconds) into the handler metadata.
Check the remaining budget in your handler:

```ruby
def get_product(request)
  return if cancelled?  # server shutting down
  remaining = deadline_ms  # nil if no deadline set
  # abort if deadline is close
  raise Tonic::Rails::RpcError.new("deadline too tight", code: :deadline_exceeded) if remaining && remaining < 10
  # ...
end
```

## Error handling

```ruby
def get_product(request)
  product = Product.find_by(id: request["id"])
  raise Tonic::Rails::RpcError.new("product not found", code: :not_found) unless product
  raise Tonic::Rails::RpcError.new("invalid id", code: :invalid_argument) if request["id"] <= 0
  # ...
end
```

Supported codes: `:ok`, `:cancelled`, `:unknown`, `:invalid_argument`,
`:not_found`, `:already_exists`, `:permission_denied`, `:unauthenticated`,
`:resource_exhausted`, `:failed_precondition`, `:aborted`, `:out_of_range`,
`:unimplemented`, `:internal`, `:unavailable`, `:data_loss`.

## Testing

```sh
bundle exec rspec
```

## Benchmarking

See [PERF.md](PERF.md) for a detailed performance comparison against pure
Rust tonic, including GVL overhead analysis.

```sh
# Ruby server
SIMULATED_DB_LATENCY_MS=2 TONIC_RAILS_DISPATCH_THREADS=50 \
  bundle exec ruby -Ibench -e 'require_relative "bench/bench_service"; Tonic::Rails.server.start'

# Rust reference server
cd bench/rust_bench && cargo build --release
SIMULATED_DB_LATENCY_MS=2 ./target/release/tonic-bench-server

# Go benchmark client
cd bench/go_bench
go run . -addr 127.0.0.1:50051 -rpc GetProduct -concurrency 50 -total 2000
```

## License

MIT

[tonic]: https://github.com/hyperium/tonic
[Magnus]: https://github.com/matsadler/magnus
[rustup]: https://rustup.rs
