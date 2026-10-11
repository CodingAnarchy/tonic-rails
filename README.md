# tonic-rails

[![CI](https://github.com/CodingAnarchy/tonic-rails/actions/workflows/ci.yml/badge.svg)](https://github.com/CodingAnarchy/tonic-rails/actions/workflows/ci.yml)
[![Gem Version](https://badge.fury.io/rb/tonic-rails.svg)](https://rubygems.org/gems/tonic-rails)

Serve gRPC from your Rails app, with the transport handled by
[tonic], Rust's production gRPC stack.

Think of it like Puma for gRPC. Puma handles sockets and HTTP so your
controllers only see requests. tonic-rails does the same for gRPC:
HTTP/2, TLS, flow control, keepalives, health checks and reflection all
run in Rust, outside the GVL. Your handlers are plain Ruby classes that
receive protobuf messages and return them, inside the Rails executor,
with your models, caches and error reporting all available.

```ruby
class ItemService
  include Tonic::Rails::Base
  rpc_service "demo.v1.ItemService"
  request_format :message

  def get_item(request)
    item = Item.find_by(id: request.id)
    raise Tonic::Rails::RpcError.new("item not found", code: :not_found) unless item

    Demo::V1::Item.new(id: item.id, name: item.name)
  end
end
```

## Why tonic-rails

- **Fast where it counts.** Transport overhead is small: an empty RPC
  runs at roughly 75–80% of a pure-Rust tonic server's throughput. I/O-bound
  handlers scale with dispatch threads because Ruby releases the GVL
  while waiting. See [PERF.md](PERF.md).
- **All four RPC kinds.** Unary, server streaming, client streaming and
  bidirectional streaming, with backpressure in both directions.
- **Production defaults.** Graceful draining on SIGTERM, health checks
  that flip to NOT_SERVING during shutdown, deadlines, message size and
  connection limits, load shedding, gzip, TLS and mutual TLS.
- **Safe by default.** Unexpected exceptions reach clients as a generic
  INTERNAL error (the details go to your logs and `Rails.error`).
  Reflection is off unless you turn it on. TLS problems stop the server
  instead of silently falling back to plaintext.
- **No Rust toolchain needed** on the platforms we ship precompiled gems
  for (Linux and macOS, x86_64 and arm64).

## Installation

```ruby
# Gemfile
gem "tonic-rails"
gem "google-protobuf"
```

Requires Ruby 3.2+ and Rails 7.1+ (or just ActiveSupport, outside Rails).
On platforms without a precompiled gem, installation builds the
extension from source, which needs a [Rust toolchain](https://rustup.rs).

To generate Ruby classes from your `.proto` files you also need `protoc`
(the Protocol Buffers compiler) during development.

## Quick start

### 1. Define the service

```protobuf
// app/protos/items.proto
syntax = "proto3";
package demo.v1;

message Item { int64 id = 1; string name = 2; }
message GetItemRequest { int64 id = 1; }

service ItemService {
  rpc GetItem(GetItemRequest) returns (Item);
}
```

Generate the Ruby classes:

```sh
bin/rails tonic_rails:compile_protos   # app/protos → lib/grpc
```

Generated files `require` each other by name, so keep them out of the
autoload paths and put their directory on the load path:

```ruby
# config/application.rb
$LOAD_PATH.unshift(File.expand_path("../lib/grpc", __dir__))

module MyApp
  class Application < Rails::Application
    config.autoload_lib(ignore: %w[grpc])
  end
end
```

### 2. Write a handler

Handlers live anywhere Rails autoloads from; `app/grpc` is a good home.
Each RPC maps to a snake_case method.

```ruby
# app/grpc/item_service.rb
require "items_pb"

class ItemService
  include Tonic::Rails::Base

  rpc_service "demo.v1.ItemService"   # every RPC, read from the descriptor
  request_format :message             # receive protobuf messages

  def get_item(request)
    item = Item.find(request.id)
    Demo::V1::Item.new(id: item.id, name: item.name)
  rescue ActiveRecord::RecordNotFound
    raise Tonic::Rails::RpcError.new("item #{request.id} not found", code: :not_found)
  end
end
```

### 3. Register and run

Register handlers in `to_prepare` so they work with code reloading:

```ruby
# config/initializers/tonic_rails.rb
Rails.application.config.to_prepare do
  Tonic::Rails.server.register(ItemService)
end
```

```sh
bin/rails tonic_rails:serve    # serves on 0.0.0.0:50051
bin/rails tonic_rails:routes   # lists registered methods
```

Run it as its own process (a separate Procfile entry, container or
Kubernetes deployment) next to your web server.

The [example app](example/) is a complete, runnable version of all of the
above.

## Handlers

### Requests and responses

By default handlers receive a string-keyed Hash. With
`request_format :message` they receive the decoded protobuf message
instead, which is faster and type-safe. Either way, return a protobuf
message or a Hash:

```ruby
def get_item(request)
  { "id" => request.id, "name" => "Widget" }   # a Hash works too
end
```

A fresh handler instance serves each call, so instance variables never
leak between requests.

### Declaring RPCs

`rpc_service "package.Service"` reads every RPC (and its streaming kind)
from the generated descriptor. To declare RPCs one at a time instead:

```ruby
class ItemService
  include Tonic::Rails::Base
  service "demo.v1.ItemService"

  rpc :GetItem, Demo::V1::GetItemRequest, Demo::V1::Item
  rpc :ListItems, Demo::V1::ListItemsRequest, Demo::V1::Item, stream: :server
end
```

RPCs declared without a handler method answer `UNIMPLEMENTED`.

### Streaming

Server streaming: return any Enumerable. Messages go out as you produce
them, and if the client reads slowly your handler waits (without holding
the GVL) instead of buffering everything in memory.

```ruby
def list_items(request)
  Enumerator.new do |stream|
    Item.where(tag: request.tag).find_each do |item|
      break if cancelled?   # the server is shutting down
      stream << Demo::V1::Item.new(id: item.id, name: item.name)
    end
  end
end
```

Client streaming: the argument is an Enumerator that yields each request
as it arrives.

```ruby
def import_items(requests)
  created = requests.count { |r| Item.create!(name: r.name) }
  Demo::V1::ImportItemsResponse.new(created: created)
end
```

Bidirectional streaming combines both: take the request Enumerator and
return an Enumerable. Responses can be sent before the client finishes
sending.

```ruby
def chat(messages)
  Enumerator.new do |out|
    messages.each { |m| out << Demo::V1::Reply.new(text: m.text.upcase) }
  end
end
```

### Errors

Raise `Tonic::Rails::RpcError` to end a call with a status:

```ruby
raise Tonic::Rails::RpcError.new("name is required", code: :invalid_argument)
```

Codes are the standard gRPC names (`:not_found`, `:permission_denied`,
`:unauthenticated`, `:unavailable`, and so on) or their numbers. The
message goes to the client, so keep internal details out of it.

Any other exception becomes `INTERNAL` with the message `internal error`.
The real exception is logged and passed to `Rails.error.report`, so your
error tracker sees it.

### Metadata, deadlines and shutdown

```ruby
def get_item(request)
  token = grpc_metadata["authorization"]   # request headers, lower-case
  deadline_ms                                # ms left before the client's deadline, or nil
  deadline_exceeded?                         # true once it has passed
  cancelled?                                 # true while the server shuts down
end
```

When the deadline passes, the client gets `DEADLINE_EXCEEDED` right away;
check `deadline_exceeded?` in long loops to stop wasted work.

### Interceptors

Interceptors wrap every call, outermost first. Call `next_call` to
continue, or raise to reject:

```ruby
Tonic::Rails.configure do |config|
  config.interceptors << lambda do |path, metadata, next_call|
    unless valid_token?(metadata["authorization"])
      raise Tonic::Rails::RpcError.new("invalid token", code: :unauthenticated)
    end
    next_call.call
  end
end
```

## Testing handlers

`Tonic::Rails::Testing` calls a handler in-process, with real protobuf
encoding, no network and no native extension:

```ruby
require "tonic/rails/testing"

response = Tonic::Rails::Testing.call(ItemService, :GetItem, { id: 1 })
expect(response.name).to eq("Widget")

expect { Tonic::Rails::Testing.call(ItemService, :GetItem, { id: 0 }) }
  .to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code_name).to eq(:not_found) }

# Streaming: pass an Array of requests, get an Array of responses
Tonic::Rails::Testing.call(ItemService, :ImportItems, [{ name: "a" }, { name: "b" }])
```

## Configuration

Set options in `config/application.rb` or an environment file:

```ruby
config.tonic_rails.bind_address = "0.0.0.0:50051"
config.tonic_rails.dispatch_threads = ENV.fetch("RAILS_MAX_THREADS", 5).to_i
```

or with a block (which runs after, and wins over, `config.tonic_rails`):

```ruby
Tonic::Rails.configure do |config|
  config.tls_cert_path = "/etc/tls/tls.crt"
  config.tls_key_path = "/etc/tls/tls.key"
end
```

| Option | Default | Description |
|--------|---------|-------------|
| `bind_address` | `"0.0.0.0:50051"` | Address to listen on |
| `dispatch_threads` | `4` | Ruby threads running handlers; match your database pool |
| `dispatch_queue_size` | `128` | Calls that may wait for a thread; beyond this, `RESOURCE_EXHAUSTED` |
| `worker_threads` | CPU count | Tokio threads for the transport |
| `max_message_size` | 4 MiB | Request and response limit (also caps decompressed size) |
| `max_connections` | `64` | Concurrent connections; `0` = unlimited |
| `concurrency_limit_per_connection` | `32` | Concurrent streams per connection; `0` = unlimited |
| `tls_cert_path`, `tls_key_path` | `nil` | PEM certificate and key; set both to enable TLS |
| `tls_client_ca_path` | `nil` | PEM CA bundle; requires client certificates (mutual TLS) |
| `drain_timeout` | `10` | Seconds to let in-flight calls finish on shutdown |
| `keepalive_interval`, `keepalive_timeout` | `15`, `5` | HTTP/2 PING interval and timeout, seconds |
| `compression` | `false` | gzip responses over 1 KiB for clients that accept it |
| `reflection` | `false` | Serve `grpc.reflection.v1`; exposes your schema |
| `interceptors` | `[]` | See [Interceptors](#interceptors) |
| `log_requests` | `false` | Log every call to stderr (errors are always logged) |
| `auto_start` | `false` | Start inside `rails server` (see below) |

### Sizing threads

Handlers run on `dispatch_threads` Ruby threads. Blocking I/O (database,
HTTP) releases the GVL, so more threads means more concurrent I/O. CPU
work does not parallelise within a process. Set threads to the
concurrency your I/O needs, size the database pool to match, and add
processes for CPU-heavy services: the listener uses `SO_REUSEPORT`, so
several processes can bind the same port.

### auto_start

`auto_start = true` starts the gRPC server in a background thread of
`rails server` (never in consoles, rake tasks or tests). It's handy in
development. In production prefer a dedicated `tonic_rails:serve`
process: forking servers such as Puma in cluster mode would need to
start one per worker.

## Operations

- **Health checks.** `grpc.health.v1.Health` reports `SERVING` for the
  server (`""`) and every registered service, and flips to `NOT_SERVING`
  as soon as shutdown begins. Point your load balancer or Kubernetes
  gRPC probes at it.
- **Graceful shutdown.** On SIGTERM or SIGINT the server stops
  accepting connections, tells clients to go away (HTTP/2 GOAWAY), lets
  in-flight calls finish for up to `drain_timeout` seconds, then exits.
- **Load shedding.** When every dispatch thread is busy and the queue is
  full, new calls fail fast with `RESOURCE_EXHAUSTED` instead of piling up.
- **Stats.** `Tonic::Rails.server.stats` returns counters for totals,
  successes, errors, cancellations, deadline and resource errors, active
  connections and queued calls.
- **Reflection.** Turn on `reflection` in development to use
  [grpcurl](https://github.com/fullstorydev/grpcurl) without proto files.

## How it works

```
 client ──HTTP/2──▶ tonic (Rust, no GVL)
                     ├─ TLS, flow control, keepalive, limits
                     ├─ grpc.health.v1, grpc.reflection.v1
                     └─ everything else ─▶ queue ─▶ Ruby dispatch thread
                                                   (takes the GVL)
                                                   decode → handler → encode
```

Rust never decodes your messages; it passes raw protobuf bytes to Ruby,
where the `google-protobuf` C extension decodes them once. Routes come
from the handlers you register, so nothing about your services is
compiled into the extension.

## Development

```sh
bundle install
bundle exec rake compile   # build the extension (needs Rust)
bundle exec rake           # Ruby specs + Rust unit tests
bundle exec rubocop
bundle exec rake cargo:lint
```

The integration specs start real servers and call them with the official
`grpc` gem; the reflection specs also need `grpcurl` on your PATH.
Benchmarks live in [bench/](bench/) (see [PERF.md](PERF.md)). See
[CONTRIBUTING.md](CONTRIBUTING.md) for more.

## License

MIT. See [LICENSE](LICENSE).

[tonic]: https://github.com/hyperium/tonic
