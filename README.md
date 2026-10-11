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

- **Fast where it counts.** Inside a Rails app, light RPCs ran at 5–6×
  the throughput of an equivalent Rails JSON endpoint and 1.4–3× the
  official `grpc` gem, and server streaming at 4–7× the `grpc` gem.
  Handlers dominated by slow queries or heavy object building see smaller
  gains. See [PERF.md](PERF.md).
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
  for: Linux and macOS (x86_64 and arm64) and Windows (x64).

## Installation

```ruby
# Gemfile
gem "tonic-rails"
gem "google-protobuf"
```

Requires Ruby 3.3+ and Rails 7.1+ (or just ActiveSupport, outside Rails).
On platforms without a precompiled gem, installation builds the
extension from source, which needs a [Rust toolchain](https://rustup.rs).

To generate Ruby classes from your `.proto` files you also need `protoc`
(the Protocol Buffers compiler) **22.0 or newer** during development.
Older versions, such as Ubuntu's `protobuf-compiler` package (3.21),
generate code that google-protobuf 4 can't load; install a current
release from the [protobuf releases page](https://github.com/protocolbuffers/protobuf/releases)
or Homebrew.

## Quick start

The generators set everything up:

```sh
bin/rails generate tonic_rails:install
bin/rails generate tonic_rails:service Item GetItem ListItems:server
bin/rails tonic_rails:compile_protos
```

`install` adds `config/initializers/tonic_rails.rb` (load path for
generated code, configuration, handler registration), `app/protos/`,
`lib/grpc/`, and the Puma plugin. `service` writes the `.proto`, a handler
in `app/grpc/` with a stub per RPC, its registration, and a spec (or
Minitest test). RPCs take an optional `:server`, `:client` or `:bidi`
streaming kind.

The rest of this section shows the same steps by hand.

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
Kubernetes deployment) next to your web server, or let Puma run it for
you (see [Running with Puma](#running-with-puma)).

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

### Callbacks and rescue_from

Handlers get controller-style callbacks and exception mapping:

```ruby
class ItemService
  include Tonic::Rails::Base
  rpc_service "demo.v1.ItemService"

  before_rpc :authenticate, except: :ListItems
  around_rpc :with_tenant
  after_rpc { StatsD.increment("grpc.#{current_rpc}") }

  rescue_from ActiveRecord::RecordNotFound, code: :not_found
  rescue_from ActiveRecord::RecordInvalid, code: :invalid_argument, message: ->(e) { e.record.errors.full_messages.join(", ") }
  rescue_from Pundit::NotAuthorizedError, with: :forbidden

  private

  def authenticate
    @user = User.from_token(grpc_metadata["authorization"])
    raise Tonic::Rails::RpcError.new("invalid token", code: :unauthenticated) unless @user
  end

  def with_tenant(&) = Tenant.switch(@user.tenant, &)

  def forbidden(_e) = raise(Tonic::Rails::RpcError.new("not allowed", code: :permission_denied))
end
```

- `before_rpc`, `after_rpc` and `around_rpc` accept method names or a
  block, plus `only:` / `except:` with RPC names (`:GetItem` or
  `:get_item`). Raising in a `before_rpc` stops the call.
- `current_rpc` is the RPC being handled and `request` the decoded
  request, so callbacks can inspect it.
- `after_rpc` runs once a streamed response has been fully sent.
- `rescue_from` covers the handler and its callbacks. With `code:`, the
  exception's message is sent to the client unless you pass `message:`.
  Handlers given with `with:` or a block must raise an `RpcError`.
  Anything unmapped becomes `INTERNAL`, as before.

### Metadata, deadlines and shutdown

```ruby
def get_item(request)
  token = grpc_metadata["authorization"]   # request headers, lower-case
  peer.ip                                    # client address ("10.0.4.17")
  peer.certificate&.subject                  # client certificate, with mutual TLS
  deadline_ms                                # ms left before the client's deadline, or nil
  deadline_exceeded?                         # true once it has passed
  cancelled?                                 # true while the server shuts down
end
```

When the deadline passes, the client gets `DEADLINE_EXCEEDED` right away;
check `deadline_exceeded?` in long loops to stop wasted work.

### Response metadata

Set response headers and trailers from a handler:

```ruby
def get_item(request)
  response_metadata["x-cache"] = "hit"           # headers
  trailing_metadata["x-cost-ms"] = "3"           # trailers, sent with the status
  response_metadata["x-trace-bin"] = trace_bytes # -bin names take binary values
  Demo::V1::Item.new(id: request.id)
end
```

For streaming responses, set headers before the first message is sent.
Trailers are sent whether the call succeeds or fails.

### Rich errors

`RpcError` can carry trailing metadata and
[rich error details](https://grpc.io/docs/guides/error/#richer-error-model):
protobuf messages packed into a `google.rpc.Status` in the standard
`grpc-status-details-bin` trailer, which official gRPC clients decode for
you.

```ruby
raise Tonic::Rails::RpcError.new(
  "invalid item",
  code: :invalid_argument,
  metadata: { "x-request-id" => request_id },
  details: [Google::Rpc::BadRequest.new(field_violations: violations)],
)
```

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

# Response metadata
result = Tonic::Rails::Testing.invoke(ItemService, :GetItem, { id: 1 })
result.headers["x-cache"]    # => "hit"
result.trailers["x-cost-ms"] # => "3"
```

Matchers for gRPC statuses work with `Testing.call` and with the official
`grpc` client's errors:

```ruby
# spec/rails_helper.rb
require "tonic/rails/testing/rspec"

expect { Tonic::Rails::Testing.call(ItemService, :GetItem, { id: 0 }) }
  .to raise_grpc_error(:not_found).with_message(/0/)
```

```ruby
# Minitest
require "tonic/rails/testing/minitest"

class ItemServiceTest < ActiveSupport::TestCase
  include Tonic::Rails::Testing::Assertions

  test "missing items" do
    assert_grpc_error(:not_found) { Tonic::Rails::Testing.call(ItemService, :GetItem, { id: 0 }) }
  end
end
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
| `health_check` | `nil` | Callable; while false or raising, health reports `NOT_SERVING` |
| `health_check_interval` | `5` | Seconds between health checks |
| `compression` | `false` | gzip responses over 1 KiB for clients that accept it |
| `reflection` | `false` | Serve `grpc.reflection.v1`; exposes your schema |
| `interceptors` | `[]` | See [Interceptors](#interceptors) |
| `log_requests` | `false` | Log one line per call through `logger` (unexpected exceptions are always logged) |
| `auto_start` | `false` | Start in a thread of `rails server`; for development (see [auto_start](#auto_start)) |

### Sizing threads

Handlers run on `dispatch_threads` Ruby threads. Blocking I/O (database,
HTTP) releases the GVL, so more threads means more concurrent I/O. CPU
work does not parallelise within a process. Set threads to the
concurrency your I/O needs, size the database pool to match, and add
processes for CPU-heavy services: the listener uses `SO_REUSEPORT`, so
several processes can bind the same port.

### Running with Puma

Add the plugin to `config/puma.rb`:

```ruby
plugin :tonic_rails
```

Puma then runs the gRPC server as one child process, in single and cluster
mode alike, so gRPC gets its own process and GVL instead of competing with
web workers. It starts after Puma boots and drains gracefully when Puma
stops or restarts. If the gRPC process dies, Puma shuts down too, so your
supervisor restarts both. It works with `rails server`, plain `puma`, and
`preload_app!`. (Not on Windows, which has no `fork()`: run
`tonic_rails:serve` as its own process there.)

For larger deployments, a dedicated `bin/rails tonic_rails:serve` process
(scaled separately) is still the most flexible option.

### auto_start

`auto_start = true` starts the gRPC server in a background thread of
`rails server` (never in consoles, rake tasks or tests). It's handy in
development; in production use the Puma plugin or a dedicated process.
Don't combine it with the plugin.

## Operations

- **Health checks.** `grpc.health.v1.Health` reports `SERVING` for the
  server (`""`) and every registered service, and flips to `NOT_SERVING`
  as soon as shutdown begins. Point your load balancer or Kubernetes
  gRPC probes at it. To make readiness reflect your dependencies, add a
  check that runs every `health_check_interval` seconds (default 5):

  ```ruby
  config.health_check = -> { ActiveRecord::Base.connection.active? }
  ```

  While it returns false or raises, the server reports `NOT_SERVING`
  (calls are still served). You can also mark the server or a service
  yourself, for example during maintenance:
  `Tonic::Rails.server.not_serving!("demo.v1.ItemService")` and
  `serving!`. A service is `SERVING` only while the check passes and it
  isn't marked otherwise.
- **Graceful shutdown.** On SIGTERM or SIGINT the server stops
  accepting connections, tells clients to go away (HTTP/2 GOAWAY), lets
  in-flight calls finish for up to `drain_timeout` seconds, then exits.
- **Load shedding.** When every dispatch thread is busy and the queue is
  full, new calls fail fast with `RESOURCE_EXHAUSTED` instead of piling up.
- **Instrumentation.** Every RPC publishes a `process_rpc.tonic_rails`
  event through `ActiveSupport::Notifications`, so APM agents,
  OpenTelemetry and your own subscribers can trace calls. See
  [Instrumentation](#instrumentation).
- **Logs.** Everything goes through your (Rails) logger. With
  `log_requests = true`, each RPC logs one line, e.g.
  `gRPC /demo.v1.ItemService/GetItem OK in 1.4ms`. Calls the transport
  rejects before reaching Ruby (unknown methods, a full queue, expired
  deadlines, oversized messages, refused connections) are always logged,
  at info for client-caused problems, warn for overload and error for faults.
- **Stats.** `Tonic::Rails.server.stats` returns counters for totals,
  successes, errors, cancellations, deadline and resource errors, active
  connections and queued calls.
- **Reflection.** Turn on `reflection` in development to use
  [grpcurl](https://github.com/fullstorydev/grpcurl) without proto files.

## Instrumentation

Every RPC is wrapped in an `ActiveSupport::Notifications` event, inside the
Rails executor, so subscribers see the same context as your handler:

```ruby
ActiveSupport::Notifications.subscribe("process_rpc.tonic_rails") do |event|
  event.payload[:path]      # "/demo.v1.ItemService/GetItem"
  event.payload[:service]   # "demo.v1.ItemService"
  event.payload[:rpc]       # "GetItem"
  event.payload[:metadata]  # request metadata
  event.payload[:status]    # :ok, :not_found, :internal, ...
  event.payload[:code]      # numeric gRPC status
  event.duration            # milliseconds
end
```

When a call fails, `event.payload[:exception_object]` is the
`Tonic::Rails::RpcError` sent to the client. If a handler raised something
unexpected, that error's `cause` is the original exception.

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
