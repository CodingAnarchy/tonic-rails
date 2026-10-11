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
- **A client too.** Make unary and server-streaming calls to other gRPC
  services, with deadlines that follow your handler's, without the
  `grpc` gem and its fork crashes. See
  [Calling other services](#calling-other-services).
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

## Browser clients (gRPC-Web)

Browsers can't speak native gRPC, but they can speak
[gRPC-Web](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md).
Turn it on and your existing handlers serve web frontends directly, with
no Envoy or other proxy:

```ruby
Tonic::Rails.configure do |config|
  config.grpc_web = true
  config.grpc_web_origins = ["https://app.example.com"]   # CORS; omit for same-origin only
  config.grpc_web_exposed_headers = %w[x-request-id]     # response metadata browsers may read
end
```

Then call it from the browser with any gRPC-Web client, for example
[Connect](https://connectrpc.com/docs/web/using-clients):

```ts
import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { ItemService } from "./gen/demo/v1/items_pb";

const client = createClient(ItemService, createGrpcWebTransport({ baseUrl: "https://api.example.com" }));
const item = await client.getItem({ id: 1n });
```

- Unary and server-streaming RPCs work; browsers can't do client or
  bidirectional streaming.
- Errors, response headers and trailers reach the browser as usual.
- Native gRPC clients keep working on the same port. Enabling gRPC-Web
  also accepts HTTP/1.1, which browsers use without TLS; over HTTPS they
  negotiate HTTP/2.
- `grpc_web_origins` sets CORS: listed origins may call with credentials
  (cookies); `"*"` allows any origin without credentials (logged as a
  warning). `grpc_web_allowed_headers` adds request headers beyond the
  standard ones (`authorization` is already allowed).

## Calling other services

tonic-rails includes a client for unary and server-streaming calls to
other gRPC services, so
an app that serves gRPC (or just calls it) doesn't need the `grpc` gem.
It runs on the same Rust stack as the server, and it keeps working after
`fork`. That matters because the `grpc` gem crashes or hangs in forked
processes (Puma with `preload_app!`, Sidekiq swarm, Resque) unless you
opt in to its Linux-only fork support and call `GRPC.prefork` and
friends around every fork yourself.

Create a client once and share it; it holds one HTTP/2 connection and is
safe to use from any thread:

```ruby
# config/initializers/grpc_clients.rb
ITEMS = Tonic::Rails::Client.new("items.internal:50051", deadline: 2)
```

Then get a stub for a service and call its methods, named in snake_case:

```ruby
items = ITEMS.stub("demo.v1.ItemService")

item = items.get_item(Demo::V1::GetItemRequest.new(id: 1))  # a message
item = items.get_item(id: 1)                                 # or its fields
item = items.get_item({ id: 1 }, deadline: 0.5, metadata: { "x-request-id" => request.request_id })
```

Stubs are built from the protobuf descriptors in the `_pb.rb` files that
`tonic_rails:compile_protos` generates, the same ones your handlers use.
There's no `grpc_tools_ruby_protoc` step and no `_services_pb.rb`.
Responses are protobuf messages.

### Server streaming

A server-streaming method returns a stream you read with `each`, or any
Enumerable method:

```ruby
items.list_items(category: "books").each { |item| index(item) }
items.list_items(category: "books").first(10)  # stops after ten
```

- Nothing is sent until you start reading. Then each message is yielded
  as it arrives, not when the stream ends.
- Stopping early, with `break`, `first(n)` or an exception, cancels the
  call, and the server sees it cancelled.
- A slow reader holds the server back. A few messages are read ahead,
  and HTTP/2 flow control stops the server after a couple of megabytes,
  so a fast server can't fill your memory.
- If the server fails partway through, you get every message it sent
  first, then `Tonic::Rails::RpcError`.
- Once the stream has ended, `headers` and `trailers` return its
  metadata.
- A stream can be read once. Call the method again for a new one.

Client and bidirectional streaming aren't supported yet; their stub
methods raise `NotImplementedError`.

### Deadlines

- `deadline:` on a call is seconds from now (`0.5`) or a `Time`. Without
  one, the client's `deadline:` applies.
- **Inside a handler, outgoing calls default to the time left on the
  handler's own deadline**, so a service that's out of time doesn't keep
  its dependencies working on an answer nobody will read. If the client
  also has a default deadline, the earlier of the two wins. A deadline on
  the call itself always wins.
- When a deadline passes, the call raises `DEADLINE_EXCEEDED`, and the
  server sees the call cancelled.
- A stream's deadline covers the whole stream, including the time your
  block spends on each message. It's set when you call the method, so a
  stream read later still keeps the handler's deadline.

Set a deadline. Without one, a call to a server that never answers waits
forever (or until `Timeout` interrupts it, which also works).

### Errors

A call that fails raises `Tonic::Rails::RpcError`, the same class your
handlers raise:

```ruby
begin
  items.get_item(id: 1)
rescue Tonic::Rails::RpcError => e
  e.code_name  # :not_found, :unavailable, :deadline_exceeded, ...
  e.message    # the server's status message
  e.details    # rich error details (google.rpc.Status), as messages
  e.metadata   # trailing metadata
end
```

Rich error details come back as their message class when its `_pb.rb` is
loaded, and as `Google::Protobuf::Any` otherwise. A server that can't be
reached raises `UNAVAILABLE`.

If a handler lets the error propagate, its caller receives the same
status, message, details and trailing metadata. Rescue it and raise your
own error when those are internal.

### Retries

Retries are off unless you turn them on, for a client or a single call:

```ruby
ITEMS = Tonic::Rails::Client.new("items.internal:50051", deadline: 2, retries: true)

items.get_item({ id: 1 }, retries: false)                    # not this call
items.get_item({ id: 1 }, retries: { max_attempts: 5 })      # or differently
```

`retries: true` tries a call up to 3 times when it fails with
`UNAVAILABLE`. A Hash changes any of these:

| Field | Default | |
|---|---|---|
| `max_attempts` | 3 | attempts in all, from 2 to 5 |
| `codes` | `[:unavailable]` | status codes worth retrying |
| `initial_backoff` | 0.1 | seconds; the cap on the first wait |
| `max_backoff` | 1 | seconds; the cap on any wait |
| `multiplier` | 2 | how much the cap grows per attempt |

The rules come from gRPC's own retry design, so they avoid the usual
mistakes of a hand-written retry loop:

- **A call is retried only if the server never started on it.** That
  means the connection failed (refused, reset, a pod restarting), or the
  server answered with only a status and no response headers. Servers do
  that when they turn a call away before any handler runs: a
  tonic-rails server that's shutting down or whose queue is full, or a
  proxy with no healthy backend. Once response headers arrive, the call
  is never repeated, even if it then fails with a code you listed.
- **Waits are random.** Each one is anywhere from zero up to a cap that
  starts at `initial_backoff` and grows by `multiplier` per attempt, up to
  `max_backoff`. Clients that failed together don't retry together.
- **The deadline covers every attempt.** If the next attempt couldn't
  start before the deadline, the client stops and raises the last
  failure.
- **The server can steer.** A `grpc-retry-pushback-ms` trailer sets the
  wait, and a negative one stops retries.
- A server stream is retried only before its first message.

Which calls are safe to retry? With the default `[:unavailable]`, nearly
all of them. By gRPC convention, `UNAVAILABLE` means the call can be tried
again, and from a tonic-rails server it's retried only when no handler
ran. For a tonic-rails server, `:resource_exhausted` is worth adding too:
a full dispatch queue returns it before any handler runs.

Take more care with codes a handler might raise after doing some work,
like `:aborted`. A tonic-rails server sends headers before it runs a
handler, so those calls still won't be retried. Other servers may answer
with only a status after a handler ran, so list those codes only for
methods that are safe to run twice.

Each retry is logged as a warning, through the client's `logger:` (by
default `Tonic::Rails.configuration.logger`, which is the Rails logger in
a Rails app):

```
tonic-rails: /demo.v1.ItemService/GetItem failed with UNAVAILABLE: ... (attempt 1 of 3); retrying in 64ms
```

### Metadata

`metadata:` on a call, and on the client for every call, works like
response metadata in handlers: `-bin` names take binary values. To read
what the server sent back, pass `return_call: true`:

```ruby
call = items.get_item({ id: 1 }, return_call: true)
call.response   # the message
call.headers    # response headers, -bin values decoded
call.trailers   # trailing metadata
```

### TLS

TLS is on unless you turn it off, and plaintext needs an explicit
`tls: false`:

```ruby
Tonic::Rails::Client.new("items.internal:50051")                           # system root certificates
Tonic::Rails::Client.new("items.internal:50051", tls_ca_path: "ca.pem")    # your own CA instead
Tonic::Rails::Client.new("items.internal:50051", tls_ca_path: "ca.pem",
                         tls_cert_path: "client.pem", tls_key_path: "client.key")  # mutual TLS
Tonic::Rails::Client.new("localhost:50051", tls: false)                    # plaintext
```

`tls_server_name:` checks the server's certificate against another name
than the host you connect to. Unreadable certificates or keys raise when
the client is created, not on the first call.

### Options

| Option | Default | |
|---|---|---|
| `tls` | `true` | `false` for plaintext |
| `tls_ca_path` | system roots | CA certificates (PEM) to trust instead |
| `tls_cert_path`, `tls_key_path` | none | client certificate and key (PEM), for mutual TLS |
| `tls_server_name` | the host | name to verify the server's certificate against |
| `deadline` | none | seconds each call may take, unless the call sets its own |
| `metadata` | `{}` | sent with every call |
| `max_message_size` | 4 MiB | largest message sent or received |
| `connect_timeout` | 5 | seconds to wait for a connection |
| `retries` | `false` | retry calls the server never started on; see [Retries](#retries) |
| `logger` | `Tonic::Rails.configuration.logger` | where retries are logged |

### Fork safety

A client created before `fork` works in both processes, on Linux and
macOS, with nothing to call. Each process opens its own connection on
its first call, and a child never touches the threads or sockets it
inherited from its parent. Create clients in initializers, before Puma or
Sidekiq forks, and use them anywhere.

On macOS, also create the first client that uses the system's root
certificates before forking. Loading them goes through macOS's security
APIs, which abort in a process forked from a multi-threaded one. Loaded
once, they're kept and inherited, so clients created later in children
are fine. Linux has no such limit.

### Migrating from the grpc gem

Call sites barely change; how you build the stub does:

```ruby
# Before: grpc gem
stub = Demo::V1::ItemService::Stub.new("items.internal:50051", GRPC::Core::ChannelCredentials.new)
item = stub.get_item(Demo::V1::GetItemRequest.new(id: 1), deadline: Time.now + 2, metadata: { "x-id" => "1" })

# After: tonic-rails
stub = Tonic::Rails::Client.new("items.internal:50051").stub("demo.v1.ItemService")
item = stub.get_item(Demo::V1::GetItemRequest.new(id: 1), deadline: Time.now + 2, metadata: { "x-id" => "1" })
```

- `stub` also accepts the grpc gem's service class
  (`Demo::V1::ItemService::Service`), which helps while both gems are
  installed.
- `deadline:` takes a `Time`, as before, or seconds.
- `return_op: true` becomes `return_call: true`, which returns the
  response with its headers and trailers instead of an operation.
- `GRPC::BadStatus` subclasses become one `Tonic::Rails::RpcError`:
  `rescue GRPC::NotFound` becomes `rescue Tonic::Rails::RpcError => e`
  and a check of `e.code_name == :not_found`. The `raise_grpc_error`
  matcher and `assert_grpc_error` accept both.
- `:this_channel_is_insecure` becomes `tls: false`.
- Server-streaming methods return a stream instead of an Enumerator.
  Code that calls `each` or other Enumerable methods on the result works
  unchanged; for `next`, call `each` first to get an Enumerator.
- Client and bidirectional streaming aren't supported yet. Stubs refuse
  those methods with a `NotImplementedError`; keep the grpc gem for them
  for now.

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
| `processes` | `1` | Server processes sharing the port (see [Threads or processes](#threads-or-processes)); Unix only |
| `on_worker_boot` | `nil` | Callable run in each worker process after fork, given its index |
| `dispatch_queue_size` | `128` | Calls that may wait for a thread; beyond this, `RESOURCE_EXHAUSTED` |
| `worker_threads` | CPU count | Tokio threads for the transport |
| `max_message_size` | 4 MiB | Request and response limit (also caps decompressed size) |
| `max_connections` | `64` | Concurrent connections; `0` = unlimited |
| `concurrency_limit_per_connection` | `32` | Concurrent streams per connection; `0` = unlimited |
| `tls_cert_path`, `tls_key_path` | `nil` | PEM certificate and key; set both to enable TLS |
| `tls_client_ca_path` | `nil` | PEM CA bundle; requires client certificates (mutual TLS) |
| `drain_timeout` | `10` | Seconds to let in-flight calls finish on shutdown |
| `keepalive_interval`, `keepalive_timeout` | `15`, `5` | HTTP/2 PING interval and timeout, seconds |
| `grpc_web` | `false` | Serve gRPC-Web for browser clients (see [Browser clients](#browser-clients-grpc-web)) |
| `grpc_web_origins` | `[]` | Browser origins allowed via CORS; `"*"` for any (no credentials) |
| `grpc_web_allowed_headers`, `grpc_web_exposed_headers` | `[]` | Extra CORS request / response headers |
| `health_check` | `nil` | Callable; while false or raising, health reports `NOT_SERVING` |
| `health_check_interval` | `5` | Seconds between health checks |
| `compression` | `false` | gzip responses over 1 KiB for clients that accept it |
| `reflection` | `false` | Serve `grpc.reflection.v1`; exposes your schema |
| `interceptors` | `[]` | See [Interceptors](#interceptors) |
| `log_requests` | `false` | Log one line per call through `logger` (unexpected exceptions are always logged) |
| `auto_start` | `false` | Start in a thread of `rails server`; for development (see [auto_start](#auto_start)) |

### Threads or processes

Handlers run on `dispatch_threads` Ruby threads. Blocking I/O (database,
HTTP) releases Ruby's global VM lock, so more threads means more
concurrent I/O. Ruby code itself runs one thread at a time per process,
though: a service that spends its time in Ruby (building large
responses, serializing, computing) tops out at one core however many
threads it has.

So:

- **Mostly waiting on I/O?** Add threads. They're cheap, and one process
  goes a long way. Size the database pool to match.
- **Mostly running Ruby?** Add processes. `processes = 4` runs four
  copies of the server on the same port and uses four cores.

```ruby
config.tonic_rails.processes = ENV.fetch("GRPC_PROCESSES", 4).to_i
config.tonic_rails.dispatch_threads = 5 # per process
```

Most services mix both. A reasonable start is one process per core and
a handful of threads in each. Every process has its own database pool,
so the database sees up to `processes × dispatch_threads` connections.

With `processes` above 1, `bin/rails tonic_rails:serve` loads the app
once and forks the workers, which share memory until they write to it.
The process you started supervises them:

- Workers share one listening socket, and a worker with fewer open
  connections picks up new ones first. gRPC clients keep connections
  open and send every call over them, so a single client connection
  is served by a single worker: spread load with several connections
  or several clients.
- If a worker dies, it's logged and replaced.
- SIGTERM or SIGINT to the supervisor drains every worker (health
  reports `NOT_SERVING`, in-flight calls finish) and it exits once all
  have stopped. Workers stop on their own if the supervisor disappears.
- If a worker fails while starting (a bad configuration, say), the
  server doesn't start.

Active Record reconnects in each worker by itself. Reopen anything else
that can't be shared across `fork` in `on_worker_boot`:

```ruby
config.tonic_rails.on_worker_boot = ->(index) { MyQueueClient.reconnect! }
```

Health checks, `health_check`, `serving!`, `not_serving!` and `stats`
work per worker: calling `serving!` or `not_serving!` in the supervisor
after the server has started doesn't reach the workers.

Multi-process mode needs `fork()`, so it isn't available on Windows. Run
one server process per core there, each on its own port.

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
`tonic_rails:serve` as its own process there.) With `processes` above 1,
that child process supervises the gRPC workers.

For larger deployments, a dedicated `bin/rails tonic_rails:serve` process
(scaled separately) is still the most flexible option.

### auto_start

`auto_start = true` starts the gRPC server in a background thread of
`rails server` (never in consoles, rake tasks or tests). It's handy in
development; in production use the Puma plugin or a dedicated process.
Don't combine it with the plugin, and it can't run multiple `processes`.

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

Calls made with `Tonic::Rails::Client` publish `request_rpc.tonic_rails`,
with the same payload plus `:address`, the server called, and
`:attempts`, how many times it was tried. For a
server-streaming call, the event covers the whole stream, including the
time your block spends on each message.

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
