# Performance

How tonic-rails compares with a pure-Rust tonic server running the same
handlers, and what that means for sizing a deployment.

## Setup

- **Service**: `bench.v1.BenchService` (`bench/protos/bench.proto`). Each
  request describes the work to do (simulated I/O calls, records to return,
  payload size, CPU iterations), so one service covers every workload shape.
- **tonic-rails**: `bench/bench_server.rb`, handlers in Ruby using
  `request_format :message`, 16 dispatch threads unless noted.
- **Rust baseline**: `bench/rust_bench`, the same handlers on tonic 0.14
  with prost, default tokio worker threads, same HTTP/2 flow-control windows.
- **Client**: `bench/go_bench`, grpc-go over one persistent HTTP/2
  connection, 50 requests in flight, 20,000 requests per scenario after a
  200-request warm-up.
- **Machine**: Apple M5 (10 cores), macOS, Ruby 3.4, everything on loopback.

Laptop benchmarks are noisy. The table shows the range seen across
three to five runs; CPU-bound Ruby scenarios varied by up to 2× between
runs, so treat single numbers as indicative, not precise.

## Results (requests/second)

| Scenario | Work per request | tonic-rails | Rust tonic | Ruby / Rust |
|----------|------------------|------------:|-----------:|------------:|
| `ping` | empty request and response | 63k–70k | 77k–90k | ~0.8 |
| `small` | one small record | 51k–66k | 74k–84k | ~0.75 |
| `io` | one 2 ms I/O call | 6.2k | 12.7k–13.0k | 0.5 (thread-bound, see below) |
| `io`, 64 threads | one 2 ms I/O call | 14.8k–18.5k | 12.7k–13.0k | ≥1 |
| `multi_io` | three 5 ms I/O calls, 10 records | 790–890 | 2.5k | 0.35 (thread-bound) |
| `multi_io`, 64 threads | | 2.6k–2.8k | 2.5k | ~1 |
| `mixed` | 2 ms I/O + light CPU + 20 records | 2.2k–3.0k | 12.2k–13.3k | ~0.2 |
| `large` | 100 records, ~35 KB response | 2.1k–4.2k | 26k–37k | ~0.1 |
| `cpu` | 20k-iteration integer loop | 1.2k–1.9k | 38k–57k | ~0.03 |
| `stream` | server stream of 100 records | 2.4k–3.5k | 22k–29k | ~0.12 |
| `upload` | client stream of 100 records | 2.3k–4.5k | 8.3k–9.7k | ~0.4 |

`ping` p50 latency is about 0.6 ms for both servers at this concurrency.

## What the numbers say

**Transport and dispatch overhead are small.** `ping` and `small` measure
the HTTP/2 stack, the hop into Ruby, protobuf decode/encode and the hop
back. tonic-rails reaches roughly 75–80% of pure Rust. Everything below
the Ruby handler is the same tonic/hyper code.

**I/O-bound handlers scale with dispatch threads, not the GVL.** Ruby
releases the GVL while sleeping or waiting on a socket, so a handler
blocked on a database does not stop the others. With 16 threads and
2 ms of I/O per call the ceiling is 16 / 2 ms = 8,000 rps, and tonic-rails
gets 6,200. With 64 threads it matches or beats the Rust server: tokio's
timer has 1 ms granularity, so the Rust handler's "2 ms" sleep often lasts
3 ms. Size `dispatch_threads` to your I/O concurrency, and your database
pool to match (`RAILS_MAX_THREADS`).

**CPU-bound and allocation-heavy handlers are bounded by Ruby.** Building
100 protobuf messages (`large`, `stream`) or running a tight loop (`cpu`)
happens under the GVL, one thread at a time per process. That is the cost
of running business logic in Ruby, and no transport can remove it. Scale
out with more processes: the listener sets `SO_REUSEPORT`, so several
processes can bind the same port and the kernel spreads connections
across them (one GVL each). The multi-process case is not part of the
numbers above.

**Streaming is incremental.** Each streamed response message is pushed
to the client as the handler yields it, with a bounded buffer (32
messages) that applies backpressure to the handler. Peak memory is a few
messages, not the whole stream.

## Design choices that matter for throughput

- **Raw-byte passthrough.** Rust never decodes protobuf; Ruby's
  `google-protobuf` C extension does it once. There is no double decode.
- **`request_format :message`.** Handlers receive the decoded message
  instead of a Hash, which skips a `to_h` conversion per request (the
  Hash format remains the default for convenience).
- **GVL batching.** A dispatch thread that holds the GVL keeps going with
  requests already queued, for up to 16 requests or 500 µs, instead of
  releasing and re-acquiring the GVL for each one. This raised `ping` and
  `small` throughput by 40–60%. The time bound means only cheap requests
  are batched, so CPU-heavy handlers keep fair GVL hand-offs and stable
  tail latency. (An unbounded batch pushed `cpu` p99 from ~30 ms to
  ~150 ms.)
- **Event-driven waits.** Request and response channels block on wakers,
  not polling, and release the GVL only when they actually have to wait.
- **Dispatch before reading the body.** The handler starts as soon as
  headers arrive and reads request messages as they stream in, which is
  what lets bidirectional streams interleave.

## Reproducing

```sh
bundle exec rake compile
bench/run_bench.sh                                   # all scenarios, both servers
CONCURRENCY=100 TOTAL=50000 SCENARIOS="ping io" bench/run_bench.sh
DISPATCH_THREADS=64 SERVERS=ruby SCENARIOS=io bench/run_bench.sh
(cd bench/go_bench && go run . -list)                # scenario descriptions
```

Requires Go, Rust and `protoc`. `bench/go_bench/health_bench` measures the
native `grpc.health.v1` endpoint, and `bench/rust_bench`'s `sleep_bench`
shows the tokio timer granularity mentioned above.
