# Performance

How tonic-rails compares with the alternatives a Rails team would
actually reach for, and with a pure-Rust ceiling.

**In short:** inside a Rails app, tonic-rails served light RPCs at
**5–6× the throughput of an equivalent Rails JSON endpoint** and
**1.4–3× the official `grpc` gem**. For server streaming it was **4–7×**
the `grpc` gem. As handlers do more work per call, the transport matters
less and the servers converge, because the handler itself becomes the
bottleneck.

## What was compared

Every server does exactly the same work per request: the code lives in
`bench/bench_work.rb`, with the Rust server mirroring it. Only the
transport differs.

| Server | What it is |
|--------|------------|
| **tonic-rails in Rails** | tonic-rails serving gRPC from inside a production-style Rails app; every RPC runs in the Rails executor, as in a real app |
| **tonic-rails** | the same handlers on tonic-rails as a plain Ruby process (no Rails) |
| **grpc gem** | the official `grpc` gem (gRPC C core), the usual way to serve gRPC from Ruby |
| **Rails JSON** | a vanilla Rails API-only app on Puma: a controller builds Hashes and renders JSON over HTTP/1.1 |
| **Rust tonic** | the same handlers in pure Rust on tonic; the ceiling, not an alternative |

Each server runs as a single process. The Ruby servers use 50 handler
threads (matching the 50 requests in flight) and YJIT, which Rails turns
on by default. The `grpc` gem needs at least as many threads as in-flight
requests: when every thread is busy it rejects new calls with
`RESOURCE_EXHAUSTED` instead of queueing them.

The client is `bench/go_bench`. It keeps 50 requests in flight and sends
20,000 per scenario after a warm-up, over one HTTP/2 connection for gRPC
or 50 keep-alive HTTP/1.1 connections for the JSON API.

## Results (requests/second)

Machine: Apple M5 (10 cores), macOS, Ruby 3.4 with YJIT, everything on
loopback. Each cell is the range across two full runs.

| Scenario | Work per request | tonic-rails in Rails | tonic-rails | grpc gem | Rails JSON | Rust tonic |
|----------|------------------|---------------------:|------------:|---------:|-----------:|-----------:|
| `ping` | empty request and response | 26k–31k | 42k–44k | 10k–19k | 5.2k–5.3k | 88k–89k |
| `small` | one small record | 23k–28k | 30k–32k | 9k–17k | 5.0k–5.1k | 88k–89k |
| `io` | one 2 ms I/O call | 17k | 16k–18k | 10k–12k | 3.8k–4.7k | 13k |
| `multi_io` | three 5 ms I/O calls, 10 records | 2.6k–2.7k | 2.6k–2.8k | 2.3k–2.4k | 2.6k | 2.5k |
| `mixed` | 2 ms I/O, light CPU, 20 records | 7.6k–8.4k | 7.4k–9.9k | 6.5k–6.7k | 3.7k–3.8k | 12k–13k |
| `large` | 100 records, ~35 KB response | 3.4k–3.8k | 3.4k–4.2k | 3.3k–3.6k | 3.0k | 42k |
| `cpu` | 20k-iteration integer loop | 6.1k–6.2k | 6.4k–7.2k | 5.5k–5.8k | 3.1k–3.2k | 63k–64k |
| `stream` | server stream of 100 records | 2.1k–3.2k | 2.8k–3.4k | 0.43k–0.49k ¹ | n/a | 32k |
| `upload` | client stream of 100 records | 3.2k–4.5k | 4.0k–4.7k | 1.6k | n/a | 11k |

¹ The `grpc` gem also rejected a few streaming calls with
`RESOURCE_EXHAUSTED` even with 50 threads: a stream holds its thread for
the whole call.

How tonic-rails in Rails compares:

| Scenario | vs Rails JSON | vs grpc gem |
|----------|--------------:|------------:|
| `ping`, `small` | 4.6–6× | 1.4–3× |
| `io` | 3.7–4.6× | 1.4–1.8× |
| `mixed` | 2–2.3× | 1.1–1.3× |
| `cpu` | ~2× | ~1.1× |
| `large` | 1.1–1.3× | ~1.05× |
| `multi_io` | ~1× | ~1.1× |
| `stream` | n/a | 4–7× |
| `upload` | n/a | 2–2.8× |

The laptop is noisy: the `grpc` gem's light-call numbers varied almost
2× between runs, and every cell above is a range for that reason. Treat
the ratios as indicative rather than precise.

## What the numbers say

**The transport is where tonic-rails wins.** `ping`, `small` and `io`
spend most of their time in the server machinery rather than in the
handler. Against Rails JSON, tonic-rails skips Rack, routing, parameter
parsing and JSON, and multiplexes over one HTTP/2 connection. Against the
`grpc` gem, it keeps HTTP/2 and its connection handling in Rust, outside
the GVL, and queues calls instead of rejecting them.

**Heavier handlers converge.** In `multi_io` each call sleeps 15 ms, so
with 50 requests in flight no server can exceed about 50 ÷ 15 ms ≈ 3,300
calls/s, and all five, Rust included, land at 2.3k–2.8k. In `large` the
time goes into building 100 protobuf messages in Ruby, which every Ruby
server pays. tonic-rails is never
slower, but if your handlers are dominated by slow queries or heavy object
construction, expect a modest gain, not a multiple.

**Streaming is a large win over the `grpc` gem.** tonic-rails pushes
streamed messages through a bounded buffer, while the transport runs in
Rust. The `grpc` gem ties up a thread per stream and pays more per
message.

**The Rails executor costs something on very cheap calls.** Running each
RPC inside the executor (connection pool check-in, CurrentAttributes
reset, and so on) takes `ping` from 42–44k to 26–31k calls/s, roughly
10–20 µs per call. It disappears into the noise once a handler does any
real work, and it's what makes Rails code safe to call from handlers.

**Rust is the ceiling, not a competitor.** For trivial handlers the pure
Rust server is 2–3× faster again, and for CPU-bound or allocation-heavy
work it's 10× or more, because Ruby runs the handler. tonic-rails narrows
the gap where the transport dominates, not where Ruby does.

**I/O-bound handlers scale with threads.** Ruby releases the GVL while a
handler waits on I/O, so `dispatch_threads` sets how much I/O can overlap.
With 16 threads tonic-rails served `io` at 6.2k calls/s; with 50 it served
16–18k, above the Rust server. That last result is partly an artifact:
tokio's 1 ms timer granularity stretches the Rust handler's 2 ms sleep.
For CPU-light, very cheap calls, fewer threads do slightly better: with 16
threads (and YJIT off) tonic-rails served `ping` at 63–70k calls/s,
because fewer threads compete for the GVL. Scale CPU-heavy services with
processes rather than threads; the listener uses `SO_REUSEPORT`, so
several processes can share a port.

## Design choices that matter for throughput

- **Raw-byte passthrough.** Rust never decodes protobuf; Ruby's
  `google-protobuf` C extension does it once.
- **`request_format :message`.** Handlers receive the decoded message
  instead of a Hash, which skips a `to_h` conversion per request.
- **GVL batching.** A dispatch thread that holds the GVL keeps going with
  requests already queued, for up to 16 requests or 500 µs, instead of
  releasing and re-acquiring it for each one. This raised `ping` and
  `small` throughput by 40–60%. The time bound keeps CPU-heavy handlers
  handing the GVL off fairly, so tail latency stays stable.
- **Event-driven waits.** Request and response channels block on wakers,
  not polling, and release the GVL only when they actually wait.
- **Dispatch before reading the body.** A handler starts as soon as the
  headers arrive and reads request messages as they stream in, which is
  what lets bidirectional streams interleave.

## Reproducing

```sh
bundle exec rake compile
DISPATCH_THREADS=50 bench/run_bench.sh               # every server and scenario
SERVERS="rails-grpc rails-json" SCENARIOS="ping io" bench/run_bench.sh
(cd bench/go_bench && go run . -list)                # scenario descriptions
```

`SERVERS` accepts `ruby` (tonic-rails), `rails-grpc` (tonic-rails in
Rails), `grpc-gem`, `rails-json` and `rust`. `RUBY_YJIT_ENABLE=0` turns
YJIT off. Requires Go, Rust and `protoc`. `bench/go_bench/health_bench`
measures the native `grpc.health.v1` endpoint, and `bench/rust_bench`'s
`sleep_bench` shows the tokio timer granularity mentioned above.
