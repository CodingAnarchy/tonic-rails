#!/usr/bin/env bash
#
# Benchmark tonic-rails against its baselines, across every scenario of
# the Go client.  All servers do the same work (bench/bench_work.rb, mirrored
# in Rust) with the same thread count, one process each (tonic-rails can
# run more with PROCESSES).
#
# Servers (SERVERS):
#   ruby        tonic-rails, plain Ruby
#   rails-grpc  tonic-rails inside a Rails app (Rails executor per RPC)
#   grpc-gem    the official grpc gem (gRPC C core)
#   rails-json  a vanilla Rails JSON API on Puma (HTTP/1.1; unary scenarios only)
#   rust        pure Rust tonic (the ceiling)
#
# Requirements: Go, Rust (cargo), protoc, and `bundle exec rake compile`.
#
# Usage:
#   bench/run_bench.sh
#   CONCURRENCY=100 TOTAL=20000 SCENARIOS="ping io large" bench/run_bench.sh
#
# Environment:
#   CONCURRENCY       in-flight requests (default 50)
#   TOTAL             requests per scenario (default 5000)
#   SCENARIOS         space-separated list (default: all)
#   DISPATCH_THREADS  handler threads for every Ruby server (default 16).
#                     The grpc gem rejects calls when all threads are busy
#                     instead of queueing, so give it >= CONCURRENCY.
#   PROCESSES         server processes for tonic-rails (default 1)
#   CONNECTIONS       gRPC connections the client spreads calls over
#                     (default 1).  A multi-process server balances
#                     connections, not calls, so use at least PROCESSES.
#   RUBY_YJIT_ENABLE  YJIT for the Ruby servers (default 1, as Rails enables
#                     it by default; set 0 to compare without)
#   SERVERS           space-separated list (default: all)
set -euo pipefail

cd "$(dirname "$0")/.."

CONCURRENCY="${CONCURRENCY:-50}"
TOTAL="${TOTAL:-5000}"
SCENARIOS="${SCENARIOS:-ping small io multi_io large cpu mixed stream upload}"
SERVERS="${SERVERS:-ruby rails-grpc grpc-gem rails-json rust}"
export DISPATCH_THREADS="${DISPATCH_THREADS:-16}"
export PROCESSES="${PROCESSES:-1}"
CONNECTIONS="${CONNECTIONS:-1}"
export RUBY_YJIT_ENABLE="${RUBY_YJIT_ENABLE:-1}"

RUBY_ADDR=127.0.0.1:50061
RUST_ADDR=127.0.0.1:50062
GRPC_GEM_ADDR=127.0.0.1:50063
RAILS_JSON_ADDR=127.0.0.1:50064
RAILS_GRPC_ADDR=127.0.0.1:50065
CLIENT=bench/go_bench/bench-client

echo "Building Go client and Rust reference server..."
(cd bench/go_bench && go build -o bench-client .)
(cd bench/rust_bench && cargo build --release --quiet)

pids=()
cleanup() { for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

wait_for() {
  for _ in $(seq 1 50); do
    if "$CLIENT" -protocol "$2" -addr "$1" -scenario ping -total 1 -warmup 0 -concurrency 1 >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "server at $1 did not start" >&2
  return 1
}

run_server() {
  local label=$1 addr=$2 protocol=${3:-grpc}
  echo
  echo "== $label ($addr)  concurrency=$CONCURRENCY total=$TOTAL"
  wait_for "$addr" "$protocol"
  for scenario in $SCENARIOS; do
    if [[ $protocol == http && ( $scenario == stream || $scenario == upload ) ]]; then
      continue # streaming has no plain-HTTP equivalent
    fi
    # A scenario with errors still prints its results; keep going.
    "$CLIENT" -protocol "$protocol" -addr "$addr" -scenario "$scenario" -concurrency "$CONCURRENCY" -total "$TOTAL" \
      -connections "$CONNECTIONS" || true
  done
  kill "${pids[${#pids[@]}-1]}" 2>/dev/null || true
}

for server in $SERVERS; do
  case $server in
    ruby)
      BIND=$RUBY_ADDR bundle exec ruby bench/bench_server.rb &
      pids+=($!)
      run_server "tonic-rails, $PROCESSES process(es) × $DISPATCH_THREADS dispatch threads" $RUBY_ADDR
      ;;
    rails-grpc)
      WITH_RAILS=1 BIND=$RAILS_GRPC_ADDR bundle exec ruby bench/bench_server.rb &
      pids+=($!)
      run_server "tonic-rails in a Rails app, $PROCESSES process(es) × $DISPATCH_THREADS dispatch threads" $RAILS_GRPC_ADDR
      ;;
    grpc-gem)
      BIND=$GRPC_GEM_ADDR bundle exec ruby bench/grpc_gem_server.rb &
      pids+=($!)
      run_server "grpc gem, $DISPATCH_THREADS threads" $GRPC_GEM_ADDR
      ;;
    rails-json)
      bundle exec puma --quiet -t "$DISPATCH_THREADS:$DISPATCH_THREADS" -b "tcp://$RAILS_JSON_ADDR" bench/rails_app.ru &
      pids+=($!)
      run_server "Rails JSON API on Puma, $DISPATCH_THREADS threads" $RAILS_JSON_ADDR http
      ;;
    rust)
      BIND=$RUST_ADDR bench/rust_bench/target/release/tonic-bench-server &
      pids+=($!)
      run_server "pure Rust tonic" $RUST_ADDR
      ;;
  esac
done
