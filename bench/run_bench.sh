#!/usr/bin/env bash
#
# Compare tonic-rails (Ruby handlers) against a pure-Rust tonic server
# running identical handlers, across every scenario of the Go client.
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
#   DISPATCH_THREADS  tonic-rails dispatch threads (default 16)
#   SERVERS           "ruby rust" (default both)
set -euo pipefail

cd "$(dirname "$0")/.."

CONCURRENCY="${CONCURRENCY:-50}"
TOTAL="${TOTAL:-5000}"
SCENARIOS="${SCENARIOS:-ping small io multi_io large cpu mixed stream upload}"
SERVERS="${SERVERS:-ruby rust}"
export DISPATCH_THREADS="${DISPATCH_THREADS:-16}"

RUBY_ADDR=127.0.0.1:50061
RUST_ADDR=127.0.0.1:50062
CLIENT=bench/go_bench/bench-client

echo "Building Go client and Rust reference server..."
(cd bench/go_bench && go build -o bench-client .)
(cd bench/rust_bench && cargo build --release --quiet)

pids=()
cleanup() { for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

wait_for() {
  for _ in $(seq 1 50); do
    if "$CLIENT" -addr "$1" -scenario ping -total 1 -warmup 0 -concurrency 1 >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "server at $1 did not start" >&2
  return 1
}

run_server() {
  local label=$1 addr=$2
  echo
  echo "== $label ($addr)  concurrency=$CONCURRENCY total=$TOTAL"
  wait_for "$addr"
  for scenario in $SCENARIOS; do
    "$CLIENT" -addr "$addr" -scenario "$scenario" -concurrency "$CONCURRENCY" -total "$TOTAL"
  done
}

for server in $SERVERS; do
  case $server in
    ruby)
      BIND=$RUBY_ADDR bundle exec ruby bench/bench_server.rb &
      pids+=($!)
      run_server "tonic-rails, $DISPATCH_THREADS dispatch threads" $RUBY_ADDR
      ;;
    rust)
      BIND=$RUST_ADDR bench/rust_bench/target/release/tonic-bench-server &
      pids+=($!)
      run_server "pure Rust tonic" $RUST_ADDR
      ;;
  esac
done
