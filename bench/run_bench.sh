#!/usr/bin/env bash
#
# Benchmark tonic-rails with realistic handlers.
#
# Uses a Go gRPC client with persistent connections for accurate
# throughput measurement (no per-request connection overhead).
#
# Usage:
#   bench/run_bench.sh
#   CONCURRENCY=20 TOTAL_REQUESTS=5000 bench/run_bench.sh
#   SIMULATED_DB_LATENCY_MS=0 bench/run_bench.sh  # isolate dispatch overhead
#
set -euo pipefail

cd "$(dirname "$0")/.."

PROTO="bench/shop.proto"
ADDR="127.0.0.1:50051"
CONCURRENCY="${CONCURRENCY:-10}"
TOTAL_REQUESTS="${TOTAL_REQUESTS:-2000}"
DB_LATENCY="${SIMULATED_DB_LATENCY_MS:-2}"

RPCS=("HealthCheck" "GetProduct" "ListProducts" "CreateProduct" "BatchGetProducts" "GetProductDetail" "SearchProducts" "CheckoutPreview" "Dashboard")

# Run Go benchmark client via subshell to avoid macOS provenance killing
# pre-built binaries. The subshell isolates the `cd` from the parent shell.

run_bench() {
  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  echo "║  DB latency: ${DB_LATENCY}ms  |  C: $CONCURRENCY  |  N: $TOTAL_REQUESTS ║"
  echo "╚══════════════════════════════════════════════════════════════╝"

  # Start server
  echo "Starting server..."
  SIMULATED_DB_LATENCY_MS=$DB_LATENCY shadowenv exec -- bundle exec ruby -Ibench -e '
    require_relative "bench/bench_service"
    Tonic::Rails.server.start
  ' 2>/dev/null &
  local server_pid=$!
  trap "kill $server_pid 2>/dev/null" EXIT

  sleep 4

  # Verify server is up
  if ! grpcurl -plaintext -proto "$PROTO" -d '{}' "$ADDR" shop.ProductService/HealthCheck >/dev/null 2>&1; then
    echo "ERROR: server not responding"
    kill $server_pid 2>/dev/null
    return 1
  fi

  # Run benchmarks for each RPC
  for rpc in "${RPCS[@]}"; do
    (cd bench/go_bench && go run . -addr "$ADDR" -rpc "$rpc" -concurrency "$CONCURRENCY" -total "$TOTAL_REQUESTS")
  done

  kill $server_pid 2>/dev/null
  trap - EXIT
  sleep 1
}

# ── Main ───────────────────────────────────────────────────────────

echo "════════════════════════════════════════════════════════════════"
echo "  tonic-rails benchmark"
echo "  Total requests per RPC: $TOTAL_REQUESTS  Concurrency: $CONCURRENCY"
echo "════════════════════════════════════════════════════════════════"

run_bench

echo ""
echo "Done."
