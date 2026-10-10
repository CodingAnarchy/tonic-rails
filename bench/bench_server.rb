# frozen_string_literal: true

# tonic-rails implementation of bench.v1.BenchService.
#
#   bundle exec ruby bench/bench_server.rb
#
# Environment:
#   BIND             address to listen on (default 127.0.0.1:50051)
#   DISPATCH_THREADS Ruby dispatch threads (default 16)
#
# The Rust reference server (bench/rust_bench) implements the same
# handlers; keep the two in sync so comparisons stay fair.

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__), __dir__)
require "tonic-rails"
require "bench_pb"

class BenchService
  include Tonic::Rails::Base

  rpc_service "bench.v1.BenchService"
  request_format :message

  TAGS = %w[alpha beta gamma].freeze
  EMPTY = Bench::V1::Empty.new.freeze

  def ping(_request)
    EMPTY
  end

  def work(request)
    simulate_io(request)
    Bench::V1::WorkResponse.new(
      records: Array.new(request.records) { |i| record(i, request.payload_bytes) },
      checksum: checksum(request.cpu_iterations),
    )
  end

  def stream_records(request)
    simulate_io(request)
    Enumerator.new do |y|
      request.records.times { |i| y << record(i, request.payload_bytes) }
    end
  end

  def upload(records)
    total = 0
    records.each { |r| total += r.value }
    Bench::V1::WorkResponse.new(checksum: total)
  end

  private

  def simulate_io(request)
    request.io_calls.times { sleep(request.io_ms / 1000.0) }
  end

  # Deterministic integer work; must match the Rust implementation.
  def checksum(iterations)
    x = 0
    iterations.times { |i| x = ((x * 31) + i) % 1_000_003 }
    x
  end

  def record(index, payload_bytes)
    Bench::V1::Record.new(
      id: index,
      name: "record-#{index}",
      tags: TAGS,
      value: index * 7,
      blob: "x" * payload_bytes,
    )
  end
end

if $PROGRAM_NAME == __FILE__
  Tonic::Rails.configure do |config|
    config.bind_address = ENV.fetch("BIND", "127.0.0.1:50051")
    config.dispatch_threads = Integer(ENV.fetch("DISPATCH_THREADS", "16"))
    config.dispatch_queue_size = 1024
    config.max_connections = 0
    config.concurrency_limit_per_connection = 0
    config.logger = Logger.new($stderr, level: :warn)
  end
  Tonic::Rails.server.register(BenchService)
  Tonic::Rails.server.start
end
