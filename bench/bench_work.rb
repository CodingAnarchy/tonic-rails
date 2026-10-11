# frozen_string_literal: true

require "bench_pb"

# The work every Ruby benchmark server does, shared so the tonic-rails,
# grpc gem and Rails servers differ only in their transport.  Keep in
# sync with bench/rust_bench/src/main.rs.
module BenchWork
  TAGS = %w[alpha beta gamma].freeze

  module_function

  # Simulated blocking I/O (think database queries); releases the GVL.
  def simulate_io(io_calls, io_ms)
    io_calls.times { sleep(io_ms / 1000.0) }
  end

  # Deterministic integer work: pure CPU, holds the GVL.
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

  # The WorkResponse for a WorkRequest-shaped object (protobuf message or
  # Struct with the same readers).
  def work_response(request)
    simulate_io(request.io_calls, request.io_ms)
    Bench::V1::WorkResponse.new(
      records: Array.new(request.records) { |i| record(i, request.payload_bytes) },
      checksum: checksum(request.cpu_iterations),
    )
  end
end
