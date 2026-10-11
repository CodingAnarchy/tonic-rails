# frozen_string_literal: true

# bench.v1.BenchService on the official `grpc` gem (gRPC C core), the
# usual way to serve gRPC from Ruby.  Baseline for tonic-rails.
#
#   bundle exec ruby bench/grpc_gem_server.rb
#
# Environment:
#   BIND             address to listen on (default 127.0.0.1:50063)
#   DISPATCH_THREADS handler thread pool size (default 16)

$LOAD_PATH.unshift(__dir__)
require "grpc"
require "bench_work"

module Bench
  module V1
    module BenchService
      class Service
        include ::GRPC::GenericService

        self.marshal_class_method = :encode
        self.unmarshal_class_method = :decode
        self.service_name = "bench.v1.BenchService"

        rpc :Ping, Empty, Empty
        rpc :Work, WorkRequest, WorkResponse
        rpc :StreamRecords, WorkRequest, stream(Record)
        rpc :Upload, stream(Record), WorkResponse
      end
    end
  end
end

class GrpcGemBenchService < Bench::V1::BenchService::Service
  EMPTY = Bench::V1::Empty.new.freeze

  def ping(_request, _call)
    EMPTY
  end

  def work(request, _call)
    BenchWork.work_response(request)
  end

  def stream_records(request, _call)
    BenchWork.simulate_io(request.io_calls, request.io_ms)
    Enumerator.new do |y|
      request.records.times { |i| y << BenchWork.record(i, request.payload_bytes) }
    end
  end

  def upload(call)
    total = 0
    call.each_remote_read { |r| total += r.value }
    Bench::V1::WorkResponse.new(checksum: total)
  end
end

if $PROGRAM_NAME == __FILE__
  threads = Integer(ENV.fetch("DISPATCH_THREADS", "16"))
  server = GRPC::RpcServer.new(pool_size: threads, max_waiting_requests: 1024)
  server.add_http2_port(ENV.fetch("BIND", "127.0.0.1:50063"), :this_port_is_insecure)
  server.handle(GrpcGemBenchService)
  warn "grpc gem server listening (#{threads} threads)"
  server.run_till_terminated_or_interrupted(%w[INT TERM])
end
