# frozen_string_literal: true

# tonic-rails implementation of bench.v1.BenchService.
#
#   bundle exec ruby bench/bench_server.rb
#
# Environment:
#   BIND             address to listen on (default 127.0.0.1:50051)
#   DISPATCH_THREADS Ruby dispatch threads (default 16)
#   WITH_RAILS=1     boot bench/rails_app.rb first, so RPCs run inside the
#                    Rails executor as in a real Rails app
#
# Handler work lives in bench_work.rb, shared with the grpc gem and Rails
# baselines; the Rust reference server mirrors it.

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__), __dir__)
require_relative "rails_app" if ENV["WITH_RAILS"] == "1" # before tonic-rails, so its Railtie loads
require "tonic-rails"
require "bench_work"

class BenchService
  include Tonic::Rails::Base

  rpc_service "bench.v1.BenchService"
  request_format :message

  EMPTY = Bench::V1::Empty.new.freeze

  def ping(_request)
    EMPTY
  end

  def work(request)
    BenchWork.work_response(request)
  end

  def stream_records(request)
    BenchWork.simulate_io(request.io_calls, request.io_ms)
    Enumerator.new do |y|
      request.records.times { |i| y << BenchWork.record(i, request.payload_bytes) }
    end
  end

  def upload(records)
    total = 0
    records.each { |r| total += r.value }
    Bench::V1::WorkResponse.new(checksum: total)
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
