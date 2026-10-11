# frozen_string_literal: true

# A minimal production-style Rails app (API-only, eager loaded) shared by
# the benchmarks:
# - bench/rails_app.ru serves a vanilla Rails JSON API doing the same work
#   as bench.v1.BenchService (POST /bench.v1.BenchService/{Ping,Work})
# - bench/bench_server.rb with WITH_RAILS=1 serves gRPC from inside this
#   app, so every RPC runs in the Rails executor as it would in production

require "rails"
require "action_controller/railtie"
$LOAD_PATH.unshift(__dir__)
require "bench_work"

class BenchApp < Rails::Application
  config.load_defaults 8.0
  config.api_only = true
  config.eager_load = true
  config.secret_key_base = "bench"
  config.logger = ActiveSupport::Logger.new($stderr)
  config.log_level = :warn
  config.hosts.clear

  routes.append do
    post "/bench.v1.BenchService/Ping" => "bench#ping"
    post "/bench.v1.BenchService/Work" => "bench#work"
  end
end

class BenchController < ActionController::API
  def ping
    render json: {}
  end

  def work
    BenchWork.simulate_io(params[:io_calls].to_i, params[:io_ms].to_i)
    payload = "x" * params[:payload_bytes].to_i
    records = Array.new(params[:records].to_i) do |i|
      { id: i, name: "record-#{i}", tags: BenchWork::TAGS, value: i * 7, blob: payload }
    end
    render json: { records: records, checksum: BenchWork.checksum(params[:cpu_iterations].to_i) }
  end
end

BenchApp.initialize!
