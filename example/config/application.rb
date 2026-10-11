# frozen_string_literal: true

require "rails"
require "active_support/all"
require "action_controller/railtie"
require "tonic-rails"

# Generated protobuf code (rake tonic_rails:compile_protos) lives outside
# the autoload paths: protoc output uses plain `require`s between files.
$LOAD_PATH.unshift(File.expand_path("../lib/grpc", __dir__))

module ExampleApp
  class Application < Rails::Application
    config.load_defaults 8.0
    config.api_only = true
    config.autoload_lib(ignore: %w[grpc])

    config.tonic_rails.bind_address = ENV.fetch("GRPC_BIND", "127.0.0.1:50052")
    config.tonic_rails.dispatch_threads = ENV.fetch("RAILS_MAX_THREADS", 5).to_i
    # Reflection lets grpcurl discover the API; keep it out of production.
    config.tonic_rails.reflection = !Rails.env.production?
  end
end
