# frozen_string_literal: true

# A minimal Rails app serving gRPC through the Puma plugin (see
# spec/integration/puma_plugin_spec.rb).
require "rails"
require "action_controller/railtie"
require "tonic-rails"
require_relative "../../lib/echo_handler"

class PumaPluginApp < Rails::Application
  config.root = File.expand_path("..", __dir__)
  config.eager_load = false
  config.logger = Logger.new($stdout)
  config.log_level = :info
  config.secret_key_base = "test"
  config.hosts.clear
  config.tonic_rails.bind_address = ENV.fetch("GRPC_BIND")
  config.tonic_rails.drain_timeout = 2
  config.tonic_rails.processes = Integer(ENV.fetch("GRPC_PROCESSES", "1"))
  config.to_prepare { Tonic::Rails.server.register(EchoHandler) }

  routes.append { get "/up", to: proc { [200, {}, ["ok"]] } }
end

PumaPluginApp.initialize!
