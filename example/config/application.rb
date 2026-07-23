# frozen_string_literal: true

require "rails"
require "active_support/all"
require "action_controller/railtie"

# Require the tonic-rails gem (loads Railtie automatically).
require "tonic-rails"

module ExampleApp
  class Application < Rails::Application
    config.load_defaults 7.2
    config.api_only = true

    # gRPC server configuration.
    config.tonic_rails.bind_address = ENV.fetch("GRPC_BIND", "0.0.0.0:50052")
    config.tonic_rails.descriptor_set_path = Rails.root.join("app.desc").to_s
    config.tonic_rails.auto_start = false  # use `rails grpc:serve` instead
  end
end
