# frozen_string_literal: true

module Tonic
  module Rails
    # Rails integration hook.  When Rails is loaded, this Railtie:
    # - wires the Rails logger into the gRPC configuration
    # - optionally auto-starts the gRPC server alongside the web server
    # - provides rake tasks for starting a standalone gRPC server
    class Railtie < ::Rails::Railtie
      config.tonic_rails = ::ActiveSupport::OrderedOptions.new
      config.tonic_rails.auto_start = false
      config.tonic_rails.bind_address = "0.0.0.0:50051"
      config.tonic_rails.tls_cert_path = nil
      config.tonic_rails.tls_key_path = nil
      config.tonic_rails.drain_timeout = 10
      config.tonic_rails.max_connections = 64
      config.tonic_rails.concurrency_limit_per_connection = 32
      config.tonic_rails.interceptors = []
      # Default to RAILS_MAX_THREADS so dispatch threads stay in sync with
      # the ActiveRecord connection pool (which Rails sizes from the same env).
      config.tonic_rails.dispatch_threads = ENV.fetch("RAILS_MAX_THREADS", 4).to_i
      config.tonic_rails.compression = false
      config.tonic_rails.reflection = false
      config.tonic_rails.keepalive_interval = 15
      config.tonic_rails.keepalive_timeout = 5

      initializer "tonic_rails.configuration" do |app|
          config.logger ||= ::Rails.logger
          config.bind_address = app.config.tonic_rails.bind_address
          config.tls_cert_path = app.config.tonic_rails.tls_cert_path
          config.tls_key_path = app.config.tonic_rails.tls_key_path
          config.drain_timeout = app.config.tonic_rails.drain_timeout
          config.max_connections = app.config.tonic_rails.max_connections
          config.concurrency_limit_per_connection = app.config.tonic_rails.concurrency_limit_per_connection
          config.interceptors = app.config.tonic_rails.interceptors
          config.auto_start = app.config.tonic_rails.auto_start
          config.reflection = app.config.tonic_rails.reflection
          config.compression = app.config.tonic_rails.compression
          config.dispatch_threads = app.config.tonic_rails.dispatch_threads
          config.keepalive_interval = app.config.tonic_rails.keepalive_interval
          config.keepalive_timeout = app.config.tonic_rails.keepalive_timeout
        end
      end

      config.after_initialize do |app|
        if app.config.tonic_rails.auto_start
          # Spawn the gRPC server in a background thread so it runs alongside
          # the Rails web server.  Services must be registered before this
          # point (e.g. in an initializer).
          thread = Thread.new do
            Tonic::Rails.server.start
          end
          thread.abort_on_exception = true
        end
      end

      rake_tasks do
        load File.expand_path("tasks/tonic-rails.rake", __dir__)
      end
    end
  end
end
