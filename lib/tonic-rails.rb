# frozen_string_literal: true

require "google/protobuf"

module Tonic
  module Rails
    class Error < StandardError; end
    class NotConfiguredError < Error; end
  end
end

require "tonic/rails/version"
require "tonic/rails/metadata"
require "tonic/rails/rpc_error"
require "tonic/rails/configuration"
require "tonic/rails/router"
require "tonic/rails/instrumentation"
require "tonic/rails/base"
require "tonic/rails/reflection"
require "tonic/rails/server"

module Tonic
  module Rails
    @configuration = Configuration.new
    @shutting_down = false
    @native_load_error = nil

    class << self
      attr_reader :configuration

      def configure
        yield @configuration
      end

      # The process-wide server used by the Railtie and rake tasks.
      def server
        @server ||= Server.new
      end

      # True once shutdown has begun.  Long-running handlers (especially
      # streams) should check this and finish early.
      def cancelled?
        @shutting_down
      end

      # @api private
      def _set_shutting_down(value)
        @shutting_down = value
      end

      # @api private
      # Log an unexpected exception (and hand it to the Rails error
      # reporter, if present) without exposing it to the client.
      def log_exception(error, context)
        configuration.logger&.error(
          "tonic-rails: #{context}: #{error.class}: #{error.message}\n" \
          "#{Array(error.backtrace).first(20).join("\n")}",
        )
        if defined?(::Rails.error) && ::Rails.respond_to?(:error)
          ::Rails.error.report(error, handled: true, source: "tonic-rails", context: { rpc: context })
        end
      rescue StandardError
        nil
      end

      # @api private
      def native_extension!
        return if defined?(::TonicRails::Native::Server)

        raise Error, "tonic-rails native extension is not available " \
                     "(#{@native_load_error&.message || 'unknown error'}). " \
                     "Reinstall the gem, or run `bundle exec rake compile` in a checkout."
      end

      # @api private
      def load_native_extension
        # Precompiled platform gems ship one binary per Ruby minor version.
        ruby_version = RUBY_VERSION[/\d+\.\d+/]
        require "tonic/rails/#{ruby_version}/tonic_rails_native"
      rescue LoadError
        begin
          require "tonic/rails/tonic_rails_native"
        rescue LoadError => e
          @native_load_error = e
        end
      end
    end

    load_native_extension
  end
end

require "tonic/rails/railtie" if defined?(Rails::Railtie)
