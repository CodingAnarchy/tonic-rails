# frozen_string_literal: true

# Define error classes first — base.rb references Tonic::Rails::Error
# in `class RpcError < Error`, so they must exist before base.rb loads.
module Tonic
  module Rails
    class Error < StandardError; end
    class NotConfiguredError < Error; end
  end
end

require "tonic/rails/version"
require "tonic/rails/configuration"
require "tonic/rails/router"
require "tonic/rails/base"
require "tonic/rails/server"

# Load the Rust native extension (tonic transport via Magnus).
# Failure is non-fatal at require time so that code that only references
# Ruby-side types (e.g. in tests without a compiled extension) still loads.
begin
  require "tonic/rails/tonic_rails_native"
rescue LoadError => e
  warn "tonic-rails: native extension not loaded — #{e.message}"
end

module Tonic
  module Rails
    @configuration = Configuration.new

    class << self
      attr_reader :configuration

      def configure
        yield @configuration
      end

      def server
        @server ||= Server.new
      end
    end
  end
end

# Load Railtie if Rails is present
require "tonic/rails/railtie" if defined?(::Rails::Railtie)
