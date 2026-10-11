# frozen_string_literal: true

require "logger"

module Tonic
  module Rails
    class Configuration
      # Address the gRPC server binds to ("host:port").  Default "0.0.0.0:50051".
      attr_accessor :bind_address

      # Maximum request and response message size in bytes (also caps
      # decompressed payloads).  Default 4 MiB.
      attr_accessor :max_message_size

      # Start the gRPC server in a background thread when `rails server`
      # boots.  Prefer a dedicated process (`bin/rails tonic_rails:serve`)
      # in production.  Default false.
      attr_accessor :auto_start

      attr_accessor :logger

      # Tokio worker threads for the transport.  nil = number of CPUs.
      attr_accessor :worker_threads

      # Ruby threads running handlers.  Match your ActiveRecord pool size
      # (RAILS_MAX_THREADS).  Default 4.
      attr_accessor :dispatch_threads

      # RPCs that may wait for a free dispatch thread.  Beyond this the
      # server answers RESOURCE_EXHAUSTED immediately.  Default 128.
      attr_accessor :dispatch_queue_size

      # PEM certificate and private key.  Set both to enable TLS.
      attr_accessor :tls_cert_path, :tls_key_path

      # PEM CA bundle.  When set, clients must present a certificate
      # signed by it (mutual TLS).
      attr_accessor :tls_client_ca_path

      # Seconds to wait for in-flight RPCs during shutdown.  Default 10.
      attr_accessor :drain_timeout

      # Concurrent TCP connections; extra connections are refused.
      # 0 = unlimited.  Default 64.
      attr_accessor :max_connections

      # Concurrent HTTP/2 streams per connection.  0 = unlimited.  Default 32.
      attr_accessor :concurrency_limit_per_connection

      # Log every request, not just errors, to stderr.  Default false.
      attr_accessor :log_requests

      # Callables wrapping every RPC, outermost first:
      #
      #   ->(path, metadata, next_call) { next_call.call }
      #
      # Raise RpcError to reject the call.
      attr_accessor :interceptors

      # gzip responses larger than 1 KiB for clients that accept it.
      # Compressed requests are always accepted.  Default false.
      attr_accessor :compression

      # Serve grpc.reflection.v1 (for grpcurl and friends).  Exposes your
      # API schema; keep it off in production.  Default false.
      attr_accessor :reflection

      # HTTP/2 PING interval and ack timeout, in seconds.  Defaults 15 / 5.
      attr_accessor :keepalive_interval, :keepalive_timeout

      # Look handler classes up again on each request so code reloading
      # works.  The Railtie enables this when Rails reloading is on.
      attr_accessor :reload_handlers

      def initialize
        @bind_address = "0.0.0.0:50051"
        @max_message_size = 4 * 1024 * 1024
        @auto_start = false
        @logger = Logger.new($stdout)
        @worker_threads = nil
        @dispatch_threads = 4
        @dispatch_queue_size = 128
        @tls_cert_path = nil
        @tls_key_path = nil
        @tls_client_ca_path = nil
        @drain_timeout = 10
        @max_connections = 64
        @concurrency_limit_per_connection = 32
        @log_requests = false
        @interceptors = []
        @compression = false
        @reflection = false
        @keepalive_interval = 15
        @keepalive_timeout = 5
        @reload_handlers = false
      end

      def tls?
        !tls_cert_path.nil? && !tls_key_path.nil?
      end

      # @raise [ArgumentError] describing the first invalid setting
      def validate!
        positive_integer!(:dispatch_threads)
        positive_integer!(:dispatch_queue_size)
        positive_integer!(:max_message_size)
        non_negative_integer!(:max_connections)
        non_negative_integer!(:concurrency_limit_per_connection)
        positive_integer!(:worker_threads) unless worker_threads.nil?
        %i[drain_timeout keepalive_interval keepalive_timeout].each { |name| non_negative_number!(name) }

        raise ArgumentError, "max_message_size must be at most 4 GiB" if max_message_size > 0xFFFF_FFFF
        raise ArgumentError, "tls_cert_path and tls_key_path must be set together" if tls_cert_path.nil? != tls_key_path.nil?
        raise ArgumentError, "tls_client_ca_path requires tls_cert_path and tls_key_path" if tls_client_ca_path && !tls?
        unless interceptors.respond_to?(:each) && interceptors.all? { |i| i.respond_to?(:call) }
          raise ArgumentError, "interceptors must be an Array of callables"
        end

        self
      end

      private

      def positive_integer!(name)
        value = public_send(name)
        return if value.is_a?(Integer) && value.positive?

        raise ArgumentError,
              "#{name} must be a positive Integer, got #{value.inspect}"
      end

      def non_negative_integer!(name)
        value = public_send(name)
        return if value.is_a?(Integer) && !value.negative?

        raise ArgumentError,
              "#{name} must be a non-negative Integer, got #{value.inspect}"
      end

      def non_negative_number!(name)
        value = public_send(name)
        return if value.is_a?(Numeric) && !value.negative?

        raise ArgumentError,
              "#{name} must be a non-negative number, got #{value.inspect}"
      end
    end
  end
end
