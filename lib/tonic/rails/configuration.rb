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

      # Server processes.  Ruby runs one thread at a time per process, so a
      # CPU-bound service uses one core however many dispatch threads it
      # has; N processes use N.  Above 1, the app is loaded once and forked
      # into N workers that share the listening socket, supervised by the
      # process that started the server.  Unix only.  Default 1.
      attr_accessor :processes

      # Called in each worker process after it's forked, with the worker's
      # index (multi-process mode only), to reopen connections that can't
      # be shared across fork:
      #
      #   config.on_worker_boot = ->(index) { Redis.current.disconnect! }
      #
      # Active Record reconnects by itself.  Default nil.
      attr_accessor :on_worker_boot

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

      # Log one line per RPC (path, status, duration) through `logger`.
      # Unexpected exceptions are always logged.  Default false.
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

      # Accept gRPC-Web requests, so browser apps can call services
      # directly (unary and server-streaming RPCs).  Also enables HTTP/1.1,
      # which browsers use when there's no TLS.  Default false.
      attr_accessor :grpc_web

      # Browser origins allowed to call gRPC-Web services (CORS), e.g.
      # ["https://app.example.com"].  Empty = same-origin only.  "*" allows
      # any origin, without credentials (cookies).  Default [].
      attr_accessor :grpc_web_origins

      # Extra request headers browsers may send (beyond content-type,
      # x-grpc-web, x-user-agent, grpc-timeout and authorization).
      attr_accessor :grpc_web_allowed_headers

      # Extra response headers browsers may read, e.g. custom response
      # metadata (grpc-status, grpc-message and grpc-status-details-bin are
      # always exposed).
      attr_accessor :grpc_web_exposed_headers

      # HTTP/2 PING interval and ack timeout, in seconds.  Defaults 15 / 5.
      attr_accessor :keepalive_interval, :keepalive_timeout

      # Callable checked every `health_check_interval` seconds (inside the
      # Rails executor).  While it returns false or raises, grpc.health.v1
      # reports NOT_SERVING so readiness probes and load balancers route
      # around this instance.  Default nil (always SERVING while up).
      #
      #   config.health_check = -> { ActiveRecord::Base.connection.active? }
      attr_accessor :health_check

      # Seconds between health checks.  Default 5.
      attr_accessor :health_check_interval

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
        @processes = 1
        @on_worker_boot = nil
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
        @grpc_web = false
        @grpc_web_origins = []
        @grpc_web_allowed_headers = []
        @grpc_web_exposed_headers = []
        @health_check = nil
        @health_check_interval = 5
      end

      def multi_process?
        processes.is_a?(Integer) && processes > 1
      end

      def tls?
        !tls_cert_path.nil? && !tls_key_path.nil?
      end

      # @raise [ArgumentError] describing the first invalid setting
      def validate!
        positive_integer!(:dispatch_threads)
        positive_integer!(:dispatch_queue_size)
        positive_integer!(:processes)
        positive_integer!(:max_message_size)
        non_negative_integer!(:max_connections)
        non_negative_integer!(:concurrency_limit_per_connection)
        positive_integer!(:worker_threads) unless worker_threads.nil?
        %i[drain_timeout keepalive_interval keepalive_timeout].each { |name| non_negative_number!(name) }

        raise ArgumentError, "max_message_size must be at most 4 GiB" if max_message_size > 0xFFFF_FFFF
        raise ArgumentError, "tls_cert_path and tls_key_path must be set together" if tls_cert_path.nil? != tls_key_path.nil?
        raise ArgumentError, "tls_client_ca_path requires tls_cert_path and tls_key_path" if tls_client_ca_path && !tls?

        validate_callables!
        validate_grpc_web!
        validate_processes!

        self
      end

      private

      def validate_processes!
        return unless multi_process?
        return if Process.respond_to?(:fork)

        raise ArgumentError, "processes > 1 needs fork(), which this platform lacks; run one server process per core instead"
      end

      def validate_grpc_web!
        %i[grpc_web_origins grpc_web_allowed_headers grpc_web_exposed_headers].each do |name|
          value = public_send(name)
          next if value.is_a?(Array) && value.all?(String)

          raise ArgumentError, "#{name} must be an Array of Strings, got #{value.inspect}"
        end
      end

      def validate_callables!
        raise ArgumentError, "health_check must respond to #call" if health_check && !health_check.respond_to?(:call)
        raise ArgumentError, "on_worker_boot must respond to #call" if on_worker_boot && !on_worker_boot.respond_to?(:call)
        unless health_check_interval.is_a?(Numeric) && health_check_interval.positive?
          raise ArgumentError, "health_check_interval must be a positive number, got #{health_check_interval.inspect}"
        end
        return if interceptors.respond_to?(:each) && interceptors.all? { |i| i.respond_to?(:call) }

        raise ArgumentError, "interceptors must be an Array of callables"
      end

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
