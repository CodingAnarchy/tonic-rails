# frozen_string_literal: true

module Tonic
  module Rails
    # Ruby-facing wrapper around the Rust native extension server.
    #
    # The native extension (TonicRails::Native::Server) handles HTTP/2,
    # TLS, keepalive, and graceful shutdown via tonic's transport layer.
    # Reflection (v1) and health-check services are built-in siblings.
    # Dispatch is raw-byte passthrough: the Ruby handler receives binary
    # protobuf bytes and returns binary protobuf bytes.
    #
    #   server = Tonic::Rails::Server.new
    #   server.register(GreeterService)
    #   server.start  # blocks; run in a thread or separate process
    #
    class Server
      attr_reader :router

      def initialize(configuration: Tonic::Rails.configuration)
        @configuration = configuration
        @router = Router.new
        @native_server = nil
        @running = false
      end

      def register(service_class)
        service_class.register_on(@router)
        self
      end

      def start
        raise Error, "Server already running" if running?
        raise Error, "No services registered" if @router.empty?
        raise Error, "dispatch_threads must be >= 1" if @configuration.dispatch_threads < 1

        ensure_native_server

        @configuration.logger.info("tonic-rails: raw-bytes mode (Ruby decode/encode via Google::Protobuf)")

        # The callback proc receives (path, request_bytes, metadata) where
        # request_bytes is a binary String.  The Ruby side decodes/encodes
        # using Google::Protobuf and the registered protobuf classes.
        router = @router
        dispatch_proc = proc { |path, request_bytes, metadata|
          router.dispatch(path, request_bytes, metadata: metadata)
        }

        # Build interceptor chain: each interceptor wraps the next.
        # Interceptors are applied in reverse order (last = outermost).
        callback = @configuration.interceptors.reverse.reduce(dispatch_proc) do |next_call, interceptor|
          proc { |path, request, metadata|
            interceptor.call(path, request, metadata, next_call)
          }
        end

        @native_server.on_request(callback)

        # Enable TLS if cert and key paths are configured.
        if @configuration.tls_cert_path && @configuration.tls_key_path
          @native_server.set_tls(@configuration.tls_cert_path, @configuration.tls_key_path)
          @configuration.logger.info("tonic-rails: TLS enabled (#{@configuration.tls_cert_path})")
        end

        # Set graceful shutdown drain timeout.
        @native_server.set_drain_timeout(@configuration.drain_timeout)

        # Set max concurrent connections (stored for future enforcement).
        @native_server.set_max_connections(@configuration.max_connections)

        # Enable per-request logging if configured.
        @native_server.set_log_requests(@configuration.log_requests)

        # Start the tokio runtime and dispatch channel (non-blocking).
        @native_server.start_server
        @running = true

        thread_count = @configuration.dispatch_threads
        @configuration.logger.info("tonic-rails: serving on #{@configuration.bind_address} (#{thread_count} dispatch threads)")

        # Install signal handlers for graceful shutdown.
        # SIGTERM/SIGINT → stop → sets shutdown flag → all workers exit
        # → main thread's serve_worker returns → stop_server drains.
        old_term = Signal.trap(:TERM) { stop }
        old_int  = Signal.trap(:INT)  { stop }

        # Spawn (N-1) Ruby worker threads.  Each calls #serve_worker which
        # blocks inside rb_thread_call_without_gvl waiting on the shared
        # channel.  When a handler does blocking I/O (releasing the GVL),
        # another worker can acquire the GVL and run its handler — giving
        # I/O concurrency within a single process.
        workers = (thread_count - 1).times.map do
          Thread.new { @native_server.serve_worker }
        end

        # The main thread is the Nth worker.
        @native_server.serve_worker

        # serve_worker returned — shutdown was triggered.
        @configuration.logger.info("tonic-rails: shutdown signal received — draining connections")

        # All workers have exited — clean up.
        workers.each(&:join)
        @native_server.stop_server
      ensure
        @running = false
        # Restore previous signal handlers.
        Signal.trap(:TERM, old_term) if old_term
        Signal.trap(:INT, old_int) if old_int
      end

      def stop
        return unless @running
        # Called from a signal trap — must be async-safe.
        # No logging, no I/O — just set the shutdown flag.
        @native_server&.shutdown
        @running = false
      end

      def running?
        @running
      end

      # Return current server stats as a Hash.
      # @return [Hash{Symbol=>Integer}] counters for total, successful,
      #   errors, deadline_exceeded, resource_exhausted requests.
      def stats
        return { total: 0, successful: 0, errors: 0, deadline_exceeded: 0, resource_exhausted: 0 } unless @native_server
        @native_server.stats
      end

      private

      def ensure_native_server
        @native_server ||= TonicRails::Native::Server.new(
          @configuration.bind_address,
          @configuration.max_message_size,
        )
      end
    end
  end
end
