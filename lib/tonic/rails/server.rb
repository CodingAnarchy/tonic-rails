# frozen_string_literal: true

module Tonic
  module Rails
    # Runs the gRPC server: a tonic transport in Rust plus a pool of Ruby
    # dispatch threads.
    #
    #   server = Tonic::Rails::Server.new
    #   server.register(ProductService)
    #   server.start   # blocks until SIGTERM / SIGINT or #stop
    #
    # Shutdown is graceful: health checks flip to NOT_SERVING, new
    # connections are refused, in-flight RPCs get `drain_timeout` seconds
    # to finish, then everything is torn down.
    class Server
      attr_reader :router, :configuration

      def initialize(configuration: Tonic::Rails.configuration)
        @configuration = configuration
        @router = Router.new
        @native = nil
        @state = :stopped
        @stop_signal = nil
        @stop_requested = false
        @health = Health.new(configuration, @router)
        @lock = Mutex.new
      end

      # @param service_class [Class, String] handler class (or its name)
      def register(service_class)
        service_class = Object.const_get(service_class) if service_class.is_a?(String)
        service_class.register_on(@router)
        self
      end

      # Start serving and block until stopped.
      # @param install_signal_handlers [Boolean] trap TERM and INT to stop.
      #   Pass false when embedding in a process that owns its signals.
      # @yield once the server is accepting connections
      def start(install_signal_handlers: true)
        @lock.synchronize do
          raise Error, "server is already running" unless @state == :stopped

          @state = :starting
          @stop_requested = false
        end

        begin
          raise Error, "no services registered" if @router.empty?

          @configuration.validate!
          Tonic::Rails.native_extension!
          Tonic::Rails._set_shutting_down(false)
          # A Queue rather than a self-pipe: pushing to it is safe from trap
          # handlers, and unlike pipes it can be waited on with a timeout
          # on every platform (Windows select() only handles sockets).
          @stop_signal = Thread::Queue.new
          stop if @stop_requested # requested while we were starting up
          @native = build_native_server
          @native.start_server
          log_pump = spawn_log_pump
          @health.attach(@native)
          @state = :running

          request_log = subscribe_request_logger
          previous_handlers = install_signal_handlers ? trap_signals : {}
          workers = Array.new(@configuration.dispatch_threads) { |i| spawn_worker(i) }
          log_startup
          yield self if block_given?

          wait_for_stop(workers)
          @health.detach
          shutdown(workers)
        ensure
          ActiveSupport::Notifications.unsubscribe(request_log) if request_log
          previous_handlers&.each { |signal, handler| Signal.trap(signal, handler || "DEFAULT") }
          @health.detach
          @native&.stop_server
          stop_log_pump(log_pump)
          @native = nil
          @stop_signal = nil
          Tonic::Rails._set_shutting_down(false)
          @state = :stopped
        end
      end

      # Request a graceful stop.  Safe to call from signal handlers and
      # other threads; returns immediately.
      def stop
        @stop_requested = true
        @stop_signal&.push(:stop)
      end

      def running?
        @state == :running
      end

      # Mark the whole server, or the given services (fully-qualified
      # names), SERVING in grpc.health.v1.  Takes effect immediately when
      # running, otherwise when the server starts.
      #
      # With `config.health_check`, a service reports SERVING only while the
      # check passes *and* it's marked serving here.
      def serving!(*services)
        @health.mark(services, true)
        self
      end

      # Mark the whole server, or the given services, NOT_SERVING (e.g. for
      # maintenance), so load balancers and readiness probes route around
      # this instance.  Calls are still served.
      def not_serving!(*services)
        @health.mark(services, false)
        self
      end

      # @return [Hash{Symbol=>Integer}] request and connection counters
      def stats
        native = @native
        return native.stats if native

        { total: 0, successful: 0, errors: 0, cancelled: 0, deadline_exceeded: 0,
          resource_exhausted: 0, unimplemented: 0, active_connections: 0, queued: 0 }
      end

      private

      def build_native_server
        config = @configuration
        native = TonicRails::Native::Server.new(config.bind_address, config.max_message_size)
        native.on_request(build_callback)
        native.set_routes(@router.paths)
        if config.tls?
          native.set_tls(config.tls_cert_path, config.tls_key_path)
          native.set_tls_client_ca(config.tls_client_ca_path) if config.tls_client_ca_path
        end
        native.set_max_connections(config.max_connections)
        native.set_concurrency_limit_per_connection(config.concurrency_limit_per_connection)
        native.set_worker_threads(config.worker_threads || 0)
        native.set_keepalive(config.keepalive_interval.to_f, config.keepalive_timeout.to_f)
        native.set_dispatch_queue_size(config.dispatch_queue_size)
        native.set_compression(config.compression ? true : false)
        native.set_reflection(config.reflection ? true : false)
        native.set_reflection_descriptors(Reflection.file_descriptors(@router.service_names)) if config.reflection
        native
      end

      # The proc the transport calls for every RPC:
      #   (path, metadata, call) → encoded response String, or nil if streamed
      def build_callback
        router = @router
        call = ->(path, metadata, transport) { router.dispatch(path, metadata, transport) }

        call = @configuration.interceptors.reverse.reduce(call) do |inner, interceptor|
          lambda do |path, metadata, transport|
            interceptor.call(path, metadata, -> { inner.call(path, metadata, transport) })
          end
        end

        guarded = lambda do |path, metadata, transport|
          Instrumentation.instrument(path, metadata) { call.call(path, metadata, transport) }
        rescue RpcError => e
          attach_error_trailers(transport, e)
          raise
        rescue StandardError => e
          Tonic::Rails.log_exception(e, path)
          raise RpcError.new("internal error", code: :internal)
        end

        wrap_with_rails_executor(guarded)
      end

      # An RpcError's own metadata and rich details travel as trailers.
      def attach_error_trailers(transport, error)
        return if error.metadata.empty? && error.details.empty?

        transport[:sender]&.add_trailers(error.trailers)
      rescue StandardError => e
        Tonic::Rails.log_exception(e, "RpcError trailers")
      end

      # Each RPC runs inside the Rails executor (or reloader, when code
      # reloading is on): ActiveRecord connections are returned, query
      # caches and CurrentAttributes are reset — the same isolation Rack
      # requests and Active Job get.
      def wrap_with_rails_executor(callback)
        return callback unless defined?(::Rails.application) && ::Rails.application

        app = ::Rails.application
        wrapper = @configuration.reload_handlers ? app.reloader : app.executor
        ->(path, metadata, transport) { wrapper.wrap { callback.call(path, metadata, transport) } }
      end

      def subscribe_request_logger
        return unless @configuration.log_requests && @configuration.logger

        Instrumentation::RequestLogger.new(@configuration.logger).subscribe
      end

      # Transport events from the Rust side (rejected calls, expired
      # deadlines, refused connections, faults) can't call Ruby directly;
      # this thread drains them into the configured logger.
      def spawn_log_pump
        native = @native
        logger = @configuration.logger
        @log_pump_running = true
        Thread.new do
          Thread.current.name = "tonic-rails-log"
          write_transport_logs(logger, native.next_log_events(0.25)) while @log_pump_running
          write_transport_logs(logger, native.next_log_events(0)) # whatever shutdown produced
        rescue StandardError => e
          warn("tonic-rails: log thread stopped: #{e.class}: #{e.message}")
        end
      end

      def stop_log_pump(thread)
        return unless thread

        @log_pump_running = false
        thread.join(2)
      end

      def write_transport_logs(logger, events)
        return unless logger

        events.each { |level, message| logger.public_send(level, "tonic-rails: #{message}") }
      end

      def spawn_worker(index)
        native = @native
        Thread.new do
          Thread.current.name = "tonic-rails-dispatch-#{index}"
          native.serve_worker
        rescue Exception => e # rubocop:disable Lint/RescueException
          Tonic::Rails.log_exception(e, "dispatch thread #{index}")
          stop
          raise
        end
      end

      def trap_signals
        %w[TERM INT].to_h { |signal| [signal, Signal.trap(signal) { stop }] }
      end

      def wait_for_stop(workers)
        loop do
          break if @stop_signal.pop(timeout: 1)

          dead = workers.reject(&:alive?)
          unless dead.empty?
            @configuration.logger&.error("tonic-rails: #{dead.size} dispatch thread(s) died; shutting down")
            break
          end
        end
      end

      def shutdown(workers)
        logger = @configuration.logger
        @state = :stopping
        Tonic::Rails._set_shutting_down(true)
        logger&.info("tonic-rails: shutting down, draining for up to #{@configuration.drain_timeout}s")

        @native.begin_shutdown
        unless @native.wait_for_drain(@configuration.drain_timeout.to_f)
          logger&.warn("tonic-rails: drain timeout expired; closing remaining connections")
        end
        @native.stop_workers
        workers.each do |worker|
          worker.join
        rescue Exception # rubocop:disable Lint/RescueException
          # Already logged by the worker.
        end
        logger&.info("tonic-rails: stopped")
      end

      def log_startup
        config = @configuration
        logger = config.logger
        return unless logger

        if config.tls?
          logger.info("tonic-rails: TLS enabled#{' (mutual TLS)' if config.tls_client_ca_path}")
        else
          logger.warn("tonic-rails: TLS is not configured; gRPC traffic is plaintext")
        end
        logger.info("tonic-rails: reflection enabled") if config.reflection
        logger.info(
          "tonic-rails: serving #{@router.service_names.join(', ')} on #{config.bind_address} " \
          "(#{config.dispatch_threads} dispatch threads)",
        )
      end
    end
  end
end
