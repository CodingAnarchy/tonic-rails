# frozen_string_literal: true

require "active_support"
require "active_support/notifications"

module Tonic
  module Rails
    # Every RPC is wrapped in an ActiveSupport::Notifications event, so APM
    # agents, OpenTelemetry and your own subscribers can observe it:
    #
    #   ActiveSupport::Notifications.subscribe("process_rpc.tonic_rails") do |event|
    #     event.payload[:path]      # "/demo.v1.ItemService/GetItem"
    #     event.payload[:service]   # "demo.v1.ItemService"
    #     event.payload[:rpc]       # "GetItem"
    #     event.payload[:metadata]  # request metadata Hash
    #     event.payload[:code]      # gRPC status code, 0 = OK
    #     event.payload[:status]    # :ok, :not_found, :internal, ...
    #     event.duration            # milliseconds
    #   end
    #
    # When a call fails, :exception_object holds the RpcError. If a handler
    # raised something unexpected, that RpcError's #cause is the original
    # exception.
    module Instrumentation
      EVENT = "process_rpc.tonic_rails"

      # Outgoing calls made with Tonic::Rails::Client publish
      # "request_rpc.tonic_rails", with the same payload plus :address, the
      # server called, and :attempts, how many times it was tried.
      REQUEST_EVENT = "request_rpc.tonic_rails"

      module_function

      # @api private
      def instrument(path, metadata, &)
        publish(EVENT, path, metadata, &)
      end

      # @api private
      def instrument_request(path, address, metadata, &)
        publish(REQUEST_EVENT, path, metadata, address: address, &)
      end

      # Yields the payload, or nil when nothing is listening.
      # @api private
      def publish(event, path, metadata, **extra)
        # Without subscribers, skip building the payload: this runs on
        # every RPC.
        return yield(nil) unless ActiveSupport::Notifications.notifier.listening?(event)

        service, rpc = path.delete_prefix("/").split("/", 2)
        payload = { path: path, service: service, rpc: rpc, metadata: metadata, code: nil, status: nil, **extra }
        ActiveSupport::Notifications.instrument(event, payload) do
          result = yield(payload)
          payload[:code] = 0
          payload[:status] = :ok
          result
        rescue RpcError => e
          payload[:code] = e.code
          payload[:status] = e.code_name
          raise
        rescue StandardError
          payload[:code] = RpcError::CODES[:internal]
          payload[:status] = :internal
          raise
        end
      end

      # Logs one line per RPC through the configured logger, e.g.
      #   gRPC /demo.v1.ItemService/GetItem OK in 1.4ms
      #   gRPC /demo.v1.ItemService/GetItem NOT_FOUND in 0.9ms: item 7 not found
      # Enabled by `config.log_requests = true`.
      class RequestLogger
        def initialize(logger)
          @logger = logger
        end

        def call(event)
          payload = event.payload
          status = (payload[:status] || :unknown).to_s.upcase
          line = format("gRPC %<path>s %<status>s in %<ms>.1fms", path: payload[:path], status: status, ms: event.duration)
          error = payload[:exception_object]
          line = "#{line}: #{error.message}" if error && payload[:code] != 0
          @logger.info(line)
        end

        # @return [Object] the subscription, for #unsubscribe
        def subscribe
          ActiveSupport::Notifications.subscribe(EVENT, self)
        end
      end
    end
  end
end
