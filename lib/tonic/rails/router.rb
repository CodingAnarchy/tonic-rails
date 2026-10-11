# frozen_string_literal: true

module Tonic
  module Rails
    # Maps gRPC method paths ("/package.Service/Method") to handler classes.
    class Router
      Route = Struct.new(:service_class, :rpc_name)

      def initialize
        @routes = {}
        @services = {}
      end

      # Register (or re-register, e.g. after a code reload) every RPC of
      # a handler class.
      # @param service_class [Class] a class including Tonic::Rails::Base
      def add(service_class)
        name = service_class.grpc_service_name
        @routes.delete_if { |_, route| route.service_class.grpc_service_name == name }
        @services[name] = service_class
        service_class.grpc_rpcs.each_key do |rpc_name|
          @routes["/#{name}/#{rpc_name}"] = Route.new(service_class, rpc_name)
        end
        self
      end

      # @param path [String] gRPC method path
      # @param metadata [Hash{String=>String}]
      # @param call [Hash] :reader, :sender and optional :timeout_ms from the transport
      # @return [String, nil] encoded unary response, or nil for streamed responses
      def dispatch(path, metadata, call)
        route = @routes[path]
        raise RpcError.new("unknown method #{path}", code: :unimplemented) unless route

        resolve(route.service_class).new.call_rpc(
          route.rpc_name,
          metadata: metadata,
          reader: call[:reader],
          sender: call[:sender],
          timeout_ms: call[:timeout_ms],
        )
      end

      # @return [Array<String>] registered gRPC method paths
      def paths
        @routes.keys
      end

      # @return [Array<String>] registered fully-qualified service names
      def service_names
        @services.keys
      end

      def empty?
        @routes.empty?
      end

      private

      # With code reloading on, look the class up again so edits to
      # handler bodies take effect without a restart.
      def resolve(service_class)
        return service_class unless Tonic::Rails.configuration.reload_handlers

        Object.const_get(service_class.name)
      rescue NameError
        service_class
      end
    end
  end
end
