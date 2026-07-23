# frozen_string_literal: true

module Tonic
  module Rails
    # Maps gRPC method paths (e.g. "/helloworld.Greeter/SayHello") to
    # Ruby service handlers.  The native extension calls back into the
    # router's #dispatch for each incoming request.
    class Router
      # @api private
      Handler = Struct.new(:service_class, :method_name, :spec, keyword_init: true)

      def initialize
        @routes = {}
      end

      # @param service_name [String] fully-qualified protobuf service name
      # @param method_name [String] RPC method name
      # @param service_class [Class] class including Tonic::Rails::Base
      # @param rpc_method [String] method to call on the service instance
      # @param spec [Hash] :request and :response protobuf classes
      def add(service_name, method_name, service_class, rpc_method, spec)
        path = "/#{service_name}/#{method_name}"
        @routes[path] = Handler.new(
          service_class: service_class,
          method_name: rpc_method,
          spec: spec,
        )
      end

      # @param path [String] gRPC method path
      # @param request_bytes [String] raw protobuf request bytes
      # @param metadata [Hash] gRPC metadata
      # @return [String] encoded protobuf response bytes
      # @raise [Error] if no handler is registered for the path
      def dispatch(path, request_bytes, metadata: {})
        handler = @routes[path]
        raise Error, "No handler registered for #{path}" unless handler

        instance = handler.service_class.new
        instance.call_rpc(handler.method_name, request_bytes, metadata: metadata)
      end

      # @return [Array<String>] all registered gRPC method paths
      def paths
        @routes.keys
      end

      # @return [Boolean]
      def empty?
        @routes.empty?
      end
    end
  end
end
