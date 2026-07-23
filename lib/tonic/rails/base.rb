# frozen_string_literal: true

require "active_support/core_ext/class/attribute"
require "active_support/core_ext/hash/keys"
require "active_support/core_ext/string/inflections"

module Tonic
  module Rails
    # Include in a class to define a gRPC service handler.
    #
    #   class GreeterService
    #     include Tonic::Rails::Base
    #
    #     service "helloworld.Greeter"
    #
    #     rpc :SayHello, HelloRequest, HelloResponse
    #
    #     def say_hello(request)
    #       HelloResponse.new(message: "Hello, #{request.name}!")
    #     end
    #   end
    #
    # The `service` declaration sets the fully-qualified protobuf service name.
    # Each `rpc` call registers a method with its request/response protobuf
    # classes.  The instance method (snake_case of the rpc name) receives a
    # decoded protobuf request and returns a protobuf response object.
    module Base
      def self.included(base)
        base.extend(ClassMethods)
        base.class_attribute :grpc_service_name
        base.class_attribute :grpc_rpcs
        base.grpc_rpcs = {}
      end

      module ClassMethods
        # @param name [String] fully-qualified protobuf service name,
        #   e.g. "helloworld.Greeter"
        def service(name)
          self.grpc_service_name = name
        end

        # @param method_name [Symbol] RPC method name as defined in the .proto
        # @param request_class [Class] protobuf request class (responds to .decode)
        # @param response_class [Class] protobuf response class (responds to .encode)
        def rpc(method_name, request_class, response_class)
          self.grpc_rpcs = grpc_rpcs.merge(
            method_name.to_s => {
              request: request_class,
              response: response_class,
            },
          )
        end

        # Register all declared RPCs with the given router.
        # Called by Tonic::Rails::Server#register.
        def register_on(router)
          raise NotConfiguredError, "#{self} has no service name" unless grpc_service_name

          grpc_rpcs.each do |method_name, spec|
            router.add(grpc_service_name, method_name, self, method_name, spec)
          end
        end
      end

      # Dispatch entry point for raw-bytes mode.
      # Decodes protobuf bytes → object → Hash, calls the handler with
      # a Hash (string keys), converts the response Hash → protobuf
      # object → encoded bytes.
      #
      # The handler ALWAYS receives a Hash and returns a Hash.
      #
      # @param method_name [String] RPC method name
      # @param request_bytes [String] raw protobuf bytes from the wire
      # @param metadata [Hash] gRPC metadata headers
      # @return [String] encoded protobuf response bytes
      def call_rpc(method_name, request_bytes, metadata: {})
        spec = self.class.grpc_rpcs[method_name.to_s]
        raise Error, "Unknown RPC: #{method_name}" unless spec

        # Decode protobuf → object → Hash with deep-stringified keys.
        request_obj = spec[:request].decode(request_bytes)
        request_hash = request_obj.to_h.deep_stringify_keys

        # Handler receives a Hash and returns a Hash.
        result_hash = send(method_name.to_s.underscore, request_hash)

        # Hash → protobuf object → encoded bytes.
        spec[:response].encode(spec[:response].new(result_hash))
      rescue RpcError
        raise  # re-raise RpcError with its original code
      rescue => e
        raise RpcError.new(e.message, code: :internal)
      end
    end

    # Represents a gRPC error with a status code.
    class RpcError < Error
      attr_reader :code

      CODES = {
        ok: 0,
        cancelled: 1,
        unknown: 2,
        invalid_argument: 3,
        deadline_exceeded: 4,
        not_found: 5,
        already_exists: 6,
        permission_denied: 7,
        resource_exhausted: 8,
        failed_precondition: 9,
        aborted: 10,
        out_of_range: 11,
        unimplemented: 12,
        internal: 13,
        unavailable: 14,
        data_loss: 15,
        unauthenticated: 16,
      }.freeze

      def initialize(message, code: :unknown)
        super(message)
        @code = CODES.fetch(code, 2)
      end
    end
  end
end
