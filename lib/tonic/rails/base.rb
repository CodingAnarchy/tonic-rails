# frozen_string_literal: true

require "active_support/core_ext/class/attribute"
require "active_support/core_ext/hash/keys"
require "active_support/core_ext/string/inflections"
require_relative "proto_parser"

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
        # @param stream [Symbol] pass `stream: :server` for server-streaming RPCs
        def rpc(method_name, request_class, response_class, stream: nil)
          self.grpc_rpcs = grpc_rpcs.merge(
            method_name.to_s => {
              request: request_class,
              response: response_class,
              stream: stream == :server,
            },
          )
        end

        # Auto-register all RPCs for a service by parsing the .proto file.
        # Eliminates the need to declare each `rpc :Method, Req, Resp` individually.
        #
        #   class ProductService
        #     include Tonic::Rails::Base
        #     rpc_service "shop.ProductService"  # scans config.proto_dir
        #
        #     def get_product(request); ... end
        #     def list_products(request); ... end
        #   end
        #
        # @param full_name [String] fully-qualified service name (package.Service)
        # @param proto_file [String, nil] explicit .proto path; if nil, scans
        #   Tonic::Rails.configuration.proto_dir
        def rpc_service(full_name, proto_file: nil)
          # Set the service name (replaces `service` declaration).
          self.grpc_service_name = full_name

          info = if proto_file
            package, service_name = full_name.split(".")
            ProtoParser.parse_service(proto_file, service_name)
          else
            proto_dir = Tonic::Rails.configuration.proto_dir
            ProtoParser.find_service(proto_dir, full_name)
          end

          unless info
            location = proto_file || "#{Tonic::Rails.configuration.proto_dir}/"
            raise Error, "rpc_service: service #{full_name} not found in #{location}"
          end

          info.rpcs.each do |rpc_info|
            request_class = ProtoParser.resolve_type(rpc_info.request_type, info.package)
            response_class = ProtoParser.resolve_type(rpc_info.response_type, info.package)

            rpc(
              rpc_info.name,
              request_class,
              response_class,
              stream: rpc_info.stream ? :server : nil,
            )
          end
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
      # a Hash (string keys), converts the response(s) → protobuf bytes.
      #
      # Unary handlers receive a Hash and return a Hash.
      # Server-streaming handlers (declared with `stream: :server`) receive
      # a Hash and return an Enumerable of Hashes (Array, Enumerator, etc.).
      # Streaming is buffered: the full Enumerable is materialized before
      # any bytes are sent on the wire.
      #
      # @param method_name [String] RPC method name
      # @return [String, Array<String>] encoded protobuf bytes (String for unary,
      #   Array of Strings for server-streaming)
      # @param metadata [Hash] gRPC metadata headers
      def call_rpc(method_name, request_bytes, metadata: {})
        spec = self.class.grpc_rpcs[method_name.to_s]
        raise Error, "Unknown RPC: #{method_name}" unless spec

        # Decode protobuf → object → Hash with deep-stringified keys.
        request_obj = spec[:request].decode(request_bytes)
        request_hash = request_obj.to_h.deep_stringify_keys

        # Store metadata so handler can access deadline, auth tokens, etc.
        @grpc_metadata = metadata

        # Capture absolute deadline at handler entry (post-dequeue) so
        # deadline_ms counts down as time elapses. Uses CLOCK_MONOTONIC
        # to avoid wall-clock jumps.
        if metadata && metadata["_timeout_ms"]
          @grpc_deadline_at = Process.clock_gettime(Process::CLOCK_MONOTONIC) +
            metadata["_timeout_ms"].to_f / 1000.0
        else
          @grpc_deadline_at = nil
        end

        # Dispatch based on RPC declaration, not payload shape.
        # This disambiguates empty streams from empty unary responses.
        result = send(method_name.to_s.underscore, request_hash)

        if spec[:stream]
          # Server-streaming: handler must return an Enumerable of Hashes.
          unless result.respond_to?(:each)
            raise RpcError.new("streaming handler must return an Enumerable, got #{result.class}", code: :internal)
          end
          # Materialize and encode each response (buffered, not incremental).
          result.map do |item|
            spec[:response].encode(spec[:response].new(item))
          end
        else
          # Unary: handler must return a Hash.
          unless result.is_a?(Hash)
            raise RpcError.new("unary handler must return a Hash, got #{result.class}", code: :internal)
          end
          spec[:response].encode(spec[:response].new(result))
        end
      rescue RpcError
        raise  # re-raise RpcError with its original code
      rescue => e
        raise RpcError.new(e.message, code: :internal)
      end

      # gRPC metadata for the current request.
      # Includes user-defined headers plus `_timeout_ms` (if the client
      # set a grpc-timeout header, the Rust layer injects the remaining
      # milliseconds here).
      # @return [Hash{String=>String}]
      attr_reader :grpc_metadata

      # Remaining time budget in milliseconds before the gRPC deadline
      # expires. Returns nil if no deadline was set by the client.
      # Decrements as time elapses — safe to poll in a loop.
      # @return [Integer, nil]
      def deadline_ms
        return nil unless @grpc_deadline_at
        remaining = (@grpc_deadline_at - Process.clock_gettime(Process::CLOCK_MONOTONIC)) * 1000
        remaining < 0 ? 0 : remaining.to_i
      end

      # Check if the server is shutting down. Handlers should abort
      # in-progress work and return quickly when this returns true.
      # @return [Boolean]
      def cancelled?
        Tonic::Rails.cancelled?
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
