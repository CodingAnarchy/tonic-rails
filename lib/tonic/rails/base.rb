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
        # @param stream [Symbol] streaming type: :server, :client, or :bidi
        def rpc(method_name, request_class, response_class, stream: nil)
          self.grpc_rpcs = grpc_rpcs.merge(
            method_name.to_s => {
              request: request_class,
              response: response_class,
              request_stream: stream == :client || stream == :bidi,
              response_stream: stream == :server || stream == :bidi,
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

          unless proto_file || Tonic::Rails.configuration.proto_dir
            raise Error, "rpc_service: proto_dir not configured in Tonic::Rails.configuration"
          end

          info = if proto_file
            *pkg, service_name = full_name.split(".")
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

            stream = if rpc_info.request_stream && rpc_info.response_stream
              :bidi
            elsif rpc_info.request_stream
              :client
            elsif rpc_info.response_stream
              :server
            else
              nil
            end

            rpc(
              rpc_info.name,
              request_class,
              response_class,
              stream: stream,
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
      # a Hash (string keys) or an Enumerator of Hashes, converts the
      # response(s) → protobuf bytes.
      #
      # RPC types and handler signatures:
      #   Unary:          def method(request_hash) → Hash
      #   Server stream:  def method(request_hash) → Enumerable of Hashes
      #   Client stream:  def method(requests) → Hash
      #   Bidi stream:    def method(requests) → Enumerable of Hashes
      #
      # For client/bidi streaming, the handler receives an Enumerator that
      # yields decoded request Hashes one at a time.  Each yield blocks
      # until the next message arrives from the client (via RequestReader,
      # which releases the GVL during the blocking receive).
      #
      # @param method_name [String] RPC method name
      # @param request_bytes [String] raw protobuf bytes (first message;
      #   empty when a RequestReader is provided via metadata)
      # @param metadata [Hash] gRPC metadata headers
      # @return [String, Array<String>, nil] encoded protobuf bytes
      def call_rpc(method_name, request_bytes, metadata: {})
        spec = self.class.grpc_rpcs[method_name.to_s]
        raise Error, "Unknown RPC: #{method_name}" unless spec

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

        # ── Request handling ────────────────────────────────────────
        if spec[:request_stream]
          # Client streaming or bidi: handler receives an Enumerator
          # of decoded request Hashes, not a single Hash.
          request_reader = metadata["_request_reader"]
          if request_reader
            # Real reader from Rust — yields raw bytes, we decode each.
            request_enum = Enumerator.new do |y|
              while (bytes = read_next(method_name, request_reader))
                obj = decode_request(method_name, bytes, spec[:request])
                y << obj.to_h.deep_stringify_keys
              end
            rescue RpcError
              raise
            rescue => e
              Tonic::Rails.configuration.logger&.warn(
                "tonic-rails: error in stream for #{method_name}: #{e.class}: #{e.message}",
              )
              raise RpcError.new("stream read error", code: :unavailable)
            end
          else
            # Buffered fallback for tests: wrap single request_bytes.
            obj = decode_request(method_name, request_bytes, spec[:request])
            request_enum = [obj.to_h.deep_stringify_keys].each
          end
          result = send(method_name.to_s.underscore, request_enum)
        else
          # Unary or server streaming: handler receives a single request Hash.
          # In real dispatch, request_bytes is empty and the first message
          # comes from the RequestReader. In unit tests (no reader), the
          # request_bytes argument is used directly.
          request_reader = metadata["_request_reader"]
          actual_bytes = if request_reader
            bytes = read_next(method_name, request_reader)
            raise RpcError.new("empty request stream", code: :invalid_argument) if bytes.nil?
            bytes
          else
            request_bytes
          end
          request_obj = decode_request(method_name, actual_bytes, spec[:request])
          request_hash = request_obj.to_h.deep_stringify_keys
          result = send(method_name.to_s.underscore, request_hash)
        end

        # ── Response handling ───────────────────────────────────────
        if spec[:response_stream]
          # Server streaming or bidi: handler must return an Enumerable.
          unless result.respond_to?(:each)
            raise RpcError.new("streaming handler must return an Enumerable, got #{result.class}", code: :internal)
          end
          # Incremental streaming: if a StreamSender is available (passed
          # through metadata by the Rust dispatch layer), send each encoded
          # message through it one at a time.  This avoids materializing the
          # entire stream in memory — peak memory is one message, not the
          # full collection.  Backpressure is provided by a bounded channel
          # (32 messages); the GVL is released during each send so other
          # Ruby workers can dispatch concurrently.
          stream_sender = metadata["_stream_sender"]
          if stream_sender
            result.each do |item|
              encoded = spec[:response].encode(spec[:response].new(item))
              begin
                stream_sender.send(encoded)
              rescue => e
                # Channel closed (client disconnected) or send cancelled
                # (server shutting down). Not a handler bug — map to
                # :unavailable so clients know to retry, not :internal.
                raise RpcError.new(
                  "stream unavailable: #{e.message}",
                  code: :unavailable,
                )
              end
            end
            nil # Signal: data sent incrementally via StreamSender.
          else
            # Buffered fallback (no StreamSender — e.g. in unit tests
            # that call call_rpc directly without the Rust dispatch).
            result.map do |item|
              spec[:response].encode(spec[:response].new(item))
            end
          end
        else
          # Unary or client streaming: handler must return a Hash.
          unless result.is_a?(Hash)
            raise RpcError.new("handler must return a Hash, got #{result.class}", code: :internal)
          end
          spec[:response].encode(spec[:response].new(result))
        end
      rescue RpcError
        raise  # re-raise RpcError with its original code
      rescue Error
        raise  # re-raise framework errors (e.g. "Unknown RPC")
      rescue => e
        # Log the full error server-side; send a sanitized message to
        # the client to avoid leaking internal details (stack traces,
        # file paths, SQL fragments, internal hostnames).
        logger = Tonic::Rails.configuration.logger
        logger&.error("tonic-rails: internal error in #{method_name}: #{e.class}: #{e.message}")
        raise RpcError.new("internal error", code: :internal)
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

      private

      def decode_request(method_name, bytes, request_class)
        request_class.decode(bytes)
      rescue => e
        Tonic::Rails.configuration.logger&.warn(
          "tonic-rails: invalid protobuf for #{method_name}: #{e.class}: #{e.message}",
        )
        raise RpcError.new("invalid protobuf request", code: :invalid_argument)
      end

      def read_next(method_name, reader)
        reader.next
      rescue => e
        Tonic::Rails.configuration.logger&.warn(
          "tonic-rails: error reading from request for #{method_name}: #{e.class}: #{e.message}",
        )
        raise RpcError.new("stream read error", code: :unavailable)
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
