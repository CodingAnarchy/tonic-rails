# frozen_string_literal: true

require "active_support/core_ext/class/attribute"
require "active_support/core_ext/hash/keys"
require "active_support/core_ext/string/inflections"

module Tonic
  module Rails
    # Include in a class to define a gRPC service handler.
    #
    #   class ProductService
    #     include Tonic::Rails::Base
    #     rpc_service "shop.ProductService"   # every RPC from the descriptor
    #
    #     def get_product(request)
    #       Shop::Product.new(id: request.id, title: "Widget")
    #     end
    #   end
    #
    # Handler methods are the snake_case RPC names.  A new instance is
    # created for every call, so instance variables never leak between
    # requests.
    #
    # Requests are decoded protobuf messages when `request_format :message`
    # is set (fastest), or string-keyed Hashes by default.  Handlers may
    # return either a protobuf message or a Hash; streaming handlers
    # return any Enumerable of them.
    module Base
      def self.included(base)
        base.extend(ClassMethods)
        base.class_attribute :grpc_service_name, instance_writer: false
        base.class_attribute :grpc_rpcs, instance_writer: false, default: {}
        base.class_attribute :grpc_request_format, instance_writer: false, default: :hash
      end

      module ClassMethods
        # @param name [String] fully-qualified service name, e.g. "shop.ProductService"
        def service(name)
          self.grpc_service_name = name.to_s
        end

        # How requests are passed to handlers.
        # @param format [Symbol] :hash (string-keyed Hash, the default) or
        #   :message (the decoded protobuf message — avoids a conversion)
        def request_format(format)
          unless %i[hash message].include?(format)
            raise ArgumentError, "request_format must be :hash or :message, got #{format.inspect}"
          end

          self.grpc_request_format = format
        end

        # Declare one RPC.
        # @param method_name [Symbol, String] RPC name as in the .proto, e.g. :GetProduct
        # @param request_class [Class] protobuf request message class
        # @param response_class [Class] protobuf response message class
        # @param stream [Symbol, nil] :server, :client, :bidi, or nil for unary
        def rpc(method_name, request_class, response_class, stream: nil)
          unless [nil, :server, :client, :bidi].include?(stream)
            raise ArgumentError, "stream must be :server, :client, :bidi, or nil, got #{stream.inspect}"
          end

          name = method_name.to_s
          self.grpc_rpcs = grpc_rpcs.merge(
            name => {
              request: request_class,
              response: response_class,
              request_stream: %i[client bidi].include?(stream),
              response_stream: %i[server bidi].include?(stream),
              handler: name.underscore.to_sym,
            }.freeze,
          )
        end

        # Declare the service and every RPC in it from the protobuf
        # descriptor pool.  Load the generated `_pb.rb` file first.
        #
        # @param full_name [String] fully-qualified service name
        def rpc_service(full_name)
          descriptor = Google::Protobuf::DescriptorPool.generated_pool.lookup(full_name.to_s)
          unless descriptor.is_a?(Google::Protobuf::ServiceDescriptor)
            raise NotConfiguredError,
                  "rpc_service: #{full_name} is not a known protobuf service. " \
                  "Require its generated _pb.rb file (protoc --ruby_out) before declaring the handler."
          end

          service(full_name)
          descriptor.each do |method|
            stream = if method.client_streaming && method.server_streaming
              :bidi
            elsif method.client_streaming
              :client
            elsif method.server_streaming
              :server
            end
            rpc(method.name, method.input_type.msgclass, method.output_type.msgclass, stream: stream)
          end
        end

        # @api private  Called by Server#register.
        def register_on(router)
          raise NotConfiguredError, "#{self} has no service name; call `service` or `rpc_service`" unless grpc_service_name
          raise NotConfiguredError, "#{self} declares no RPCs" if grpc_rpcs.empty?

          router.add(self)
        end
      end

      # gRPC request metadata (lower-case header names).  Binary (`-bin`)
      # values are base64-encoded, as sent on the wire.
      # @return [Hash{String=>String}]
      attr_reader :grpc_metadata

      # Milliseconds left before the client's deadline, or nil if the
      # client did not set one.
      # @return [Integer, nil]
      def deadline_ms
        return unless @grpc_deadline_at

        remaining = ((@grpc_deadline_at - Process.clock_gettime(Process::CLOCK_MONOTONIC)) * 1000).to_i
        remaining.negative? ? 0 : remaining
      end

      # True when the deadline has passed.
      def deadline_exceeded?
        !deadline_ms.nil? && deadline_ms.zero?
      end

      # True when the server is shutting down.
      def cancelled?
        Tonic::Rails.cancelled?
      end

      # @api private
      # Run one RPC.  `reader` yields raw request messages (`#next`, then
      # `#error` once exhausted); `sender` accepts raw response messages
      # (`#push`) for server-streaming RPCs.
      # @return [String, nil] the encoded unary response, or nil when
      #   messages were streamed through `sender`
      def call_rpc(rpc_name, metadata:, reader:, sender: nil, timeout_ms: nil)
        spec = self.class.grpc_rpcs[rpc_name.to_s]
        raise RpcError.new("method not implemented", code: :unimplemented) unless spec
        raise RpcError.new("method #{rpc_name} not implemented", code: :unimplemented) unless respond_to?(spec[:handler])

        @grpc_metadata = metadata
        @grpc_deadline_at = timeout_ms && (Process.clock_gettime(Process::CLOCK_MONOTONIC) + (timeout_ms / 1000.0))

        request = if spec[:request_stream]
          Enumerator.new do |y|
            while (message = read_request(reader, spec))
              y << message
            end
          end
        else
          read_request(reader, spec) || raise(RpcError.new("missing request message", code: :invalid_argument))
        end

        result = public_send(spec[:handler], request)

        if spec[:response_stream]
          unless result.respond_to?(:each)
            raise TypeError, "streaming handler #{self.class}##{spec[:handler]} must return an Enumerable, got #{result.class}"
          end

          result.each { |item| push_response(sender, encode_response(item, spec)) }
          nil
        else
          encode_response(result, spec)
        end
      rescue RpcError
        raise
      rescue StandardError => e
        Tonic::Rails.log_exception(e, "#{self.class}##{rpc_name}")
        raise RpcError.new("internal error", code: :internal)
      end

      private

      def read_request(reader, spec)
        bytes = reader.next
        if bytes.nil?
          code, message = reader.error
          raise RpcError.new(message, code: code) if code

          return
        end

        message = begin
          spec[:request].decode(bytes)
        rescue Google::Protobuf::ParseError, ArgumentError
          raise RpcError.new("invalid protobuf request", code: :invalid_argument)
        end
        self.class.grpc_request_format == :message ? message : message.to_h.deep_stringify_keys
      end

      def encode_response(item, spec)
        klass = spec[:response]
        message = case item
        when klass then item
        when Hash then klass.new(item)
        else
          raise TypeError, "#{self.class}##{spec[:handler]} must return a #{klass} or Hash, got #{item.class}"
        end
        klass.encode(message)
      end

      def push_response(sender, bytes)
        sender.push(bytes)
      rescue IOError
        # The client went away; nothing left to deliver to.
        raise RpcError.new("client cancelled the stream", code: :cancelled)
      end
    end
  end
end
