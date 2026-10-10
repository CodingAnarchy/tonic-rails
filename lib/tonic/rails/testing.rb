# frozen_string_literal: true

module Tonic
  module Rails
    # Call handlers in-process, without a network or the native extension.
    # Requests and responses go through real protobuf encoding, so tests
    # exercise the same conversion the server does.
    #
    #   require "tonic/rails/testing"
    #
    #   response = Tonic::Rails::Testing.call(ProductService, :GetProduct, { id: 1 })
    #   response.title  # => "Widget"
    #
    #   expect { Tonic::Rails::Testing.call(ProductService, :GetProduct, { id: 0 }) }
    #     .to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code_name).to eq(:not_found) }
    module Testing
      # Request side of a call: yields pre-encoded messages.
      class Reader
        def initialize(messages, error: nil)
          @messages = messages.dup
          @error = error
        end

        def next
          @messages.shift
        end

        attr_reader :error
      end

      # Response side of a call: collects encoded messages.
      class Sender
        attr_reader :messages

        def initialize
          @messages = []
        end

        def push(bytes)
          @messages << bytes
        end
      end

      module_function

      # @param service_class [Class] handler class
      # @param rpc_name [Symbol, String] RPC name as in the .proto
      # @param request [Google::Protobuf::AbstractMessage, Hash, Array] the
      #   request; an Array of them for client- and bidi-streaming RPCs
      # @param metadata [Hash{String=>String}]
      # @param timeout_ms [Integer, nil] simulated client deadline
      # @return [Google::Protobuf::AbstractMessage, Array] the response, or
      #   an Array of responses for server- and bidi-streaming RPCs
      # @raise [RpcError] when the handler ends the call with a status
      def call(service_class, rpc_name, request, metadata: {}, timeout_ms: nil)
        spec = service_class.grpc_rpcs.fetch(rpc_name.to_s) do
          raise ArgumentError, "#{service_class} has no RPC #{rpc_name}"
        end

        requests = spec[:request_stream] ? Array(request) : [request]
        reader = Reader.new(requests.map { |r| encode(spec[:request], r) })
        sender = Sender.new

        result = service_class.new.call_rpc(
          rpc_name.to_s,
          metadata: metadata.transform_keys(&:to_s),
          reader: reader,
          sender: sender,
          timeout_ms: timeout_ms,
        )

        if spec[:response_stream]
          sender.messages.map { |bytes| spec[:response].decode(bytes) }
        else
          spec[:response].decode(result)
        end
      end

      def encode(klass, message)
        message = klass.new(message) if message.is_a?(Hash)
        klass.encode(message)
      end
    end
  end
end
