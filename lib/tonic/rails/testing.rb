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

      # Response side of a call: collects encoded messages and metadata.
      class Sender
        attr_reader :messages, :headers, :trailers

        def initialize
          @messages = []
          @headers = {}
          @trailers = {}
        end

        def push(bytes)
          @messages << bytes
        end

        def send_headers(headers)
          raise "response headers must be sent before any response message" unless @messages.empty?

          @headers.merge!(headers)
        end

        def add_trailers(trailers)
          @trailers.merge!(trailers)
        end
      end

      # What a call produced: the response (or responses, for streaming),
      # plus the response headers and trailers as they'd go on the wire.
      Result = Struct.new(:response, :headers, :trailers)

      module_function

      # @param service_class [Class] handler class
      # @param rpc_name [Symbol, String] RPC name as in the .proto
      # @param request [Google::Protobuf::AbstractMessage, Hash, Array] the
      #   request; an Array of them for client- and bidi-streaming RPCs
      # @param metadata [Hash{String=>String}]
      # @param timeout_ms [Integer, nil] simulated client deadline
      # @param peer [Tonic::Rails::Peer, nil] simulated client (address, certificates)
      # @return [Google::Protobuf::AbstractMessage, Array] the response, or
      #   an Array of responses for server- and bidi-streaming RPCs
      # @raise [RpcError] when the handler ends the call with a status
      def call(service_class, rpc_name, request, metadata: {}, timeout_ms: nil, peer: nil)
        invoke(service_class, rpc_name, request, metadata: metadata, timeout_ms: timeout_ms, peer: peer).response
      end

      # Like #call, but returns a Result with the response metadata too:
      #
      #   result = Tonic::Rails::Testing.invoke(ItemService, :GetItem, { id: 1 })
      #   result.response.name           # => "Widget"
      #   result.headers["x-cache"]      # => "hit"
      #   result.trailers["x-cost-ms"]   # => "3"
      #
      # @return [Result]
      def invoke(service_class, rpc_name, request, metadata: {}, timeout_ms: nil, peer: nil)
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
          peer: peer,
        )

        response = if spec[:response_stream]
          sender.messages.map { |bytes| spec[:response].decode(bytes) }
        else
          spec[:response].decode(result)
        end
        Result.new(response, sender.headers, sender.trailers)
      end

      def encode(klass, message)
        message = klass.new(message) if message.is_a?(Hash)
        klass.encode(message)
      end
    end
  end
end
