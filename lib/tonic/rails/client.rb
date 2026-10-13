# frozen_string_literal: true

require "active_support/inflector"
require "tonic/rails/client/retries"

module Tonic
  module Rails
    # Calls other gRPC services, without the grpc gem.
    #
    #   ITEMS = Tonic::Rails::Client.new("items.internal:50051", deadline: 2)
    #
    #   items = ITEMS.stub("demo.v1.ItemService")
    #   item = items.get_item(Demo::V1::GetItemRequest.new(id: 1))
    #   item = items.get_item(id: 1)
    #   item = items.get_item({ id: 1 }, deadline: 0.5, metadata: { "x-request-id" => request_id })
    #   items.list_items(category: "books").each { |item| ... }
    #   items.import_items(rows.lazy.map { |row| { name: row[:name] } })
    #   items.sync(changes).each { |ack| ... }
    #
    # Retries are opt-in: `retries: true` (or a Hash; see RetryPolicy) on
    # the client or a call retries calls the server never started on.
    # `method_retries:` sets them per service or method.
    #
    # Stubs are built from the protobuf descriptors that
    # `tonic_rails:compile_protos` (protoc --ruby_out) generates; no
    # `_services_pb.rb` is needed.  A failed call raises
    # Tonic::Rails::RpcError.
    #
    # A client holds one HTTP/2 connection and is safe to share between
    # threads, so create it once (a constant or an initializer).  It keeps
    # working across `fork`: each process connects on its own, with no
    # hooks to call.
    class Client
      include Retries

      # A response with its headers and trailers; see `return_call:`.
      Call = Data.define(:response, :headers, :trailers)

      DEFAULT_MAX_MESSAGE_SIZE = 4 * 1024 * 1024

      # @return [String] "host:port"
      attr_reader :address

      # @return [Numeric, nil] default deadline in seconds
      attr_reader :deadline

      # @return [RetryPolicy, nil] how calls are retried, unless they say otherwise
      attr_reader :retries

      # @return [Hash{String=>RetryPolicy, nil}] retry policies by service or
      #   method name, which take precedence over #retries
      attr_reader :method_retries

      # @param address [String] "host:port"
      # @param tls [Boolean] false for plaintext, which must be explicit
      # @param tls_ca_path [String, nil] CA certificates (PEM) to trust instead
      #   of the system's root certificates
      # @param tls_cert_path [String, nil] client certificate (PEM), for mutual TLS
      # @param tls_key_path [String, nil] client private key (PEM), for mutual TLS
      # @param tls_server_name [String, nil] name to verify the server's
      #   certificate against, if not the host in `address`
      # @param deadline [Numeric, nil] seconds each call may take, unless the
      #   call sets its own
      # @param metadata [Hash] sent with every call (see Tonic::Rails::Metadata)
      # @param max_message_size [Integer] largest message sent or received, in bytes
      # @param connect_timeout [Numeric] seconds to wait for a connection
      # @param retries [RetryPolicy, Hash, Boolean] retry calls the server
      #   never started on (see RetryPolicy); off by default
      # @param method_retries [Hash{String=>RetryPolicy, Hash, Boolean}]
      #   retries for some services ("demo.v1.ItemService") or methods
      #   ("demo.v1.ItemService/GetItem"), instead of `retries`.  A method's
      #   entry beats its service's.
      # @param logger [Logger, nil] where retries are logged; defaults to
      #   Tonic::Rails.configuration.logger
      def initialize(address, tls: true, tls_ca_path: nil, tls_cert_path: nil, tls_key_path: nil,
                     tls_server_name: nil, deadline: nil, metadata: {}, max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
                     connect_timeout: 5, retries: false, method_retries: {}, logger: nil)
        Tonic::Rails.native_extension!
        validate_address!(address)
        validate_tls!(tls, tls_cert_path, tls_key_path)
        positive!(:deadline, deadline) unless deadline.nil?
        positive!(:connect_timeout, connect_timeout)
        unless max_message_size.is_a?(Integer) && max_message_size.positive?
          raise ArgumentError, "max_message_size must be a positive Integer"
        end

        @address = address
        @deadline = deadline
        @retries = RetryPolicy.from(retries)
        @method_retries = method_retry_policies(method_retries)
        @logger = logger
        @metadata = Metadata.encode(metadata)
        @stubs = {}
        tls_options = [read_pem(tls_ca_path, "CA"), read_pem(tls_cert_path, "certificate"),
                       read_pem(tls_key_path, "key"), tls_server_name]
        @native = TonicRails::Native::Channel.new(
          "#{tls ? 'https' : 'http'}://#{address}", max_message_size, (connect_timeout * 1000).ceil,
          (tls_options if tls)
        )
      end

      # A stub with one method per RPC of the service, named in snake_case
      # (`GetItem` becomes `get_item`).
      #
      # @param service [String, Google::Protobuf::ServiceDescriptor, #service_name]
      #   the fully-qualified service name, its descriptor, or a grpc gem
      #   service class (`Demo::V1::ItemService::Service`)
      # @return [Tonic::Rails::Client::Stub]
      def stub(service)
        descriptor = service_descriptor(service)
        @stubs[descriptor.name] ||= Stub.for(descriptor).new(self)
      end

      def inspect
        "#<#{self.class.name} #{address}>"
      end

      # The deadline of the RPC this thread is handling, as a monotonic
      # clock time; outgoing calls default to it.
      # @api private
      def self.current_deadline
        Thread.current[:tonic_rails_deadline]
      end

      # @api private
      def self.with_deadline(deadline)
        previous = Thread.current[:tonic_rails_deadline]
        Thread.current[:tonic_rails_deadline] = deadline
        yield
      ensure
        Thread.current[:tonic_rails_deadline] = previous
      end

      # Make a unary call.  Stubs call this; use them instead.
      # @api private
      def unary(path, input, output, request, deadline: nil, metadata: {}, return_call: false, retries: nil)
        bytes = input.encode(request_message(path, input, request))
        metadata = call_metadata(metadata)
        deadline_at = deadline_at(deadline)
        policy = retry_policy(path, retries)

        Instrumentation.instrument_request(path, address, metadata) do |payload|
          code, error_message, response, headers, trailers, details = with_retries(path, policy, deadline_at, payload) do
            @native.unary(path, bytes, metadata, remaining_ms(deadline_at))
          end
          headers = decode_metadata(headers)
          trailers = decode_metadata(trailers)
          check_status!(code, error_message, trailers, details)

          response = decode_response(output, response, path)
          return_call ? Call.new(response: response, headers: headers, trailers: trailers) : response
        end
      end

      # Start a server-streaming call.  Stubs call this; use them instead.
      # @api private
      def server_streaming(path, input, output, request, deadline: nil, metadata: {}, retries: nil)
        bytes = input.encode(request_message(path, input, request))
        metadata = call_metadata(metadata)
        # Fixed now, so the stream keeps the deadline of the RPC this
        # thread is handling even if it's read later.
        deadline_at = deadline_at(deadline)
        policy = retry_policy(path, retries)

        ServerStream.new(path) do |stream, block|
          Instrumentation.instrument_request(path, address, metadata) do |payload|
            # A message arrives only after the response headers, which
            # commit the call, so an attempt that yielded anything is never
            # retried.
            code, error_message, _, headers, trailers, details = with_retries(path, policy, deadline_at, payload) do
              read_stream(@native.server_streaming(path, bytes, metadata, remaining_ms(deadline_at)), output, path, &block)
            end
            trailers = decode_metadata(trailers)
            stream.finish(decode_metadata(headers), trailers)
            check_status!(code, error_message, trailers, details)
          end
        end
      end

      # Make a client-streaming call.  Stubs call this; use them instead.
      # @api private
      def client_streaming(path, input, output, requests, deadline: nil, metadata: {}, return_call: false,
                           retries: nil)
        enumerable!(path, requests)
        metadata = call_metadata(metadata)
        deadline_at = deadline_at(deadline)
        policy = retry_policy(path, retries)

        Instrumentation.instrument_request(path, address, metadata) do |payload|
          code, error_message, responses, headers, trailers, details = with_retries(path, policy, deadline_at, payload) do
            streaming_call(path, input, metadata, deadline_at).client_stream(requests)
          end
          headers = decode_metadata(headers)
          trailers = decode_metadata(trailers)
          check_status!(code, error_message, trailers, details)

          response = single_response(output, responses, path)
          return_call ? Call.new(response: response, headers: headers, trailers: trailers) : response
        end
      end

      # Start a bidirectional-streaming call.  Stubs call this; use them
      # instead.
      # @api private
      def bidi_streaming(path, input, output, requests, deadline: nil, metadata: {}, retries: nil)
        enumerable!(path, requests)
        metadata = call_metadata(metadata)
        deadline_at = deadline_at(deadline)
        policy = retry_policy(path, retries)

        ServerStream.new(path) do |stream, block|
          Instrumentation.instrument_request(path, address, metadata) do |payload|
            code, error_message, _, headers, trailers, details = with_retries(path, policy, deadline_at, payload) do
              streaming_call(path, input, metadata, deadline_at).bidi(requests) do |bytes|
                block.call(decode_response(output, bytes, path))
              end
            end
            trailers = decode_metadata(trailers)
            stream.finish(decode_metadata(headers), trailers)
            check_status!(code, error_message, trailers, details)
          end
        end
      end

      private

      def validate_address!(address)
        return if address.is_a?(String) && address.match?(%r{\A[^\s/]+:\d+\z})

        raise ArgumentError, "address must be \"host:port\", got #{address.inspect}"
      end

      def validate_tls!(tls, cert_path, key_path)
        raise ArgumentError, "tls must be true or false" unless [true, false].include?(tls)
        raise ArgumentError, "tls_cert_path and tls_key_path must be set together" if cert_path.nil? != key_path.nil?
        raise ArgumentError, "tls_cert_path and tls_key_path need tls: true" if cert_path && !tls
      end

      def positive!(name, value)
        raise ArgumentError, "#{name} must be a positive number of seconds" unless value.is_a?(Numeric) && value.positive?
      end

      def read_pem(path, what)
        return unless path
        raise ArgumentError, "TLS #{what} file not found: #{path}" unless File.file?(path)

        File.read(path)
      end

      def service_descriptor(service)
        return service if service.is_a?(Google::Protobuf::ServiceDescriptor)

        name = service.respond_to?(:service_name) ? service.service_name : service.to_s
        descriptor = Google::Protobuf::DescriptorPool.generated_pool.lookup(name)
        return descriptor if descriptor.is_a?(Google::Protobuf::ServiceDescriptor)

        raise NotConfiguredError,
              "#{name} is not a known protobuf service. Require its generated _pb.rb file (protoc --ruby_out) first."
      end

      def request_message(path, input, request)
        case request
        when input then request
        when Hash then input.new(request)
        else raise TypeError, "#{path} expects a #{input} or Hash, got #{request.class}"
        end
      end

      def call_metadata(metadata)
        metadata.empty? ? @metadata : @metadata.merge(Metadata.encode(metadata))
      end

      # When the call must end, as a monotonic clock time: the call's own
      # deadline, or else the earlier of the client's default and the
      # deadline of the RPC being handled.
      def deadline_at(deadline)
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        case deadline
        when nil then [(now + @deadline if @deadline), self.class.current_deadline].compact.min
        when Numeric then now + deadline
        when Time then now + (deadline - Time.now)
        else raise ArgumentError, "deadline must be seconds (Numeric) or a Time, got #{deadline.class}"
        end
      end

      # Milliseconds left until `deadline_at`, or nil for no deadline.
      def remaining_ms(deadline_at)
        return unless deadline_at

        remaining = deadline_at - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise RpcError.new("deadline exceeded", code: :deadline_exceeded) unless remaining.positive?

        (remaining * 1000).ceil
      end

      def enumerable!(path, requests)
        return if requests.respond_to?(:each)

        raise TypeError, "#{path} streams its requests: pass an Enumerable of them, got #{requests.class}"
      end

      def streaming_call(path, input, metadata, deadline_at)
        native = @native.streaming(path, metadata, remaining_ms(deadline_at))
        StreamingCall.new(native) { |request| input.encode(request_message(path, input, request)) }
      end

      def single_response(output, responses, path)
        raise RpcError.new("server sent no response message", code: :internal) if responses.empty?
        raise RpcError.new("server sent more than one response message", code: :internal) if responses.size > 1

        decode_response(output, responses.first, path)
      end

      # Yield each message of a native stream, then return its result.
      def read_stream(native, output, path)
        while (bytes = native.next)
          yield decode_response(output, bytes, path)
        end
        native.result
      ensure
        native.close
      end

      def check_status!(code, message, trailers, details)
        return if code.zero?

        raise RpcError.new(message.empty? ? nil : message, code: code, metadata: trailers,
                                                           details: RpcError.decode_details(details))
      end

      # Wire-form [name, value] pairs to a Hash: `-bin` values decoded,
      # repeated names collected into an Array, and transport headers
      # (content-type, grpc-*) dropped.
      def decode_metadata(pairs)
        pairs.each_with_object({}) do |(name, value), metadata|
          next if name.start_with?("grpc-") || Metadata::RESERVED.include?(name)

          value = value.unpack1("m") if name.end_with?("-bin")
          metadata[name] = metadata.key?(name) ? [*metadata[name], value] : value
        end
      end

      def decode_response(output, bytes, path)
        output.decode(bytes)
      rescue Google::Protobuf::ParseError
        raise RpcError.new("invalid protobuf response from #{path}", code: :internal)
      end

      # Calls an RPC's methods.  Built per service from its descriptor; get
      # one with Client#stub.
      class Stub
        class << self
          # @api private
          attr_reader :service_name

          # @api private
          def for(descriptor)
            Class.new(self) do
              @service_name = descriptor.name
              descriptor.each { |method| define_rpc(method) }
            end
          end

          def inspect
            service_name ? "#{Stub.name}(#{service_name})" : super
          end

          private

          def define_rpc(method)
            path = "/#{service_name}/#{method.name}"
            name = ActiveSupport::Inflector.underscore(method.name)
            input = method.input_type.msgclass
            output = method.output_type.msgclass
            if method.client_streaming && method.server_streaming
              define_method(name) do |requests, deadline: nil, metadata: {}, retries: nil|
                @client.bidi_streaming(path, input, output, requests, deadline: deadline, metadata: metadata,
                                                                      retries: retries)
              end
            elsif method.client_streaming
              define_method(name) do |requests, deadline: nil, metadata: {}, return_call: false, retries: nil|
                @client.client_streaming(path, input, output, requests, deadline: deadline, metadata: metadata,
                                                                        return_call: return_call, retries: retries)
              end
            elsif method.server_streaming
              define_method(name) do |request = nil, deadline: nil, metadata: {}, retries: nil, **fields|
                request = rpc_request(name, request, fields)
                @client.server_streaming(path, input, output, request, deadline: deadline, metadata: metadata,
                                                                       retries: retries)
              end
            else
              # `get_item(id: 1)` works as well as `get_item({ id: 1 })`.
              define_method(name) do |request = nil, deadline: nil, metadata: {}, return_call: false, retries: nil,
                                      **fields|
                request = rpc_request(name, request, fields)
                @client.unary(path, input, output, request, deadline: deadline, metadata: metadata,
                                                            return_call: return_call, retries: retries)
              end
            end
          end
        end

        # @return [Tonic::Rails::Client]
        attr_reader :client

        def initialize(client)
          @client = client
        end

        def inspect
          "#<#{self.class.inspect} #{client.address}>"
        end

        private

        def rpc_request(name, request, fields)
          unless request.nil? || fields.empty?
            raise ArgumentError, "#{name}: pass the request or its fields, not both (#{fields.keys.join(', ')})"
          end

          request || fields
        end
      end

      # The responses of a server- or bidirectional-streaming call.  Nothing
      # is sent until it's read; then messages are yielded as they arrive, a
      # few at most buffered ahead of the reader.
      #
      #   items.list_items(category: "books").each { |item| ... }
      #   items.list_items(category: "books").first(10)
      #
      # Stopping early (`break`, `first(n)`, an exception) cancels the call.
      # An error from the server raises Tonic::Rails::RpcError after the
      # messages that came before it.  A stream can be read once.
      class ServerStream
        include Enumerable

        # @return [Hash, nil] response headers, once the stream has ended
        attr_reader :headers

        # @return [Hash, nil] trailing metadata, once the stream has ended
        attr_reader :trailers

        # @api private
        def initialize(path, &read)
          @path = path
          @read = read
        end

        # @yieldparam response [Google::Protobuf::AbstractMessage]
        # @return [self, Enumerator] an Enumerator without a block
        def each(&block)
          return enum_for(:each) unless block
          raise Error, "the #{@path} stream has already been read" unless @read

          read = @read
          @read = nil
          read.call(self, block)
          self
        end

        # @api private
        def finish(headers, trailers)
          @headers = headers
          @trailers = trailers
        end

        def inspect
          "#<#{self.class.name} #{@path}>"
        end
      end
    end
  end
end

require "tonic/rails/client/retry_policy"
require "tonic/rails/client/streaming_call"
