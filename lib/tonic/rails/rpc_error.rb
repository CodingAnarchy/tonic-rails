# frozen_string_literal: true

module Tonic
  module Rails
    # Raise from a handler (or interceptor) to end the RPC with a gRPC status.
    #
    #   raise Tonic::Rails::RpcError.new("item not found", code: :not_found)
    #
    # Optionally attach trailing metadata and rich error details (protobuf
    # messages such as google.rpc.BadRequest, sent in the standard
    # grpc-status-details-bin trailer as a google.rpc.Status):
    #
    #   raise Tonic::Rails::RpcError.new(
    #     "invalid item", code: :invalid_argument,
    #     metadata: { "x-request-id" => request_id },
    #     details: [Google::Rpc::BadRequest.new(field_violations: [...])],
    #   )
    #
    # The message, metadata and details go to the client, so keep internal
    # details out of them.  Any other exception becomes INTERNAL with a
    # generic message and is logged server-side.
    #
    # Tonic::Rails::Client raises it too, for calls that fail.
    class RpcError < Error
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

      CODE_NAMES = CODES.invert.freeze

      # @return [Integer] numeric gRPC status code
      attr_reader :code

      # @return [Hash] trailing metadata sent with the status
      attr_reader :metadata

      # @return [Array<Google::Protobuf::AbstractMessage>] rich error details
      attr_reader :details

      # @param message [String] status message sent to the client
      # @param code [Symbol, Integer] gRPC status code (see CODES)
      # @param metadata [Hash] trailing metadata (see Tonic::Rails::Metadata)
      # @param details [Array<Google::Protobuf::AbstractMessage>] messages
      #   packed into google.rpc.Status.details
      def initialize(message = nil, code: :unknown, metadata: {}, details: [])
        @code = self.class.normalize_code(code)
        @metadata = metadata
        @details = Array(details)
        super(message || code_name.to_s)
      end

      # @api private
      # @return [Hash{String=>String}] trailers to send with this status
      def trailers
        trailers = Metadata.encode(metadata)
        trailers["grpc-status-details-bin"] = [status_proto].pack("m0") unless details.empty?
        trailers
      end

      # The encoded google.rpc.Status (code = 1, message = 2, details = 3 as
      # google.protobuf.Any), built directly so googleapis protos aren't a
      # dependency.
      def status_proto
        require "google/protobuf/well_known_types"
        buf = (+"").b
        buf << field_key(1, 0) << varint(code)
        buf << length_delimited(2, message.b)
        details.each do |detail|
          any = detail.is_a?(Google::Protobuf::Any) ? detail : Google::Protobuf::Any.pack(detail)
          buf << length_delimited(3, Google::Protobuf::Any.encode(any))
        end
        buf
      end

      private

      def field_key(field, wire_type)
        varint((field << 3) | wire_type)
      end

      def length_delimited(field, bytes)
        field_key(field, 2) + varint(bytes.bytesize) + bytes
      end

      def varint(value)
        bytes = []
        loop do
          byte = value & 0x7f
          value >>= 7
          bytes << (value.zero? ? byte : byte | 0x80)
          break if value.zero?
        end
        bytes.pack("C*")
      end

      public

      # @return [Symbol] e.g. :not_found
      def code_name
        CODE_NAMES.fetch(@code)
      end

      # Unpacks the details of an encoded google.rpc.Status (from the
      # grpc-status-details-bin trailer).  Details whose message type is
      # loaded come back as that message; others stay Google::Protobuf::Any.
      # @api private
      # @return [Array<Google::Protobuf::AbstractMessage>]
      def self.decode_details(bytes)
        return [] if bytes.nil? || bytes.empty?

        require "google/protobuf/well_known_types"
        details = []
        each_length_delimited_field(bytes.b) do |field, value|
          details << unpack_detail(Google::Protobuf::Any.decode(value)) if field == 3
        end
        details
      rescue Google::Protobuf::ParseError, ArgumentError
        []
      end

      # Yields (field number, bytes) for each length-delimited field,
      # skipping varints; raises ArgumentError on anything malformed.
      def self.each_length_delimited_field(buf)
        pos = 0
        while pos < buf.bytesize
          key, pos = read_varint(buf, pos)
          case key & 7
          when 0 then _, pos = read_varint(buf, pos)
          when 2
            length, pos = read_varint(buf, pos)
            raise ArgumentError, "truncated google.rpc.Status" if pos + length > buf.bytesize

            yield key >> 3, buf.byteslice(pos, length)
            pos += length
          else raise ArgumentError, "unexpected wire type in google.rpc.Status"
          end
        end
      end
      private_class_method :each_length_delimited_field

      def self.read_varint(buf, pos)
        value = 0
        shift = 0
        loop do
          byte = buf.getbyte(pos) or raise ArgumentError, "truncated varint"
          pos += 1
          value |= (byte & 0x7f) << shift
          return [value, pos] if byte < 0x80

          shift += 7
          raise ArgumentError, "varint too long" if shift > 63
        end
      end
      private_class_method :read_varint

      def self.unpack_detail(any)
        type = Google::Protobuf::DescriptorPool.generated_pool.lookup(any.type_name)
        type.is_a?(Google::Protobuf::Descriptor) ? any.unpack(type.msgclass) : any
      end
      private_class_method :unpack_detail

      # @api private
      def self.normalize_code(code)
        case code
        when Symbol, String
          CODES.fetch(code.to_sym) { raise ArgumentError, "unknown gRPC status code: #{code.inspect}" }
        when Integer
          raise ArgumentError, "unknown gRPC status code: #{code}" unless CODE_NAMES.key?(code)

          code
        else
          raise ArgumentError, "gRPC status code must be a Symbol or Integer, got #{code.class}"
        end
      end
    end
  end
end
