# frozen_string_literal: true

module Tonic
  module Rails
    # Raise from a handler to end the RPC with a gRPC status.
    #
    #   raise Tonic::Rails::RpcError.new("product not found", code: :not_found)
    #
    # The message is sent to the client, so keep internal details out of
    # it.  Any other exception becomes INTERNAL with a generic message
    # and is logged server-side.
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

      # @param message [String] status message sent to the client
      # @param code [Symbol, Integer] gRPC status code (see CODES)
      def initialize(message = nil, code: :unknown)
        @code = self.class.normalize_code(code)
        super(message || code_name.to_s)
      end

      # @return [Symbol] e.g. :not_found
      def code_name
        CODE_NAMES.fetch(@code)
      end

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
