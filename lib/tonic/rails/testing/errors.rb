# frozen_string_literal: true

module Tonic
  module Rails
    module Testing
      # Shared helpers for the RSpec matcher and Minitest assertions.
      # @api private
      module Errors
        module_function

        # Exceptions that carry a gRPC status: tonic-rails' own, and the
        # official grpc gem client's, when it's loaded.
        def classes
          [Tonic::Rails::RpcError, (GRPC::BadStatus if defined?(GRPC::BadStatus))].compact
        end

        def status?(error)
          classes.any? { |klass| error.is_a?(klass) }
        end

        # @return [Integer]
        def code(error)
          error.code
        end

        # @return [String] the status message the client sees.  The grpc
        # gem keeps it in #details (its #message adds the code).
        def message(error)
          text = defined?(GRPC::BadStatus) && error.is_a?(GRPC::BadStatus) ? error.details.to_s : error.message
          text.dup.force_encoding(Encoding::UTF_8)
        end

        def code_name(code)
          Tonic::Rails::RpcError::CODE_NAMES.fetch(code, code)
        end

        def message_matches?(expected, actual)
          expected.is_a?(Regexp) ? expected.match?(actual) : expected.to_s == actual
        end

        def describe(error)
          "#{code_name(code(error)).to_s.upcase} (#{message(error).inspect})"
        end
      end
    end
  end
end
