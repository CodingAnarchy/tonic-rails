# frozen_string_literal: true

require "tonic/rails/testing"
require "tonic/rails/testing/errors"

module Tonic
  module Rails
    module Testing
      # Minitest assertions for gRPC statuses:
      #
      #   class ItemServiceTest < ActiveSupport::TestCase
      #     include Tonic::Rails::Testing::Assertions
      #
      #     test "missing items" do
      #       error = assert_grpc_error(:not_found) { Tonic::Rails::Testing.call(ItemService, :GetItem, { id: 0 }) }
      #       assert_match(/0/, error.message)
      #     end
      #   end
      module Assertions
        # Asserts the block raises a gRPC status (Tonic::Rails::RpcError or
        # GRPC::BadStatus) with the given code and, optionally, message
        # (String or Regexp).  Returns the error.
        def assert_grpc_error(code = nil, message = nil, &)
          errors = Testing::Errors
          error = assert_raises(*errors.classes, &)
          if code
            expected = Tonic::Rails::RpcError.normalize_code(code)
            assert_equal(
              errors.code_name(expected), errors.code_name(errors.code(error)),
              "Expected gRPC status #{errors.code_name(expected).to_s.upcase}, got #{errors.describe(error)}"
            )
          end
          if message
            assert(
              errors.message_matches?(message, errors.message(error)),
              "Expected gRPC message #{message.inspect}, got #{errors.message(error).inspect}",
            )
          end
          error
        end
      end
    end
  end
end
