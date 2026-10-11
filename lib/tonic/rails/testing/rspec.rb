# frozen_string_literal: true

require "rspec/expectations"
require "tonic/rails/testing"
require "tonic/rails/testing/errors"

# Matches a block that ends with a gRPC status, raised as a
# Tonic::Rails::RpcError (Testing.call) or a GRPC::BadStatus (the official
# client):
#
#   expect { Tonic::Rails::Testing.call(ItemService, :GetItem, { id: 0 }) }
#     .to raise_grpc_error(:not_found)
#   expect { stub.get_item(request) }.to raise_grpc_error(:invalid_argument).with_message(/name/)
#   expect { ... }.to raise_grpc_error   # any non-OK status
RSpec::Matchers.define :raise_grpc_error do |expected_code = nil|
  errors = Tonic::Rails::Testing::Errors

  supports_block_expectations

  chain(:with_message) { |message| @expected_message = message }

  match do |block|
    @expected = expected_code && Tonic::Rails::RpcError.normalize_code(expected_code)
    begin
      block.call
      @error = nil
    rescue Exception => e # rubocop:disable Lint/RescueException
      raise unless errors.status?(e)

      @error = e
    end
    return false unless @error

    (@expected.nil? || errors.code(@error) == @expected) &&
      (!defined?(@expected_message) || errors.message_matches?(@expected_message, errors.message(@error)))
  end

  description do
    text = +"raise a gRPC error"
    text << " #{errors.code_name(@expected).to_s.upcase}" if @expected
    text << " with message #{@expected_message.inspect}" if defined?(@expected_message)
    text
  end

  failure_message do
    actual = @error ? "got #{errors.describe(@error)}" : "nothing was raised"
    "expected block to #{description}, but #{actual}"
  end

  failure_message_when_negated do
    "expected block not to #{description}, but got #{errors.describe(@error)}"
  end
end
