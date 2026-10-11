# frozen_string_literal: true

require "grpc"
require "minitest"
require "tonic/rails/testing/rspec"
require "tonic/rails/testing/minitest"

RSpec.describe "gRPC error test helpers" do # rubocop:disable RSpec/DescribeClass
  def not_found = raise(Tonic::Rails::RpcError.new("item 7 not found", code: :not_found))

  describe "raise_grpc_error" do
    it "matches by code, as a symbol or a number" do
      expect { not_found }.to raise_grpc_error(:not_found)
      expect { not_found }.to raise_grpc_error(5)
      expect { not_found }.to raise_grpc_error
    end

    it "matches messages by string or regexp" do
      expect { not_found }.to raise_grpc_error(:not_found).with_message("item 7 not found")
      expect { not_found }.to raise_grpc_error.with_message(/item \d+/)
      expect { not_found }.not_to raise_grpc_error(:not_found).with_message("other")
    end

    it "matches GRPC::BadStatus from the official client" do
      expect { raise GRPC::PermissionDenied, "nope" }.to raise_grpc_error(:permission_denied).with_message("nope")
    end

    it "fails with a clear message" do
      expect { expect { not_found }.to raise_grpc_error(:internal) }
        .to raise_error(RSpec::Expectations::ExpectationNotMetError,
                        /raise a gRPC error INTERNAL, but got NOT_FOUND \("item 7 not found"\)/)
      expect { expect { :ok }.to raise_grpc_error(:internal) }
        .to raise_error(RSpec::Expectations::ExpectationNotMetError, /but nothing was raised/)
    end

    it "lets other exceptions through" do
      expect { expect { raise ArgumentError }.to raise_grpc_error }.to raise_error(ArgumentError)
    end

    it "supports negation" do
      expect { :ok }.not_to raise_grpc_error
      expect { not_found }.not_to raise_grpc_error(:internal)
    end

    it "rejects unknown codes" do
      expect { expect { not_found }.to raise_grpc_error(:nope) }.to raise_error(ArgumentError)
    end
  end

  describe Tonic::Rails::Testing::Assertions do
    let(:test) do
      Class.new do
        include Minitest::Assertions
        include Tonic::Rails::Testing::Assertions

        attr_accessor :assertions

        def initialize = @assertions = 0
      end.new
    end

    it "returns the error when code and message match" do
      error = test.assert_grpc_error(:not_found, /item/) { not_found }
      expect(error.code).to eq(5)
    end

    it "fails on the wrong code or message" do
      expect { test.assert_grpc_error(:internal) { not_found } }
        .to raise_error(Minitest::Assertion, /Expected gRPC status INTERNAL, got NOT_FOUND/)
      expect { test.assert_grpc_error(:not_found, "other") { not_found } }
        .to raise_error(Minitest::Assertion, /Expected gRPC message "other"/)
    end

    it "fails when nothing is raised" do
      expect { test.assert_grpc_error(:not_found) { :ok } }.to raise_error(Minitest::Assertion)
    end
  end
end
