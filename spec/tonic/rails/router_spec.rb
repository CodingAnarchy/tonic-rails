# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tonic::Rails::Router do
  let(:router) { described_class.new }

  let(:service_class) do
    Class.new do
      include Tonic::Rails::Base

      service "test.Greeter"

      rpc :SayHello, String, String

      def say_hello(request)
        {"message" => "Hello, #{request['name']}!"}
      end
    end
  end

  describe "#add and #dispatch" do
    before do
      service_class.register_on(router)
    end

    it "dispatches to the registered handler (raw-bytes mode)" do
      # Mock request class: decode returns a Hash (has to_h).
      request_class = Class.new do
        def self.decode(bytes); {name: bytes}; end
      end
      # Mock response class: new accepts a Hash, encode passes through.
      response_class = Class.new do
        def self.new(hash); hash; end
        def self.encode(msg); msg; end
      end

      service_class.grpc_rpcs["SayHello"][:request] = request_class
      service_class.grpc_rpcs["SayHello"][:response] = response_class

      result = router.dispatch("/test.Greeter/SayHello", "World")
      expect(result).to eq({"message" => "Hello, World!"})
    end

    it "raises when no handler is registered for the path" do
      expect {
        router.dispatch("/unknown.Service/Unknown", "")
      }.to raise_error(Tonic::Rails::Error, /No handler registered/)
    end
  end


  describe "#paths" do
    it "returns all registered paths" do
      klass = Class.new do
        include Tonic::Rails::Base
        service "test.Svc"
        rpc :MethodA, String, String
        rpc :MethodB, String, String
      end
      klass.register_on(router)

      expect(router.paths).to contain_exactly(
        "/test.Svc/MethodA",
        "/test.Svc/MethodB",
      )
    end
  end

  describe "#empty?" do
    it "returns true when no routes are registered" do
      expect(router).to be_empty
    end

    it "returns false when routes are registered" do
      klass = Class.new do
        include Tonic::Rails::Base
        service "test.Svc"
        rpc :Foo, String, String
      end
      klass.register_on(router)

      expect(router).not_to be_empty
    end
  end
end
