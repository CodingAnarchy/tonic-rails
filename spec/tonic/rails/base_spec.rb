# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tonic::Rails::Base do
  let(:service_class) do
    Class.new do
      include Tonic::Rails::Base

      service "helloworld.Greeter"

      rpc :SayHello, String, String

      def say_hello(request)
        {"message" => "Hello, #{request['name']}!"}
      end
    end
  end

  describe ".service" do
    it "sets the fully-qualified service name" do
      expect(service_class.grpc_service_name).to eq("helloworld.Greeter")
    end
  end

  describe ".rpc" do
    it "registers the method with request and response classes" do
      expect(service_class.grpc_rpcs).to include("SayHello")
      spec = service_class.grpc_rpcs["SayHello"]
      expect(spec[:request]).to eq(String)
      expect(spec[:response]).to eq(String)
    end
  end

  describe ".register_on" do
    it "raises if no service name is set" do
      klass = Class.new { include Tonic::Rails::Base }
      expect { klass.register_on(Tonic::Rails::Router.new) }
        .to raise_error(Tonic::Rails::NotConfiguredError)
    end
  end

  describe "#call_rpc" do
    it "decodes the request to a Hash, calls the method, and encodes the response" do
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

      instance = service_class.new
      result = instance.call_rpc("SayHello", "World")
      expect(result).to eq({"message" => "Hello, World!"})
    end

    it "raises RpcError for unknown methods" do
      instance = service_class.new
      expect { instance.call_rpc("Unknown", "") }
        .to raise_error(Tonic::Rails::Error, /Unknown RPC/)
    end
  end

end

RSpec.describe Tonic::Rails::RpcError do
  it "maps symbolic codes to numeric gRPC status codes" do
    expect(Tonic::Rails::RpcError.new("msg", code: :not_found).code).to eq(5)
    expect(Tonic::Rails::RpcError.new("msg", code: :internal).code).to eq(13)
    expect(Tonic::Rails::RpcError.new("msg", code: :unauthenticated).code).to eq(16)
  end

  it "defaults to UNKNOWN (2) for unrecognized codes" do
    expect(Tonic::Rails::RpcError.new("msg", code: :bogus).code).to eq(2)
  end
end
