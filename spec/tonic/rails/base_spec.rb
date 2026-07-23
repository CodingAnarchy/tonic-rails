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

  describe "class name independence" do
    it "routes by service declaration, not Ruby class name" do
      # A handler class named "BarController" serves "foo.FooService".
      # Routing is by the `service` declaration + router path, not by
      # the Ruby class name. End-to-end proof also exists in the
      # integration test for the underscore-named service (UnderscoreService
      # serving test.My_Service via grpcurl).
      BarController = Class.new do
        include Tonic::Rails::Base
        service "foo.FooService"
        rpc :DoThing, String, String
        def do_thing(request)
          {"result" => "done by BarController"}
        end
      end

      router = Tonic::Rails::Router.new
      BarController.register_on(router)

      request_class = Class.new { def self.decode(b); {}; end }
      response_class = Class.new do
        def self.new(h); h; end
        def self.encode(m); m; end
      end
      BarController.grpc_rpcs["DoThing"][:request] = request_class
      BarController.grpc_rpcs["DoThing"][:response] = response_class

      result = router.dispatch("/foo.FooService/DoThing", "")
      expect(result).to eq({"result" => "done by BarController"})
    ensure
      Object.send(:remove_const, :BarController) if defined?(BarController)
    end
  end
  describe ".register_on" do
    it "raises if no service name is set" do
      klass = Class.new { include Tonic::Rails::Base }
      expect { klass.register_on(Tonic::Rails::Router.new) }
        .to raise_error(Tonic::Rails::NotConfiguredError)
    end
  end

  describe ".rpc_service" do
    let(:proto_path) { File.expand_path("../../../bench/shop.proto", __dir__) }

    it "auto-registers all RPCs from the proto file" do
      require_relative "../../../bench/shop_pb"
      file = proto_path

      svc = Class.new do
        include Tonic::Rails::Base
        rpc_service "shop.ProductService", proto_file: file
      end

      expect(svc.grpc_service_name).to eq("shop.ProductService")
      expect(svc.grpc_rpcs.keys).to include("GetProduct", "ListProducts", "HealthCheck")
      expect(svc.grpc_rpcs["GetProduct"][:request]).to eq(Shop::GetProductRequest)
      expect(svc.grpc_rpcs["GetProduct"][:response]).to eq(Shop::Product)
    end

    it "detects server-streaming RPCs from the proto" do
      require_relative "../../../bench/shop_pb"
      file = proto_path

      svc = Class.new do
        include Tonic::Rails::Base
        rpc_service "shop.ProductService", proto_file: file
      end

      # shop.proto has no streaming RPCs, so all should be unary
      svc.grpc_rpcs.each_value do |spec|
        expect(spec[:stream]).to be(false)
      end
    end

    it "raises when the service is not found in the proto file" do
      file = proto_path

      expect {
        Class.new do
          include Tonic::Rails::Base
          rpc_service "shop.Nonexistent", proto_file: file
        end
      }.to raise_error(Tonic::Rails::Error, /not found/)
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

  describe "server-streaming RPCs" do
    let(:streaming_service) do
      Class.new do
        include Tonic::Rails::Base

        service "test.Stream"

        rpc :ListItems, String, String, stream: :server

        def list_items(request)
          [
            {"id" => "1", "name" => request["name"]},
            {"id" => "2", "name" => request["name"]},
            {"id" => "3", "name" => request["name"]},
          ]
        end
      end
    end

    it "registers stream: true in the RPC spec when stream: :server" do
      spec = streaming_service.grpc_rpcs["ListItems"]
      expect(spec[:stream]).to be(true)
    end

    it "registers stream: false by default (unary)" do
      spec = service_class.grpc_rpcs["SayHello"]
      expect(spec[:stream]).to be(false)
    end

    it "returns an Array of encoded responses for streaming RPCs" do
      request_class = Class.new do
        def self.decode(bytes); {name: bytes}; end
      end
      response_class = Class.new do
        def self.new(hash); hash; end
        def self.encode(msg); "encoded:#{msg['id']}"; end
      end

      streaming_service.grpc_rpcs["ListItems"][:request] = request_class
      streaming_service.grpc_rpcs["ListItems"][:response] = response_class

      instance = streaming_service.new
      result = instance.call_rpc("ListItems", "Widget")
      expect(result).to be_an(Array)
      expect(result.length).to eq(3)
      expect(result).to eq(["encoded:1", "encoded:2", "encoded:3"])
    end

    it "returns an empty Array for a streaming handler that yields nothing" do
      empty_service = Class.new do
        include Tonic::Rails::Base
        service "test.EmptyStream"
        rpc :Empty, String, String, stream: :server
        def empty(request); []; end
      end

      request_class = Class.new { def self.decode(b); {}; end }
      response_class = Class.new do
        def self.new(h); h; end
        def self.encode(m); "x"; end
      end

      empty_service.grpc_rpcs["Empty"][:request] = request_class
      empty_service.grpc_rpcs["Empty"][:response] = response_class

      result = empty_service.new.call_rpc("Empty", "")
      expect(result).to eq([])
    end

    it "raises RpcError when a streaming handler returns a non-Enumerable" do
      bad_service = Class.new do
        include Tonic::Rails::Base
        service "test.BadStream"
        rpc :Bad, String, String, stream: :server
        def bad(request); "not an array"; end
      end

      request_class = Class.new { def self.decode(b); {}; end }
      response_class = Class.new do
        def self.new(h); h; end
        def self.encode(m); m; end
      end

      bad_service.grpc_rpcs["Bad"][:request] = request_class
      bad_service.grpc_rpcs["Bad"][:response] = response_class

      expect { bad_service.new.call_rpc("Bad", "") }
        .to raise_error(Tonic::Rails::RpcError, /streaming handler must return an Enumerable/)
    end

    it "raises RpcError when a unary handler returns a non-Hash" do
      bad_service = Class.new do
        include Tonic::Rails::Base
        service "test.BadUnary"
        rpc :Bad, String, String
        def bad(request); ["not", "a", "hash"]; end
      end

      request_class = Class.new { def self.decode(b); {}; end }
      response_class = Class.new do
        def self.new(h); h; end
        def self.encode(m); m; end
      end

      bad_service.grpc_rpcs["Bad"][:request] = request_class
      bad_service.grpc_rpcs["Bad"][:response] = response_class

      expect { bad_service.new.call_rpc("Bad", "") }
        .to raise_error(Tonic::Rails::RpcError, /unary handler must return a Hash/)
    end

    it "supports Enumerator return for lazy streaming" do
      enum_service = Class.new do
        include Tonic::Rails::Base
        service "test.EnumStream"
        rpc :Gen, String, String, stream: :server
        def gen(request)
          Enumerator.new do |y|
            3.times { |i| y << {"id" => i.to_s} }
          end
        end
      end

      request_class = Class.new { def self.decode(b); {}; end }
      response_class = Class.new do
        def self.new(h); h; end
        def self.encode(m); "enc:#{m['id']}"; end
      end

      enum_service.grpc_rpcs["Gen"][:request] = request_class
      enum_service.grpc_rpcs["Gen"][:response] = response_class

      result = enum_service.new.call_rpc("Gen", "")
      expect(result).to eq(["enc:0", "enc:1", "enc:2"])
    end
  end

  describe "#deadline_ms" do
    it "returns nil when no deadline is set" do
      instance = service_class.new
      instance.instance_variable_set(:@grpc_deadline_at, nil)
      expect(instance.deadline_ms).to be_nil
    end

    it "returns remaining milliseconds when a deadline is set" do
      instance = service_class.new
      future = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5.0
      instance.instance_variable_set(:@grpc_deadline_at, future)
      remaining = instance.deadline_ms
      expect(remaining).to be_between(4500, 5000).inclusive
    end

    it "returns 0 when the deadline has passed" do
      instance = service_class.new
      past = Process.clock_gettime(Process::CLOCK_MONOTONIC) - 1.0
      instance.instance_variable_set(:@grpc_deadline_at, past)
      expect(instance.deadline_ms).to eq(0)
    end

    it "counts down as time elapses" do
      instance = service_class.new
      future = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 0.1
      instance.instance_variable_set(:@grpc_deadline_at, future)
      first = instance.deadline_ms
      sleep 0.05
      second = instance.deadline_ms
      expect(second).to be < first
    end
  end

  describe "#cancelled?" do
    it "returns false when server is not shutting down" do
      Tonic::Rails._clear_shutting_down
      instance = service_class.new
      expect(instance.cancelled?).to be(false)
    end

    it "returns true after _set_shutting_down is called" do
      Tonic::Rails._set_shutting_down
      instance = service_class.new
      expect(instance.cancelled?).to be(true)
    end

    after do
      Tonic::Rails._clear_shutting_down
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
