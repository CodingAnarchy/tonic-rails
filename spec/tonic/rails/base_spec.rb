# frozen_string_literal: true

RSpec.describe Tonic::Rails::Base do
  let(:testing) { Tonic::Rails::Testing }
  let(:request_class) { Echo::V1::EchoRequest }
  let(:response_class) { Echo::V1::EchoResponse }

  def hash_service(&block)
    req = request_class
    res = response_class
    Class.new do
      include Tonic::Rails::Base

      service "echo.v1.EchoService"
      rpc :Unary, req, res
      rpc :ServerStream, req, res, stream: :server
      rpc :ClientStream, req, res, stream: :client
      rpc :Bidi, req, res, stream: :bidi

      def self.name = "HashEchoService"

      class_eval(&block) if block
    end
  end

  describe ".rpc" do
    it "records request/response classes, streaming flags and the handler name" do
      spec = hash_service.grpc_rpcs["ServerStream"]
      expect(spec).to include(
        request: request_class,
        response: response_class,
        request_stream: false,
        response_stream: true,
        handler: :server_stream,
      )
      expect(hash_service.grpc_rpcs["Bidi"]).to include(request_stream: true, response_stream: true)
    end

    it "rejects unknown stream kinds" do
      expect { hash_service.rpc(:X, request_class, response_class, stream: :sideways) }
        .to raise_error(ArgumentError, /stream must be/)
    end

    it "does not leak RPCs into sibling classes" do
      a = hash_service
      b = hash_service
      a.rpc(:Extra, request_class, response_class)
      expect(b.grpc_rpcs).not_to have_key("Extra")
    end
  end

  describe ".rpc_service" do
    it "declares every RPC from the descriptor pool" do
      expect(EchoHandler.grpc_service_name).to eq("echo.v1.EchoService")
      expect(EchoHandler.grpc_rpcs.keys).to contain_exactly("Unary", "ServerStream", "ClientStream", "Bidi")
      expect(EchoHandler.grpc_rpcs["ClientStream"]).to include(request_stream: true, response_stream: false)
      expect(EchoHandler.grpc_rpcs["Unary"]).to include(request: request_class, response: response_class)
    end

    it "raises a helpful error for unknown services" do
      expect { Class.new { include Tonic::Rails::Base }.rpc_service("nope.Missing") }
        .to raise_error(Tonic::Rails::NotConfiguredError, /not a known protobuf service/)
    end

    it "raises for names that are not services" do
      expect { Class.new { include Tonic::Rails::Base }.rpc_service("echo.v1.EchoRequest") }
        .to raise_error(Tonic::Rails::NotConfiguredError)
    end
  end

  describe ".request_format" do
    it "only accepts :hash or :message" do
      expect { hash_service.request_format(:json) }.to raise_error(ArgumentError)
    end
  end

  describe ".register_on" do
    it "requires a service name" do
      klass = Class.new { include Tonic::Rails::Base }
      expect { klass.register_on(Tonic::Rails::Router.new) }.to raise_error(Tonic::Rails::NotConfiguredError)
    end

    it "requires at least one RPC" do
      klass = Class.new do
        include Tonic::Rails::Base

        service "x.Y"
      end
      expect { klass.register_on(Tonic::Rails::Router.new) }.to raise_error(Tonic::Rails::NotConfiguredError, /no RPCs/)
    end
  end

  describe "unary RPCs" do
    it "passes a string-keyed Hash by default and accepts a Hash response" do
      svc = hash_service do
        def unary(request)
          { "message" => "#{request['message']}:#{request['detail']['mood']}" }
        end
      end
      response = testing.call(svc, :Unary, { message: "hi", detail: { mood: :MOOD_HAPPY } })
      expect(response).to eq(response_class.new(message: "hi:MOOD_HAPPY"))
    end

    it "passes the protobuf message with request_format :message" do
      response = testing.call(EchoHandler, :Unary, request_class.new(message: "hi"))
      expect(response.message).to eq("hi")
    end

    it "exposes metadata and the remaining deadline" do
      response = testing.call(EchoHandler, :Unary, { message: "x" }, metadata: { "x-user" => "7" }, timeout_ms: 5_000)
      expect(response.metadata.to_h).to eq("x-user" => "7")
      expect(response.deadline_ms).to be_between(4_000, 5_000)
    end

    it "reports no deadline when the client sets none" do
      expect(testing.call(EchoHandler, :Unary, { message: "x" }).deadline_ms).to eq(-1)
    end

    it "propagates RpcError with its code and message" do
      expect { testing.call(EchoHandler, :Unary, { fail_with: "rpc:not_found" }) }
        .to raise_error(Tonic::Rails::RpcError, "failed with not_found") { |e| expect(e.code).to eq(5) }
    end

    it "hides unexpected exceptions behind INTERNAL" do
      expect { testing.call(EchoHandler, :Unary, { fail_with: "crash" }) }
        .to raise_error(Tonic::Rails::RpcError, "internal error") { |e| expect(e.code_name).to eq(:internal) }
    end

    it "logs unexpected exceptions server-side" do
      logger = instance_spy(Logger)
      allow(Tonic::Rails.configuration).to receive(:logger).and_return(logger)
      expect { testing.call(EchoHandler, :Unary, { fail_with: "crash" }) }.to raise_error(Tonic::Rails::RpcError)
      expect(logger).to have_received(:error).with(/RuntimeError: secret internal detail/)
    end

    it "rejects responses of the wrong type as INTERNAL" do
      svc = hash_service do
        def unary(_request) = "not a message"
      end
      expect { testing.call(svc, :Unary, {}) }.to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code).to eq(13) }
    end

    it "returns UNIMPLEMENTED when the handler method is missing" do
      expect { testing.call(hash_service, :Unary, {}) }
        .to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code_name).to eq(:unimplemented) }
    end
  end

  describe "call_rpc wire handling" do
    let(:sender) { Tonic::Rails::Testing::Sender.new }

    def reader(*messages, error: nil)
      Tonic::Rails::Testing::Reader.new(messages, error: error)
    end

    it "maps undecodable request bytes to INVALID_ARGUMENT" do
      expect { EchoHandler.new.call_rpc("Unary", metadata: {}, reader: reader("\xFF\xFF\xFF".b)) }
        .to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code_name).to eq(:invalid_argument) }
    end

    it "requires a request message for unary RPCs" do
      expect { EchoHandler.new.call_rpc("Unary", metadata: {}, reader: reader) }
        .to raise_error(Tonic::Rails::RpcError, /missing request/)
    end

    it "surfaces transport errors reported by the reader" do
      expect { EchoHandler.new.call_rpc("Unary", metadata: {}, reader: reader(error: [8, "too big"])) }
        .to raise_error(Tonic::Rails::RpcError, "too big") { |e| expect(e.code).to eq(8) }
    end

    it "maps a closed response stream to CANCELLED" do
      closed = Object.new
      def closed.push(_bytes) = raise(IOError, "closed")
      bytes = request_class.encode(request_class.new(count: 3))
      expect { EchoHandler.new.call_rpc("ServerStream", metadata: {}, reader: reader(bytes), sender: closed) }
        .to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code_name).to eq(:cancelled) }
    end

    it "returns UNIMPLEMENTED for undeclared RPCs" do
      expect { EchoHandler.new.call_rpc("Nope", metadata: {}, reader: reader) }
        .to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code_name).to eq(:unimplemented) }
    end
  end

  describe "server streaming" do
    it "streams each element of the returned Enumerable" do
      responses = testing.call(EchoHandler, :ServerStream, { message: "m", count: 3 })
      expect(responses.map(&:index)).to eq([0, 1, 2])
      expect(responses.map(&:message).uniq).to eq(["m"])
    end

    it "supports empty streams" do
      expect(testing.call(EchoHandler, :ServerStream, { count: 0 })).to eq([])
    end

    it "accepts Hash elements" do
      svc = hash_service do
        def server_stream(request)
          Array.new(2) { |i| { "message" => request["message"], "index" => i } }
        end
      end
      expect(testing.call(svc, :ServerStream, { message: "h" }).map(&:index)).to eq([0, 1])
    end

    it "requires an Enumerable" do
      svc = hash_service do
        def server_stream(_request) = 42
      end
      expect { testing.call(svc, :ServerStream, {}) }
        .to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code_name).to eq(:internal) }
    end

    it "keeps messages already sent when the handler fails midway" do
      sender = Tonic::Rails::Testing::Sender.new
      svc = hash_service do
        def server_stream(_request)
          Enumerator.new do |y|
            y << { "index" => 1 }
            raise Tonic::Rails::RpcError.new("stop", code: :aborted)
          end
        end
      end
      reader = Tonic::Rails::Testing::Reader.new([request_class.encode(request_class.new)])
      expect { svc.new.call_rpc("ServerStream", metadata: {}, reader: reader, sender: sender) }
        .to raise_error(Tonic::Rails::RpcError, "stop")
      expect(sender.messages.size).to eq(1)
    end
  end

  describe "client streaming" do
    it "yields each request lazily and returns one response" do
      response = testing.call(EchoHandler, :ClientStream, [{ message: "a" }, { message: "b" }, { message: "c" }])
      expect(response.message).to eq("a,b,c")
      expect(response.index).to eq(3)
    end

    it "handles an empty request stream" do
      expect(testing.call(EchoHandler, :ClientStream, []).index).to eq(0)
    end

    it "raises transport errors from inside the enumeration" do
      reader = Tonic::Rails::Testing::Reader.new([request_class.encode(request_class.new(message: "a"))],
                                                 error: [13, "truncated request message"])
      expect { EchoHandler.new.call_rpc("ClientStream", metadata: {}, reader: reader) }
        .to raise_error(Tonic::Rails::RpcError, "truncated request message")
    end
  end

  describe "bidi streaming" do
    it "responds to each request" do
      responses = testing.call(EchoHandler, :Bidi, [{ message: "a" }, { message: "b" }])
      expect(responses.map { |r| [r.message, r.index] }).to eq([["a", 0], ["b", 1]])
    end
  end

  describe "response metadata" do
    it "sends response headers and trailers" do
      result = testing.invoke(EchoHandler, :Unary, { message: "with-metadata" })
      expect(result.response.message).to eq("with-metadata")
      expect(result.headers).to eq("x-echo-header" => "h1", "x-echo-bin" => "AP8=")
      expect(result.trailers).to eq("x-echo-trailer" => "t1")
    end

    it "sends headers before the first streamed message" do
      result = testing.invoke(EchoHandler, :ServerStream, { message: "with-metadata", count: 2 })
      expect(result.response.size).to eq(2)
      expect(result.headers).to include("x-echo-header" => "h1")
    end

    it "keeps trailers when the handler fails" do
      svc = hash_service do
        def unary(_request)
          trailing_metadata["x-attempt"] = "1"
          raise Tonic::Rails::RpcError.new("busy", code: :unavailable)
        end
      end
      sender = Tonic::Rails::Testing::Sender.new
      reader = Tonic::Rails::Testing::Reader.new([request_class.encode(request_class.new)])
      expect { svc.new.call_rpc("Unary", metadata: {}, reader: reader, sender: sender) }
        .to raise_error(Tonic::Rails::RpcError)
      expect(sender.trailers).to eq("x-attempt" => "1")
    end

    it "treats invalid metadata as a handler bug" do
      svc = hash_service do
        def unary(_request)
          response_metadata["grpc-status"] = "0"
          {}
        end
      end
      expect { testing.call(svc, :Unary, {}) }
        .to raise_error(Tonic::Rails::RpcError) { |e| expect(e.code_name).to eq(:internal) }
    end
  end

  describe "isolation" do
    it "uses a fresh handler instance per call" do
      svc = hash_service do
        def unary(_request)
          @calls = (@calls || 0) + 1 # rubocop:disable RSpec/InstanceVariable
          { "index" => @calls } # rubocop:disable RSpec/InstanceVariable
        end
      end
      2.times { expect(testing.call(svc, :Unary, {}).index).to eq(1) }
    end
  end

  describe "#cancelled?" do
    before { Tonic::Rails._set_shutting_down(false) }
    after { Tonic::Rails._set_shutting_down(false) }

    it "reflects server shutdown" do
      handler = EchoHandler.new
      expect(handler.cancelled?).to be(false)
      Tonic::Rails._set_shutting_down(true)
      expect(handler.cancelled?).to be(true)
    end
  end
end
