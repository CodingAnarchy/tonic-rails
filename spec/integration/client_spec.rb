# frozen_string_literal: true

require "timeout"
require_relative "../fixtures/lib/echo_handler"
require_relative "../support/integration_server"
require_relative "../support/test_certificates"

# Forwards Unary to another server, to show outgoing calls inherit the
# handler's deadline.
class ProxyEchoHandler
  include Tonic::Rails::Base

  rpc_service "echo.v1.EchoService"
  request_format :message

  class << self
    attr_accessor :upstream
  end

  def unary(request)
    self.class.upstream.stub("echo.v1.EchoService").unary(request)
  end
end

RSpec.describe Tonic::Rails::Client, :integration do
  before(:all) do
    skip "native extension not compiled (run `bundle exec rake compile`)" unless IntegrationServer.native_available?
    # Interrupted calls leave their handlers sleeping, so give it threads to spare.
    @handle = IntegrationServer.start { |config| config.dispatch_threads = 16 }
  end

  after(:all) { IntegrationServer.stop(@handle) }

  let(:address) { @handle[:address] }
  let(:client) { described_class.new(address, tls: false) }
  let(:stub) { client.stub("echo.v1.EchoService") }

  def expect_rpc_error(code, message = nil, &block)
    expect(&block).to raise_error(Tonic::Rails::RpcError) do |error|
      expect(error.code_name).to eq(code)
      expect(error.message).to match(message) if message
    end
  end

  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  describe "stubs" do
    it "makes unary calls with a message or a Hash" do
      expect(stub.unary(Echo::V1::EchoRequest.new(message: "message")))
        .to be_a(Echo::V1::EchoResponse).and have_attributes(message: "message")
      expect(stub.unary({ message: "hash" }).message).to eq("hash")
      expect(stub.unary(message: "fields", detail: { mood: :MOOD_HAPPY }).message).to eq("fields")
    end

    it "builds stubs from a service name, a descriptor or a grpc gem service class" do
      descriptor = Google::Protobuf::DescriptorPool.generated_pool.lookup("echo.v1.EchoService")
      service_class = Struct.new(:service_name).new("echo.v1.EchoService")
      [descriptor, service_class, :"echo.v1.EchoService"].each do |service|
        expect(client.stub(service).unary(message: "hi").message).to eq("hi")
      end
      expect(client.stub(descriptor)).to be(stub)
    end

    it "names methods in snake_case and refuses streaming RPCs" do
      expect(stub).to respond_to(:unary, :server_stream, :client_stream, :bidi)
      expect { stub.server_stream(count: 2) }.to raise_error(NotImplementedError, /ServerStream is a streaming RPC/)
    end

    it "rejects unknown services and the wrong request type" do
      expect { client.stub("echo.v1.Missing") }.to raise_error(Tonic::Rails::NotConfiguredError, /not a known protobuf service/)
      expect { stub.unary(Echo::V1::EchoResponse.new) }.to raise_error(TypeError, /expects a Echo::V1::EchoRequest/)
      expect { stub.unary({ message: "a" }, message: "b") }.to raise_error(ArgumentError, /not both/)
    end
  end

  describe "metadata" do
    it "sends request metadata, including the client's defaults and binary values" do
      client = described_class.new(address, tls: false, metadata: { "x-client" => "c" })
      response = client.stub("echo.v1.EchoService").unary({}, metadata: { "x-call" => "1", "x-trace-bin" => "\x00\x01".b })
      expect(response.metadata.to_h).to include("x-client" => "c", "x-call" => "1", "x-trace-bin" => "AAE=")
    end

    it "returns response headers and trailers with return_call" do
      call = stub.unary({ message: "with-metadata" }, return_call: true)
      expect(call.response.message).to eq("with-metadata")
      expect(call.headers).to include("x-echo-header" => "h1", "x-echo-bin" => "\x00\xFF".b)
      expect(call.headers.keys).not_to include("content-type")
      expect(call.trailers).to eq("x-echo-trailer" => "t1")
    end
  end

  describe "errors" do
    it "raises RpcError with the status code and message" do
      expect_rpc_error(:not_found, "failed with not_found") { stub.unary(fail_with: "rpc:not_found") }
      expect_rpc_error(:internal, "internal error") { stub.unary(fail_with: "crash") }
    end

    it "includes rich details and trailing metadata" do
      expect { stub.unary(fail_with: "details") }.to raise_error(Tonic::Rails::RpcError) do |error|
        expect(error.code_name).to eq(:invalid_argument)
        expect(error.metadata).to eq("x-error-id" => "e42")
        expect(error.details).to eq([Echo::V1::Detail.new(mood: :MOOD_HAPPY)])
      end
    end

    it "raises UNAVAILABLE when the server can't be reached" do
      client = described_class.new("127.0.0.1:#{IntegrationServer.free_port}", tls: false)
      expect_rpc_error(:unavailable, /connect/) { client.stub("echo.v1.EchoService").unary({}) }
    end

    it "enforces max_message_size on responses" do
      client = described_class.new(address, tls: false, max_message_size: 1024)
      expect { client.stub("echo.v1.EchoService").unary(pad_bytes: 2048) }.to raise_error(Tonic::Rails::RpcError)
      expect(client.stub("echo.v1.EchoService").unary(pad_bytes: 10).padding.bytesize).to eq(10)
    end

    it "can be re-raised by a handler" do
      error = begin
        stub.unary(fail_with: "details")
      rescue Tonic::Rails::RpcError => e
        e
      end
      expect(error.trailers).to include("x-error-id" => "e42", "grpc-status-details-bin" => a_kind_of(String))
    end
  end

  describe "deadlines" do
    it "sends the call's deadline and raises DEADLINE_EXCEEDED when it passes" do
      expect(stub.unary({}, deadline: 5).deadline_ms).to be_between(4_000, 5_000)
      expect(stub.unary({}, deadline: Time.now + 5).deadline_ms).to be_between(4_000, 5_000)

      started = monotonic
      expect_rpc_error(:deadline_exceeded) { stub.unary({ sleep_ms: 2_000 }, deadline: 0.2) }
      expect(monotonic - started).to be < 1
      expect_rpc_error(:deadline_exceeded) { stub.unary({}, deadline: Time.now - 1) }
    end

    it "uses the client's default deadline" do
      client = described_class.new(address, tls: false, deadline: 3)
      expect(client.stub("echo.v1.EchoService").unary({}).deadline_ms).to be_between(2_000, 3_000)
      expect(client.stub("echo.v1.EchoService").unary({}, deadline: 10).deadline_ms).to be > 3_000
      expect(stub.unary({}).deadline_ms).to eq(-1)
    end

    it "defaults to the deadline of the RPC being handled" do
      ProxyEchoHandler.upstream = described_class.new(address, tls: false, deadline: 30)
      proxy = IntegrationServer.start(services: [ProxyEchoHandler])
      proxied = described_class.new(proxy[:address], tls: false).stub("echo.v1.EchoService")

      expect(proxied.unary({}, deadline: 2).deadline_ms).to be_between(1_000, 2_000)
      expect(proxied.unary({}).deadline_ms).to be_between(29_000, 30_000)
      expect_rpc_error(:deadline_exceeded) { proxied.unary({ sleep_ms: 2_000 }, deadline: 0.3) }
    ensure
      IntegrationServer.stop(proxy)
    end
  end

  describe "the GVL" do
    it "lets other threads run during a call" do
      ticks = 0
      ticker = Thread.new do
        loop do
          ticks += 1
          sleep 0.01
        end
      end
      stub.unary(sleep_ms: 300)
      ticker.kill
      expect(ticks).to be > 10
    end

    it "is interrupted by Timeout and Thread#raise" do
      started = monotonic
      expect { Timeout.timeout(0.2) { stub.unary(sleep_ms: 5_000) } }.to raise_error(Timeout::Error)

      caller = Thread.new { stub.unary(sleep_ms: 5_000) }
      sleep 0.2
      caller.raise(ArgumentError, "stop")
      expect { caller.join }.to raise_error(ArgumentError, "stop")
      expect(monotonic - started).to be < 2

      expect(stub.unary(message: "still works").message).to eq("still works")
    end
  end

  describe "fork" do
    before { skip "fork() is not available" unless Process.respond_to?(:fork) }

    it "keeps working in the parent and a forked child, with no hooks" do
      expect(stub.unary(message: "before fork").message).to eq("before fork")
      unused = described_class.new(address, tls: false)
      unused.stub("echo.v1.EchoService").unary({})

      reader, writer = IO.pipe
      pid = fork do
        reader.close
        unused = nil
        GC.start # frees a client connected in the parent
        messages = Array.new(3) { |i| stub.unary(message: "child #{i}").message }
        writer.write(messages.join(","))
        writer.close
        exit!(0)
      rescue Exception => e # rubocop:disable Lint/RescueException
        writer.write("#{e.class}: #{e.message}")
        exit!(1)
      end
      writer.close

      expect(stub.unary(message: "during fork").message).to eq("during fork")
      _, status = Process.wait2(pid)
      expect(reader.read).to eq("child 0,child 1,child 2")
      expect(status).to be_success
      expect(stub.unary(message: "after fork").message).to eq("after fork")
    ensure
      reader&.close
    end
  end

  describe "TLS" do
    before(:all) do
      @certs = TestCertificates.new
      @tls = IntegrationServer.start do |config|
        config.tls_cert_path = @certs.path("server.pem")
        config.tls_key_path = @certs.path("server.key")
      end
      @mtls = IntegrationServer.start do |config|
        config.tls_cert_path = @certs.path("server.pem")
        config.tls_key_path = @certs.path("server.key")
        config.tls_client_ca_path = @certs.path("ca.pem")
      end
    end

    after(:all) do
      IntegrationServer.stop(@tls)
      IntegrationServer.stop(@mtls)
      @certs&.cleanup
    end

    def call(handle, address: handle[:address], **options)
      described_class.new(address, deadline: 5, **options).stub("echo.v1.EchoService").unary(message: "whoami").message
    end

    it "trusts a custom CA" do
      expect(call(@tls, tls_ca_path: @certs.path("ca.pem"))).to eq("127.0.0.1")
      expect(call(@tls, address: @tls[:address].sub("127.0.0.1", "localhost"), tls_ca_path: @certs.path("ca.pem")))
        .to eq("127.0.0.1")
    end

    it "verifies the server against the system's root certificates by default" do
      expect_rpc_error(:unavailable) { call(@tls) }
    end

    it "checks the server name" do
      expect(call(@tls, tls_ca_path: @certs.path("ca.pem"), tls_server_name: "localhost")).to eq("127.0.0.1")
      expect_rpc_error(:unavailable) { call(@tls, tls_ca_path: @certs.path("ca.pem"), tls_server_name: "example.com") }
    end

    it "sends a client certificate for mutual TLS" do
      identity = { tls_cert_path: @certs.path("client.pem"), tls_key_path: @certs.path("client.key") }
      expect(call(@mtls, tls_ca_path: @certs.path("ca.pem"), **identity)).to eq("127.0.0.1 /CN=client")
      expect_rpc_error(:unavailable) { call(@mtls, tls_ca_path: @certs.path("ca.pem")) }
    end

    it "uses TLS unless told otherwise" do
      expect_rpc_error(:unavailable) { call(@handle) }
      expect_rpc_error(:unavailable) { call(@tls, tls: false) }
    end

    it "rejects unusable TLS files when the client is created" do
      expect { described_class.new(address, tls_ca_path: @certs.path("missing.pem")) }
        .to raise_error(ArgumentError, /TLS CA file not found/)
      @certs.write("bad.key", "not a key")
      expect do
        described_class.new(address, tls_cert_path: @certs.path("client.pem"), tls_key_path: @certs.path("bad.key"))
      end.to raise_error(ArgumentError, /invalid TLS configuration/)
    end
  end

  describe "configuration" do
    it "validates its arguments" do
      expect { described_class.new("localhost", tls: false) }.to raise_error(ArgumentError, /host:port/)
      expect { described_class.new("http://localhost:1", tls: false) }.to raise_error(ArgumentError, /host:port/)
      expect { described_class.new(address, tls: nil) }.to raise_error(ArgumentError, /tls must be true or false/)
      expect { described_class.new(address, tls: false, deadline: 0) }.to raise_error(ArgumentError, /deadline/)
      expect { described_class.new(address, tls: false, max_message_size: 0) }.to raise_error(ArgumentError)
      expect { described_class.new(address, tls: false, tls_cert_path: "a") }.to raise_error(ArgumentError, /together/)
      expect { described_class.new(address, tls: false, metadata: { "grpc-x" => "1" }) }.to raise_error(ArgumentError, /reserved/)
    end
  end

  describe "instrumentation" do
    it "publishes request_rpc.tonic_rails for each call" do
      events = []
      subscription = ActiveSupport::Notifications.subscribe("request_rpc.tonic_rails") { |event| events << event }
      stub.unary({})
      expect { stub.unary(fail_with: "rpc:not_found") }.to raise_error(Tonic::Rails::RpcError)

      expect(events.map(&:payload)).to match(
        [
          a_hash_including(path: "/echo.v1.EchoService/Unary", service: "echo.v1.EchoService", rpc: "Unary",
                           address: address, code: 0, status: :ok),
          a_hash_including(code: 5, status: :not_found),
        ],
      )
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription)
    end
  end
end
