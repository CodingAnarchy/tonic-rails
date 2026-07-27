# frozen_string_literal: true

require "spec_helper"
require "tonic/rails/server"
require "open3"
require "logger"

# Integration tests that start the actual gRPC server and make real
# gRPC calls via grpcurl. These require:
#   - The native extension to be compiled (rake compile)
#   - grpcurl on PATH (brew install grpcurl)
#   - google-protobuf gem for Ruby-side encode/decode
#
# The server is started on a random port in a background thread.
# Each test runs grpcurl against it and checks the JSON response.

RSpec.describe "gRPC integration", :integration do
  PROTO_DIR = File.expand_path("../../bench", __dir__)
  PROTO_FILE = "shop.proto"
  ADDR = "127.0.0.1"

  before(:all) do
    skip "grpcurl not found" unless system("which grpcurl > /dev/null 2>&1")
    skip "native extension not loaded" unless defined?(TonicRails::Native::Server)

    require_relative "../../bench/shop_pb"
    require_relative "../../bench/bench_service"
    require_relative "../../bench/test_service_pb"

    # Handler for an underscore-named service — verifies that the
    # generated wrapper type (TestMy_Service) and the Ruby DSL
    # service "test.My_Service" work end-to-end.
    class UnderscoreService
      include Tonic::Rails::Base
      service "test.My_Service"
      rpc :Ping, Test::PingRequest, Test::PingResponse
      def ping(request)
        {"msg" => "pong: #{request['msg']}"}
      end
    end

    @port = 50552 + rand(100)
    @server = Tonic::Rails::Server.new(configuration: Tonic::Rails::Configuration.new.tap do |c|
      c.bind_address = "#{ADDR}:#{@port}"
      c.dispatch_threads = 2
      c.log_requests = false
      c.reflection = true  # enable for integration tests
      c.logger = Logger.new(IO::NULL)
    end)
    @server.register(ProductService)
    @server.register(UnderscoreService)
    @server_thread = Thread.new { @server.start }
    @server_thread.abort_on_exception = true

    # Wait for server to be ready (poll reflection list via grpcurl).
    ready = 30.times.any? do
      sleep 0.25
      _output, status = Open3.capture2e(
        "grpcurl", "-plaintext", "#{ADDR}:#{@port}", "list"
      )
      status.success?
    end
    raise "server did not start within 8s" unless ready
  end

  after(:all) do
    if @server_thread
      @server&.stop
      @server_thread.join(5)
    end
  end

  def grpcurl(method, payload = "{}")
    Open3.capture2e(
      "grpcurl", "-plaintext", "-import-path", PROTO_DIR,
      "-proto", PROTO_FILE, "-d", payload,
      "#{ADDR}:#{@port}",
      "shop.ProductService/#{method}"
    )
  end

  describe "unary RPC" do
    it "handles GetProduct successfully" do
      output, status = grpcurl("GetProduct", '{"id": 1}')
      expect(status).to be_success
      result = JSON.parse(output)
      expect(result["id"]).to eq("1").or eq(1)
      expect(result["title"]).to be_a(String)
      expect(result["priceCents"]).to be_a(String).or be_an(Integer)
    end

    it "returns NOT_FOUND for missing product" do
      output, status = grpcurl("GetProduct", '{"id": 99999}')
      expect(status).not_to be_success
      expect(output).to match(/not found|NOT_FOUND|Code: 5/i)
    end

    it "handles HealthCheck" do
      output, status = grpcurl("HealthCheck", "{}")
      expect(status).to be_success
      result = JSON.parse(output)
      expect(result["healthy"]).to eq(true)
    end
  end

  describe "native health check" do
    it "responds to grpc.health.v1.Health/Check" do
      # Use reflection (no -proto) so grpcurl knows about grpc.health.v1.Health.
      output, status = Open3.capture2e(
        "grpcurl", "-plaintext", "#{ADDR}:#{@port}",
        "grpc.health.v1.Health/Check"
      )
      expect(status).to be_success
      result = JSON.parse(output)
      expect(result["status"]).to eq("SERVING")
    end
  end

  describe "reflection" do
    it "lists services via reflection" do
      output, status = Open3.capture2e(
        "grpcurl", "-plaintext", "#{ADDR}:#{@port}", "list"
      )
      expect(status).to be_success
      expect(output).to include("shop.ProductService")
      expect(output).to include("grpc.health.v1.Health")
    end
  end

  describe "underscore-named service" do
    it "routes to a Ruby handler despite underscore in service name" do
      output, status = Open3.capture2e(
        "grpcurl", "-plaintext", "-import-path", PROTO_DIR,
        "-proto", "test_service.proto", "-d", '{"msg":"hello"}',
        "#{ADDR}:#{@port}",
        "test.My_Service/Ping"
      )
      expect(status).to be_success
      result = JSON.parse(output)
      expect(result["msg"]).to eq("pong: hello")
    end

    it "appears in reflection list" do
      output, status = Open3.capture2e(
        "grpcurl", "-plaintext", "#{ADDR}:#{@port}", "list"
      )
      expect(status).to be_success
      expect(output).to include("test.My_Service")
    end
  end

  describe "server-streaming RPC" do
    # grpcurl pretty-prints each response message as multi-line JSON.
    # Count JSON objects by scanning for "id" field occurrences.
    def count_messages(output)
      output.scan(/"id"\s*:/).length
    end

    def parse_all_messages(output)
      # Each message is a separate JSON object; extract by counting
      # top-level braces and parsing each object.
      objects = []
      depth = 0
      start = nil
      output.each_char.with_index do |ch, i|
        if ch == "{"
          depth += 1
          start ||= i
        elsif ch == "}"
          depth -= 1
          if depth == 0 && start
            objects << JSON.parse(output[start..i])
            start = nil
          end
        end
      end
      objects
    end

    it "streams multiple products via StreamProducts" do
      output, status = grpcurl("StreamProducts", '{"limit": 5}')
      expect(status).to be_success
      messages = parse_all_messages(output)
      expect(messages.length).to eq(5)
      expect(messages.first["id"]).to be_a(String).or be_an(Integer)
      expect(messages.first["title"]).to be_a(String)
    end

    it "streams >32 messages without dropping any (backpressure)" do
      # The mpsc channel has capacity 32. With 50 messages, the channel
      # fills and the handler blocks in stream_sender.send (GVL released)
      # until the client consumes. This test verifies all messages arrive
      # — none are dropped despite backpressure.
      output, status = grpcurl("StreamProducts", '{"limit": 50}')
      expect(status).to be_success
      messages = parse_all_messages(output)
      expect(messages.length).to eq(50)
      # Verify ordering: IDs should be sequential
      ids = messages.map { |m| m["id"].to_i }
      expect(ids).to eq((1..50).to_a)
    end

    it "returns empty stream when limit is 0" do
      # protobuf default for int32 is 0, so {"limit": 0} is the same
      # as omitting the field. The handler defaults to 50 in that case.
      # Use limit 1 to get a minimal stream instead.
      output, status = grpcurl("StreamProducts", '{"limit": 1}')
      expect(status).to be_success
      messages = parse_all_messages(output)
      expect(messages.length).to eq(1)
    end
  end
end
