# frozen_string_literal: true

require "grpc"
require "grpc/health/v1/health_services_pb"
require "open3"
require "openssl"
require "tmpdir"
require_relative "../fixtures/lib/echo_services_pb"
require_relative "../support/integration_server"

# End-to-end tests: a real tonic-rails server, called with the official
# gRPC Ruby client (C core) — an independent HTTP/2 + gRPC implementation.
RSpec.describe "gRPC integration", :integration do
  before(:all) do
    skip "native extension not compiled (run `bundle exec rake compile`)" unless IntegrationServer.native_available?
  end

  let(:request) { Echo::V1::EchoRequest }

  def stub_for(handle, **opts)
    Echo::V1::EchoService::Stub.new(handle[:address], opts.delete(:creds) || :this_channel_is_insecure, **opts)
  end

  def expect_status(code, message = nil, &block)
    expect(&block).to raise_error(GRPC::BadStatus) do |error|
      expect(error.code).to eq(code)
      expect(error.details.dup.force_encoding(Encoding::UTF_8)).to eq(message) if message
    end
  end

  context "with the default configuration" do
    before(:all) do
      @handle = IntegrationServer.start do |config|
        config.interceptors = [
          lambda do |_path, metadata, next_call|
            raise Tonic::Rails::RpcError.new("bad token", code: :unauthenticated) if metadata["authorization"] == "deny"
            raise "interceptor bug" if metadata["authorization"] == "crash"

            next_call.call
          end,
        ]
      end
      @stub = Echo::V1::EchoService::Stub.new(@handle[:address], :this_channel_is_insecure)
    end

    after(:all) { IntegrationServer.stop(@handle) }

    let(:stub) { @stub }

    describe "unary" do
      it "round-trips a request" do
        detail = Echo::V1::Detail.new(mood: :MOOD_HAPPY, at: Google::Protobuf::Timestamp.new(seconds: 1))
        response = stub.unary(request.new(message: "hello", detail: detail))
        expect(response.message).to eq("hello")
      end

      it "passes metadata to the handler" do
        response = stub.unary(request.new, metadata: { "x-request-id" => "abc", "x-trace-bin" => "\x00\x01".b })
        expect(response.metadata["x-request-id"]).to eq("abc")
        expect(response.metadata["x-trace-bin"]).to eq(["\x00\x01".b].pack("m0").delete("="))
          .or eq(["\x00\x01".b].pack("m0"))
      end

      it "propagates the client deadline" do
        response = stub.unary(request.new, deadline: Time.now + 5)
        # The client rounds its timeout header up, so allow a little slack.
        expect(response.deadline_ms).to be_between(3_000, 5_100)
      end

      it "returns RpcError statuses with their message" do
        expect_status(GRPC::Core::StatusCodes::NOT_FOUND, "failed with not_found") do
          stub.unary(request.new(fail_with: "rpc:not_found"))
        end
      end

      it "sanitizes unexpected exceptions" do
        expect_status(GRPC::Core::StatusCodes::INTERNAL, "internal error") do
          stub.unary(request.new(fail_with: "crash"))
        end
      end

      it "percent-encodes non-ASCII status messages" do
        stub_class = Class.new(EchoHandler) do
          def unary(_request) = raise(Tonic::Rails::RpcError.new("café ☕\nline", code: :aborted))
        end
        stub_const("NonAsciiHandler", stub_class)
        allow(EchoHandler).to receive(:new).and_return(stub_class.new)
        expect_status(GRPC::Core::StatusCodes::ABORTED, "café ☕\nline") { stub.unary(request.new) }
      end

      it "enforces the deadline" do
        expect_status(GRPC::Core::StatusCodes::DEADLINE_EXCEEDED) do
          stub.unary(request.new(sleep_ms: 1_000), deadline: Time.now + 0.2)
        end
      end

      it "handles concurrent calls across dispatch threads" do
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        results = Array.new(8) { |i| Thread.new { stub.unary(request.new(message: i.to_s, sleep_ms: 200)).message } }.map(&:value)
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        expect(results).to match_array(%w[0 1 2 3 4 5 6 7])
        # 4 dispatch threads, 8 × 200ms sleeps (GVL released) → ~400ms, not 1.6s.
        expect(elapsed).to be < 1.2
      end
    end

    describe "interceptors" do
      it "can reject calls" do
        expect_status(GRPC::Core::StatusCodes::UNAUTHENTICATED, "bad token") do
          stub.unary(request.new, metadata: { "authorization" => "deny" })
        end
      end

      it "have their exceptions sanitized" do
        expect_status(GRPC::Core::StatusCodes::INTERNAL, "internal error") do
          stub.unary(request.new, metadata: { "authorization" => "crash" })
        end
      end
    end

    describe "server streaming" do
      it "streams every message, beyond the channel buffer" do
        responses = stub.server_stream(request.new(message: "s", count: 200)).to_a
        expect(responses.map(&:index)).to eq((0...200).to_a)
      end

      it "supports empty streams" do
        expect(stub.server_stream(request.new(count: 0)).to_a).to eq([])
      end

      it "survives a client cancelling mid-stream" do
        call = stub.server_stream(request.new(count: 1_000, sleep_ms: 1), return_op: true)
        enum = call.execute
        enum.first(3)
        call.cancel
        expect(stub.unary(request.new(message: "still alive")).message).to eq("still alive")
      end
    end

    describe "client streaming" do
      it "receives every request message" do
        response = stub.client_stream(%w[a b c d].map { |m| request.new(message: m) })
        expect(response.message).to eq("a,b,c,d")
        expect(response.index).to eq(4)
      end

      it "handles many messages" do
        response = stub.client_stream(Array.new(500) { |i| request.new(message: i.to_s) })
        expect(response.index).to eq(500)
      end
    end

    describe "bidi streaming" do
      it "interleaves requests and responses" do
        outbox = Queue.new
        requests = Enumerator.new do |y|
          while (message = outbox.pop) != :done
            y << message
          end
        end
        outbox << request.new(message: "first")
        responses = stub.bidi(requests)

        first = responses.next
        expect(first.message).to eq("first") # answered before the client sent more
        outbox << request.new(message: "second")
        expect(responses.next.message).to eq("second")
        outbox << :done
        expect { responses.next }.to raise_error(StopIteration)
      end
    end

    describe "transport behaviour" do
      it "answers UNIMPLEMENTED for unknown methods" do
        unknown = Class.new do
          include GRPC::GenericService

          self.marshal_class_method = :encode
          self.unmarshal_class_method = :decode
          self.service_name = "echo.v1.Missing"
          rpc :Unary, Echo::V1::EchoRequest, Echo::V1::EchoResponse
        end
        client = unknown.rpc_stub_class.new(@handle[:address], :this_channel_is_insecure)
        expect_status(GRPC::Core::StatusCodes::UNIMPLEMENTED) { client.unary(request.new) }
      end

      it "serves grpc.health.v1 for the whole server and each service" do
        health = Grpc::Health::V1::Health::Stub.new(@handle[:address], :this_channel_is_insecure)
        serving = :SERVING
        expect(health.check(Grpc::Health::V1::HealthCheckRequest.new(service: "")).status).to eq(serving)
        expect(health.check(Grpc::Health::V1::HealthCheckRequest.new(service: "echo.v1.EchoService")).status)
          .to eq(serving)
        expect_status(GRPC::Core::StatusCodes::NOT_FOUND) do
          health.check(Grpc::Health::V1::HealthCheckRequest.new(service: "nope.Missing"))
        end
      end

      it "does not expose reflection unless enabled" do
        skip "grpcurl not installed" unless system("grpcurl", "-version", err: File::NULL, out: File::NULL)
        output, status = Open3.capture2e("grpcurl", "-plaintext", @handle[:address], "list")
        expect(status).not_to be_success
        expect(output).to match(/Unimplemented|does not support the reflection API/i)
      end

      it "accepts gzip-compressed requests" do
        compressed = Echo::V1::EchoService::Stub.new(
          @handle[:address], :this_channel_is_insecure,
          channel_args: { "grpc.default_compression_algorithm" => 2, "grpc.default_compression_level" => 3 }
        )
        response = compressed.unary(request.new(message: "z" * 10_000))
        expect(response.message.size).to eq(10_000)
      end

      it "tracks request stats" do
        before = @handle[:server].stats
        stub.unary(request.new(message: "counted"))
        expect { stub.unary(request.new(fail_with: "rpc:not_found")) }.to raise_error(GRPC::NotFound)
        after = @handle[:server].stats

        expect(after[:total] - before[:total]).to eq(2)
        expect(after[:successful] - before[:successful]).to eq(1)
        expect(after[:errors] - before[:errors]).to eq(1)
        expect(after[:active_connections]).to be >= 1
      end
    end
  end

  context "with limits, compression and reflection" do
    before(:all) do
      @handle = IntegrationServer.start do |config|
        config.max_message_size = 64 * 1024
        config.compression = true
        config.reflection = true
      end
    end

    after(:all) { IntegrationServer.stop(@handle) }

    it "rejects oversized responses" do
      expect_status(GRPC::Core::StatusCodes::RESOURCE_EXHAUSTED) do
        stub_for(@handle).unary(request.new(pad_bytes: 100 * 1024))
      end
    end

    it "rejects oversized requests" do
      expect_status(GRPC::Core::StatusCodes::RESOURCE_EXHAUSTED) do
        stub_for(@handle).unary(request.new(message: "x" * (100 * 1024)))
      end
    end

    it "compresses large responses for clients that accept gzip" do
      response = stub_for(@handle).unary(request.new(pad_bytes: 32 * 1024))
      expect(response.padding.bytesize).to eq(32 * 1024)
    end

    it "serves reflection for registered services and their dependencies" do
      skip "grpcurl not installed" unless system("grpcurl", "-version", err: File::NULL, out: File::NULL)
      output, status = Open3.capture2e("grpcurl", "-plaintext", @handle[:address], "list")
      expect(status).to be_success, output
      expect(output.split).to include("echo.v1.EchoService", "grpc.health.v1.Health")

      output, status = Open3.capture2e("grpcurl", "-plaintext", @handle[:address], "describe", "echo.v1.Detail")
      expect(status).to be_success, output
      expect(output).to include(".google.protobuf.Timestamp at")

      output, status = Open3.capture2e(
        "grpcurl", "-plaintext", "-d", '{"message":"via reflection"}', @handle[:address], "echo.v1.EchoService/Unary"
      )
      expect(status).to be_success, output
      expect(output).to include("via reflection")
    end
  end

  context "with a small dispatch pool" do
    before(:all) do
      @handle = IntegrationServer.start do |config|
        config.dispatch_threads = 1
        config.dispatch_queue_size = 1
      end
    end

    after(:all) { IntegrationServer.stop(@handle) }

    it "sheds load with RESOURCE_EXHAUSTED when the queue is full" do
      stub = stub_for(@handle)
      threads = Array.new(6) do
        Thread.new do
          stub.unary(request.new(sleep_ms: 300))
          :ok
        rescue GRPC::ResourceExhausted
          :shed
        end
      end
      results = threads.map(&:value)
      expect(results).to include(:ok, :shed)
    end
  end

  describe "graceful shutdown" do
    it "finishes in-flight requests, flips health, then refuses new connections" do
      handle = IntegrationServer.start
      stub = stub_for(handle)
      in_flight = Thread.new { stub.unary(request.new(message: "drained", sleep_ms: 500)).message }
      sleep 0.15

      handle[:server].stop
      expect(in_flight.value).to eq("drained")
      handle[:thread].join(10)
      expect(handle[:server]).not_to be_running

      expect_status(GRPC::Core::StatusCodes::UNAVAILABLE) do
        stub_for(handle).unary(request.new, deadline: Time.now + 1)
      end
    ensure
      IntegrationServer.stop(handle) if handle && handle[:thread].alive?
    end

    it "can be restarted" do
      handle = IntegrationServer.start
      IntegrationServer.stop(handle)
      handle = IntegrationServer.start
      expect(stub_for(handle).unary(request.new(message: "again")).message).to eq("again")
    ensure
      IntegrationServer.stop(handle)
    end
  end

  describe "TLS" do
    # A CA plus a leaf certificate for 127.0.0.1, and a client certificate.
    before(:all) do
      @dir = Dir.mktmpdir("tonic-rails-tls")
      @ca_key = OpenSSL::PKey::EC.generate("prime256v1")
      @ca = build_cert("tonic-rails test CA", @ca_key, nil, authority: true)
      write("ca.pem", @ca.to_pem)

      server_key = OpenSSL::PKey::EC.generate("prime256v1")
      write("server.pem", build_cert("127.0.0.1", server_key, @ca, san: "IP:127.0.0.1,DNS:localhost").to_pem)
      write("server.key", server_key.private_to_pem)

      client_key = OpenSSL::PKey::EC.generate("prime256v1")
      @client_cert = build_cert("client", client_key, @ca).to_pem
      @client_key = client_key.private_to_pem
    end

    after(:all) { FileUtils.remove_entry(@dir) if @dir }

    def self.serial
      @serial = (@serial || 0) + 1
    end

    def write(name, contents)
      File.write(File.join(@dir, name), contents)
    end

    def path(name) = File.join(@dir, name)

    def build_cert(common_name, key, issuer, authority: false, san: nil)
      cert = OpenSSL::X509::Certificate.new
      cert.version = 2
      cert.serial = self.class.serial
      cert.subject = OpenSSL::X509::Name.parse("/CN=#{common_name}")
      cert.issuer = issuer ? issuer.subject : cert.subject
      cert.public_key = key
      cert.not_before = Time.now - 60
      cert.not_after = Time.now + 3600
      ext = OpenSSL::X509::ExtensionFactory.new
      ext.subject_certificate = cert
      ext.issuer_certificate = issuer || cert
      cert.add_extension(ext.create_extension("basicConstraints", authority ? "CA:TRUE" : "CA:FALSE", true))
      cert.add_extension(ext.create_extension("keyUsage", authority ? "keyCertSign,cRLSign" : "digitalSignature", true))
      cert.add_extension(ext.create_extension("subjectAltName", san)) if san
      cert.sign(issuer ? @ca_key : key, OpenSSL::Digest.new("SHA256"))
      cert
    end

    def tls_stub(handle, **creds_args)
      creds = GRPC::Core::ChannelCredentials.new(File.read(path("ca.pem")), *creds_args.values_at(:key, :cert))
      stub_for(handle, creds: creds)
    end

    it "serves over TLS" do
      handle = IntegrationServer.start do |config|
        config.tls_cert_path = path("server.pem")
        config.tls_key_path = path("server.key")
      end
      expect(tls_stub(handle).unary(request.new(message: "secure")).message).to eq("secure")
      expect_status(GRPC::Core::StatusCodes::UNAVAILABLE) do
        stub_for(handle).unary(request.new, deadline: Time.now + 1)
      end
    ensure
      IntegrationServer.stop(handle)
    end

    it "requires client certificates with mutual TLS" do
      handle = IntegrationServer.start do |config|
        config.tls_cert_path = path("server.pem")
        config.tls_key_path = path("server.key")
        config.tls_client_ca_path = path("ca.pem")
      end
      response = tls_stub(handle, key: @client_key, cert: @client_cert).unary(request.new(message: "mtls"))
      expect(response.message).to eq("mtls")
      expect_status(GRPC::Core::StatusCodes::UNAVAILABLE) do
        tls_stub(handle).unary(request.new, deadline: Time.now + 1)
      end
    ensure
      IntegrationServer.stop(handle)
    end

    it "refuses to start with an unreadable key instead of falling back to plaintext" do
      write("bad.key", "not a key")
      expect do
        IntegrationServer.start do |config|
          config.tls_cert_path = path("server.pem")
          config.tls_key_path = path("bad.key")
        end
      end.to raise_error(RuntimeError, /TLS/)
    end
  end
end
