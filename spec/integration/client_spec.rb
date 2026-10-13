# frozen_string_literal: true

require "logger"
require "stringio"
require "timeout"
require_relative "../fixtures/lib/counting_stream_handler"
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

    it "names methods in snake_case" do
      expect(stub).to respond_to(:unary, :server_stream, :client_stream, :bidi)
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

  describe "server streaming" do
    before(:all) { @counting = IntegrationServer.start(services: [CountingStreamHandler]) }
    after(:all) { IntegrationServer.stop(@counting) }

    before { CountingStreamHandler.ended.clear }

    let(:counting) { described_class.new(@counting[:address], tls: false).stub("echo.v1.EchoService") }

    def stream_ended = CountingStreamHandler.ended.pop(timeout: 5)

    it "yields each response" do
      stream = stub.server_stream(message: "hi", count: 3)
      expect(stream).to be_a(Tonic::Rails::Client::ServerStream)
      expect(stream.map { |response| [response.message, response.index] }).to eq([["hi", 0], ["hi", 1], ["hi", 2]])
      expect(stub.server_stream({ count: 0 }).to_a).to eq([])
      expect(stub.server_stream(count: 2).each).to be_a(Enumerator)
      expect(stub.server_stream({ count: 1 }, deadline: 5).first.deadline_ms).to be_between(4_000, 5_000)
    end

    it "yields responses as they arrive, not when the stream ends" do
      started = monotonic
      arrived = counting.server_stream(count: 3, sleep_ms: 300).map { monotonic - started }
      expect(arrived.first).to be < 0.25
      expect(arrived.last).to be > 0.5
      expect(stream_ended).to eq(:completed)
    end

    it "returns headers and trailers once the stream has ended" do
      stream = stub.server_stream(message: "with-metadata", count: 2)
      expect(stream.headers).to be_nil
      stream.to_a
      expect(stream.headers).to include("x-echo-header" => "h1", "x-echo-bin" => "\x00\xFF".b)
      expect(stream.trailers).to eq("x-echo-trailer" => "t1")
    end

    it "can be read once" do
      stream = stub.server_stream(count: 1)
      stream.to_a
      expect { stream.to_a }.to raise_error(Tonic::Rails::Error, /already been read/)
    end

    it "raises RpcError after the responses that came before it" do
      received = []
      expect_rpc_error(:aborted, "failed mid-stream") do
        counting.server_stream(count: 2, fail_with: "mid-stream").each { |response| received << response.index }
      end
      expect(received).to eq([0, 1])
      expect_rpc_error(:not_found) { stub.server_stream(fail_with: "rpc:not_found").to_a }
    end

    it "cancels the call on the server when the reader stops early" do
      expect(counting.server_stream.first(3).map(&:index)).to eq([0, 1, 2])
      expect(stream_ended).to eq(:cancelled)

      counting.server_stream.each { |response| break if response.index == 1 }
      expect(stream_ended).to eq(:cancelled)

      stop = proc { raise ArgumentError, "stop" }
      expect { counting.server_stream.each(&stop) }.to raise_error(ArgumentError, "stop")
      expect(stream_ended).to eq(:cancelled)
    end

    it "holds back a fast server while the reader is slow" do
      sent = nil
      counting.server_stream(pad_bytes: 64 * 1024).each do # rubocop:disable Lint/UnreachableLoop
        sleep 0.5
        sent = CountingStreamHandler.sent
        break
      end
      # Unbounded, the server would have sent thousands by now.
      expect(sent).to be < 200
      expect(stream_ended).to eq(:cancelled)
    end

    it "applies the deadline to the whole stream, including a slow reader" do
      received = []
      expect_rpc_error(:deadline_exceeded) do
        counting.server_stream({ sleep_ms: 100 }, deadline: 0.35).each { |response| received << response.index }
      end
      expect(received.size).to be_between(2, 5)

      # The stream has no end: only the deadline stops it.  Messages read
      # ahead before it passed are still yielded, so allow for those.
      started = monotonic
      expect_rpc_error(:deadline_exceeded) { counting.server_stream({}, deadline: 0.3).each { sleep 0.05 } }
      expect(monotonic - started).to be < 3
    end

    it "is interrupted by Timeout" do
      started = monotonic
      expect { Timeout.timeout(0.2) { counting.server_stream(sleep_ms: 5_000).to_a } }.to raise_error(Timeout::Error)
      expect(monotonic - started).to be < 1
    end
  end

  describe "client streaming" do
    before(:all) { @counting = IntegrationServer.start(services: [CountingStreamHandler]) }
    after(:all) { IntegrationServer.stop(@counting) }

    before { CountingStreamHandler.ended.clear }

    let(:counting) { described_class.new(@counting[:address], tls: false).stub("echo.v1.EchoService") }

    def stream_ended = CountingStreamHandler.ended.pop(timeout: 5)

    it "sends each request from any Enumerable and returns the response" do
      requests = [Echo::V1::EchoRequest.new(message: "a"), { message: "b" }]
      expect(stub.client_stream(requests)).to have_attributes(message: "a,b", index: 2)
      expect(stub.client_stream((1..3).lazy.map { |i| { message: i.to_s } }).message).to eq("1,2,3")
      expect(stub.client_stream([]).index).to eq(0)
      expect(stub.client_stream([{}], deadline: 5)).to be_a(Echo::V1::EchoResponse)
    end

    it "returns headers and trailers with return_call" do
      call = stub.client_stream([{ message: "with-metadata" }], return_call: true)
      expect(call.response.message).to eq("with-metadata")
      expect(call.headers).to include("x-echo-header" => "h1", "x-echo-bin" => "\x00\xFF".b)
      expect(call.trailers).to eq("x-echo-trailer" => "t1")
    end

    it "reads requests as it sends them, holding back while the server is slow" do
      yielded = 0
      endless = Enumerator.new do |y|
        loop do
          yielded += 1
          y << { message: "x" * 256 * 1024, sleep_ms: 10 }
        end
      end
      expect_rpc_error(:deadline_exceeded) { counting.client_stream(endless, deadline: 0.5) }
      # Unbounded, it would have read thousands by now.  HTTP/2 flow control
      # lets a few dozen through.  (Large messages keep the server's backlog
      # short: its handler reads what's buffered before it sees the cancel,
      # #57.)
      expect(yielded).to be < 100
      expect(stream_ended).to eq(:cancelled)
    end

    it "raises the server's error, and stops reading requests once the call has failed" do
      # Endless: the call has to stop reading it.
      requests = Enumerator.new do |y|
        y << { fail_with: "mid-stream" }
        loop { y << {} }
      end
      expect_rpc_error(:aborted, "failed mid-stream") { counting.client_stream(requests, deadline: 5) }
      expect_rpc_error(:not_found) { stub.client_stream([{ fail_with: "rpc:not_found" }]) }
    end

    it "cancels the call when the Enumerable raises, and re-raises its error" do
      requests = Enumerator.new do |y|
        y << {}
        sleep 0.2
        raise ArgumentError, "bad row"
      end
      expect { counting.client_stream(requests, deadline: 5) }.to raise_error(ArgumentError, "bad row")
      expect(stream_ended).to eq(:cancelled)
      expect { stub.client_stream([{}, Echo::V1::EchoResponse.new]) }.to raise_error(TypeError, /expects a Echo::V1::EchoRequest/)
    end

    it "rejects requests that aren't Enumerable" do
      expect { stub.client_stream(Echo::V1::EchoRequest.new) }.to raise_error(TypeError, /pass an Enumerable/)
    end
  end

  describe "bidirectional streaming" do
    before(:all) { @counting = IntegrationServer.start(services: [CountingStreamHandler]) }
    after(:all) { IntegrationServer.stop(@counting) }

    before { CountingStreamHandler.ended.clear }

    let(:counting) { described_class.new(@counting[:address], tls: false).stub("echo.v1.EchoService") }

    def stream_ended = CountingStreamHandler.ended.pop(timeout: 5)

    it "yields a response for each request" do
      stream = stub.bidi([{ message: "a" }, Echo::V1::EchoRequest.new(message: "b")])
      expect(stream).to be_a(Tonic::Rails::Client::ServerStream)
      expect(stream.map { |response| [response.message, response.index] }).to eq([["a", 0], ["b", 1]])
      expect { stream.to_a }.to raise_error(Tonic::Rails::Error, /already been read/)
      expect(stub.bidi([]).to_a).to eq([])
      expect(stub.bidi([{}]).each).to be_a(Enumerator)
    end

    it "sends requests while reading responses, so a request can depend on a response" do
      replies = Queue.new
      requests = Enumerator.new do |y|
        y << { message: "1" }
        while (reply = replies.pop) != :done
          y << { message: (reply.to_i + 1).to_s }
        end
      end
      messages = []
      stub.bidi(requests, deadline: 5).each do |response|
        messages << response.message
        replies << (messages.size < 3 ? response.message : :done)
      end
      expect(messages).to eq(%w[1 2 3])
    end

    it "returns headers and trailers once the stream has ended" do
      stream = stub.bidi([{ message: "with-metadata" }])
      stream.to_a
      expect(stream.headers).to include("x-echo-header" => "h1")
      expect(stream.trailers).to eq("x-echo-trailer" => "t1")
    end

    it "holds back both sides while the reader is slow" do
      yielded = 0
      endless = Enumerator.new do |y|
        loop do
          yielded += 1
          y << { message: "x" * 16 * 1024, pad_bytes: 64 * 1024 }
        end
      end
      counting.bidi(endless).each do # rubocop:disable Lint/UnreachableLoop
        sleep 0.5
        break
      end
      # Unbounded, both would be in the tens of thousands by now.
      expect(yielded).to be < 1000
      expect(CountingStreamHandler.received).to be < 1000
      expect(stream_ended).to eq(:cancelled)
    end

    it "cancels the call and stops reading requests when the reader stops early" do
      waiting = Queue.new
      requests = Enumerator.new do |y|
        y << {}
        waiting.pop # never answered
      end
      threads = Thread.list.size
      expect(counting.bidi(requests).first.index).to eq(0)
      expect(stream_ended).to eq(:cancelled)
      expect(Thread.list.size).to eq(threads)

      stop = proc { raise ArgumentError, "stop" }
      expect { counting.bidi(requests).each(&stop) }.to raise_error(ArgumentError, "stop")
      expect(stream_ended).to eq(:cancelled)
    end

    it "raises the Enumerable's error to the reader, cancelling the call" do
      requests = Enumerator.new do |y|
        y << {}
        sleep 0.2
        raise ArgumentError, "bad input"
      end
      received = []
      expect { counting.bidi(requests, deadline: 5).each { |response| received << response.index } }
        .to raise_error(ArgumentError, "bad input")
      expect(received).to eq([0])
      expect(stream_ended).to eq(:cancelled)
    end

    it "raises RpcError after the responses that came before it" do
      received = []
      expect_rpc_error(:aborted, "failed mid-stream") do
        counting.bidi([{}, {}, { fail_with: "mid-stream" }]).each { |response| received << response.index }
      end
      expect(received).to eq([0, 1])
    end

    it "applies the deadline to the whole call" do
      waiting = Queue.new
      requests = Enumerator.new do |y|
        y << {}
        waiting.pop
      end
      started = monotonic
      received = []
      expect_rpc_error(:deadline_exceeded) { counting.bidi(requests, deadline: 0.3).each { |r| received << r.index } }
      expect(received).to eq([0])
      expect(monotonic - started).to be < 3
    end
  end

  describe "retries" do
    let(:log) { StringIO.new }
    let(:logger) { Logger.new(log, formatter: ->(_, _, _, message) { "#{message}\n" }) }

    def refused = "127.0.0.1:#{IntegrationServer.free_port}"

    def attempts_of
      attempts = []
      subscription = ActiveSupport::Notifications.subscribe("request_rpc.tonic_rails") do |event|
        attempts << event.payload[:attempts]
      end
      yield
      attempts
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription)
    end

    def echo(address, **options)
      described_class.new(address, tls: false, logger: logger, **options).stub("echo.v1.EchoService")
    end

    it "is off unless asked for" do
      attempts = attempts_of { expect_rpc_error(:unavailable) { echo(refused).unary({}) } }
      expect(attempts).to eq([1])
      expect(log.string).to be_empty
    end

    it "retries a call that never reached the server, and logs each attempt" do
      stub = echo(refused, retries: { max_attempts: 3, initial_backoff: 0.01 })
      attempts = attempts_of { expect_rpc_error(:unavailable) { stub.unary({}) } }
      expect(attempts).to eq([3])
      expect(log.string.lines).to match(
        [
          %r{\Atonic-rails: /echo.v1.EchoService/Unary failed with UNAVAILABLE: .+ \(attempt 1 of 3\); retrying in \d+ms\n},
          /\(attempt 2 of 3\)/,
          %r{\Atonic-rails: /echo.v1.EchoService/Unary failed with UNAVAILABLE: .+ after 3 attempts\n},
        ],
      )
    end

    it "retries calls the server rejected before a handler ran" do
      busy = IntegrationServer.start do |config|
        config.dispatch_threads = 1
        config.dispatch_queue_size = 1
      end
      # One call running on the only dispatch thread, then one in the queue.
      blockers = [[1, 0], [2, 1]].map do |total, queued|
        thread = Thread.new { echo(busy[:address]).unary(sleep_ms: 500) }
        Timeout.timeout(5) { sleep 0.01 until busy[:server].stats.values_at(:total, :queued) == [total, queued] }
        thread
      end

      expect_rpc_error(:resource_exhausted, /dispatch queue full/) { echo(busy[:address]).unary({}) }
      # Waits of up to 2s each are all but certain to outlast the blockers.
      retrying = echo(busy[:address], retries: { codes: [:resource_exhausted], max_attempts: 5, initial_backoff: 2,
                                                 multiplier: 1, max_backoff: 2 })
      attempts = attempts_of { expect(retrying.unary(message: "made it").message).to eq("made it") }
      expect(attempts.first).to be > 1
      blockers.each(&:join)
    ensure
      IntegrationServer.stop(busy) if busy
    end

    it "never repeats a call that reached a handler" do
      stub = echo(address, retries: { max_attempts: 5, initial_backoff: 0.01 })
      attempts = attempts_of { expect_rpc_error(:unavailable) { stub.unary(fail_with: "rpc:unavailable") } }
      expect(attempts).to eq([1])
    end

    it "stops at the deadline instead of waiting to retry" do
      stub = echo(refused, retries: { max_attempts: 5, initial_backoff: 1, multiplier: 1, max_backoff: 1 })
      started = monotonic
      # The last attempt's failure: UNAVAILABLE when the port refused it,
      # DEADLINE_EXCEEDED when the deadline passed during the attempt (a
      # retry that started just before it, or Windows, which retries a
      # connection to a closed port for seconds).
      expect { stub.unary({}, deadline: 0.3) }.to raise_error(Tonic::Rails::RpcError) do |error|
        expect(error.code_name).to eq(:unavailable).or eq(:deadline_exceeded)
      end
      expect(monotonic - started).to be < 0.5
    end

    it "can be set or turned off per call" do
      stub = echo(refused, retries: { max_attempts: 3, initial_backoff: 0.01 })
      attempts = attempts_of do
        expect_rpc_error(:unavailable) { stub.unary({}, retries: false) }
        expect_rpc_error(:unavailable) { echo(refused).unary({}, retries: { max_attempts: 2, initial_backoff: 0.01 }) }
      end
      expect(attempts).to eq([1, 2])
    end

    it "can be set per service or method, the most specific setting winning" do
      quick = { max_attempts: 2, initial_backoff: 0.01 }
      client = described_class.new(
        refused, tls: false, logger: logger, retries: { max_attempts: 4, initial_backoff: 0.01 },
                 method_retries: { "echo.v1.EchoService" => quick, "/echo.v1.EchoService/Unary" => false,
                                   "echo.v1.EchoService/ClientStream" => true }
      )
      stub = client.stub("echo.v1.EchoService")
      expect(client.method_retries.keys).to eq(%w[echo.v1.EchoService echo.v1.EchoService/Unary
                                                  echo.v1.EchoService/ClientStream])

      attempts = attempts_of do
        expect_rpc_error(:unavailable) { stub.unary({}) }                           # its method: off
        expect_rpc_error(:unavailable) { stub.server_stream({}).to_a }              # its service: 2
        expect_rpc_error(:unavailable) { stub.unary({}, retries: quick) }           # the call: 2
        other = described_class.new(refused, tls: false, retries: quick, method_retries: { "other.Service" => false })
        expect_rpc_error(:unavailable) { other.stub("echo.v1.EchoService").unary({}) } # the client: 2
      end
      expect(attempts).to eq([1, 2, 2, 2])
    end

    it "validates method_retries" do
      expect { described_class.new(address, tls: false, method_retries: [["echo.v1.EchoService", true]]) }
        .to raise_error(ArgumentError, /must be a Hash/)
      expect { described_class.new(address, tls: false, method_retries: { "echo.v1.EchoService/Unary/x" => true }) }
        .to raise_error(ArgumentError, /package.Service/)
      expect { described_class.new(address, tls: false, method_retries: { "echo.v1.EchoService" => { tries: 2 } }) }
        .to raise_error(ArgumentError)
    end

    it "follows the server's grpc-retry-pushback-ms" do
      client = described_class.new(address, tls: false, retries: true)
      delay = lambda do |pushback, attempt: 1|
        trailers = pushback ? [["grpc-retry-pushback-ms", pushback]] : []
        client.send(:retry_delay, client.retries, attempt, [14, "", nil, [], trailers, nil, false], nil)
      end

      expect(delay.call("250")).to eq(0.25)
      expect(delay.call("-1")).to be_nil
      expect(delay.call("soon")).to be_nil
      expect(delay.call(nil)).to be_between(0, 0.1)
      expect(delay.call(nil, attempt: 3)).to be_nil
    end

    it "retries client and bidi streams that failed before reading a request" do
      stub = echo(refused, retries: { max_attempts: 2, initial_backoff: 0.01 })
      read = 0
      requests = Enumerator.new { |y| y << { message: (read += 1).to_s } }
      attempts = attempts_of do
        expect_rpc_error(:unavailable) { stub.client_stream(requests) }
        expect_rpc_error(:unavailable) { stub.bidi(requests).to_a }
      end
      expect(attempts).to eq([2, 2])
      expect(read).to eq(0)
      expect(echo(address, retries: true).client_stream(requests).message).to eq("1")
    end

    it "never repeats a stream once it has read a request" do
      stub = echo(address, retries: { max_attempts: 3, initial_backoff: 0.01, codes: [:aborted] })
      attempts = attempts_of do
        expect_rpc_error(:aborted) { stub.client_stream([{ fail_with: "rpc:aborted" }]) }
        expect_rpc_error(:aborted) { stub.bidi([{ fail_with: "rpc:aborted" }]).to_a }
      end
      expect(attempts).to eq([1, 1])
    end

    it "retries a server stream that failed before any message" do
      stub = echo(refused, retries: { max_attempts: 2, initial_backoff: 0.01 })
      attempts = attempts_of { expect_rpc_error(:unavailable) { stub.server_stream(count: 2).to_a } }
      expect(attempts).to eq([2])
      expect(echo(address, retries: true).server_stream(count: 2).map(&:index)).to eq([0, 1])
    end
  end

  describe "the GVL" do
    it "lets other threads run during a call" do
      # Holding the GVL for the call would leave this thread no turns at all.
      ticks = 0
      ticker = Thread.new { loop { ticks += 1 } }
      stub.unary(sleep_ms: 300)
      ticker.kill
      expect(ticks).to be > 1_000
    end

    it "is interrupted by Timeout and Thread#raise" do
      started = monotonic
      expect { Timeout.timeout(0.2) { stub.unary(sleep_ms: 5_000) } }.to raise_error(Timeout::Error)

      caller = Thread.new { stub.unary(sleep_ms: 5_000) }
      caller.report_on_exception = false
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
        messages += stub.server_stream(message: "child stream", count: 2).map(&:message)
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
      expect(reader.read).to eq("child 0,child 1,child 2,child stream,child stream")
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
      stub.server_stream(count: 2).to_a

      expect(events.map(&:payload)).to match(
        [
          a_hash_including(path: "/echo.v1.EchoService/Unary", service: "echo.v1.EchoService", rpc: "Unary",
                           address: address, code: 0, status: :ok, attempts: 1),
          a_hash_including(code: 5, status: :not_found),
          a_hash_including(path: "/echo.v1.EchoService/ServerStream", code: 0, status: :ok),
        ],
      )
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription)
    end
  end
end
