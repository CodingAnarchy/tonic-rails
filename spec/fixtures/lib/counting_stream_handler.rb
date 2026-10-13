# frozen_string_literal: true

require_relative "echo_pb"

# Records how many messages it sent and received, and how each call ended.
# Server streams send `count` responses (without end for 0), sleeping
# `sleep_ms` after each.  Client streams and bidi calls sleep the request's
# `sleep_ms` after reading it; bidi answers each request as it arrives.
class CountingStreamHandler
  include Tonic::Rails::Base

  rpc_service "echo.v1.EchoService"
  request_format :message

  class << self
    attr_accessor :sent, :received

    def ended = (@ended ||= Queue.new)
  end

  def server_stream(request)
    Enumerator.new do |y|
      track do
        self.class.sent = 0
        limit = request.count
        until limit.positive? && self.class.sent == limit
          y << Echo::V1::EchoResponse.new(index: self.class.sent, padding: "x" * request.pad_bytes)
          self.class.sent += 1
          pause(request)
        end
        raise Tonic::Rails::RpcError.new("failed mid-stream", code: :aborted) if request.fail_with == "mid-stream"
      end
    end
  end

  def client_stream(requests)
    track do
      self.class.received = 0
      requests.each do |request|
        self.class.received += 1
        fail_if_asked(request)
        pause(request)
      end
      Echo::V1::EchoResponse.new(index: self.class.received)
    end
  end

  def bidi(requests)
    Enumerator.new do |y|
      track do
        self.class.received = 0
        requests.each do |request|
          self.class.received += 1
          fail_if_asked(request)
          y << Echo::V1::EchoResponse.new(message: request.message, index: self.class.received - 1,
                                          padding: "x" * request.pad_bytes)
          pause(request)
        end
      end
    end
  end

  private

  def track
    outcome = :completed
    yield
  rescue Exception => e # rubocop:disable Lint/RescueException
    outcome = e.is_a?(Tonic::Rails::RpcError) ? e.code_name : e.class
    raise
  ensure
    self.class.ended << outcome
  end

  def pause(request)
    sleep(request.sleep_ms / 1000.0) if request.sleep_ms.positive?
  end

  def fail_if_asked(request)
    raise Tonic::Rails::RpcError.new("failed mid-stream", code: :aborted) if request.fail_with == "mid-stream"
  end
end
