# frozen_string_literal: true

require_relative "echo_pb"

# Streams `count` responses (without end for 0), sleeping `sleep_ms` after
# each, and records how many it sent and how the stream ended.
class CountingStreamHandler
  include Tonic::Rails::Base

  rpc_service "echo.v1.EchoService"
  request_format :message

  class << self
    attr_accessor :sent

    def ended = (@ended ||= Queue.new)
  end

  def server_stream(request)
    Enumerator.new do |y|
      self.class.sent = 0
      outcome = :completed
      begin
        limit = request.count
        until limit.positive? && self.class.sent == limit
          y << Echo::V1::EchoResponse.new(index: self.class.sent, padding: "x" * request.pad_bytes)
          self.class.sent += 1
          sleep(request.sleep_ms / 1000.0) if request.sleep_ms.positive?
        end
        raise Tonic::Rails::RpcError.new("failed mid-stream", code: :aborted) if request.fail_with == "mid-stream"
      rescue Exception => e # rubocop:disable Lint/RescueException
        outcome = e.is_a?(Tonic::Rails::RpcError) ? e.code_name : e.class
        raise
      ensure
        self.class.ended << outcome
      end
    end
  end
end
