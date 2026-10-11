# frozen_string_literal: true

require_relative "echo_pb"

# Handler for echo.v1.EchoService used across the specs.  Requests are
# protobuf messages; responses mix messages and Hashes to cover both.
class EchoHandler
  include Tonic::Rails::Base

  rpc_service "echo.v1.EchoService"
  request_format :message

  def unary(request)
    work(request)
    response(request)
  end

  def server_stream(request)
    work(request)
    Enumerator.new do |y|
      request.count.times { |i| y << response(request, index: i) }
    end
  end

  def client_stream(requests)
    messages = requests.map do |request|
      work(request)
      request.message
    end
    { "message" => messages.join(","), "index" => messages.size }
  end

  def bidi(requests)
    Enumerator.new do |y|
      requests.each_with_index do |request, i|
        work(request)
        y << response(request, index: i)
      end
    end
  end

  private

  def work(request)
    sleep(request.sleep_ms / 1000.0) if request.sleep_ms.positive?
    case request.fail_with
    when ""
      nil
    when "crash"
      raise "secret internal detail"
    when /\Arpc:(\w+)\z/
      raise Tonic::Rails::RpcError.new("failed with #{Regexp.last_match(1)}", code: Regexp.last_match(1).to_sym)
    end
  end

  def response(request, index: 0)
    Echo::V1::EchoResponse.new(
      message: request.message,
      index: index,
      metadata: grpc_metadata.reject { |k, _| k.start_with?(":") },
      deadline_ms: deadline_ms || -1,
      padding: "x" * request.pad_bytes,
    )
  end
end
