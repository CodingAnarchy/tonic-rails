# frozen_string_literal: true

require_relative "echo_pb"

# Handler for echo.v1.EchoService used across the specs.  Requests are
# protobuf messages; responses mix messages and Hashes to cover both.
class EchoHandler
  include Tonic::Rails::Base

  rpc_service "echo.v1.EchoService"
  request_format :message

  def unary(request)
    add_metadata(request)
    work(request)
    response(request)
  end

  def server_stream(request)
    add_metadata(request)
    work(request)
    Enumerator.new do |y|
      request.count.times { |i| y << response(request, index: i) }
    end
  end

  def client_stream(requests)
    messages = requests.map do |request|
      add_metadata(request)
      work(request)
      request.message
    end
    { "message" => messages.join(","), "index" => messages.size }
  end

  def bidi(requests)
    Enumerator.new do |y|
      requests.each_with_index do |request, i|
        add_metadata(request)
        work(request)
        y << response(request, index: i)
      end
    end
  end

  private

  # message "with-metadata" sets response headers and trailers.
  def add_metadata(request)
    return unless request.message == "with-metadata"

    response_metadata["x-echo-header"] = "h1"
    response_metadata["x-echo-bin"] = "\x00\xFF".b
    trailing_metadata["x-echo-trailer"] = "t1"
  end

  def whoami
    [peer.ip, peer.certificate&.subject&.to_s].compact.join(" ")
  end

  def work(request)
    sleep(request.sleep_ms / 1000.0) if request.sleep_ms.positive?
    case request.fail_with
    when ""
      nil
    when "crash"
      raise "secret internal detail"
    when /\Arpc:(\w+)\z/
      raise Tonic::Rails::RpcError.new("failed with #{Regexp.last_match(1)}", code: Regexp.last_match(1).to_sym)
    when "details"
      raise Tonic::Rails::RpcError.new(
        "invalid detail",
        code: :invalid_argument,
        metadata: { "x-error-id" => "e42" },
        details: [Echo::V1::Detail.new(mood: :MOOD_HAPPY)],
      )
    end
  end

  def response(request, index: 0)
    Echo::V1::EchoResponse.new(
      message: request.message == "whoami" ? whoami : request.message,
      index: index,
      metadata: grpc_metadata.reject { |k, _| k.start_with?(":") },
      deadline_ms: deadline_ms || -1,
      padding: "x" * request.pad_bytes,
    )
  end
end
