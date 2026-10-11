# frozen_string_literal: true

require "net/http"

# A minimal gRPC-Web client over HTTP/1.1, implementing the wire format
# browsers use (https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md):
# length-prefixed frames, with the status sent as a final "trailers" frame
# (flag 0x80) in the body, base64-encoded for the -text variant.
module GrpcWebClient
  Response = Struct.new(:http_status, :headers, :messages, :trailers) do
    def grpc_status = trailers["grpc-status"]&.to_i

    def grpc_message = trailers["grpc-message"]
  end

  module_function

  def call(address, path, message, text: false, origin: nil, headers: {}, ca_file: nil)
    content_type = text ? "application/grpc-web-text" : "application/grpc-web+proto"
    frame = [0, message.bytesize].pack("CN") + message
    request = Net::HTTP::Post.new(path)
    request["content-type"] = content_type
    request["accept"] = content_type
    request["x-grpc-web"] = "1"
    request["origin"] = origin if origin
    headers.each { |name, value| request[name] = value }
    request.body = text ? [frame].pack("m0") : frame

    response = http(address, ca_file: ca_file) { |connection| connection.request(request) }
    body = response.body.to_s.b
    body = decode_text(body) if text
    messages, trailers = parse_frames(body)
    Response.new(response.code.to_i, response.to_hash, messages, trailers)
  end

  # A CORS preflight request, as a browser sends before a cross-origin call.
  def preflight(address, path, origin:)
    request = Net::HTTP::Options.new(path)
    request["origin"] = origin
    request["access-control-request-method"] = "POST"
    request["access-control-request-headers"] = "content-type,x-grpc-web,x-user-agent"
    http(address) { |connection| connection.request(request) }
  end

  def http(address, ca_file: nil, &)
    host, port = address.split(":")
    options = { open_timeout: 2, read_timeout: 10 }
    options.merge!(use_ssl: true, ca_file: ca_file, verify_mode: OpenSSL::SSL::VERIFY_PEER) if ca_file
    Net::HTTP.start(host, Integer(port), **options, &)
  end

  # Text responses may be several base64 chunks back to back, each padded
  # separately; every chunk is a whole number of 4-character groups.
  def decode_text(body)
    body.delete("\r\n").scan(/.{4}/).map { |group| group.unpack1("m") }.join.b
  end

  def parse_frames(body)
    messages = []
    trailers = {}
    offset = 0
    while offset + 5 <= body.bytesize
      flag, length = body.byteslice(offset, 5).unpack("CN")
      payload = body.byteslice(offset + 5, length)
      offset += 5 + length
      if flag.anybits?(0x80)
        payload.split("\r\n").each do |line|
          name, value = line.split(":", 2)
          trailers[name.strip.downcase] = value.to_s.strip
        end
      else
        messages << payload
      end
    end
    [messages, trailers]
  end
end
