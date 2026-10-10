# frozen_string_literal: true

# gRPC client stub for echo.v1.EchoService (equivalent to the output of
# grpc_tools_ruby_protoc --grpc_out), used by the integration specs.

require "grpc"
require_relative "echo_pb"

module Echo
  module V1
    module EchoService
      class Service
        include ::GRPC::GenericService

        self.marshal_class_method = :encode
        self.unmarshal_class_method = :decode
        self.service_name = "echo.v1.EchoService"

        rpc :Unary, ::Echo::V1::EchoRequest, ::Echo::V1::EchoResponse
        rpc :ServerStream, ::Echo::V1::EchoRequest, stream(::Echo::V1::EchoResponse)
        rpc :ClientStream, stream(::Echo::V1::EchoRequest), ::Echo::V1::EchoResponse
        rpc :Bidi, stream(::Echo::V1::EchoRequest), stream(::Echo::V1::EchoResponse)
      end

      Stub = Service.rpc_stub_class
    end
  end
end
