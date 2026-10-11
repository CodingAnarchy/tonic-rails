# frozen_string_literal: true

RSpec.describe Tonic::Rails::Router do
  subject(:router) { described_class.new }

  let(:call) do
    bytes = Echo::V1::EchoRequest.encode(Echo::V1::EchoRequest.new(message: "routed"))
    { reader: Tonic::Rails::Testing::Reader.new([bytes]), sender: nil }
  end

  it "routes every RPC of a service" do
    router.add(EchoHandler)
    expect(router.paths).to contain_exactly(
      "/echo.v1.EchoService/Unary", "/echo.v1.EchoService/ServerStream",
      "/echo.v1.EchoService/ClientStream", "/echo.v1.EchoService/Bidi"
    )
    expect(router.service_names).to eq(["echo.v1.EchoService"])
  end

  it "dispatches to a new handler instance" do
    router.add(EchoHandler)
    bytes = router.dispatch("/echo.v1.EchoService/Unary", {}, call)
    expect(Echo::V1::EchoResponse.decode(bytes).message).to eq("routed")
  end

  it "answers UNIMPLEMENTED for unknown paths" do
    expect { router.dispatch("/nope.Svc/Method", {}, call) }
      .to raise_grpc_error(:unimplemented)
  end

  it "replaces a service's routes when it is registered again" do
    first = Class.new do
      include Tonic::Rails::Base

      service "echo.v1.EchoService"
      rpc :Old, Echo::V1::EchoRequest, Echo::V1::EchoResponse
    end
    router.add(first)
    router.add(EchoHandler)
    expect(router.paths).not_to include("/echo.v1.EchoService/Old")
  end

  it "re-resolves handler classes by name when reloading" do
    stub_const("ReloadableEcho", Class.new(EchoHandler))
    router.add(ReloadableEcho)
    replacement = Class.new(EchoHandler) do
      def unary(_request) = { "message" => "reloaded" }
    end
    stub_const("ReloadableEcho", replacement)

    allow(Tonic::Rails.configuration).to receive(:reload_handlers).and_return(true)
    bytes = router.dispatch("/echo.v1.EchoService/Unary", {}, call)
    expect(Echo::V1::EchoResponse.decode(bytes).message).to eq("reloaded")
  end

  it "reports emptiness" do
    expect(router).to be_empty
    router.add(EchoHandler)
    expect(router).not_to be_empty
  end
end
