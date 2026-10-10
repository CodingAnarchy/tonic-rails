# frozen_string_literal: true

RSpec.describe Tonic::Rails::Server do
  subject(:server) { described_class.new(configuration: config) }

  let(:config) { Tonic::Rails::Configuration.new.tap { |c| c.logger = Logger.new(IO::NULL) } }

  it "registers handler classes" do
    server.register(EchoHandler)
    expect(server.router.paths).to include("/echo.v1.EchoService/Unary")
  end

  it "registers handler classes by name" do
    server.register("EchoHandler")
    expect(server.router.service_names).to eq(["echo.v1.EchoService"])
  end

  it "refuses to start without services" do
    expect { server.start }.to raise_error(Tonic::Rails::Error, /no services registered/)
    expect(server).not_to be_running
  end

  it "validates configuration before starting" do
    server.register(EchoHandler)
    config.dispatch_threads = 0
    expect { server.start }.to raise_error(ArgumentError, /dispatch_threads/)
  end

  it "reports zeroed stats while stopped" do
    expect(server.stats).to include(total: 0, active_connections: 0, queued: 0)
  end

  it "ignores stop while not running" do
    expect { server.stop }.not_to raise_error
  end
end
