# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tonic::Rails::Server do
  let(:configuration) do
    Tonic::Rails::Configuration.new.tap do |c|
      c.bind_address = "127.0.0.1:0"  # ephemeral port for tests
    end
  end

  let(:server) { described_class.new(configuration: configuration) }

  describe "#register" do
    it "registers a service class with the router" do
      service_class = Class.new do
        include Tonic::Rails::Base
        service "test.Svc"
        rpc :Ping, String, String
      end

      server.register(service_class)
      expect(server.router.paths).to include("/test.Svc/Ping")
    end

    it "returns self for chaining" do
      service_class = Class.new do
        include Tonic::Rails::Base
        service "test.Svc"
        rpc :Ping, String, String
      end

      expect(server.register(service_class)).to be(server)
    end
  end

  describe "#start" do
    it "raises when no services are registered" do
      expect { server.start }.to raise_error(Tonic::Rails::Error, /No services registered/)
    end

    it "raises when already running" do
      service_class = Class.new do
        include Tonic::Rails::Base
        service "test.Svc"
        rpc :Ping, String, String
      end
      server.register(service_class)
      server.define_singleton_method(:running?) { true }

      expect { server.start }.to raise_error(Tonic::Rails::Error, /already running/)
    end
  end

  describe "#running?" do
    it "returns false before start" do
      expect(server).not_to be_running
    end
  end
end
