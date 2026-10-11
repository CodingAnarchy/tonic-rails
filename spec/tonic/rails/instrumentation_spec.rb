# frozen_string_literal: true

RSpec.describe Tonic::Rails::Instrumentation do
  def capture_events
    events = []
    subscription = ActiveSupport::Notifications.subscribe(described_class::EVENT) { |event| events << event }
    yield
    events
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription)
  end

  it "publishes successful calls with their path, service and status" do
    events = capture_events do
      expect(described_class.instrument("/demo.v1.ItemService/GetItem", { "x" => "1" }) { :result }).to eq(:result)
    end
    expect(events.size).to eq(1)
    expect(events.first.payload).to include(
      path: "/demo.v1.ItemService/GetItem", service: "demo.v1.ItemService", rpc: "GetItem",
      metadata: { "x" => "1" }, code: 0, status: :ok
    )
    expect(events.first.duration).to be >= 0
  end

  it "records the status of an RpcError" do
    events = capture_events do
      expect { described_class.instrument("/s.S/M", {}) { raise Tonic::Rails::RpcError.new("gone", code: :not_found) } }
        .to raise_error(Tonic::Rails::RpcError)
    end
    expect(events.first.payload).to include(code: 5, status: :not_found)
    expect(events.first.payload[:exception_object].message).to eq("gone")
  end

  it "records unexpected exceptions as INTERNAL" do
    events = capture_events do
      expect { described_class.instrument("/s.S/M", {}) { raise ArgumentError, "boom" } }.to raise_error(ArgumentError)
    end
    expect(events.first.payload).to include(code: 13, status: :internal)
    expect(events.first.payload[:exception_object]).to be_a(ArgumentError)
  end

  describe Tonic::Rails::Instrumentation::RequestLogger do
    let(:io) { StringIO.new }

    def log_for(&block)
      subscription = described_class.new(Logger.new(io)).subscribe
      begin
        Tonic::Rails::Instrumentation.instrument("/s.S/M", {}, &block)
      rescue StandardError
        nil
      ensure
        ActiveSupport::Notifications.unsubscribe(subscription)
      end
      io.string
    end

    it "logs successful calls" do
      expect(log_for { :ok }).to match(%r{gRPC /s\.S/M OK in \d+\.\dms})
    end

    it "includes the status and message of failed calls" do
      expect(log_for { raise Tonic::Rails::RpcError.new("nope", code: :permission_denied) })
        .to match(%r{gRPC /s\.S/M PERMISSION_DENIED in \d+\.\dms: nope})
    end
  end
end
