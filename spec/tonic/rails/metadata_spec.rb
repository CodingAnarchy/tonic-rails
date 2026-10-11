# frozen_string_literal: true

RSpec.describe Tonic::Rails::Metadata do
  it "lower-cases names and stringifies values" do
    expect(described_class.encode("X-Request-Id" => 42, x_mode: "fast")).to eq("x-request-id" => "42", "x_mode" => "fast")
  end

  it "base64-encodes -bin values" do
    expect(described_class.encode("x-trace-bin" => "\x00\xFF".b)).to eq("x-trace-bin" => "AP8=")
  end

  it "joins repeated values" do
    expect(described_class.encode("x-tags" => %w[a b])).to eq("x-tags" => "a,b")
  end

  it "rejects reserved names" do
    %w[grpc-status grpc-message content-type te].each do |name|
      expect { described_class.encode(name => "x") }.to raise_error(ArgumentError, /reserved/)
    end
  end

  it "allows grpc-status-details-bin" do
    expect(described_class.encode("grpc-status-details-bin" => "x")).to have_key("grpc-status-details-bin")
  end

  it "rejects invalid names and non-printable values" do
    expect { described_class.encode("bad name" => "x") }.to raise_error(ArgumentError, /invalid gRPC metadata name/)
    expect { described_class.encode("x-text" => "line\nbreak") }.to raise_error(ArgumentError, /printable ASCII/)
  end
end
