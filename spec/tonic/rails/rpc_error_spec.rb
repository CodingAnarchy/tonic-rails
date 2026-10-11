# frozen_string_literal: true

RSpec.describe Tonic::Rails::RpcError do
  it "accepts symbolic codes" do
    error = described_class.new("gone", code: :not_found)
    expect([error.code, error.code_name, error.message]).to eq([5, :not_found, "gone"])
  end

  it "accepts numeric codes" do
    expect(described_class.new("denied", code: 7).code_name).to eq(:permission_denied)
  end

  it "defaults to UNKNOWN" do
    expect(described_class.new("?").code).to eq(2)
  end

  it "defaults the message to the code name" do
    expect(described_class.new(code: :unavailable).message).to eq("unavailable")
  end

  it "rejects unknown codes" do
    expect { described_class.new("x", code: :nope) }.to raise_error(ArgumentError, /unknown gRPC status code/)
    expect { described_class.new("x", code: 99) }.to raise_error(ArgumentError)
    expect { described_class.new("x", code: 1.5) }.to raise_error(ArgumentError)
  end

  describe "#trailers" do
    it "is empty without metadata or details" do
      expect(described_class.new("x").trailers).to eq({})
    end

    it "encodes metadata and details as a google.rpc.Status" do
      require "google/rpc/status_pb"
      error = described_class.new(
        "bad", code: :invalid_argument, metadata: { "x-id" => "1" }, details: [Echo::V1::Detail.new(mood: :MOOD_HAPPY)]
      )
      trailers = error.trailers
      expect(trailers["x-id"]).to eq("1")

      status = Google::Rpc::Status.decode(trailers["grpc-status-details-bin"].unpack1("m0"))
      expect([status.code, status.message]).to eq([3, "bad"])
      expect(status.details.first.unpack(Echo::V1::Detail).mood).to eq(:MOOD_HAPPY)
    end
  end

  describe ".decode_details" do
    it "unpacks the details of an encoded google.rpc.Status" do
      require "google/protobuf/well_known_types"
      unknown = Google::Protobuf::Any.new(type_url: "type.googleapis.com/acme.Unknown", value: "\x08\x01".b)
      status = described_class.new("bad", details: [Echo::V1::Detail.new(mood: :MOOD_HAPPY), unknown]).status_proto
      expect(described_class.decode_details(status)).to eq([Echo::V1::Detail.new(mood: :MOOD_HAPPY), unknown])
    end

    it "returns nothing for missing or malformed details" do
      expect(described_class.decode_details(nil)).to eq([])
      expect(described_class.decode_details("\x1a\x7f".b)).to eq([])
      expect(described_class.decode_details("\xff".b)).to eq([])
    end
  end

  it "is a Tonic::Rails::Error" do
    expect(described_class.new("x")).to be_a(Tonic::Rails::Error)
  end
end
