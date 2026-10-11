# frozen_string_literal: true

RSpec.describe Tonic::Rails::Reflection do
  def decode(blobs)
    blobs.map { |b| Google::Protobuf::FileDescriptorProto.decode(b) }
  end

  it "includes the service's file and every file its messages reference" do
    files = decode(described_class.file_descriptors(["echo.v1.EchoService"]))
    expect(files.map(&:name)).to contain_exactly("echo.proto", "google/protobuf/timestamp.proto")
    expect(files.find { |f| f.name == "echo.proto" }.service.map(&:name)).to eq(["EchoService"])
  end

  it "ignores unknown service names" do
    expect(described_class.file_descriptors(["nope.Missing"])).to eq([])
  end
end
