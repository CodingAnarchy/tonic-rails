# frozen_string_literal: true

require "openssl"

RSpec.describe Tonic::Rails::Peer do
  it "splits IPv4 addresses" do
    peer = described_class.new("10.0.4.17:51234")
    expect([peer.ip, peer.port]).to eq(["10.0.4.17", 51_234])
  end

  it "splits IPv6 addresses" do
    peer = described_class.new("[::1]:443")
    expect([peer.ip, peer.port]).to eq(["::1", 443])
  end

  it "has no certificate without mutual TLS" do
    peer = described_class.new("127.0.0.1:1")
    expect(peer.certificate).to be_nil
    expect(peer).not_to be_authenticated
  end

  it "decodes the client certificate chain" do
    key = OpenSSL::PKey::EC.generate("prime256v1")
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = 1
    cert.subject = cert.issuer = OpenSSL::X509::Name.parse("/CN=billing")
    cert.public_key = key
    cert.not_before = Time.now
    cert.not_after = Time.now + 60
    cert.sign(key, OpenSSL::Digest.new("SHA256"))

    peer = described_class.new("127.0.0.1:1", [cert.to_der])
    expect(peer).to be_authenticated
    expect(peer.certificate.subject.to_s).to eq("/CN=billing")
    expect(peer.inspect).to eq("#<Tonic::Rails::Peer 127.0.0.1:1 (certificate)>")
  end

  it "is available to handlers under test" do
    response = Tonic::Rails::Testing.call(EchoHandler, :Unary, { message: "whoami" }, peer: described_class.new("192.0.2.1:9"))
    expect(response.message).to eq("192.0.2.1")
  end

  it "defaults to an unknown peer" do
    expect(Tonic::Rails::Testing.call(EchoHandler, :Unary, { message: "whoami" }).message).to eq("")
  end
end
