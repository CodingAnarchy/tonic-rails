# frozen_string_literal: true

require "fileutils"
require "openssl"
require "tmpdir"

# A throwaway CA, a server certificate for 127.0.0.1 and localhost, and a
# client certificate, as PEM files in a temporary directory:
# ca.pem, server.pem, server.key, client.pem and client.key.
class TestCertificates
  def initialize
    @dir = Dir.mktmpdir("tonic-rails-tls")
    @serial = 0
    @ca_key = OpenSSL::PKey::EC.generate("prime256v1")
    @ca = build("tonic-rails test CA", @ca_key, nil, authority: true)
    write("ca.pem", @ca.to_pem)
    issue("server", "127.0.0.1", san: "IP:127.0.0.1,DNS:localhost")
    issue("client", "client")
  end

  def path(name) = File.join(@dir, name)

  def read(name) = File.read(path(name))

  def write(name, contents)
    File.write(path(name), contents)
  end

  def cleanup
    FileUtils.remove_entry(@dir)
  end

  private

  def issue(name, common_name, san: nil)
    key = OpenSSL::PKey::EC.generate("prime256v1")
    write("#{name}.pem", build(common_name, key, @ca, san: san).to_pem)
    write("#{name}.key", key.private_to_pem)
  end

  def build(common_name, key, issuer, authority: false, san: nil)
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = (@serial += 1)
    cert.subject = OpenSSL::X509::Name.parse("/CN=#{common_name}")
    cert.issuer = issuer ? issuer.subject : cert.subject
    cert.public_key = key
    cert.not_before = Time.now - 60
    cert.not_after = Time.now + 3600
    ext = OpenSSL::X509::ExtensionFactory.new
    ext.subject_certificate = cert
    ext.issuer_certificate = issuer || cert
    cert.add_extension(ext.create_extension("basicConstraints", authority ? "CA:TRUE" : "CA:FALSE", true))
    cert.add_extension(ext.create_extension("keyUsage", authority ? "keyCertSign,cRLSign" : "digitalSignature", true))
    cert.add_extension(ext.create_extension("subjectAltName", san)) if san
    cert.sign(issuer ? @ca_key : key, OpenSSL::Digest.new("SHA256"))
    cert
  end
end
