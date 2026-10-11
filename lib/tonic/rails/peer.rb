# frozen_string_literal: true

module Tonic
  module Rails
    # The client on the other end of an RPC, available in handlers as
    # `peer`:
    #
    #   peer.address       # => "10.0.4.17:51234"
    #   peer.ip            # => "10.0.4.17"
    #   peer.certificate   # => OpenSSL::X509::Certificate, with mutual TLS
    #   peer.certificate&.subject.to_s  # => "/CN=billing-service"
    class Peer
      # @return [String, nil] "ip:port" ("[ipv6]:port" for IPv6)
      attr_reader :address

      # @param address [String, nil]
      # @param certificates [Array<String>] DER-encoded chain, leaf first
      def initialize(address = nil, certificates = [])
        @address = address
        @certificate_ders = certificates
      end

      # @return [String, nil]
      def ip
        return unless address

        host = address.rpartition(":").first
        host.delete_prefix("[").delete_suffix("]")
      end

      # @return [Integer, nil]
      def port
        address && Integer(address.rpartition(":").last, exception: false)
      end

      # The client certificate presented over mutual TLS, or nil.
      # @return [OpenSSL::X509::Certificate, nil]
      def certificate
        certificate_chain.first
      end

      # @return [Array<OpenSSL::X509::Certificate>] leaf first
      def certificate_chain
        @certificate_chain ||= begin
          require "openssl"
          @certificate_ders.map { |der| OpenSSL::X509::Certificate.new(der) }
        end
      end

      # True when the client authenticated with a certificate.
      def authenticated?
        !@certificate_ders.empty?
      end

      def inspect
        "#<#{self.class.name} #{address || 'unknown'}#{' (certificate)' if authenticated?}>"
      end
    end
  end
end
