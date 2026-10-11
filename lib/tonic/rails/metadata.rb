# frozen_string_literal: true

module Tonic
  module Rails
    # Converts handler-supplied gRPC metadata into header pairs.
    #
    # Names are lower-cased and must be valid gRPC metadata names; names
    # ending in "-bin" carry binary values, which are base64-encoded as the
    # gRPC spec requires.  Other values must be printable ASCII.
    module Metadata
      NAME = /\A[0-9a-z_.-]+\z/
      PRINTABLE = /\A[\x20-\x7e]*\z/
      RESERVED = %w[content-type te user-agent content-length connection transfer-encoding].freeze

      module_function

      # @param metadata [Hash{String,Symbol=>String,Array<String>}]
      # @return [Hash{String=>String}]
      # @raise [ArgumentError] for invalid or reserved names and values
      def encode(metadata)
        metadata.each_with_object({}) do |(name, value), out|
          name = name.to_s.downcase
          validate_name!(name)
          out[name] = encode_value(name, value)
        end
      end

      def validate_name!(name)
        raise ArgumentError, "invalid gRPC metadata name #{name.inspect}" unless NAME.match?(name)
        return unless RESERVED.include?(name) || (name.start_with?("grpc-") && name != "grpc-status-details-bin")

        raise ArgumentError, "gRPC metadata name #{name.inspect} is reserved"
      end

      def encode_value(name, value)
        values = Array(value).map(&:to_s)
        if name.end_with?("-bin")
          values.map { |v| [v].pack("m0") }.join(",")
        else
          values.each do |v|
            next if PRINTABLE.match?(v)

            raise ArgumentError, "gRPC metadata #{name} must be printable ASCII; use a -bin name for binary"
          end
          values.join(",")
        end
      end
    end
  end
end
