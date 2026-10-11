# frozen_string_literal: true

module Tonic
  module Rails
    class Client
      # When and how often a call is retried, following gRPC's retry design
      # (A6).  Pass one, a Hash of its fields or `true` for the defaults as
      # `retries:` to Client.new or to a single call.
      #
      #   Tonic::Rails::Client.new("items.internal:50051", retries: true)
      #   Tonic::Rails::Client.new("items.internal:50051", retries: { max_attempts: 4 })
      #   items.get_item({ id: 1 }, retries: false)
      #
      # Only calls the server hasn't started on are retried: the connection
      # failed, or the server answered with a status and no response
      # headers.  Each wait is random, up to a cap that grows from
      # `initial_backoff` by `multiplier` per attempt to `max_backoff`.
      # Retrying stops when the next attempt couldn't start before the
      # call's deadline.
      RetryPolicy = Data.define(:max_attempts, :codes, :initial_backoff, :max_backoff, :multiplier) do
        # Defaults for the fields left out.
        def self.defaults
          { max_attempts: 3, codes: [:unavailable], initial_backoff: 0.1, max_backoff: 1, multiplier: 2 }
        end

        # @param option [RetryPolicy, Hash, true, false, nil]
        # @return [RetryPolicy, nil] nil for no retries
        def self.from(option)
          case option
          when nil, false then nil
          when true then new(**defaults)
          when Hash then new(**defaults, **option)
          when self then option
          else raise ArgumentError, "retries must be true, false, a Hash or a #{name}, got #{option.class}"
          end
        end

        # @param max_attempts [Integer] attempts in all, from 2 to 5.  gRPC
        #   caps it at 5, so a misconfigured client can't multiply an
        #   outage's traffic by more than that.
        # @param codes [Array<Symbol, Integer>] status codes worth retrying
        # @param initial_backoff [Numeric] seconds; the cap on the first wait
        # @param max_backoff [Numeric] seconds; the cap on any wait
        # @param multiplier [Numeric] how much the cap grows per attempt
        def initialize(max_attempts:, codes:, initial_backoff:, max_backoff:, multiplier:)
          unless max_attempts.is_a?(Integer) && max_attempts.between?(2, 5)
            raise ArgumentError, "max_attempts must be an Integer from 2 to 5"
          end

          codes = Array(codes).map { |code| RpcError.normalize_code(code) }.uniq.freeze
          raise ArgumentError, "codes must name at least one status code other than OK" if codes.empty? || codes.include?(0)

          [[:initial_backoff, initial_backoff], [:max_backoff, max_backoff]].each do |field, value|
            raise ArgumentError, "#{field} must be a positive number of seconds" unless value.is_a?(Numeric) && value.positive?
          end
          raise ArgumentError, "multiplier must be at least 1" unless multiplier.is_a?(Numeric) && multiplier >= 1

          super
        end

        # @param code [Integer]
        def retry?(code)
          codes.include?(code)
        end

        # Seconds to wait after failed attempt number `attempt` (from 1).
        def backoff(attempt)
          rand * [initial_backoff * (multiplier**(attempt - 1)), max_backoff].min
        end
      end
    end
  end
end
