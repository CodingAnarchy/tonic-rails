# frozen_string_literal: true

module Tonic
  module Rails
    class Client
      # Choosing a call's retry policy, and running its attempts under it.
      # @api private
      module Retries
        # Trailer a server sends to ask for a retry after so many
        # milliseconds, or (with a negative value) for none.
        RETRY_PUSHBACK = "grpc-retry-pushback-ms"
        private_constant :RETRY_PUSHBACK

        METHOD_NAME = %r{\A[A-Za-z_][\w.]*(/[A-Za-z_]\w*)?\z}
        private_constant :METHOD_NAME

        private

        def method_retry_policies(option)
          raise ArgumentError, "method_retries must be a Hash" unless option.is_a?(Hash)

          option.to_h do |name, retries|
            name = name.to_s.delete_prefix("/")
            unless name.match?(METHOD_NAME)
              raise ArgumentError, "method_retries keys must be \"package.Service\" or \"package.Service/Method\", " \
                                   "got #{name.inspect}"
            end

            [name, RetryPolicy.from(retries)]
          end.freeze
        end

        # The most specific policy for a call: its own, its method's, its
        # service's, then the client's.
        def retry_policy(path, retries)
          return RetryPolicy.from(retries) unless retries.nil?
          return @retries if @method_retries.empty?

          method = path.delete_prefix("/")
          service = method.split("/", 2).first
          return @method_retries[method] if @method_retries.key?(method)
          return @method_retries[service] if @method_retries.key?(service)

          @retries
        end

        # Run attempts of a call (the block returns a native result) until one
        # succeeds or shouldn't be retried; return its result.
        def with_retries(path, policy, deadline_at, payload)
          attempt = 1
          loop do
            payload[:attempts] = attempt if payload
            result = yield
            delay = retry_delay(policy, attempt, result, deadline_at)
            if delay.nil?
              log_gave_up(path, attempt, result) if attempt > 1 && !result[0].zero?
              return result
            end

            log_retry(path, attempt, policy.max_attempts, result, delay)
            sleep(delay)
            attempt += 1
          end
        end

        # Seconds to wait before retrying, or nil not to retry.  The server
        # can ask for a delay, or for no retry, with grpc-retry-pushback-ms.
        def retry_delay(policy, attempt, result, deadline_at)
          code, _, _, _, trailers, _, committed = result
          return if policy.nil? || code.zero? || committed || attempt >= policy.max_attempts || !policy.retry?(code)

          pushback = trailers.assoc(RETRY_PUSHBACK)&.last
          delay = if pushback.nil? then policy.backoff(attempt)
          elsif pushback.match?(/\A\d+\z/) then pushback.to_i / 1000.0
          end
          return if delay.nil?
          return if deadline_at && Process.clock_gettime(Process::CLOCK_MONOTONIC) + delay >= deadline_at

          delay
        end

        def logger
          @logger || Tonic::Rails.configuration.logger
        end

        def log_retry(path, attempt, max_attempts, result, delay)
          logger&.warn(format("tonic-rails: %<path>s failed with %<status>s (attempt %<attempt>d of %<max>d); " \
                              "retrying in %<ms>dms", path: path, status: status_text(result), attempt: attempt,
                                                      max: max_attempts, ms: (delay * 1000).round))
        end

        def log_gave_up(path, attempts, result)
          logger&.warn("tonic-rails: #{path} failed with #{status_text(result)} after #{attempts} attempts")
        end

        def status_text(result)
          code, message = result
          text = RpcError::CODE_NAMES.fetch(code, code).to_s.upcase
          message.empty? ? text : "#{text}: #{message}"
        end
      end
    end
  end
end
