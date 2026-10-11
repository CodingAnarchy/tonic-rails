# frozen_string_literal: true

module Tonic
  module Rails
    # What grpc.health.v1 reports, per service ("" = the whole server).
    #
    # A service reports SERVING only while the app hasn't marked it
    # NOT_SERVING (Server#not_serving!) *and* the optional
    # `config.health_check` passes.  Neither overrides the other.
    # @api private
    class Health
      def initialize(configuration, router)
        @configuration = configuration
        @router = router
        @marks = {}
        @check_passing = true
        @native = nil
        @checker = nil
      end

      def mark(services, serving)
        names = services.empty? ? names_to_report : services.map(&:to_s)
        names.each { |name| @marks[name] = serving }
        publish(names)
      end

      # Start reporting through a running native server.
      def attach(native)
        @native = native
        @check_passing = true
        publish(names_to_report)
        start_checker
      end

      # Stop the checker before shutdown flips everything NOT_SERVING, so
      # a passing check can't flip it back.
      def detach
        stop_checker
        @native = nil
      end

      private

      def names_to_report
        ["", *@router.service_names, *@marks.keys].uniq
      end

      def publish(names)
        native = @native
        return unless native

        names.each { |name| native.set_health(name, @check_passing && @marks.fetch(name, true)) }
      end

      def start_checker
        check = @configuration.health_check
        return unless check

        @wakeup = Thread::Queue.new
        @checker = Thread.new do
          Thread.current.name = "tonic-rails-health"
          loop do
            passing = run_check(check)
            changed(passing) if passing != @check_passing
            break if @wakeup.pop(timeout: @configuration.health_check_interval)
          end
        end
      end

      def stop_checker
        return unless @checker

        @wakeup << :stop
        @checker.join(@configuration.drain_timeout.to_f + 5)
        @checker = nil
      end

      def changed(passing)
        @check_passing = passing
        publish(names_to_report)
        logger = @configuration.logger
        return unless logger

        if passing
          logger.info("tonic-rails: health check passing; SERVING")
        else
          logger.warn("tonic-rails: health check failing; NOT_SERVING")
        end
      end

      # Runs inside the Rails executor so database connections are returned.
      def run_check(check)
        result = if defined?(::Rails.application) && ::Rails.application
          ::Rails.application.executor.wrap { check.call }
        else
          check.call
        end
        result ? true : false
      rescue StandardError => e
        Tonic::Rails.log_exception(e, "health check")
        false
      end
    end
  end
end
