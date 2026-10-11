# frozen_string_literal: true

require "socket"

module Tonic
  module Rails
    # Multi-process mode (`config.processes > 1`).
    #
    # The process that calls Server#start becomes a supervisor: it binds the
    # listening socket, then forks one worker per process.  Workers inherit
    # the loaded app (copy-on-write) and the socket, and each runs a full
    # server (its own tokio runtime, created after the fork) accepting on
    # it.  The supervisor never starts a runtime itself, so nothing
    # multi-threaded crosses a fork.
    #
    # Sharing one socket rather than binding with SO_REUSEPORT per worker
    # means connections spread across workers on every Unix (macOS doesn't
    # load-balance SO_REUSEPORT), port 0 works, a bind error surfaces once,
    # and connections waiting in the backlog survive a worker's death.
    #
    # Lifecycle:
    # - startup fails if a worker exits before it's serving (usually a
    #   configuration or boot error, which a retry wouldn't fix)
    # - a worker that dies later is logged and replaced, with backoff if
    #   replacements keep failing to boot
    # - stopping (TERM/INT or Server#stop) closes the supervisor's copy of
    #   the socket and sends TERM to every worker, which flips its health to
    #   NOT_SERVING and drains; workers still running after `drain_timeout`
    #   plus a margin are killed
    # - a worker whose supervisor disappears stops itself
    class Cluster
      # Seconds to wait beyond drain_timeout before killing a worker.
      STOP_MARGIN = 5
      MAX_BACKOFF = 30

      Worker = Struct.new(:index, :pid, :ready_io, :ready)

      def initialize(server, configuration, stop_signal)
        @server = server
        @configuration = configuration
        @stop_signal = stop_signal
        @workers = []
        @failures = Hash.new(0)
        @respawn_at = {}
      end

      # Run the workers until stopped.
      # @yield once every worker is serving
      def run(install_signal_handlers:)
        @listener = Socket.for_fd(TonicRails::Native.bind_listener(@configuration.bind_address))
        @listener.autoclose = true
        previous_handlers = install_signal_handlers ? trap_signals : {}
        @configuration.processes.times { |index| @workers << spawn(index) }
        return unless booted?

        @started = true
        yield if block_given?
        supervise
      ensure
        @listener&.close
        stop_workers
        previous_handlers&.each { |signal, handler| Signal.trap(signal, handler || "DEFAULT") }
      end

      private

      def logger = @configuration.logger

      def trap_signals
        server = @server
        %w[TERM INT].to_h { |signal| [signal, Signal.trap(signal) { server.stop }] }
      end

      def spawn(index)
        reader, writer = IO.pipe
        pid = fork do
          reader.close
          @workers.each { |worker| worker&.ready_io&.close }
          run_worker(index, writer)
        end
        writer.close
        Worker.new(index, pid, reader, false)
      end

      # In the worker process.
      def run_worker(index, ready)
        Process.setproctitle("tonic-rails worker #{index}")
        supervisor = Process.ppid
        Thread.new do
          sleep 1 while Process.ppid == supervisor
          @server.stop
        end
        @server.run_worker(@listener, index) do
          ready.write("1")
          ready.close
        end
      rescue StandardError => e
        Tonic::Rails.log_exception(e, "worker #{index}")
        exit 1
      end

      # Wait until every worker is serving: true once they are, false if a
      # stop was requested first.  Raises if a worker exited before serving.
      def booted?
        until @workers.all?(&:ready)
          return false if @stop_signal.pop(timeout: 0.05)

          @workers.each do |worker|
            next if ready?(worker)

            status = reap(worker)
            raise Error, "tonic-rails worker #{worker.index} exited during startup (#{describe(status)})" if status
          end
        end
        true
      end

      def supervise
        loop do
          break if @stop_signal.pop(timeout: 0.5)

          @workers.each_index { |index| supervise_worker(index) }
        end
      end

      def supervise_worker(index)
        worker = @workers[index]
        if worker.nil?
          @workers[index] = spawn(index) if monotonic >= @respawn_at[index]
          return
        end

        ready?(worker)
        status = reap(worker)
        return unless status

        replace(worker, status)
      end

      # A worker died after startup.  Replace it right away if it had been
      # serving; if replacements keep dying before they serve, back off.
      def replace(worker, status)
        index = worker.index
        @failures[index] = worker.ready ? 0 : @failures[index] + 1
        delay = @failures[index].zero? ? 0 : [2**(@failures[index] - 1), MAX_BACKOFF].min
        logger&.error(
          "tonic-rails: worker #{index} (pid #{worker.pid}) exited unexpectedly (#{describe(status)}); " \
          "starting a replacement#{" in #{delay}s" if delay.positive?}",
        )
        @workers[index] = nil
        @respawn_at[index] = monotonic + delay
      end

      def ready?(worker)
        return true if worker.ready

        case worker.ready_io.read_nonblock(1, exception: false)
        when String
          worker.ready = true
          worker.ready_io.close
          logger&.info("tonic-rails: worker #{worker.index} (pid #{worker.pid}) serving") if @started
          true
        else
          false
        end
      end

      # The worker's exit status if it has exited, otherwise nil.
      def reap(worker)
        _, status = Process.waitpid2(worker.pid, Process::WNOHANG)
        worker.ready_io.close if status
        status
      rescue Errno::ECHILD
        worker.ready_io.close
        :unknown
      end

      def stop_workers
        live = @workers.compact
        return if live.empty?

        logger&.info(
          "tonic-rails: shutting down #{live.size} workers, draining for up to #{@configuration.drain_timeout}s",
        )
        live.each { |worker| signal(worker, :TERM) }
        deadline = monotonic + @configuration.drain_timeout.to_f + STOP_MARGIN
        until (live = live.reject { |worker| reap(worker) }).empty?
          if monotonic > deadline
            logger&.warn("tonic-rails: #{live.size} worker(s) still running after the drain timeout; killing them")
            live.each { |worker| kill(worker) }
            break
          end
          sleep 0.05
        end
        @workers = []
        logger&.info("tonic-rails: stopped")
      end

      def kill(worker)
        signal(worker, :KILL)
        Process.wait(worker.pid)
      rescue Errno::ECHILD
        nil
      end

      def signal(worker, name)
        Process.kill(name, worker.pid)
      rescue Errno::ESRCH
        nil
      end

      def describe(status)
        return "status unknown" unless status.is_a?(Process::Status)
        return "signal #{Signal.signame(status.termsig)}" if status.signaled?

        "exit status #{status.exitstatus}"
      end

      def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
