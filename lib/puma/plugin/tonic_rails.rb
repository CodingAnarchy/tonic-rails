# frozen_string_literal: true

require "puma/plugin"

# Runs the tonic-rails gRPC server alongside Puma:
#
#   # config/puma.rb
#   plugin :tonic_rails
#
# The gRPC server runs as one child process of the Puma master, in single
# and cluster mode alike, so it has its own GVL and never competes with web
# workers.  It starts once Puma has booted, drains gracefully when Puma
# stops or restarts, stops itself if Puma goes away, and if it dies
# unexpectedly Puma shuts down too, so a supervisor can restart both.
Puma::Plugin.create do
  def start(launcher)
    @launcher = launcher
    @puma_pid = Process.pid
    @grpc_pid = nil

    events = launcher.events
    if events.respond_to?(:after_booted) # Puma 7+
      events.after_booted { start_grpc }
      events.after_stopped { stop_grpc }
      events.before_restart { stop_grpc }
    else
      events.on_booted { start_grpc }
      events.on_stopped { stop_grpc }
      events.on_restart { stop_grpc }
    end

    in_background { monitor_grpc }
  end

  private

  def start_grpc
    @grpc_pid = fork { run_grpc_server }
    log "tonic-rails: gRPC server started (pid #{@grpc_pid})"
  end

  def run_grpc_server
    # The web listeners belong to Puma; don't hold them open in this process.
    @launcher.binder.ios.each { |io| io.close rescue nil } # rubocop:disable Style/RescueModifier
    load_rails_app
    Thread.new { stop_when_puma_exits }
    Tonic::Rails.server.start
  end

  # `rails server` and preload_app! boot the app before forking; plain
  # `puma` in cluster mode does not, so boot it here.
  def load_rails_app
    return if defined?(Rails.application) && Rails.application&.initialized?

    require File.expand_path("config/environment", Dir.pwd)
  end

  def stop_grpc
    pid = @grpc_pid
    return unless pid

    @grpc_pid = nil
    log "tonic-rails: stopping gRPC server (pid #{pid})"
    Process.kill(:TERM, pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  def monitor_grpc
    loop do
      sleep 1
      pid = @grpc_pid
      next unless pid && Process.waitpid(pid, Process::WNOHANG)

      @grpc_pid = nil
      log "tonic-rails: gRPC server (pid #{pid}) exited unexpectedly; stopping Puma"
      Process.kill(:TERM, @puma_pid)
      break
    rescue Errno::ECHILD
      nil
    end
  end

  def stop_when_puma_exits
    sleep 1 while Process.ppid == @puma_pid
    Tonic::Rails.server.stop
  end

  def log(message)
    @launcher.log_writer.log(message)
  end
end
