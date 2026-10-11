# frozen_string_literal: true

require "grpc"
require "timeout"
require_relative "../fixtures/lib/echo_services_pb"
require_relative "../support/integration_server"

# Runs a server in its own process with the default signal handling, as
# `bin/rails tonic_rails:serve` does, and stops it with real signals.
RSpec.describe "Signal handling", :integration do
  before(:all) do
    skip "POSIX signals" if Gem.win_platform?
    skip "native extension not compiled" unless IntegrationServer.native_available?
  end

  def start_server_process
    port = IntegrationServer.free_port
    script = <<~RUBY
      $stdout.sync = true
      require "tonic-rails"
      require #{File.expand_path('../fixtures/lib/echo_handler', __dir__).dump}
      Tonic::Rails.configure do |c|
        c.bind_address = "127.0.0.1:#{port}"
        c.drain_timeout = 5
        c.logger = Logger.new($stdout)
      end
      Tonic::Rails.server.register(EchoHandler)
      Tonic::Rails.server.start
      puts "exited cleanly"
    RUBY
    reader, writer = IO.pipe
    pid = Process.spawn(RbConfig.ruby, "-I", File.expand_path("../../lib", __dir__), "-e", script, out: writer, err: writer)
    writer.close
    output = +""
    Thread.new { reader.each_line { |line| output << line } }
    { pid: pid, address: "127.0.0.1:#{port}", output: output }
  end

  def stub_for(address)
    Echo::V1::EchoService::Stub.new(address, :this_channel_is_insecure)
  end

  def wait_until_serving(address)
    Timeout.timeout(20) do
      loop do
        break if stub_for(address).unary(Echo::V1::EchoRequest.new(message: "up"), deadline: Time.now + 1)
      rescue GRPC::BadStatus
        sleep 0.1
      end
    end
  end

  %w[TERM INT].each do |signal|
    it "drains in-flight calls and exits on SIG#{signal}" do
      server = start_server_process
      wait_until_serving(server[:address])
      in_flight = Thread.new { stub_for(server[:address]).unary(Echo::V1::EchoRequest.new(message: "done", sleep_ms: 500)).message }
      sleep 0.2

      Process.kill(signal, server[:pid])
      expect(in_flight.value).to eq("done")
      _, status = Timeout.timeout(15) { Process.wait2(server[:pid]) }
      expect(status).to be_success
      expect(server[:output]).to include("shutting down", "exited cleanly")
    ensure
      Process.kill(:KILL, server[:pid]) rescue nil # rubocop:disable Style/RescueModifier
    end
  end
end
