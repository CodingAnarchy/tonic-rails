# frozen_string_literal: true

require "grpc"
require "net/http"
require "timeout"
require_relative "../fixtures/lib/echo_services_pb"
require_relative "../support/integration_server"

RSpec.describe "Puma plugin", :integration do
  before(:all) do
    skip "native extension not compiled" unless IntegrationServer.native_available?
    begin
      require "puma"
    rescue LoadError
      skip "puma not installed"
    end
  end

  def app_dir
    File.expand_path("../fixtures/puma_app", __dir__)
  end

  def start_puma(workers:, preload: false)
    grpc_port = IntegrationServer.free_port
    http_port = IntegrationServer.free_port
    env = {
      "GRPC_BIND" => "127.0.0.1:#{grpc_port}", "HTTP_PORT" => http_port.to_s,
      "WORKERS" => workers.to_s, "PRELOAD" => preload ? "1" : "0",
      "RUBYLIB" => [File.expand_path("../../lib", __dir__), ENV.fetch("RUBYLIB", nil)].compact.join(File::PATH_SEPARATOR)
    }
    reader, writer = IO.pipe
    pid = Process.spawn(env, RbConfig.ruby, "-S", "puma", "-C", "config/puma.rb", "config.ru",
                        chdir: app_dir, out: writer, err: writer, pgroup: true)
    writer.close
    output = +""
    Thread.new { reader.each_line { |line| output << line } }
    { pid: pid, grpc: "127.0.0.1:#{grpc_port}", http: http_port, output: output }
  end

  def wait_until(seconds = 30)
    Timeout.timeout(seconds) { sleep 0.2 until yield }
  end

  def grpc_up?(address)
    stub = Echo::V1::EchoService::Stub.new(address, :this_channel_is_insecure)
    stub.unary(Echo::V1::EchoRequest.new(message: "ping"), deadline: Time.now + 1).message == "ping"
  rescue GRPC::BadStatus
    false
  end

  def exited?(pid)
    Process.waitpid(pid, Process::WNOHANG)
  rescue Errno::ECHILD
    true
  end

  def grpc_child_pid(output)
    output[/gRPC server started \(pid (\d+)\)/, 1]&.to_i
  end

  def stop(puma)
    Process.kill(:TERM, puma[:pid])
    wait_until(45) { exited?(puma[:pid]) } # CI macOS runners can be slow
  rescue Errno::ESRCH
    nil
  end

  [
    { workers: 0, preload: false, label: "single mode" },
    { workers: 2, preload: false, label: "cluster mode" },
    { workers: 2, preload: true, label: "cluster mode with preload_app!" },
  ].each do |mode|
    it "serves gRPC next to Puma in #{mode[:label]} and stops with it" do
      puma = start_puma(workers: mode[:workers], preload: mode[:preload])
      begin
        wait_until { grpc_up?(puma[:grpc]) }
        expect(Net::HTTP.get(URI("http://127.0.0.1:#{puma[:http]}/up"))).to eq("ok")
        grpc_pid = grpc_child_pid(puma[:output])
        expect(grpc_pid).to be_a(Integer)

        stop(puma)
        wait_until(10) { !Process.kill(0, grpc_pid) rescue true } # rubocop:disable Style/RescueModifier
        expect(grpc_up?(puma[:grpc])).to be(false)
      rescue StandardError
        warn puma[:output]
        raise
      ensure
        Process.kill(:KILL, -puma[:pid]) rescue nil # rubocop:disable Style/RescueModifier
      end
    end
  end

  it "stops Puma if the gRPC server dies" do
    puma = start_puma(workers: 0)
    begin
      wait_until { grpc_up?(puma[:grpc]) }
      Process.kill(:KILL, grpc_child_pid(puma[:output]))
      wait_until(20) { exited?(puma[:pid]) }
      expect(puma[:output]).to include("exited unexpectedly; stopping Puma")
    ensure
      Process.kill(:KILL, -puma[:pid]) rescue nil # rubocop:disable Style/RescueModifier
    end
  end
end
