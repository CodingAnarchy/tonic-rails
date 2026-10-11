# frozen_string_literal: true

require "grpc"
require "grpc/health/v1/health_services_pb"
require "timeout"
require_relative "../fixtures/lib/echo_services_pb"
require_relative "../support/integration_server"

# Multi-process mode (config.processes > 1), run as `bin/rails
# tonic_rails:serve` would: a supervisor process with forked workers,
# stopped with real signals.
RSpec.describe "Multi-process mode", :integration do
  before(:all) do
    skip "fork" unless Process.respond_to?(:fork)
    skip "native extension not compiled" unless IntegrationServer.native_available?
  end

  def start_cluster(processes: 3, setup: "")
    port = IntegrationServer.free_port
    script = <<~RUBY
      $stdout.sync = true
      require "tonic-rails"
      require #{File.expand_path('../fixtures/lib/echo_handler', __dir__).dump}

      # Answers with the pid of the worker that served the call.
      class PidHandler < EchoHandler
        def unary(request)
          work(request)
          { "message" => Process.pid.to_s }
        end
      end

      Tonic::Rails.configure do |c|
        c.bind_address = "127.0.0.1:#{port}"
        c.processes = #{processes}
        c.drain_timeout = 5
        c.logger = Logger.new($stdout)
      end
      #{setup}
      Tonic::Rails.server.register(PidHandler)
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

  def kill(cluster)
    Process.kill(:KILL, cluster[:pid]) if cluster
  rescue Errno::ESRCH
    nil
  end

  # Each channel gets its own connection (the grpc gem otherwise shares
  # one between channels to the same address).
  def channel(address)
    GRPC::Core::Channel.new(address, { "grpc.use_local_subchannel_pool" => 1 }, :this_channel_is_insecure)
  end

  def stub(channel)
    Echo::V1::EchoService::Stub.new(nil, nil, channel_override: channel)
  end

  def served_by(channel, **request)
    stub(channel).unary(Echo::V1::EchoRequest.new(**request), deadline: Time.now + 10).message.to_i
  end

  def wait_for_output(cluster, pattern, timeout: 20)
    Timeout.timeout(timeout) { sleep 0.05 until cluster[:output].match?(pattern) }
  rescue Timeout::Error
    raise "timed out waiting for #{pattern.inspect}; output:\n#{cluster[:output]}"
  end

  def alive?(pid)
    Process.kill(0, pid)
    true
  rescue Errno::ESRCH
    false
  end

  it "spreads connections over every worker, sharing one port" do
    cluster = start_cluster
    wait_for_output(cluster, /serving echo\.v1\.EchoService/)
    expect(cluster[:output]).to include("(3 processes × 4 dispatch threads each)")

    pids = Array.new(9) { served_by(channel(cluster[:address])) }
    expect(pids.uniq.size).to be >= 2
    expect(pids).not_to include(cluster[:pid])
  ensure
    kill(cluster)
  end

  it "drains every worker on SIGTERM, reporting NOT_SERVING, and exits once all have stopped" do
    cluster = start_cluster
    wait_for_output(cluster, /serving echo\.v1\.EchoService/)
    channels = Array.new(6) { channel(cluster[:address]) }
    workers = channels.map { |c| served_by(c) }.uniq

    health = Grpc::Health::V1::Health::Stub.new(nil, nil, channel_override: channels.first)
    watch = health.watch(Grpc::Health::V1::HealthCheckRequest.new(service: ""), return_op: true)
    statuses = Queue.new
    watcher = Thread.new { watch.execute.each { |response| statuses << response.status } rescue nil } # rubocop:disable Style/RescueModifier
    expect(statuses.pop).to eq(:SERVING)

    in_flight = channels.map { |c| Thread.new { served_by(c, sleep_ms: 1000) } }
    sleep 0.3
    Process.kill(:TERM, cluster[:pid])

    expect(Timeout.timeout(5) { statuses.pop }).to eq(:NOT_SERVING)
    watch.cancel
    expect(in_flight.map(&:value)).to all(be_positive)
    _, status = Timeout.timeout(20) { Process.wait2(cluster[:pid]) }
    expect(status).to be_success
    expect(cluster[:output]).to include("shutting down 3 workers", "tonic-rails: stopped", "exited cleanly")
    expect(workers.select { |pid| alive?(pid) }).to be_empty
  ensure
    watcher&.kill
    kill(cluster)
  end

  it "replaces a worker that dies, and logs it" do
    cluster = start_cluster(processes: 2)
    wait_for_output(cluster, /serving echo\.v1\.EchoService/)
    victim = served_by(channel(cluster[:address]))

    Process.kill(:KILL, victim)
    wait_for_output(cluster, /worker \d \(pid #{victim}\) exited unexpectedly \(signal KILL\); starting a replacement/)
    wait_for_output(cluster, /worker \d \(pid \d+\) serving/)

    pids = Array.new(6) { served_by(channel(cluster[:address])) }
    expect(pids).not_to include(victim)
  ensure
    kill(cluster)
  end

  it "fails to start, stopping the other workers, when a worker can't boot" do
    cluster = start_cluster(setup: <<~RUBY)
      Tonic::Rails.configuration.on_worker_boot = lambda do |index|
        raise "no database" if index == 1
        puts "worker \#{index} booted in \#{Process.pid}"
      end
    RUBY
    _, status = Timeout.timeout(20) { Process.wait2(cluster[:pid]) }
    expect(status).not_to be_success
    expect(cluster[:output]).to include("worker 1: RuntimeError: no database", "worker 1 exited during startup (exit status 1)")
    expect(cluster[:output]).not_to include("exited cleanly")
    booted = cluster[:output].scan(/worker \d booted in (\d+)/).flatten.map(&:to_i)
    Timeout.timeout(10) { sleep 0.05 while booted.any? { |pid| alive?(pid) } }
  ensure
    kill(cluster)
  end

  it "stops its workers if the supervisor is killed" do
    cluster = start_cluster(processes: 2)
    wait_for_output(cluster, /serving echo\.v1\.EchoService/)
    workers = Array.new(4) { served_by(channel(cluster[:address])) }.uniq

    Process.kill(:KILL, cluster[:pid])
    Process.wait(cluster[:pid])
    Timeout.timeout(15) { sleep 0.1 while workers.any? { |pid| alive?(pid) } }
    expect(workers.select { |pid| alive?(pid) }).to be_empty
  ensure
    kill(cluster)
  end
end
