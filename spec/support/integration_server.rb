# frozen_string_literal: true

require "socket"
require "timeout"

# Starts a real tonic-rails server on a free port for integration specs.
module IntegrationServer
  def self.native_available?
    defined?(TonicRails::Native::Server) ? true : false
  end

  def self.free_port
    server = TCPServer.new("127.0.0.1", 0)
    server.addr[1]
  ensure
    server&.close
  end

  # @yield [Tonic::Rails::Configuration] to customise settings
  # @return [Hash] :server, :thread, :address
  def self.start(services: [EchoHandler])
    config = Tonic::Rails::Configuration.new
    config.bind_address = "127.0.0.1:#{free_port}"
    config.dispatch_threads = 4
    config.drain_timeout = 5
    config.logger = Logger.new(IO::NULL)
    yield config if block_given?

    server = Tonic::Rails::Server.new(configuration: config)
    services.each { |s| server.register(s) }
    ready = Queue.new
    thread = Thread.new do
      server.start(install_signal_handlers: false) { ready << true }
    rescue Exception => e # rubocop:disable Lint/RescueException
      ready << e
    end
    result = Timeout.timeout(10) { ready.pop }
    raise result if result.is_a?(Exception)

    { server: server, thread: thread, address: config.bind_address }
  end

  def self.stop(handle)
    return unless handle

    handle[:server].stop
    handle[:thread].join(15) || raise("server did not stop")
  end
end
