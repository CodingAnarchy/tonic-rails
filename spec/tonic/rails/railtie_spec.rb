# frozen_string_literal: true

require "json"
require "open3"

# Boots a minimal Rails app in a subprocess (Rails must load before
# tonic-rails for the Railtie to register).
RSpec.describe "Tonic::Rails::Railtie" do
  def boot(app_config, after_boot = "")
    script = <<~RUBY
      require "rails"
      require "action_controller/railtie"
      require "tonic-rails"
      require "tonic/rails/testing"
      require #{File.expand_path('../../fixtures/lib/echo_handler', __dir__).dump}

      class RailtieTestApp < Rails::Application
        config.eager_load = false
        config.logger = Logger.new(nil)
        config.secret_key_base = "test"
        #{app_config}
      end
      RailtieTestApp.initialize!
      #{after_boot}
    RUBY
    output, status = Open3.capture2e(RbConfig.ruby, "-I", File.expand_path("../../../lib", __dir__), "-e", script)
    [output, status]
  end

  it "applies config.tonic_rails settings and the Rails logger" do
    output, status = boot(<<~CONFIG, <<~CHECK)
      config.tonic_rails.bind_address = "127.0.0.1:6000"
      config.tonic_rails.dispatch_threads = 9
      config.tonic_rails.reflection = true
      config.enable_reloading = true
    CONFIG
      c = Tonic::Rails.configuration
      puts JSON.generate([c.bind_address, c.dispatch_threads, c.reflection, c.logger.equal?(Rails.logger), c.reload_handlers])
    CHECK
    expect(status).to be_success, output
    expect(JSON.parse(output.lines.last)).to eq(["127.0.0.1:6000", 9, true, true, true])
  end

  it "rejects unknown settings" do
    output, status = boot('config.tonic_rails.bind_adress = "typo"')
    expect(status).not_to be_success
    expect(output).to include("unknown tonic-rails setting config.tonic_rails.bind_adress")
  end

  it "does not auto-start outside a server process" do
    output, status = boot("config.tonic_rails.auto_start = true", "puts Tonic::Rails.server.running?")
    expect(status).to be_success, output
    expect(output.lines.last.strip).to eq("false")
  end

  it "runs each RPC inside the Rails executor" do
    output, status = boot("", <<~CHECK)
      handler = Class.new(EchoHandler) do
        def unary(_request) = { "message" => Rails.application.executor.active?.to_s }
      end
      Object.const_set(:ExecutorProbe, handler)
      server = Tonic::Rails::Server.new
      server.register(ExecutorProbe)
      callback = server.send(:build_callback)
      bytes = Echo::V1::EchoRequest.encode(Echo::V1::EchoRequest.new)
      response = callback.call("/echo.v1.EchoService/Unary", {}, { reader: Tonic::Rails::Testing::Reader.new([bytes]) })
      puts Echo::V1::EchoResponse.decode(response).message
    CHECK
    expect(status).to be_success, output
    expect(output.lines.last.strip).to eq("true")
  end
end
