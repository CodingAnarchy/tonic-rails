# frozen_string_literal: true

RSpec.describe Tonic::Rails::Configuration do
  subject(:config) { described_class.new }

  it "has production-safe defaults" do
    expect(config).to have_attributes(
      bind_address: "0.0.0.0:50051",
      max_message_size: 4 * 1024 * 1024,
      dispatch_threads: 4,
      reflection: false,
      compression: false,
      auto_start: false,
      max_connections: 64,
    )
    expect(config.validate!).to be(config)
  end

  it "requires TLS cert and key together" do
    config.tls_cert_path = "cert.pem"
    expect { config.validate! }.to raise_error(ArgumentError, /set together/)
    config.tls_key_path = "key.pem"
    expect(config).to be_tls
    expect(config.validate!).to be(config)
  end

  it "requires TLS for client certificate verification" do
    config.tls_client_ca_path = "ca.pem"
    expect { config.validate! }.to raise_error(ArgumentError, /requires tls_cert_path/)
  end

  {
    dispatch_threads: [0, -1, 1.5, nil],
    dispatch_queue_size: [0],
    max_message_size: [0, 2**33],
    max_connections: [-1],
    worker_threads: [0],
    drain_timeout: [-1, "10"],
  }.each do |name, values|
    values.each do |value|
      it "rejects #{name} = #{value.inspect}" do
        config.public_send("#{name}=", value)
        expect { config.validate! }.to raise_error(ArgumentError, /#{name}/)
      end
    end
  end

  it "validates the health check" do
    config.health_check = "yes"
    expect { config.validate! }.to raise_error(ArgumentError, /health_check/)
    config.health_check = -> { true }
    config.health_check_interval = 0
    expect { config.validate! }.to raise_error(ArgumentError, /health_check_interval/)
  end

  it "requires gRPC-Web settings to be lists of strings" do
    config.grpc_web_origins = "https://app.example.com"
    expect { config.validate! }.to raise_error(ArgumentError, /grpc_web_origins/)
    config.grpc_web_origins = ["https://app.example.com"]
    config.grpc_web_exposed_headers = [:x_id]
    expect { config.validate! }.to raise_error(ArgumentError, /grpc_web_exposed_headers/)
  end

  it "requires callable interceptors" do
    config.interceptors = [Object.new]
    expect { config.validate! }.to raise_error(ArgumentError, /interceptors/)
  end
end
