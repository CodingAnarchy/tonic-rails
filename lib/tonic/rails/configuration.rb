# frozen_string_literal: true

require "logger"

module Tonic
  module Rails
    class Configuration
      # Address the gRPC server binds to.
      # @return [String] host:port, defaults to "0.0.0.0:50051"
      attr_accessor :bind_address

      # Maximum message size in bytes for both request and response.
      # @return [Integer]
      attr_accessor :max_message_size

      # Whether to auto-start the gRPC server alongside the Rails server.
      # Set to false if you prefer to start it explicitly (e.g. in a separate process).
      # @return [Boolean]
      attr_accessor :auto_start

      # Logger used by the gRPC server and dispatch layer.
      # @return [Logger]
      attr_accessor :logger

      # Number of tokio worker threads for the tonic transport layer.
      # Defaults to the number of CPU cores.
      # @return [Integer, nil]
      attr_accessor :worker_threads

      # Number of Ruby dispatch threads that drain the request channel.
      # Each thread calls rb_thread_call_without_gvl to wait for requests
      # and re-acquires the GVL to run handlers.  When one handler does
      # blocking I/O (sleep, DB, HTTP — releases the GVL internally),
      # another dispatch thread can pick up the next request and run its
      # handler concurrently.  Default: 4.
      # @return [Integer]
      attr_accessor :dispatch_threads

      # Path to a PEM-encoded TLS certificate file.  When both this and
      # tls_key_path are set, the server accepts TLS-encrypted gRPC
      # connections with ALPN "h2" negotiation.
      # @return [String, nil]
      attr_accessor :tls_cert_path

      # Path to a PEM-encoded TLS private key file.
      # @return [String, nil]
      attr_accessor :tls_key_path

      # Graceful shutdown drain timeout in seconds.  When the server
      # receives a shutdown signal, it stops accepting new connections
      # and waits this long for active connections to finish in-flight
      # requests before force-closing them.  Default: 10.
      # @return [Integer]
      attr_accessor :drain_timeout

      # Maximum number of concurrent TCP connections.  New connections
      # exceeding this limit are dropped.  Default: 10 000.
      # @return [Integer]
      attr_accessor :max_connections

      # When true, log every request (including successful ones) to stderr.
      # Errors are always logged regardless.  Default: false.
      # @return [Boolean]
      attr_accessor :log_requests

      # Array of interceptors (procs/lambdas) that wrap each request.
      # Each interceptor is called with (path, request, metadata, next_call)
      # and must call next_call.(path, request, metadata) to continue the
      # chain, or return early to short-circuit.  Interceptors are applied
      # in reverse order (last added = outermost wrapper).
      # @return [Array[Proc]]
      attr_accessor :interceptors

      # Enable gzip compression for gRPC responses. When true, the server
      # compresses responses larger than 1KB if the client indicates gzip
      # support via the grpc-accept-encoding header. Inbound compressed
      # requests are always decompressed regardless of this setting.
      # @return [Boolean]
      attr_accessor :compression

      # Enable gRPC reflection service (grpc.reflection.v1.ServerReflection).
      # When enabled, clients like grpcurl can discover services without
      # local .proto files.  Disable in production to avoid exposing API
      # schema.  Default: true (enabled for development convenience).
      # @return [Boolean]
      attr_accessor :reflection

      # Directory containing .proto files. Used by rpc_service to
      # auto-register RPCs from proto declarations. Default: "protos".
      # @return [String]
      attr_accessor :proto_dir
      def initialize
        @bind_address = "0.0.0.0:50051"
        @max_message_size = 4 * 1024 * 1024  # 4 MB
        @auto_start = false
        @logger = Logger.new($stdout)
        @worker_threads = nil
        @dispatch_threads = 4
        @tls_cert_path = nil
        @tls_key_path = nil
        @drain_timeout = 10
        @max_connections = 10_000
        @log_requests = false
        @interceptors = []
        @compression = false
        @reflection = true
        @proto_dir = "protos"
      end
    end
  end
end
