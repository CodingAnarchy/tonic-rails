# frozen_string_literal: true

require "fileutils"

namespace :tonic_rails do
  desc "Start the gRPC server (boots the Rails app with its registered services)"
  task serve: :environment do
    # Eager-load so handlers registered via config.to_prepare and
    # autoloaded constants are in place, as they would be in production.
    Rails.application.eager_load! unless Rails.application.config.eager_load
    Tonic::Rails.server.start
  end

  desc "List registered gRPC methods"
  task routes: :environment do
    router = Tonic::Rails.server.router
    if router.empty?
      puts "No gRPC services registered."
    else
      router.paths.sort.each { |path| puts path }
    end
  end

  desc "Generate Ruby protobuf classes (PROTO_DIR=app/protos RUBY_OUT=lib/grpc; requires protoc)"
  task :compile_protos do
    proto_dir = ENV.fetch("PROTO_DIR", "app/protos")
    ruby_out = ENV.fetch("RUBY_OUT", "lib/grpc")
    protos = Dir.glob(File.join(proto_dir, "**", "*.proto"))
    abort "No .proto files found in #{proto_dir}/" if protos.empty?
    version = begin
      IO.popen(%w[protoc --version], err: File::NULL, &:read)[/\d+(\.\d+)+/]
    rescue SystemCallError
      nil
    end
    abort "protoc not found on PATH. Install the Protocol Buffers compiler (22.0 or newer)." unless version
    # protoc before 22 emits the Ruby DSL that google-protobuf 4 removed
    # (`undefined method 'build' for DescriptorPool` at load time).
    major = version.split(".").then { |parts| parts.first == "3" ? parts[1].to_i : parts.first.to_i }
    if major < 22
      abort "protoc #{version} is too old for google-protobuf 4: install protoc 22.0 or newer " \
            "(distribution packages are often older; see https://github.com/protocolbuffers/protobuf/releases)."
    end

    FileUtils.mkdir_p(ruby_out)
    sh "protoc", "--ruby_out=#{ruby_out}", "-I#{proto_dir}", *protos
    puts "Compiled #{protos.size} proto file(s) to #{ruby_out}/"
  end
end
