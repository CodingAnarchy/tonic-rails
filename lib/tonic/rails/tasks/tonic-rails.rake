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
    abort "protoc not found on PATH. Install the Protocol Buffers compiler." unless system("protoc", "--version", out: File::NULL)

    FileUtils.mkdir_p(ruby_out)
    sh "protoc", "--ruby_out=#{ruby_out}", "-I#{proto_dir}", *protos
    puts "Compiled #{protos.size} proto file(s) to #{ruby_out}/"
  end
end
