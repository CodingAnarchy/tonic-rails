# frozen_string_literal: true
require "fileutils"

namespace :tonic_rails do
  desc "Start the gRPC server (loads Rails environment + registered services)"
  task serve: :environment do
    Tonic::Rails.server.start
  end

  desc "List registered gRPC service routes"
  task routes: :environment do
    router = Tonic::Rails.server.router
    if router.empty?
      puts "No gRPC services registered."
    else
      router.paths.each { |path| puts path }
    end
  end

  desc "Compile .proto files to Ruby protobuf classes (requires protoc)"
  task :compile_protos do
    proto_dir = ENV["PROTO_DIR"] || "app/protos"
    ruby_out  = ENV["RUBY_OUT"]  || "app/grpc"
    protos = Dir.glob("#{proto_dir}/**/*.proto")
    if protos.empty?
      abort "No .proto files found in #{proto_dir}/"
    end
    unless system("protoc --version > /dev/null 2>&1")
      abort "protoc not found. Install it via: brew install protobuf"
    end
    FileUtils.mkdir_p(ruby_out)
    sh "protoc --ruby_out=#{ruby_out} -I#{proto_dir} #{protos.join(' ')}"
    puts "Compiled #{protos.size} proto file(s) to #{ruby_out}/"
  end
end
