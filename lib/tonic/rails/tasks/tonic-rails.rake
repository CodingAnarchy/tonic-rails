# frozen_string_literal: true

namespace :grpc do
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

  desc "Generate FileDescriptorSet (.desc) from .proto files for prost-reflect"
  task :generate_descriptors do
    proto_dir = ENV["PROTO_DIR"] || "protos"
    output = ENV["DESC_OUTPUT"] || "app.desc"
    protos = Dir.glob("#{proto_dir}/**/*.proto")
    if protos.empty?
      abort "No .proto files found in #{proto_dir}/"
    end
    sh "protoc --descriptor_set_out=#{output} --include_imports -I#{proto_dir} #{protos.join(' ')}"
    puts "Generated #{output}"
  end
end
