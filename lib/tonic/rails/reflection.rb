# frozen_string_literal: true

require "set"
require "google/protobuf/descriptor_pb"

module Tonic
  module Rails
    # Builds the protobuf file descriptors served by gRPC reflection from
    # the descriptors already loaded in Ruby (the generated `_pb.rb` files).
    module Reflection
      module_function

      # @param service_names [Array<String>] fully-qualified service names
      # @return [Array<String>] serialized google.protobuf.FileDescriptorProto,
      #   covering each service's file and every file its messages reference
      def file_descriptors(service_names)
        pool = Google::Protobuf::DescriptorPool.generated_pool
        files = {}
        seen = Set.new
        pending = []

        service_names.each do |name|
          service = pool.lookup(name)
          next unless service.is_a?(Google::Protobuf::ServiceDescriptor)

          add_file(files, service.file_descriptor)
          service.each { |method| pending << method.input_type << method.output_type }
        end

        until pending.empty?
          descriptor = pending.pop
          next unless seen.add?(descriptor.name)

          add_file(files, descriptor.file_descriptor)
          next unless descriptor.is_a?(Google::Protobuf::Descriptor)

          descriptor.each do |field|
            subtype = field.subtype
            pending << subtype if subtype
          end
        end

        files.values.map { |fd| Google::Protobuf::FileDescriptorProto.encode(fd.to_proto) }
      end

      def add_file(files, file_descriptor)
        files[file_descriptor.name] ||= file_descriptor
      end
    end
  end
end
