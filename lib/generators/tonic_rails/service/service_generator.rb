# frozen_string_literal: true

require "rails/generators"

module TonicRails
  module Generators
    # rails generate tonic_rails:service Item GetItem ListItems:server ImportItems:client Sync:bidi
    #
    # Creates a .proto with the service and its messages, a handler with a
    # method per RPC, the handler's registration, and a test.
    class ServiceGenerator < ::Rails::Generators::NamedBase
      source_root File.expand_path("templates", __dir__)

      STREAM_KINDS = %w[unary server client bidi].freeze

      argument :rpcs, type: :array, default: [], banner: "Rpc[:server|:client|:bidi] ..."

      class_option :package, type: :string,
                             desc: "Protobuf package (default: <app name>.v1)"

      def check_rpcs
        rpc_specs.each do |rpc|
          next if rpc[:name].match?(/\A[A-Z][A-Za-z0-9]*\z/) && STREAM_KINDS.include?(rpc[:kind])

          raise Thor::Error, "Invalid RPC #{rpc[:raw].inspect}: use CamelCase names, optionally :server, :client or :bidi"
        end
      end

      def create_proto
        template "service.proto.tt", "app/protos/#{package_path}/#{service_file}_service.proto"
      end

      def create_handler
        template "handler.rb.tt", "app/grpc/#{service_file}_service.rb"
      end

      def register_handler
        initializer = "config/initializers/tonic_rails.rb"
        invoke "tonic_rails:install", [], skip_next_steps: true unless File.exist?(File.join(destination_root, initializer))
        inject_into_file initializer, "  Tonic::Rails.server.register(#{service_class})\n",
                         after: "Rails.application.config.to_prepare do\n"
      end

      def create_test
        if File.directory?(File.join(destination_root, "spec"))
          template "service_spec.rb.tt", "spec/grpc/#{service_file}_service_spec.rb"
        else
          template "service_test.rb.tt", "test/grpc/#{service_file}_service_test.rb"
        end
      end

      def show_next_steps
        say "\nNext: bin/rails tonic_rails:compile_protos, then fill in app/grpc/#{service_file}_service.rb", :green
      end

      private

      # "Item" and "ItemService" both mean the service ItemService.
      def service_base
        class_name.delete_suffix("Service")
      end

      def service_file
        service_base.underscore
      end

      def service_class
        "#{service_base}Service"
      end

      def package
        options[:package] || "#{app_name}.v1"
      end

      def app_name
        app = defined?(::Rails.application) && ::Rails.application
        name = app ? app.class.module_parent_name : File.basename(destination_root)
        name.to_s.underscore.tr("-", "_").presence || "app"
      end

      def package_path
        package.tr(".", "/")
      end

      def ruby_module
        package.split(".").map(&:camelize).join("::")
      end

      def full_service_name
        "#{package}.#{service_class}"
      end

      def rpc_specs
        list = rpcs.empty? ? ["Get#{service_base}"] : rpcs
        list.map do |raw|
          name, kind = raw.split(":", 2)
          { raw: raw, name: name.to_s, kind: kind || "unary", method: name.to_s.underscore }
        end
      end

      def proto_rpc_line(rpc)
        request = "#{rpc[:name]}Request"
        response = "#{rpc[:name]}Response"
        request = "stream #{request}" if %w[client bidi].include?(rpc[:kind])
        response = "stream #{response}" if %w[server bidi].include?(rpc[:kind])
        "rpc #{rpc[:name]}(#{request}) returns (#{response});"
      end
    end
  end
end
