# frozen_string_literal: true

require "rails/generators"

module TonicRails
  module Generators
    # rails generate tonic_rails:install
    #
    # Sets an app up for gRPC: an initializer (load path for generated
    # protobuf code, configuration, handler registration), the proto and
    # generated-code directories, and the Puma plugin.
    class InstallGenerator < ::Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      class_option :skip_puma, type: :boolean, default: false,
                               desc: "Don't add `plugin :tonic_rails` to config/puma.rb"
      class_option :skip_next_steps, type: :boolean, default: false, hide: true

      def create_initializer
        template "tonic_rails.rb.tt", "config/initializers/tonic_rails.rb"
      end

      def create_directories
        create_file "app/protos/.keep"
        create_file "lib/grpc/.keep"
      end

      def add_puma_plugin
        return if options[:skip_puma]

        puma = "config/puma.rb"
        return unless File.exist?(File.join(destination_root, puma))
        return if File.read(File.join(destination_root, puma)).match?(/plugin\s+:tonic_rails/)

        append_to_file puma, "\n# Run the tonic-rails gRPC server alongside Puma.\nplugin :tonic_rails\n"
      end

      def show_next_steps
        return if options[:skip_next_steps]

        say "\nNext: rails generate tonic_rails:service Item GetItem ListItems:server", :green
      end
    end
  end
end
