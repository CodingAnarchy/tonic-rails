# frozen_string_literal: true

module Tonic
  module Rails
    # Rails integration:
    # - `config.tonic_rails.*` settings are copied onto Tonic::Rails.configuration
    #   (an initializer calling Tonic::Rails.configure can still override them)
    # - the Rails logger is used by default
    # - handler classes are looked up per request when code reloading is on
    # - `auto_start` runs the server inside `rails server`
    # - rake tasks: tonic_rails:serve, tonic_rails:routes, tonic_rails:compile_protos
    class Railtie < ::Rails::Railtie
      config.tonic_rails = ::ActiveSupport::OrderedOptions.new

      initializer "tonic_rails.configuration" do |app|
        configuration = Tonic::Rails.configuration
        configuration.logger = ::Rails.logger if ::Rails.logger
        configuration.reload_handlers = Railtie.reloading_enabled?(app)

        app.config.tonic_rails.each do |key, value|
          setter = "#{key}="
          raise ArgumentError, "unknown tonic-rails setting config.tonic_rails.#{key}" unless configuration.respond_to?(setter)

          configuration.public_send(setter, value)
        end
      end

      config.after_initialize do
        configuration = Tonic::Rails.configuration
        # Only inside an actual web server process — not consoles, rake
        # tasks, or test runs.  With Puma, prefer `plugin :tonic_rails`
        # (lib/puma/plugin/tonic_rails.rb), which also covers cluster mode.
        if configuration.auto_start && defined?(::Rails::Server)
          if configuration.multi_process?
            raise Error, "auto_start can't run multiple processes (processes = #{configuration.processes}); " \
                         "use `plugin :tonic_rails` in config/puma.rb or `bin/rails tonic_rails:serve`"
          end

          server = Tonic::Rails.server
          thread = Thread.new do
            Thread.current.name = "tonic-rails"
            server.start(install_signal_handlers: false)
          end
          thread.report_on_exception = true
          at_exit do
            server.stop
            thread.join(configuration.drain_timeout.to_f + 5)
          end
        end
      end

      def self.reloading_enabled?(app)
        app.config.respond_to?(:enable_reloading) ? app.config.enable_reloading : !app.config.cache_classes
      end

      rake_tasks do
        load File.expand_path("tasks/tonic-rails.rake", __dir__)
      end
    end
  end
end
