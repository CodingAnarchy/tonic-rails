# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

require "rake/extensiontask"

# Build the Rust native extension as part of `rake compile`
Rake::ExtensionTask.new("tonic_rails_native") do |ext|
  ext.ext_dir = "ext/tonic_rails_native"
  ext.lib_dir = "lib/tonic/rails"
  ext.source_pattern = "**/*.{rs,toml,proto}"
end

task :spec => :compile
task default: :spec
