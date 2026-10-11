# frozen_string_literal: true

require "bundler/gem_tasks"
require "rb_sys/extensiontask"

GEMSPEC = Gem::Specification.load("tonic-rails.gemspec")

# Platforms for precompiled gems (built in CI with rb-sys-dock).
CROSS_PLATFORMS = %w[
  aarch64-linux-gnu
  aarch64-linux-musl
  arm64-darwin
  x64-mingw-ucrt
  x86_64-darwin
  x86_64-linux-gnu
  x86_64-linux-musl
].freeze

RbSys::ExtensionTask.new("tonic_rails_native", GEMSPEC) do |ext|
  ext.lib_dir = "lib/tonic/rails"
  ext.cross_compile = true
  ext.cross_platform = CROSS_PLATFORMS
  # Precompiled gems need no build-time dependencies.
  ext.cross_compiling do |spec|
    spec.dependencies.reject! { |dep| dep.name == "rb_sys" }
  end
end

# Development-only tasks: the cross-compilation containers install
# just enough to build the extension.
begin
  require "rspec/core/rake_task"
  RSpec::Core::RakeTask.new(:spec)
  task spec: :compile
rescue LoadError
  nil
end

begin
  require "rubocop/rake_task"
  RuboCop::RakeTask.new
rescue LoadError
  nil
end

namespace :cargo do
  # Build against the Ruby running rake (rb-sys reads RUBY), in a target
  # dir of our own so editor tooling builds never mix in.
  env = { "RUBY" => RbConfig.ruby, "CARGO_TARGET_DIR" => File.expand_path("tmp/cargo", __dir__) }
  manifest = "--manifest-path=ext/tonic_rails_native/Cargo.toml"

  desc "Run the Rust unit tests"
  task(:test) { sh env, "cargo test #{manifest}" }

  desc "Check Rust formatting and lints"
  task :lint do
    sh env, "cargo fmt #{manifest} --check"
    sh env, "cargo clippy #{manifest} --all-targets -- -D warnings"
  end
end

task default: %i[spec cargo:test]
