# frozen_string_literal: true

require_relative "lib/tonic/rails/version"

Gem::Specification.new do |spec|
  spec.name = "tonic-rails"
  spec.version = Tonic::Rails::VERSION
  spec.authors = ["Matt Tanous"]
  spec.email = ["matt.tanous@hey.com"]

  spec.summary = "High-performance gRPC endpoints for Rails, powered by tonic via a Rust native extension"
  spec.description = <<~DESC
    Tonic::Rails serves gRPC from Rails applications. The transport layer
    (HTTP/2 framing, connection management, TLS, health checks, reflection)
    is handled by tonic in Rust and exposed to Ruby through a native
    extension built with Magnus. Protobuf (de)serialization and business
    logic remain in Ruby, giving you Rails-grade handlers, routing,
    interceptors, and the Rails executor around every RPC.
  DESC
  spec.homepage = "https://github.com/CodingAnarchy/tonic-rails"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.3"
  spec.required_rubygems_version = ">= 3.3.11"

  spec.metadata = {
    "allowed_push_host" => "https://rubygems.org",
    "homepage_uri" => spec.homepage,
    "source_code_uri" => spec.homepage,
    "bug_tracker_uri" => "#{spec.homepage}/issues",
    "changelog_uri" => "#{spec.homepage}/blob/main/CHANGELOG.md",
    "rubygems_mfa_required" => "true",
  }

  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").select do |f|
      f.start_with?("lib/", "ext/") || %w[Cargo.toml Cargo.lock LICENSE README.md CHANGELOG.md].include?(f)
    end
  end
  spec.require_paths = ["lib"]

  # Rust native extension — compiled at install time via rb_sys + cargo
  # unless a precompiled platform gem is available.
  spec.extensions = ["ext/tonic_rails_native/extconf.rb"]

  spec.add_dependency "activesupport", ">= 7.1"
  spec.add_dependency "google-protobuf", "~> 4.27"
  spec.add_dependency "rb_sys", "~> 0.9.110"
end
