# frozen_string_literal: true

require_relative "lib/tonic/rails/version"

Gem::Specification.new do |spec|
  spec.name = "tonic-rails"
  spec.version = Tonic::Rails::VERSION
  spec.authors = ["Shopify"]
  spec.email = ["gems@shopify.com"]

  spec.summary = "gRPC endpoints for Rails, powered by tonic via a Rust native extension"
  spec.description = <<~DESC
    Tonic::Rails integrates gRPC into Rails applications. The transport layer
    (HTTP/2 framing, connection management) is handled by tonic, exposed to
    Ruby through a Rust native extension built with Magnus. Protobuf
    (de)serialization and business logic remain in Ruby, giving you
    Rails-grade controllers, routing, and middleware for your gRPC services.
  DESC
  spec.homepage = "https://github.com/shopify/tonic-rails"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1"

  spec.metadata = {
    "allowed_push_host" => "https://rubygems.org",
    "homepage_uri" => spec.homepage,
    "source_code_uri" => spec.homepage,
    "changelog_uri" => "#{spec.homepage}/blob/main/CHANGELOG.md",
    "rubygems_mfa_required" => "true",
  }

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    Dir["{lib,ext,sig}/**/*", "*.gemspec", "Gemfile", "Rakefile", "LICENSE*"]
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Rust native extension — built at install time via extconf.rb + cargo
  spec.extensions = ["ext/tonic_rails_native/extconf.rb"]

  spec.add_dependency "rake", ">= 13.0"
  spec.add_dependency "activesupport", ">= 6.0"
  spec.add_dependency "google-protobuf", "~> 4.0"

  spec.add_development_dependency "rb_sys", "~> 0.9"

  spec.add_development_dependency "bundler", ">= 2.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "rails", ">= 7.0"
end
