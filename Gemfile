# frozen_string_literal: true

source "https://rubygems.org"

gemspec

gem "rake", ">= 13.0"
gem "rake-compiler", "~> 1.2"

group :development, :test do
  gem "bundler-audit", require: false
  gem "grpc", "~> 1.70", require: false # client for integration specs
  gem "rails", ">= 7.1", require: false
  gem "rspec", "~> 3.13"
  # parallel (a RuboCop dependency) 2.x requires Ruby 3.3; keep the
  # lockfile installable on every supported Ruby.
  gem "parallel", "< 2", require: false
  gem "rubocop", "~> 1.70", require: false
  gem "rubocop-rspec", require: false
end
