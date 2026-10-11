# frozen_string_literal: true

require "logger"
require "tonic-rails"
require "tonic/rails/testing"
require_relative "fixtures/lib/echo_handler"

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.disable_monkey_patching!
  config.warnings = true
  config.default_formatter = "doc" if config.files_to_run.one?
  config.order = :random
  Kernel.srand config.seed

  config.before(:suite) do
    Tonic::Rails.configuration.logger = Logger.new(IO::NULL)
  end
end
