# frozen_string_literal: true

# Vanilla Rails JSON baseline (see rails_app.rb):
#
#   bundle exec puma -t 16:16 -b tcp://127.0.0.1:50064 bench/rails_app.ru

require_relative "rails_app"
run BenchApp
