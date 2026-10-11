# frozen_string_literal: true

bind "tcp://127.0.0.1:#{ENV.fetch('HTTP_PORT')}"
workers Integer(ENV.fetch("WORKERS", "0"))
threads 1, 2
preload_app! if ENV["PRELOAD"] == "1"
plugin :tonic_rails
