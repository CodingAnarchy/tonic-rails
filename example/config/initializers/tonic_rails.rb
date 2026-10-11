# frozen_string_literal: true

# Handlers live in app/grpc and are autoloaded, so register them in
# to_prepare: it runs after boot and again after each code reload.
Rails.application.config.to_prepare do
  Tonic::Rails.server.register(ItemService)
end
