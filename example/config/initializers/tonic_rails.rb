# frozen_string_literal: true

# Register gRPC services with tonic-rails.
# Services are defined in app/grpc/ and included via Tonic::Rails::Base.

require_relative "../../app/grpc/product_service"

Tonic::Rails.server.register(ProductService)
