# frozen_string_literal: true

# Example gRPC handler for demo.v1.ItemService.
#
# Run:  bin/rails tonic_rails:serve
# Try:  grpcurl -plaintext -d '{"id": 1}' 127.0.0.1:50052 demo.v1.ItemService/GetItem
#       grpcurl -plaintext -d '{"tag": "featured"}' 127.0.0.1:50052 demo.v1.ItemService/ListItems

require "items_pb"

class ItemService
  include Tonic::Rails::Base

  # Declares every RPC in demo.v1.ItemService from the generated descriptor.
  rpc_service "demo.v1.ItemService"

  # Handlers receive decoded protobuf messages (the fast path).  Without
  # this line they receive string-keyed Hashes instead.
  request_format :message

  # Unary: return a protobuf message (or a Hash).
  def get_item(request)
    record = store.find(request.id)
    raise Tonic::Rails::RpcError.new("item #{request.id} not found", code: :not_found) unless record

    to_proto(record)
  end

  def create_item(request)
    to_proto(create(request))
  end

  # Server streaming: return any Enumerable; each element is sent as it
  # is produced, with backpressure from the client.
  def list_items(request)
    Enumerator.new do |stream|
      store.each_tagged(request.tag) do |record|
        break if cancelled? # the server is shutting down

        stream << to_proto(record)
      end
    end
  end

  # Client streaming: `requests` yields each message as it arrives.
  def import_items(requests)
    created = requests.count { |request| create(request) }
    Demo::V1::ImportItemsResponse.new(created: created)
  end

  private

  def create(request)
    name = request.name.strip
    raise Tonic::Rails::RpcError.new("name is required", code: :invalid_argument) if name.empty?

    store.create(name: name, tags: request.tags.to_a)
  end

  def store
    ItemStore.instance
  end

  def to_proto(record)
    Demo::V1::Item.new(id: record.id, name: record.name, tags: record.tags)
  end
end
