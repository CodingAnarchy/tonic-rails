# frozen_string_literal: true

# Thread-safe in-memory stand-in for an ActiveRecord model.
class ItemStore
  Record = Struct.new(:id, :name, :tags, keyword_init: true)

  def self.instance
    @instance ||= new.tap do |store|
      store.create(name: "First item", tags: %w[featured])
      store.create(name: "Second item", tags: %w[featured archived])
      store.create(name: "Third item", tags: [])
    end
  end

  def initialize
    @records = {}
    @next_id = 0
    @lock = Mutex.new
  end

  def find(id)
    @lock.synchronize { @records[id] }
  end

  def create(name:, tags:)
    @lock.synchronize do
      @next_id += 1
      @records[@next_id] = Record.new(id: @next_id, name: name, tags: tags)
    end
  end

  def each_tagged(tag, &)
    records = @lock.synchronize { @records.values }
    records = records.select { |r| r.tags.include?(tag) } unless tag.empty?
    records.each(&)
  end
end
