# frozen_string_literal: true

require "open3"
require "tmpdir"
require "rails/generators"
require "generators/tonic_rails/install/install_generator"
require "generators/tonic_rails/service/service_generator"

RSpec.describe "tonic_rails generators" do # rubocop:disable RSpec/DescribeClass
  let(:root) { File.join(@tmpdir, "shop") } # rubocop:disable RSpec/InstanceVariable

  around do |example|
    Dir.mktmpdir do |dir|
      @tmpdir = dir
      FileUtils.mkdir_p(File.join(dir, "shop", "config"))
      example.run
    end
  end

  def generate(klass, *args)
    klass.start([*args, "--quiet"], destination_root: root)
  end

  def read(path) = File.read(File.join(root, path))

  def exist?(path) = File.exist?(File.join(root, path))

  # A new-style app: lib/ is autoloaded (and eager-loaded), as `rails new`
  # sets up since Rails 7.1.
  def write_rails_app
    File.write(File.join(root, "config/application.rb"), <<~RUBY)
      require "rails"
      require "action_controller/railtie"
      require "tonic-rails"

      module Shop
        class Application < Rails::Application
          config.load_defaults 8.0
          config.autoload_lib(ignore: %w[assets tasks])
          config.eager_load = true
          config.logger = Logger.new(nil)
          config.secret_key_base = "test"
        end
      end
    RUBY
  end

  describe "tonic_rails:install" do
    it "creates the initializer and directories" do
      generate(TonicRails::Generators::InstallGenerator)
      expect(read("config/initializers/tonic_rails.rb"))
        .to include("Rails.autoloaders.main.ignore(generated)", "Rails.application.config.to_prepare do")
      expect(exist?("app/protos/.keep") && exist?("lib/grpc/.keep")).to be(true)
    end

    it "adds the Puma plugin once" do
      File.write(File.join(root, "config/puma.rb"), "threads 5, 5\n")
      2.times { generate(TonicRails::Generators::InstallGenerator, "--force") }
      expect(read("config/puma.rb").scan("plugin :tonic_rails").size).to eq(1)
    end

    it "can skip Puma" do
      File.write(File.join(root, "config/puma.rb"), "threads 5, 5\n")
      generate(TonicRails::Generators::InstallGenerator, "--skip-puma")
      expect(read("config/puma.rb")).not_to include("tonic_rails")
    end
  end

  describe "tonic_rails:service" do
    it "creates the proto, handler, registration and spec" do
      FileUtils.mkdir_p(File.join(root, "spec"))
      generate(TonicRails::Generators::ServiceGenerator, "Item", "GetItem", "ListItems:server", "ImportItems:client", "Sync:bidi")

      proto = read("app/protos/shop/v1/item_service.proto")
      expect(proto).to include(
        "package shop.v1;",
        "service ItemService {",
        "rpc GetItem(GetItemRequest) returns (GetItemResponse);",
        "rpc ListItems(ListItemsRequest) returns (stream ListItemsResponse);",
        "rpc ImportItems(stream ImportItemsRequest) returns (ImportItemsResponse);",
        "rpc Sync(stream SyncRequest) returns (stream SyncResponse);",
        "message SyncResponse {",
      )
      handler = read("app/grpc/item_service.rb")
      expect(handler).to include(
        'require "shop/v1/item_service_pb"', 'rpc_service "shop.v1.ItemService"',
        "def get_item(request)", "def list_items(request)", "def import_items(requests)", "def sync(requests)"
      )
      expect(read("config/initializers/tonic_rails.rb"))
        .to include("Rails.application.config.to_prepare do\n  Tonic::Rails.server.register(ItemService)\n")
      expect(read("spec/grpc/item_service_spec.rb")).to include("raise_grpc_error(:unimplemented)")
    end

    it "writes a Minitest test when the app has no spec directory" do
      generate(TonicRails::Generators::ServiceGenerator, "ItemService")
      expect(read("test/grpc/item_service_test.rb")).to include("assert_grpc_error(:unimplemented)", "ItemServiceTest")
      expect(read("app/protos/shop/v1/item_service.proto")).to include("rpc GetItem(GetItemRequest)")
    end

    it "accepts a custom package" do
      generate(TonicRails::Generators::ServiceGenerator, "Order", "--package", "billing.v2")
      expect(read("app/protos/billing/v2/order_service.proto")).to include("package billing.v2;")
      expect(read("app/grpc/order_service.rb")).to include('rpc_service "billing.v2.OrderService"', "Billing::V2::GetOrderRequest")
    end

    it "registers several services in the same initializer" do
      generate(TonicRails::Generators::ServiceGenerator, "Item")
      generate(TonicRails::Generators::ServiceGenerator, "Order")
      expect(read("config/initializers/tonic_rails.rb")).to include("register(ItemService)", "register(OrderService)")
    end

    it "rejects malformed RPC names and stream kinds" do
      %w[get_item GetItem:sideways].each do |rpc|
        generator = TonicRails::Generators::ServiceGenerator.new(["Item", rpc], {}, destination_root: root)
        expect { generator.invoke_all }.to raise_error(Thor::Error, /Invalid RPC/)
      end
    end

    it "boots in a Rails app that autoloads lib/ and eager-loads" do
      skip "protoc not installed" unless system("protoc", "--version", out: File::NULL, err: File::NULL)

      write_rails_app
      generate(TonicRails::Generators::ServiceGenerator, "Item", "GetItem")
      Dir.chdir(root) do
        system("protoc", "--ruby_out=lib/grpc", "-Iapp/protos", "app/protos/shop/v1/item_service.proto", exception: true)
      end
      script = <<~RUBY
        require "./config/application"
        Rails.application.initialize!
        require "tonic/rails/testing"
        puts Tonic::Rails.server.router.paths.inspect
        begin
          Tonic::Rails::Testing.call(ItemService, :GetItem, Shop::V1::GetItemRequest.new)
        rescue Tonic::Rails::RpcError => e
          puts e.code_name
        end
      RUBY
      output, status = Open3.capture2e(RbConfig.ruby, "-I", File.expand_path("../../lib", __dir__), "-e", script, chdir: root)
      expect(status).to be_success, output
      expect(output).to include('["/shop.v1.ItemService/GetItem"]', "unimplemented")
    end

    it "generates code that compiles and runs" do
      skip "protoc not installed" unless system("protoc", "--version", out: File::NULL, err: File::NULL)

      generate(TonicRails::Generators::ServiceGenerator, "Item", "GetItem", "ListItems:server")
      Dir.chdir(root) do
        system("protoc", "--ruby_out=lib/grpc", "-Iapp/protos", "app/protos/shop/v1/item_service.proto", exception: true)
      end
      script = <<~RUBY
        require "tonic-rails"
        require "tonic/rails/testing"
        $LOAD_PATH.unshift("lib/grpc")
        require "./app/grpc/item_service"
        kinds = ItemService.grpc_rpcs.transform_values { |s| [s[:request_stream], s[:response_stream]] }
        puts kinds.inspect
        begin
          Tonic::Rails::Testing.call(ItemService, :GetItem, Shop::V1::GetItemRequest.new)
        rescue Tonic::Rails::RpcError => e
          puts e.code_name
        end
      RUBY
      output, status = Open3.capture2e(RbConfig.ruby, "-I", File.expand_path("../../lib", __dir__), "-e", script, chdir: root)
      expect(status).to be_success, output
      expect(output).to include('"GetItem" => [false, false]', '"ListItems" => [false, true]', "unimplemented")
    end
  end
end
