# frozen_string_literal: true

RSpec.describe Tonic::Rails::Base do # callbacks and rescue_from
  let(:testing) { Tonic::Rails::Testing }
  let(:events) { [] }

  def service(&block)
    log = events
    Class.new(EchoHandler) do
      define_method(:log) { |entry| log << entry }
      class_eval(&block)
    end
  end

  def error_for(svc, rpc, request)
    testing.call(svc, rpc, request)
    raise "expected an RpcError"
  rescue Tonic::Rails::RpcError => e
    e
  end

  describe "before_rpc" do
    it "runs before the handler with the decoded request available" do
      svc = service do
        before_rpc { log([:before, current_rpc, request.message]) }
      end
      testing.call(svc, :Unary, { message: "hi" })
      expect(events).to eq([[:before, "Unary", "hi"]])
    end

    it "stops the call when it raises" do
      svc = service do
        before_rpc :authenticate

        def authenticate
          raise Tonic::Rails::RpcError.new("no token", code: :unauthenticated) unless grpc_metadata["authorization"]
        end

        def unary(_request) = raise("handler must not run")
      end
      expect(error_for(svc, :Unary, {}).code_name).to eq(:unauthenticated)
      expect(testing.call(svc, :ServerStream, { count: 1 }, metadata: { "authorization" => "t" }).size).to eq(1)
    end

    it "honours only: and except: with any RPC name spelling" do
      svc = service do
        before_rpc(only: :Unary) { log(:only_unary) }
        before_rpc(except: [:unary, "ClientStream"]) { log(:not_unary) }
      end
      testing.call(svc, :Unary, {})
      expect(events).to eq([:only_unary])
      events.clear
      testing.call(svc, :ServerStream, { count: 1 })
      expect(events).to eq([:not_unary])
    end
  end

  describe "after_rpc and around_rpc" do
    it "runs after_rpc once a stream has been sent" do
      sender = Tonic::Rails::Testing::Sender.new
      svc = service do
        after_rpc { log([:after, current_rpc]) }
      end
      reader = Tonic::Rails::Testing::Reader.new([Echo::V1::EchoRequest.encode(Echo::V1::EchoRequest.new(count: 3))])
      svc.new.call_rpc("ServerStream", metadata: {}, reader: reader, sender: sender)
      expect(events).to eq([[:after, "ServerStream"]])
      expect(sender.messages.size).to eq(3)
    end

    it "skips after_rpc when the call fails" do
      svc = service do
        after_rpc { log(:after) }
      end
      error_for(svc, :Unary, { fail_with: "rpc:aborted" })
      expect(events).to eq([])
    end

    it "wraps the call with around_rpc" do
      svc = service do
        around_rpc :timed

        def timed
          log(:start)
          yield
          log(:finish)
        end
      end
      testing.call(svc, :Unary, {})
      expect(events).to eq(%i[start finish])
    end

    it "is inherited by subclasses" do
      parent = service { before_rpc { log(:parent) } }
      child = Class.new(parent) { before_rpc { log(:child) } }
      testing.call(child, :Unary, {})
      expect(events).to eq(%i[parent child])
    end
  end

  describe "rescue_from" do
    # Rescuable stores handlers by class name, so the class needs one.
    let(:not_found) { stub_const("SpecItemMissing", Class.new(StandardError)) }

    it "maps an exception to a status with code:" do
      klass = not_found
      svc = service do
        rescue_from klass, code: :not_found
        define_method(:unary) { |_request| raise klass, "Couldn't find Item 7" }
      end
      error = error_for(svc, :Unary, {})
      expect([error.code_name, error.message]).to eq([:not_found, "Couldn't find Item 7"])
    end

    it "accepts a fixed or computed message" do
      klass = not_found
      svc = service do
        rescue_from klass, code: :not_found, message: ->(e) { "missing: #{e.message.length}" }
        define_method(:unary) { |_request| raise klass, "abc" }
      end
      expect(error_for(svc, :Unary, {}).message).to eq("missing: 3")
    end

    it "supports with: and blocks" do
      klass = not_found
      svc = service do
        rescue_from(klass, with: :deny)
        rescue_from(ArgumentError) { |e| raise Tonic::Rails::RpcError.new("bad: #{e.message}", code: :invalid_argument) }
        define_method(:unary) { |request| raise(request.message == "arg" ? ArgumentError.new("x") : klass) }
        define_method(:deny) { |_e| raise Tonic::Rails::RpcError.new("denied", code: :permission_denied) }
      end
      expect(error_for(svc, :Unary, {}).code_name).to eq(:permission_denied)
      expect(error_for(svc, :Unary, { message: "arg" }).message).to eq("bad: x")
    end

    it "covers exceptions raised in callbacks" do
      klass = not_found
      svc = service do
        rescue_from klass, code: :unauthenticated, message: "who are you?"
        before_rpc { raise klass }
      end
      expect(error_for(svc, :Unary, {}).code_name).to eq(:unauthenticated)
    end

    it "turns a handler that returns instead of raising into INTERNAL" do
      klass = not_found
      svc = service do
        rescue_from(klass) { |_e| nil }
        define_method(:unary) { |_request| raise klass }
      end
      expect(error_for(svc, :Unary, {}).code_name).to eq(:internal)
    end

    it "leaves unmapped exceptions as INTERNAL" do
      expect(error_for(EchoHandler, :Unary, { fail_with: "crash" }).code_name).to eq(:internal)
    end

    it "rejects unknown codes when declared" do
      expect { service { rescue_from StandardError, code: :not_a_code } }.to raise_error(ArgumentError)
    end
  end
end
