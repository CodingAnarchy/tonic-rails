# frozen_string_literal: true

module Tonic
  module Rails
    class Client
      # One attempt at a client- or bidirectional-streaming call: writes its
      # requests from an Enumerable as it yields them, and reads its
      # responses.  The Enumerable isn't touched until the call has started,
      # so a call that fails before then can be retried with it.
      # @api private
      class StreamingCall
        # @param native [TonicRails::Native::ResponseStream]
        # @yieldparam request a request from the Enumerable
        # @yieldreturn [String] it, encoded
        def initialize(native, &encode)
          @native = native
          @encode = encode
          @started = false
          @stopped = false
          @lock = Mutex.new
        end

        # Send every request, then collect the response messages.
        # @return [Array] the native result, with the raw responses in place
        #   of the response
        def client_stream(requests)
          write(requests)
          responses = []
          while (bytes = @native.next)
            responses << bytes
          end
          result(responses)
        ensure
          @native.close
        end

        # Write the requests from another thread while yielding each raw
        # response, so a request can wait on an earlier response.  The
        # writer is stopped when the call ends, however it ends.
        # @return [Array] the native result
        def bidi(requests, &)
          thread = start_writer(requests)
          begin
            while (bytes = @native.next)
              yield bytes
            end
            result = result(nil)
          ensure
            @native.close
            # It may be waiting on the Enumerable for a request that will
            # never be sent.
            thread.kill if stop_writer
            error = thread.value
          end
          # The Enumerable raised, which cancelled the call.
          raise error if error

          result
        end

        private

        # Requests read from the Enumerable commit the call: it can't be
        # retried without them.
        def result(responses)
          code, message, _, headers, trailers, details, committed = @native.result
          [code, message, responses, headers, trailers, details, committed || @started]
        end

        # Write every request, then end the request stream.  If the
        # Enumerable raises, the call is cancelled and the error re-raised.
        def write(requests)
          return unless @native.ready

          @lock.synchronize do
            return if @stopped

            @started = true
          end
          finished = false
          requests.each do |request|
            break unless @native.push(@encode.call(request))
          end
          @native.close_send
          finished = true
        ensure
          @native.close if @started && !finished
        end

        # Write from a new thread, which keeps the handler's deadline.  Its
        # value is the exception the Enumerable raised, if any.
        def start_writer(requests)
          deadline = Client.current_deadline
          thread = Thread.new do
            Client.with_deadline(deadline) { write(requests) }
            nil
          rescue Exception => e # rubocop:disable Lint/RescueException -- handed to the reader
            e
          end
          thread.report_on_exception = false
          thread
        end

        # Stop the writer from starting, if it hasn't yet.
        # @return [Boolean] whether it had started
        def stop_writer
          @lock.synchronize do
            @stopped = true
            @started
          end
        end
      end
    end
  end
end
