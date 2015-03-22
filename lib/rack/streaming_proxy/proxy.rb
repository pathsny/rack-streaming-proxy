require 'rack'
require 'logger'
require 'rack/streaming_proxy/session'
require 'rack/streaming_proxy/request'
require 'thread'

class Rack::StreamingProxy::Proxy

  class << self
    attr_accessor :logger, :log_verbosity, :num_retries_on_5xx, :raise_on_5xx

    def set_default_configuration
      # Logs to stdout by default unless configured with another logger via Railtie.
      @logger ||= Logger.new(STDOUT)

      # At :low verbosity by default -- will not output :debug level messages.
      # :high verbosity outputs :debug level messages.
      # This is independent of the Logger's log_level, as set in Rails, for example,
      # although the Logger's level can override this setting.
      @log_verbosity ||= :low

      # No retries are performed by default.
      @num_retries_on_5xx ||= 0

      # If the proxy cannot recover from 5xx's through retries (see num_retries_on_5xx),
      # then it by default passes through the content from the destination
      # e.g. the Apache error page. If you want an exception to be raised instead so
      # you can handle it yourself (i.e. display your own error page), set raise_on_5xx to true.
      @raise_on_5xx ||= false
    end

    def log(level, message)
      unless log_verbosity == :low && level == :debug
        @logger.send level, "[Rack::StreamingProxy] #{message}"
      end
    end

  end

  # The block provided to the initializer is given a Rack::Request
  # and should return:
  #
  #   * nil/false to skip the proxy and continue down the stack
  #   * a complete uri (with query string if applicable) to proxy to
  #
  # Example:
  #
  #   use Rack::StreamingProxy::Proxy do |req|
  #     if req.path.start_with?('/search')
  #       "http://some_other_service/search?#{req.query}"
  #     end
  #   end
  #
  # Most headers, request body, and HTTP method are preserved.
  #
  def initialize(app, &block)
    self.class.set_default_configuration
    @app   = app
    @block = block
  end

  def call(env)
    current_request = Rack::Request.new(env)

    # Decide whether this request should be proxied.
    if destination_uri = @block.call(current_request)
      self.class.log :info, "Starting proxy request to: #{destination_uri}"

      request  = Rack::StreamingProxy::Request.new(destination_uri, current_request)
      begin
        http_session = Net::HTTP.new(request.host, request.port)
        http_session.use_ssl = request.use_ssl?
        env['rack.hijack'].call
        io = env['rack.hijack_io']
        Thread.new {
          http_session.start do |session|
              socket, io = nil
              begin
                req = request.http_request
                session.send(:begin_transport, req)
                socket = session.instance_variable_get(:@socket)
                req.exec socket, Net::HTTP::HTTPVersion, req.path
                catch(:response) {
                  env['rack.hijack'].call
                  io = env['rack.hijack_io']
                  IO.copy_stream(socket.io, io)
                }  
            rescue Exception => e
              puts e
              socket.close if socket and not socket.closed?
              io.close if io and not io.closed?
            end  
          end
        }
      rescue Exception => e # Rescuing only for the purpose of logging to rack.errors
        log_rack_error(env, e)
        raise e
      end
    # Continue down the middleware stack if the request is not to be proxied.
    else
      @app.call(env)
    end
  end

private

  def log_rack_error(env, e)
    env['rack.errors'].puts e.message
    env['rack.errors'].puts e.backtrace #.collect { |line| "\t" + line }
    env['rack.errors'].flush
  end

end
