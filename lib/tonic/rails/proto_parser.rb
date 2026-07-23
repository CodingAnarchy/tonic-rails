# frozen_string_literal: true

module Tonic
  module Rails
    # Lightweight .proto file parser that extracts service RPC definitions.
    # Used by Base.rpc_service to auto-register RPCs from proto declarations
    # without manually calling `rpc :Method, Req, Resp` for each method.
    #
    # Only parses what we need: package name, service blocks, and rpc lines.
    # Does not validate proto syntax — assumes well-formed input from protoc.
    class ProtoParser
      ServiceInfo = Struct.new(:package, :service_name, :rpcs, keyword_init: true)
      RpcInfo = Struct.new(:name, :request_type, :response_type, :stream, keyword_init: true)

      # Parse a .proto file and return the ServiceInfo for the named service.
      # Returns nil if the service is not found.
      #
      # @param proto_file [String] path to the .proto file
      # @param service_name [String] the service name without package (e.g. "ProductService")
      # @return [ServiceInfo, nil]
      def self.parse_service(proto_file, service_name)
        content = File.read(proto_file)

        package = extract_package(content)
        return nil unless package

        rpcs = extract_service_rpcs(content, service_name)
        return nil if rpcs.nil?

        ServiceInfo.new(package: package, service_name: service_name, rpcs: rpcs)
      end

      # Scan all .proto files in a directory for a service matching the
      # fully-qualified name (e.g. "shop.ProductService").
      #
      # @param proto_dir [String] directory containing .proto files
      # @param full_name [String] fully-qualified service name (package.Service)
      # @return [ServiceInfo, nil]
      def self.find_service(proto_dir, full_name)
        package, service_name = full_name.split(".")
        return nil unless service_name

        Dir.glob("#{proto_dir}/**/*.proto").each do |file|
          info = parse_service(file, service_name)
          return info if info && info.package == package
        end

        nil
      end

      # Resolve a proto type name to a Ruby class.
      # "shop.GetProductRequest" → Shop::GetProductRequest
      # "GetProductRequest" (within package "shop") → Shop::GetProductRequest
      def self.resolve_type(type_name, package)
        # If the type is already fully-qualified (contains dots), use as-is.
        # Otherwise, prepend the package.
        full = type_name.include?(".") ? type_name : "#{package}.#{type_name}"

        # Convert "shop.GetProductRequest" → "Shop::GetProductRequest"
        # Capitalize only package segments; keep the message name verbatim
        # (capitalize lowercases the rest: "GetProductRequest" → "Getproductrequest").
        segs = full.split(".")
        type_name = segs.pop
        ruby_name = (segs.map(&:capitalize) + [type_name]).join("::")

        ruby_name.split("::").inject(Object) do |mod, const|
          mod.const_get(const)
        end
      rescue NameError
        raise Error, "Cannot resolve protobuf type #{full} → #{ruby_name}. " \
                     "Ensure proto files are compiled with: rake tonic_rails:compile_protos"
      end

      def self.extract_package(content)
        content.each_line do |line|
          trimmed = line.strip
          next if trimmed.start_with?("//")
          if trimmed.start_with?("package ")
            name = trimmed.sub(/^package\s+/, "").sub(/;.*$/, "").strip
            return name unless name.empty?
          end
        end
        nil
      end

      def self.extract_service_rpcs(content, service_name)
        lines = content.lines
        in_target_service = false
        brace_depth = 0
        rpcs = []

        lines.each do |line|
          trimmed = line.strip
          next if trimmed.start_with?("//")

          if !in_target_service
            # Look for "service <Name> {"
            if trimmed.match?(/^service\s+#{Regexp.escape(service_name)}\s*\{/)
              in_target_service = true
              brace_depth = count_braces(trimmed)
              next
            end
          else
            brace_depth += count_braces(trimmed)

            # Check for end of service block
            if brace_depth <= 0
              break
            end

            # Parse rpc lines: "rpc MethodName(RequestType) returns (ResponseType);"
            # Or streaming: "rpc MethodName(RequestType) returns (stream ResponseType);"
            if (match = trimmed.match(/^rpc\s+(\w+)\s*\(\s*(\w+)\s*\)\s*returns\s*\(\s*(?:(stream)\s+)?(\w+)\s*\)\s*;/))
              rpcs << RpcInfo.new(
                name: match[1],
                request_type: match[2],
                response_type: match[4],
                stream: match[3] == "stream",
              )
            end
          end
        end

        in_target_service ? rpcs : nil
      end

      def self.count_braces(line)
        line.count("{") - line.count("}")
      end

      private_class_method :extract_package, :extract_service_rpcs, :count_braces
    end
  end
end
