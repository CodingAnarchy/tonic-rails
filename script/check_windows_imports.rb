# frozen_string_literal: true

# Fails if the Windows build of the extension imports anything from Ruby's
# DLL that isn't part of Ruby's C API.
#
#   ruby script/check_windows_imports.rb lib/tonic/rails/tonic_rails_native.so
#
# Ruby's DLL exports a few Windows API names of its own (`Sleep`, for one).
# The mingw linker can bind the extension's imports to those instead of to
# kernel32, and Ruby's versions expect a Ruby thread: called from a tokio
# thread, `Sleep` crashed the process (#41).

# Names Ruby's C API exports that the extension may use.
RUBY_API = /\A(rb_|ruby_|st_|onig|Init_|RUBY_)/

# Reads the import table of a PE32+ image (a Windows DLL).
class ImportTable
  def initialize(path)
    @path = path
    @data = File.binread(path)
  end

  # @return [Hash{String=>Array<String>}] imported names by DLL
  def imports
    pe = u32(0x3C)
    raise "#{@path}: not a PE image" unless @data[pe, 4] == "PE\0\0"

    sections = u16(pe + 6)
    optional = pe + 24
    raise "only PE32+ images are supported" unless u16(optional) == 0x20B

    @sections = Array.new(sections) do |i|
      at = optional + u16(pe + 20) + (i * 40)
      { rva: u32(at + 12), size: [u32(at + 8), u32(at + 16)].max, offset: u32(at + 20) }
    end
    descriptors(file_offset(u32(optional + 112 + 8)))
  end

  private

  def descriptors(at)
    result = Hash.new { |hash, dll| hash[dll] = [] }
    loop do
      name = u32(at + 12)
      break if name.zero?

      lookup = u32(at)
      thunks = u32(at + 16)

      dll = cstring(file_offset(name))
      thunk = file_offset(lookup.zero? ? thunks : lookup)
      while (entry = u64(thunk)) != 0
        result[dll] << (entry[63] == 1 ? "##{entry & 0xFFFF}" : cstring(file_offset(entry & 0x7FFFFFFF) + 2))
        thunk += 8
      end
      at += 20
    end
    result
  end

  def file_offset(rva)
    section = @sections.find { |s| rva >= s[:rva] && rva < s[:rva] + s[:size] }
    raise format("RVA 0x%x is outside every section", rva) unless section

    rva - section[:rva] + section[:offset]
  end

  def u16(at) = @data.unpack1("v", offset: at)
  def u32(at) = @data.unpack1("V", offset: at)
  def u64(at) = @data.unpack1("Q<", offset: at)
  def cstring(at) = @data[at, 256].unpack1("Z*")
end

path = ARGV.fetch(0) { abort "usage: #{$PROGRAM_NAME} path/to/tonic_rails_native.so" }
imports = ImportTable.new(path).imports
ruby_dlls = imports.keys.grep(/ruby\d+\.dll\z/i)
abort "#{path}: no import from Ruby's DLL; is this the Windows build?" if ruby_dlls.empty?

bad = ruby_dlls.flat_map { |dll| imports[dll].grep_v(RUBY_API).map { |name| "#{name} (from #{dll})" } }
if bad.empty?
  puts "#{path}: #{ruby_dlls.sum { |dll| imports[dll].size }} imports from #{ruby_dlls.join(', ')}, all Ruby C API"
else
  abort "#{path} imports non-Ruby functions from Ruby's DLL, which bind to Ruby's versions " \
        "instead of the system's:\n  #{bad.join("\n  ")}"
end
