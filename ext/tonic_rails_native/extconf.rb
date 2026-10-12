# frozen_string_literal: true

require "fileutils"
require "mkmf"
require "rb_sys/mkmf"
require "shellwords"

# On mingw, Ruby's DLL exports its own `Sleep`, which releases the GVL
# around the wait: on a thread Ruby doesn't own it crashes, and on a Ruby
# thread that has already released the GVL it takes the GVL back from
# whichever thread holds it (#41).  rb-sys links Ruby's import library ahead of
# kernel32, so the extension's `Sleep` (from mingw's DLL startup code, and
# from std via crossbeam) would bind to Ruby's.  Link a one-function import
# library binding `Sleep` to kernel32 ahead of Ruby's instead.  (Putting
# all of kernel32 first goes too far: some toolchains' kernel32 import
# libraries claim functions kernel32.dll doesn't export.)
# script/check_windows_imports.rb checks the result in CI.
if RbConfig::CONFIG["target_os"].to_s.include?("mingw")
  dir = File.expand_path("kernel32_sleep")
  FileUtils.mkdir_p(dir)
  File.write(File.join(dir, "sleep.def"), "LIBRARY kernel32.dll\nEXPORTS\nSleep\n")
  dlltool = RbConfig::CONFIG["DLLTOOL"].to_s.empty? ? "dlltool" : RbConfig::CONFIG["DLLTOOL"]
  unless system(*Shellwords.split(dlltool), "-d", File.join(dir, "sleep.def"),
                "-l", File.join(dir, "libtonic_rails_sleep.a"))
    abort "tonic-rails: #{dlltool} failed to build the kernel32 Sleep import library"
  end
  RbConfig::CONFIG["LIBS"] = "-L#{Shellwords.escape(dir)} -ltonic_rails_sleep #{RbConfig::CONFIG['LIBS']}"
end

# Builds the Rust crate with cargo and installs the library as
# lib/tonic/rails/tonic_rails_native.{so,bundle}, matching
# `require "tonic/rails/tonic_rails_native"`.
create_rust_makefile("tonic/rails/tonic_rails_native")
