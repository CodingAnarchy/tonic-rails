# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

# On mingw, Ruby's DLL exports its own versions of some Windows functions
# (`Sleep`, for one), which expect a Ruby thread and crash on any other
# (#41).  rb-sys links Ruby's import library ahead of kernel32, so the
# extension's calls would bind to Ruby's.  rb-sys passes `LIBS` first, so
# naming kernel32 there binds them to the system's instead.
# script/check_windows_imports.rb checks the result in CI.
if RbConfig::CONFIG["target_os"].to_s.include?("mingw")
  RbConfig::CONFIG["LIBS"] = "-lkernel32 #{RbConfig::CONFIG['LIBS']}"
end

# Builds the Rust crate with cargo and installs the library as
# lib/tonic/rails/tonic_rails_native.{so,bundle}, matching
# `require "tonic/rails/tonic_rails_native"`.
create_rust_makefile("tonic/rails/tonic_rails_native")
