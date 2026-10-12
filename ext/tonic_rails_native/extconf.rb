# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

# Builds the Rust crate with cargo and installs the library as
# lib/tonic/rails/tonic_rails_native.{so,bundle}, matching
# `require "tonic/rails/tonic_rails_native"`.
create_rust_makefile("tonic/rails/tonic_rails_native") do |r|
  # On mingw, Ruby's DLL exports its own versions of some Windows functions
  # (`Sleep`, for one), and Ruby's import library comes before kernel32 on
  # the link line, so the extension's calls would bind to Ruby's.  Those
  # expect a Ruby thread and crash on any other (#41).  Linking kernel32
  # first binds them to the system's.  script/check_windows_imports.rb
  # checks the result in CI.
  r.extra_rustflags = ["-l", "dylib=kernel32"] if RbConfig::CONFIG["target_os"].to_s.include?("mingw")
end
