# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

# Builds the Rust crate with cargo and installs the library as
# lib/tonic/rails/tonic_rails_native.{so,bundle}, matching
# `require "tonic/rails/tonic_rails_native"`.
create_rust_makefile("tonic/rails/tonic_rails_native")
