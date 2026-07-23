# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

# create_rust_makefile is the rb-sys helper that wires cargo into
# Ruby's native extension build system.  It generates a Makefile
# that invokes `cargo build --release` and copies the resulting
# cdylib into the gem's lib directory.
create_rust_makefile("tonic_rails_native")
