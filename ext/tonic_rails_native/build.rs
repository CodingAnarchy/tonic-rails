//! Build script — compiles .proto files with tonic-build.
//!
//! Generates Rust message types + service traits from the bundled
//! `protos/shop.proto`.  Also writes an encoded `FileDescriptorSet`
//! binary for tonic-reflection.
//!
//! User apps can override the proto directory with the `PROTO_DIR` env var.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check for PROTO_DIR override (user app protos), otherwise use bundled proto.
    let proto_dir = std::env::var("PROTO_DIR").unwrap_or_else(|_| "protos".to_string());
    let proto_path = std::path::Path::new(&proto_dir).join("shop.proto");

    if !proto_path.exists() {
        panic!(
            "Proto file not found: {}. Set PROTO_DIR env var or ensure protos/shop.proto exists.",
            proto_path.display()
        );
    }

    let out_dir = std::env::var("OUT_DIR")?;
    let desc_path = std::path::Path::new(&out_dir).join("shop.desc");

    // Build server codegen (generates service traits) + file descriptor set
    // binary for tonic-reflection.  The generated service traits are unused
    // (dispatch is raw-byte via a generic tower Service), but the file
    // descriptor set feeds tonic-reflection.
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(&desc_path)
        .compile_protos(&[&proto_path], &[&proto_dir])?;

    println!("cargo:rerun-if-changed={}", proto_path.display());
    Ok(())
}
