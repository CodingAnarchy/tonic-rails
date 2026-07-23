fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path("src/shop_descriptor.bin")
        .compile_protos(&["protos/shop.proto"], &["protos"])?;
    Ok(())
}
