fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../protos/bench.proto");
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&["../protos/bench.proto"], &["../protos"])?;
    Ok(())
}
