//! Generates the gRPC service code from the shared protobuf contract.
//!
//! The proto lives outside the crate, in the repository's `proto/` directory,
//! because it is the contract between two languages and neither side owns it.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../proto/engine.proto";
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=../proto");
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&[proto], &[".."])?;
    Ok(())
}
