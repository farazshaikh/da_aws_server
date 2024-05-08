use anyhow::Result;

fn main() -> Result<()> {
    tonic_build::compile_protos("./disperser.proto")?;
    Ok(())
}
