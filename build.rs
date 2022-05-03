fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .type_attribute("hyparview.Peer", "#[derive(Eq, Hash)]")
        .type_attribute("hyparview.Empty", "#[derive(Copy)]")
        .type_attribute("hyparview.NeighborResponse", "#[derive(Copy)]")
        .compile(&["proto/hyparview.proto"], &["proto/"])?;
    Ok(())
}
