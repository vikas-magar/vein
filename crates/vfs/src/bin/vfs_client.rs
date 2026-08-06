use common::transport::build_quic_client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let logical_path = "/demo/cluster_test.bin";
    let data = vec![42u8; 16 * 1024 * 1024]; // 1MB of dummy data

    for port in [4001, 4002] {
        println!("Connecting to VFS Cluster via node on port {}...", port);
        let client = build_quic_client("cert.pem")?;
        let mut conn = client.connect(
            s2n_quic::client::Connect::new(format!("127.0.0.1:{}", port).parse::<std::net::SocketAddr>()?).with_server_name("localhost")
        ).await?;
        conn.keep_alive(true)?;

        let mut stream = conn.open_bidirectional_stream().await?;
        stream.write_u8(1).await?; // Opcode 1: Push
        stream.write_u64(logical_path.len() as u64).await?;
        stream.write_all(logical_path.as_bytes()).await?;
        stream.write_all(&data).await?;
        stream.shutdown().await?;

        let status = stream.read_u8().await?;
        if status == 0 {
            println!("Success! Data pushed, routed, voted on, and committed to cluster metadata on node port {}.", port);
            return Ok(());
        } else {
            println!("Push rejected by node on port {} (likely not the owner). Trying next...", port);
        }
    }
    
    println!("Failed to push data to any node.");

    Ok(())
}
