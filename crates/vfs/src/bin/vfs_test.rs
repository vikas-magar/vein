use common::transport::build_quic_client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vfs::VfsConfig;
use vfs::block::{BlockManager, MmapBlockManager};
use vfs::ingest::buffer::IngestionBuffer;
use vfs::metadata::{MetadataEngine, RedbMetadataEngine};
use vfs::rpc::router::RpcRouter;
use vfs::topology::crush::{ClusterMap, Node};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::logging::init("vfs-test")?;

    tracing::info!("Initializing VeinFS Integration Test...");

    // 1. Setup VfsConfig
    let mut storage_config = HashMap::new();
    storage_config.insert("bucket".to_string(), "veinfs".to_string());
    storage_config.insert("endpoint".to_string(), "http://localhost:9000".to_string());
    storage_config.insert("access_key_id".to_string(), "admin".to_string());
    storage_config.insert("secret_access_key".to_string(), "password123".to_string());
    storage_config.insert("region".to_string(), "us-east-1".to_string());
    // MinIO local testing requires Path Style access. In production, this config is completely dynamic.
    storage_config.insert("enable_virtual_host_style".to_string(), "false".to_string());

    let config = VfsConfig {
        local_cache_dir: std::path::PathBuf::from("/tmp/veinfs/test_blocks"),
        metadata_db_path: std::path::PathBuf::from("/tmp/veinfs/test_metadata.redb"),
        block_size_bytes: 16 * 1024 * 1024, // 16MB block size (fixes S3 EntityTooSmall)
        max_ingestion_buffer_bytes: 1024 * 1024, // 1MB chunk ingestion flush threshold
        node_ip: "127.0.0.1".to_string(),
        storage_backend: "s3".to_string(),
        storage_config,
        metadata_hash_algo: Default::default(),
    };

    // Clean up old state
    let _ = tokio::fs::remove_dir_all(&config.local_cache_dir).await;
    let _ = tokio::fs::remove_file(&config.metadata_db_path).await;
    let _ = tokio::fs::create_dir_all("/tmp/veinfs").await;

    // 2. Start VFS Node
    let metadata = Arc::new(RedbMetadataEngine::init(&config).await?);
    let block_manager = Arc::new(MmapBlockManager::init(&config).await?);
    let ingestion = Arc::new(IngestionBuffer::new(
        config.clone(),
        block_manager.clone(),
        metadata.clone(),
    ));

    let mut cluster_map = ClusterMap::new();
    cluster_map.add_node(Node {
        id: "node1".to_string(),
        weight: 1.0,
        ip: config.node_ip.clone(),
        port: 0,
    });
    let cluster_map = Arc::new(cluster_map);
    let local_node_id = "node1".to_string();
    let archipelago = Arc::new(tokio::sync::RwLock::new(
        vfs::consensus::archipelago::ArchipelagoState::new(1),
    ));

    let router = RpcRouter::new(
        ingestion,
        metadata,
        block_manager,
        cluster_map,
        local_node_id,
        archipelago,
        "/Users/anvi/Next/vein/cert.pem".to_string(),
    )
    .await?;

    let bind_addr = "127.0.0.1:4000".to_string();
    let bind_addr_clone = bind_addr.clone();

    tokio::spawn(async move {
        router
            .start(
                &bind_addr_clone,
                "/Users/anvi/Next/vein/cert.pem",
                "/Users/anvi/Next/vein/key.pem",
            )
            .await
            .unwrap();
    });

    // Wait for server to boot
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // 3. Connect Client
    tracing::info!("Connecting QUIC client to {}", bind_addr);
    let client = build_quic_client("/Users/anvi/Next/vein/cert.pem")?;
    let connect_opts = s2n_quic::client::Connect::new(bind_addr.parse::<std::net::SocketAddr>()?)
        .with_server_name("localhost");
    let mut connection = client.connect(connect_opts).await?;
    connection.keep_alive(true)?;

    // 4. Push 35MB of data
    let logical_path = "/test/large.bin".to_string();
    let data_len = 35 * 1024 * 1024;
    let mut data = vec![0u8; data_len];
    for i in 0..data_len {
        data[i] = (i % 256) as u8;
    }

    tracing::info!("Pushing 35MB of random data to {}", logical_path);
    let mut stream = connection.open_bidirectional_stream().await?;
    stream.write_u8(1).await?; // Opcode 1: Push
    stream.write_u64(logical_path.len() as u64).await?;
    stream.write_all(logical_path.as_bytes()).await?;

    let mut offset = 0;
    while offset < data_len {
        let end = std::cmp::min(offset + 65536, data_len);
        stream.write_all(&data[offset..end]).await?;
        offset = end;
    }

    // Shut down the write half to signal EOF on the transport stream
    stream.shutdown().await?;

    let status = stream.read_u8().await?;
    assert_eq!(status, 0, "Push failed");
    tracing::info!("Push success (35MB).");

    // 5. Send Logical EOF (Opcode 4) to flush remaining ingestion buffers
    let mut stream = connection.open_bidirectional_stream().await?;
    stream.write_u8(4).await?; // Opcode 4: EOF
    stream.write_u64(logical_path.len() as u64).await?;
    stream.write_all(logical_path.as_bytes()).await?;
    let status = stream.read_u8().await?;
    assert_eq!(status, 0, "EOF failed");
    tracing::info!("Logical EOF flush success.");

    // Wait a bit to let the background uploader send blocks to MinIO
    // Since block size is 5MB, 10MB means 2 sealed blocks and 1 active block.
    tracing::info!("Waiting 10 seconds for background S3 uploads...");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    // 6. Get Metadata
    let mut stream = connection.open_bidirectional_stream().await?;
    stream.write_u8(2).await?; // Opcode 2: Get Metadata
    stream.write_u64(logical_path.len() as u64).await?;
    stream.write_all(logical_path.as_bytes()).await?;
    let status = stream.read_u8().await?;
    assert_eq!(status, 0, "Get Metadata failed");
    let extents_len = stream.read_u64().await?;
    let mut extents_bytes = vec![0u8; extents_len as usize];
    stream.read_exact(&mut extents_bytes).await?;
    let extents: Vec<vfs::metadata::Extent> = bincode::deserialize(&extents_bytes)?;
    tracing::info!(
        "Got {} physical extents for logical path {}",
        extents.len(),
        logical_path
    );

    // 7. Pull Data
    let mut pulled_data = Vec::new();
    for extent in extents {
        let mut stream = connection.open_bidirectional_stream().await?;
        stream.write_u8(3).await?; // Opcode 3: Pull
        let extent_bytes = bincode::serialize(&extent)?;
        stream.write_all(&extent_bytes).await?;
        stream.shutdown().await?;

        let status = stream.read_u8().await?;
        assert_eq!(status, 0, "Pull failed");
        let data_len = stream.read_u64().await?;
        let mut block_data = vec![0u8; data_len as usize];
        stream.read_exact(&mut block_data).await?;
        pulled_data.extend_from_slice(&block_data);
        tracing::info!("Pulled {} bytes from block {}", data_len, extent.block_id);
    }

    // Verify perfect integrity
    assert_eq!(
        data, pulled_data,
        "DATA CORRUPTION DETECTED: Pulled bytes do not match pushed bytes!"
    );
    tracing::info!("SUCCESS! Data integrity perfectly preserved across Memory, NVMe, and S3.");

    // 8. Test Delete
    let mut stream = connection.open_bidirectional_stream().await?;
    stream.write_u8(5).await?; // Opcode 5: Delete
    stream.write_u64(logical_path.len() as u64).await?;
    stream.write_all(logical_path.as_bytes()).await?;
    let status = stream.read_u8().await?;
    assert_eq!(status, 0, "Delete failed");
    tracing::info!("Delete (GC) success.");

    Ok(())
}
