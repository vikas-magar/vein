use clap::Parser;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use vfs::VfsConfig;
use vfs::block::{BlockManager, MmapBlockManager};
use vfs::consensus::archipelago::ArchipelagoState;
use vfs::ingest::buffer::IngestionBuffer;
use vfs::metadata::{MetadataEngine, RedbMetadataEngine};
use vfs::rpc::router::RpcRouter;

#[derive(Parser, Debug)]
#[command(name = "vfs", about = "VFS Distributed Storage Node")]
pub struct Cli {
    #[arg(short, long)]
    pub node_id: String,
    #[arg(short, long, default_value = "cluster.toml")]
    pub config: String,
    #[arg(long, default_value = ".")]
    pub tls_dir: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    pub storage: StorageConfig,
    pub peers: Vec<PeerConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct StorageConfig {
    pub backend: String,
    pub endpoint: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub enable_virtual_host_style: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PeerConfig {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub weight: f64,
}

pub fn load_topology(
    cluster_config: &ClusterConfig,
    node_id: &str,
) -> Result<(vfs::topology::crush::ClusterMap, String), String> {
    let mut cluster_map = vfs::topology::crush::ClusterMap::new();
    let mut bind_addr = None;

    for peer in &cluster_config.peers {
        cluster_map.add_node(vfs::topology::crush::Node {
            id: peer.id.clone(),
            weight: peer.weight,
            ip: peer.ip.clone(),
            port: peer.port,
        });

        if peer.id == node_id {
            bind_addr = Some(format!("{}:{}", peer.ip, peer.port));
        }
    }

    let bind_addr = bind_addr.ok_or_else(|| {
        format!(
            "Node ID '{}' not found in configuration peers list",
            node_id
        )
    })?;

    Ok((cluster_map, bind_addr))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    tracing::info!("Starting VFS Node: {}", cli.node_id);

    let config_str = std::fs::read_to_string(&cli.config)?;
    let cluster_config: ClusterConfig = toml::from_str(&config_str)?;

    let (cluster_map, bind_addr) = load_topology(&cluster_config, &cli.node_id)?;

    tracing::info!(
        "Topology loaded. {} peers found. Binding to {}",
        cluster_config.peers.len(),
        bind_addr
    );

    // Isolate node state directories
    let base_dir = format!("/tmp/veinfs/{}", cli.node_id);
    tokio::fs::create_dir_all(&base_dir).await?;

    let mut storage_config_map = HashMap::new();
    storage_config_map.insert("bucket".to_string(), cluster_config.storage.bucket);
    storage_config_map.insert("endpoint".to_string(), cluster_config.storage.endpoint);
    storage_config_map.insert(
        "access_key_id".to_string(),
        cluster_config.storage.access_key_id,
    );
    storage_config_map.insert(
        "secret_access_key".to_string(),
        cluster_config.storage.secret_access_key,
    );
    storage_config_map.insert("region".to_string(), cluster_config.storage.region);
    storage_config_map.insert(
        "enable_virtual_host_style".to_string(),
        cluster_config.storage.enable_virtual_host_style.to_string(),
    );

    let vfs_config = VfsConfig {
        local_cache_dir: std::path::PathBuf::from(format!("{}/blocks", base_dir)),
        metadata_db_path: std::path::PathBuf::from(format!("{}/metadata.redb", base_dir)),
        block_size_bytes: 16 * 1024 * 1024,
        max_ingestion_buffer_bytes: 1024 * 1024,
        node_ip: bind_addr
            .split(':')
            .next()
            .unwrap_or("127.0.0.1")
            .to_string(),
        storage_backend: cluster_config.storage.backend,
        storage_config: storage_config_map,
        metadata_hash_algo: Default::default(),
    };

    let metadata = Arc::new(RedbMetadataEngine::init(&vfs_config).await?);
    let block_manager = Arc::new(MmapBlockManager::init(&vfs_config).await?);
    let ingestion = Arc::new(IngestionBuffer::new(
        vfs_config.clone(),
        block_manager.clone(),
        metadata.clone(),
    ));

    let total_nodes = cluster_config.peers.len();
    let archipelago = Arc::new(tokio::sync::RwLock::new(ArchipelagoState::new(total_nodes)));

    let cert_path = format!("{}/cert.pem", cli.tls_dir);
    let key_path = format!("{}/key.pem", cli.tls_dir);
    let ca_cert_path = cert_path.clone();

    let router = RpcRouter::new(
        ingestion,
        metadata,
        block_manager,
        Arc::new(cluster_map),
        cli.node_id.clone(),
        archipelago,
        ca_cert_path,
    )
    .await?;

    tracing::info!("Starting VFS RpcRouter on {}", bind_addr);

    // Start router (this blocks until termination)
    router.start(&bind_addr, &cert_path, &key_path).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CONFIG_TOML: &str = r#"
[storage]
backend = "s3"
endpoint = "http://127.0.0.1:9000"
bucket = "veinfs-test"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
region = "us-east-1"
enable_virtual_host_style = false

[[peers]]
id = "node1"
ip = "127.0.0.1"
port = 4001
weight = 1.0

[[peers]]
id = "node2"
ip = "127.0.0.2"
port = 4002
weight = 2.0
"#;

    #[test]
    fn test_config_parsing_and_topology_discovery() {
        let cluster_config: ClusterConfig = toml::from_str(TEST_CONFIG_TOML).unwrap();
        assert_eq!(cluster_config.peers.len(), 2);
        assert_eq!(cluster_config.storage.backend, "s3");

        let (cluster_map, bind_addr) = load_topology(&cluster_config, "node1").unwrap();
        assert_eq!(bind_addr, "127.0.0.1:4001");
        assert_eq!(cluster_map.nodes.len(), 2);
        assert_eq!(cluster_map.nodes[0].id, "node1");
        assert_eq!(cluster_map.nodes[0].addr(), "127.0.0.1:4001");
        assert_eq!(cluster_map.nodes[1].id, "node2");
        assert_eq!(cluster_map.nodes[1].addr(), "127.0.0.2:4002");
    }

    #[test]
    fn test_node_id_not_found_error() {
        let cluster_config: ClusterConfig = toml::from_str(TEST_CONFIG_TOML).unwrap();
        let err = load_topology(&cluster_config, "node99").unwrap_err();
        assert_eq!(
            err,
            "Node ID 'node99' not found in configuration peers list"
        );
    }
}
