use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(name = "vfs", about = "VeinFS Distributed Storage Node")]
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
            weight: peer.weight as f64,
            ip: peer.ip.clone(),
            port: peer.port,
        });

        if peer.id == node_id {
            bind_addr = Some(format!("{}:{}", peer.ip, peer.port));
        }
    }

    let bind_addr = bind_addr.ok_or_else(|| {
        format!("Node ID '{}' not found in configuration peers list", node_id)
    })?;

    Ok((cluster_map, bind_addr))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    tracing::info!("Starting VeinFS Node: {}", cli.node_id);

    let config_str = std::fs::read_to_string(&cli.config)?;
    let cluster_config: ClusterConfig = toml::from_str(&config_str)?;

    let mut cluster_map = vfs::topology::crush::ClusterMap::new();
    let mut bind_addr = None;

    for peer in &cluster_config.peers {
        cluster_map.add_node(vfs::topology::crush::Node {
            id: peer.id.clone(),
            weight: peer.weight as f64,
            ip: peer.ip.clone(),
            port: peer.port,
        });

        if peer.id == cli.node_id {
            bind_addr = Some(format!("{}:{}", peer.ip, peer.port));
        }
    }

    let bind_addr = bind_addr.ok_or_else(|| {
        format!("Node ID '{}' not found in configuration peers list", cli.node_id)
    })?;

    tracing::info!("Topology loaded. {} peers found. Binding to {}", cluster_config.peers.len(), bind_addr);

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
        assert_eq!(err, "Node ID 'node99' not found in configuration peers list");
    }
}

