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
    pub weight: f32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    Ok(())
}
