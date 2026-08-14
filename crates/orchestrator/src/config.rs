use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MasterConfig {
    pub listen_addr: SocketAddr,
    pub max_concurrent_queries: usize,
    pub default_shuffle_partitions: u16,
    pub task_timeout: Duration,
    pub vcs_addr: SocketAddr,
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8080".parse().unwrap(),
            max_concurrent_queries: 64,
            default_shuffle_partitions: 32,
            task_timeout: Duration::from_secs(300),
            vcs_addr: "127.0.0.1:9090".parse().unwrap(),
        }
    }
}
