# Distributed VFS Binary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone, multi-node capable executable (`vfs`) for the VeinFS distributed storage system driven by a shared TOML configuration.

**Architecture:** A new `src/bin/vfs.rs` file that parses CLI flags (`clap`) and a TOML cluster config (`serde`), builds a `ClusterMap`, initializes the local `redb` metadata and `mmap` block managers, and starts the `RpcRouter` QUIC server.

**Tech Stack:** Rust, `clap`, `serde`, `toml`, `tokio`, `tracing`.

## Global Constraints

- No `unwrap()` or `expect()` in production code, except in the outermost `main()` function bootstrapping logic where failing to start is the intended behavior (e.g. failing to read the config file).
- All existing tests (50) must continue to pass.
- Code should be clean, properly documented.

---

### Task 1: Add Dependencies & Define CLI/Config Structs

**Files:**
- Modify: `crates/vfs/Cargo.toml`
- Create: `crates/vfs/src/bin/vfs.rs`

**Interfaces:**
- Produces: `Cli`, `ClusterConfig`, `StorageConfig`, `PeerConfig` definitions in `vfs.rs`.

- [ ] **Step 1: Add dependencies to Cargo.toml**
Add `toml`, `serde`, and `serde_derive` (with `derive` feature for serde) to the `[dependencies]` section if missing. `clap` should already be in the workspace or add it with `derive` feature.

```toml
[dependencies]
# (Add these alongside existing deps)
serde = { version = "1.0", features = ["derive"] }
toml = "0.8"
clap = { version = "4.0", features = ["derive"] }
```

- [ ] **Step 2: Create vfs.rs and define structs**

```rust
use clap::Parser;
use serde::Deserialize;
use std::collections::HashMap;

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
```

- [ ] **Step 3: Run `cargo check -p vfs` to verify it compiles**

- [ ] **Step 4: Commit**
```bash
git add crates/vfs/Cargo.toml crates/vfs/src/bin/vfs.rs
git commit -m "feat(vfs): add CLI and TOML config structures for standalone binary"
```

---

### Task 2: Implement Topology Discovery and Configuration Loading

**Files:**
- Modify: `crates/vfs/src/bin/vfs.rs`

**Interfaces:**
- Consumes: `Cli`, `ClusterConfig`, `vfs::topology::crush::{ClusterMap, Node}`
- Produces: Parsed config, `bind_addr`, `ClusterMap`

- [ ] **Step 1: Add config loading and topology mapping to `main()`**

Inside `main()`:
```rust
    let cli = Cli::parse();
    tracing::info!("Starting VeinFS Node: {}", cli.node_id);

    let config_str = std::fs::read_to_string(&cli.config)?;
    let cluster_config: ClusterConfig = toml::from_str(&config_str)?;

    let mut cluster_map = vfs::topology::crush::ClusterMap::new();
    let mut bind_addr = None;

    for peer in &cluster_config.peers {
        cluster_map.add_node(vfs::topology::crush::Node {
            id: peer.id.clone(),
            weight: peer.weight,
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
```

- [ ] **Step 2: Run `cargo check -p vfs` to verify it compiles**

- [ ] **Step 3: Commit**
```bash
git add crates/vfs/src/bin/vfs.rs
git commit -m "feat(vfs): implement topology discovery and config loading"
```

---

### Task 3: Wire Core VFS Engines and Start RPC Router

**Files:**
- Modify: `crates/vfs/src/bin/vfs.rs`

**Interfaces:**
- Consumes: `cluster_config`, `cluster_map`, `bind_addr`, `vfs::VfsConfig`, `vfs::metadata::RedbMetadataEngine`, `vfs::block::MmapBlockManager`, `vfs::ingest::buffer::IngestionBuffer`, `vfs::consensus::archipelago::ArchipelagoState`, `vfs::rpc::router::RpcRouter`
- Produces: Running `RpcRouter` QUIC server.

- [ ] **Step 1: Map config, initialize engines, and start router**

Append to `main()`:
```rust
    use std::sync::Arc;
    use vfs::VfsConfig;
    use vfs::metadata::{MetadataEngine, RedbMetadataEngine};
    use vfs::block::{BlockManager, MmapBlockManager};
    use vfs::ingest::buffer::IngestionBuffer;
    use vfs::consensus::archipelago::ArchipelagoState;
    use vfs::rpc::router::RpcRouter;

    // Isolate node state directories
    let base_dir = format!("/tmp/veinfs/{}", cli.node_id);
    tokio::fs::create_dir_all(&base_dir).await?;

    let mut storage_config_map = HashMap::new();
    storage_config_map.insert("bucket".to_string(), cluster_config.storage.bucket);
    storage_config_map.insert("endpoint".to_string(), cluster_config.storage.endpoint);
    storage_config_map.insert("access_key_id".to_string(), cluster_config.storage.access_key_id);
    storage_config_map.insert("secret_access_key".to_string(), cluster_config.storage.secret_access_key);
    storage_config_map.insert("region".to_string(), cluster_config.storage.region);
    storage_config_map.insert("enable_virtual_host_style".to_string(), cluster_config.storage.enable_virtual_host_style.to_string());

    let vfs_config = VfsConfig {
        local_cache_dir: std::path::PathBuf::from(format!("{}/blocks", base_dir)),
        metadata_db_path: std::path::PathBuf::from(format!("{}/metadata.redb", base_dir)),
        block_size_bytes: 16 * 1024 * 1024,
        max_ingestion_buffer_bytes: 1024 * 1024,
        node_ip: bind_addr.split(':').next().unwrap_or("127.0.0.1").to_string(),
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
    let archipelago = Arc::new(tokio::sync::RwLock::new(
        ArchipelagoState::new(total_nodes),
    ));

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
    ).await?;

    tracing::info!("Starting VFS RpcRouter on {}", bind_addr);
    
    // Start router (this blocks until termination)
    router.start(&bind_addr, &cert_path, &key_path).await?;
```

- [ ] **Step 2: Run `cargo build -p vfs` to ensure everything compiles**

- [ ] **Step 3: Commit**
```bash
git add crates/vfs/src/bin/vfs.rs
git commit -m "feat(vfs): wire engines and start RpcRouter QUIC server"
```
