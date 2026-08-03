# Distributed VFS Binary Design

## Overview
This specification defines the standalone executable for VeinFS (VFS). The executable will live at `crates/vfs/src/bin/vfs.rs` and will allow deploying the VFS distributed storage system as a multi-node cluster driven by a shared TOML configuration file and a node-specific CLI flag.

## 1. CLI Definition

The binary uses `clap` to parse arguments:

```rust
#[derive(Parser, Debug)]
#[command(name = "vfs", about = "VeinFS Distributed Storage Node")]
struct Cli {
    /// The unique ID of this node (must match an entry in the config file)
    #[arg(short, long)]
    node_id: String,

    /// Path to the cluster configuration TOML file
    #[arg(short, long, default_value = "cluster.toml")]
    config: String,

    /// Directory containing cert.pem and key.pem for QUIC
    #[arg(long, default_value = ".")]
    tls_dir: String,
}
```

## 2. Configuration (`cluster.toml`)

The application uses `serde` and `toml` to load the cluster state. The configuration file is shared identically across all nodes in the cluster.

```rust
#[derive(Deserialize, Debug, Clone)]
struct ClusterConfig {
    storage: StorageConfig,
    peers: Vec<PeerConfig>,
}

#[derive(Deserialize, Debug, Clone)]
struct StorageConfig {
    backend: String,
    endpoint: String,
    bucket: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    enable_virtual_host_style: bool,
}

#[derive(Deserialize, Debug, Clone)]
struct PeerConfig {
    id: String,
    ip: String,
    port: u16,
    weight: f32,
}
```

## 3. Bootstrapping Flow

When `cargo run --bin vfs` is executed, the following sequence occurs:

1. **Parse CLI & Config**: `clap` parses the CLI args. `std::fs::read_to_string` loads the `cluster.toml` file, and `toml::from_str` deserializes it into `ClusterConfig`.
2. **Topology Discovery**:
   - The binary iterates over `config.peers` and constructs a `vfs::topology::crush::ClusterMap`.
   - It searches for `peer.id == cli.node_id`. If not found, it panics/fatally logs that the configured node ID is missing from the cluster topology.
   - It extracts the `ip` and `port` of the matched peer to construct the local `bind_addr` (e.g., `"127.0.0.1:4001"`).
3. **Local Storage Setup**:
   - Computes a local data directory: `/tmp/veinfs/<node-id>`.
   - Ensures the directory exists.
   - Computes paths for local caches and the `redb` metadata database within this directory to ensure multiple nodes on the same host do not collide.
4. **VFS Initialization**:
   - Maps `StorageConfig` to the internal `vfs::VfsConfig`.
   - Initializes `RedbMetadataEngine` and `MmapBlockManager`.
   - Initializes `IngestionBuffer` and `ArchipelagoState` (passing the total node count from `peers.len()`).
   - Initializes `RpcRouter` with the assembled components.
5. **Start Networking**:
   - Invokes `router.start(bind_addr, cert_path, key_path)`.
   - Awaits the termination signal (`tokio::signal::ctrl_c()`).

## 4. Dependencies
- Adds `serde`, `serde_derive`, and `toml` as dependencies in `crates/vfs/Cargo.toml` (if not already present).
- `clap` is already standard in the workspace.
