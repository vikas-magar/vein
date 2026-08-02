use crate::block::BlockManager;
use crate::consensus::archipelago::ProposalId;
use crate::consensus::op::ConsensusOp;
use crate::ingest::buffer::IngestionBuffer;
use crate::metadata::{Extent, MetadataEngine};
use crate::recovery::heal::HealDaemon;
use crate::rpc::peer_pool::PeerPool;
use crate::topology::crush::{ClusterMap, Node};
use crate::{Result, VfsError};
use s2n_quic::{stream::BidirectionalStream, Server};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, PartialEq, Clone)]
pub enum RoutingDecision {
    Local,
    Forward(Node),
}

pub struct RpcRouter {
    ingestion: Arc<IngestionBuffer>,
    metadata: Arc<dyn MetadataEngine>,
    block_manager: Arc<dyn BlockManager>,
    cluster_map: Arc<ClusterMap>,
    local_node_id: String,
    archipelago: Arc<tokio::sync::RwLock<crate::consensus::archipelago::ArchipelagoState>>,
    sequence: Arc<AtomicU64>,
    heal_daemon: Arc<HealDaemon>,
    peer_pool: Arc<tokio::sync::Mutex<PeerPool>>,
}

impl RpcRouter {
    pub fn new(
        ingestion: Arc<IngestionBuffer>,
        metadata: Arc<dyn MetadataEngine>,
        block_manager: Arc<dyn BlockManager>,
        cluster_map: Arc<ClusterMap>,
        local_node_id: String,
        archipelago: Arc<tokio::sync::RwLock<crate::consensus::archipelago::ArchipelagoState>>,
        ca_cert_path: String,
    ) -> Result<Self> {
        let peer_pool = PeerPool::new(&ca_cert_path)?;
        Ok(Self {
            ingestion,
            metadata,
            block_manager,
            cluster_map,
            local_node_id,
            archipelago,
            sequence: Arc::new(AtomicU64::new(1)),
            heal_daemon: Arc::new(HealDaemon::new()),
            peer_pool: Arc::new(tokio::sync::Mutex::new(peer_pool)),
        })
    }

    pub fn routing_decision(&self, path: &str) -> Result<RoutingDecision> {
        let owner = self.cluster_map.get_owner(path)?;
        if owner.id == self.local_node_id {
            Ok(RoutingDecision::Local)
        } else {
            Ok(RoutingDecision::Forward(owner))
        }
    }

    pub async fn start(&self, bind_addr: &str, cert_path: &str, key_path: &str) -> Result<()> {
        // Fire-and-forget: the handle is intentionally dropped; the task runs for
        // the lifetime of the process. The double-start guard in HealDaemon::start()
        // ensures only one loop is ever spawned even if RpcRouter::start() is called again.
        let _ = self.heal_daemon.start();

        let mut server = Server::builder()
            .with_tls((Path::new(cert_path), Path::new(key_path)))
            .map_err(|e| VfsError::Internal(e.to_string()))?
            .with_io(bind_addr)
            .map_err(|e| VfsError::Internal(e.to_string()))?
            .start()
            .map_err(|e| VfsError::Internal(e.to_string()))?;

        tracing::info!("VeinFS RPC Router listening on QUIC {}", bind_addr);


        while let Some(mut connection) = server.accept().await {
            let router = self.clone();
            tokio::spawn(async move {
                while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                    let router = router.clone();
                    tokio::spawn(async move {
                        if let Err(e) = router.handle_stream(stream).await {
                            tracing::error!("Stream error: {:?}", e);
                        }
                    });
                }
            });
        }

        Ok(())
    }

    async fn handle_stream(&self, mut stream: BidirectionalStream) -> Result<()> {
        let opcode = match stream.read_u8().await {
            Ok(op) => op,
            Err(_) => return Ok(()),
        };
        
        let result = match opcode {
            1 => self.handle_push(&mut stream).await,
            2 => self.handle_get_metadata(&mut stream).await,
            3 => self.handle_pull(&mut stream).await,
            4 => self.handle_eof(&mut stream).await,
            5 => self.handle_delete(&mut stream).await,
            0x10 => self.handle_vote_request(&mut stream).await,
            _ => Err(VfsError::Internal("Unknown Opcode".to_string())),
        };

        if let Err(e) = result {
            // P1-11 Fix: Error Framing. Send 0x01 on failure along with error message
            let _ = stream.write_all(&[0x01]).await;
            let msg = e.to_string();
            let _ = stream.write_u32(msg.len() as u32).await;
            let _ = stream.write_all(msg.as_bytes()).await;
            tracing::error!("Request failed: {}", e);
        }
        
        Ok(())
    }

    async fn handle_push(&self, stream: &mut BidirectionalStream) -> Result<()> {
        let path_len = stream.read_u64().await?;
        if path_len > 4096 {
            return Err(VfsError::Internal(format!("Path too long: {}", path_len)));
        }
        
        let mut path_buf = vec![0u8; path_len as usize];
        stream.read_exact(&mut path_buf).await?;
        let path = String::from_utf8(path_buf).map_err(|_| VfsError::Internal("Invalid UTF8 path".to_string()))?;

        match self.routing_decision(&path)? {
            RoutingDecision::Forward(owner) => {
                // Drain the incoming stream to cleanly end the QUIC stream,
                // then reply 0x01 (not-owner) so the client knows the write
                // was NOT accepted and must be retried against the correct node.
                // 0x00 (success) here would be silent data loss.
                tracing::info!("Not owner for {}; owner is {}. Draining and rejecting.", path, owner.ip);
                let mut buf = [0u8; 65536];
                loop {
                    let n = stream.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                }
                stream.write_all(&[0x01]).await?; // 0x01 = NOT_OWNER; client must retry
                return Ok(());
            }
            RoutingDecision::Local => {
                tracing::info!("Proposing metadata update to Archipelago");
                let pid = crate::consensus::archipelago::ProposalId {
                    node_id: self.local_node_id.clone(),
                    sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
                };
                let mut arch = self.archipelago.write().await;
                if let Err(e) = arch.propose(pid, b"mock_payload".to_vec()) {
                    tracing::error!("Failed to propose to Archipelago: {}", e);
                }
            }
        }

        let mut buf = [0u8; 65536]; // 64KB chunks
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            self.ingestion.ingest_chunk(&path, &buf[..n]).await?;
        }

        stream.write_all(&[0x00]).await?;
        Ok(())
    }

    async fn handle_get_metadata(&self, stream: &mut BidirectionalStream) -> Result<()> {
        let path_len = stream.read_u64().await?;
        if path_len > 4096 {
            return Err(VfsError::Internal(format!("Path too long: {}", path_len)));
        }
        
        let mut path_buf = vec![0u8; path_len as usize];
        stream.read_exact(&mut path_buf).await?;
        let path = String::from_utf8(path_buf).map_err(|_| VfsError::Internal("Invalid UTF8 path".to_string()))?;

        let extents = self.metadata.get_file(&path).await?;
        let serialized = bincode::serialize(&extents).map_err(|_| VfsError::Internal("Serialize failed".to_string()))?;
        
        stream.write_all(&[0x00]).await?;
        stream.write_u64(serialized.len() as u64).await?;
        stream.write_all(&serialized).await?;

        Ok(())
    }

    async fn handle_pull(&self, stream: &mut BidirectionalStream) -> Result<()> {
        let mut extent_buf = Vec::new(); 
        stream.read_to_end(&mut extent_buf).await?;
        let extent: Extent = bincode::deserialize(&extent_buf)
            .map_err(|_| VfsError::Internal("Invalid extent".to_string()))?;

        let data = self.block_manager.read_extent(&extent).await?;
        
        stream.write_all(&[0x00]).await?;
        stream.write_u64(data.len() as u64).await?;
        stream.write_all(&data).await?;
        
        Ok(())
    }

    async fn handle_eof(&self, stream: &mut BidirectionalStream) -> Result<()> {
        let path_len = stream.read_u64().await?;
        if path_len > 4096 {
            return Err(VfsError::Internal(format!("Path too long: {}", path_len)));
        }
        
        let mut path_buf = vec![0u8; path_len as usize];
        stream.read_exact(&mut path_buf).await?;
        let path = String::from_utf8(path_buf).map_err(|_| VfsError::Internal("Invalid UTF8 path".to_string()))?;

        self.ingestion.flush_file(&path).await?;
        
        stream.write_all(&[0x00]).await?;
        Ok(())
    }

    /// P1-13 Fix: DeleteFile Opcode for Garbage Collection
    async fn handle_delete(&self, stream: &mut BidirectionalStream) -> Result<()> {
        let path_len = stream.read_u64().await?;
        if path_len > 4096 {
            return Err(VfsError::Internal(format!("Path too long: {}", path_len)));
        }
        
        let mut path_buf = vec![0u8; path_len as usize];
        stream.read_exact(&mut path_buf).await?;
        let path = String::from_utf8(path_buf).map_err(|_| VfsError::Internal("Invalid UTF8 path".to_string()))?;

        self.metadata.delete_file(&path).await?;
        
        stream.write_all(&[0x00]).await?;
        Ok(())
    }

    async fn handle_vote_request(&self, stream: &mut BidirectionalStream) -> Result<()> {
        let mut pid_buf = [0u8; 16];
        stream.read_exact(&mut pid_buf).await?;
        let pid = ProposalId::from_wire_bytes(&pid_buf)?;

        let payload_len = stream.read_u64().await?;
        if payload_len > 10 * 1024 * 1024 {
            return Err(VfsError::Internal(format!(
                "Payload too long: {}",
                payload_len
            )));
        }

        let mut payload_buf = vec![0u8; payload_len as usize];
        stream.read_exact(&mut payload_buf).await?;
        let _op = ConsensusOp::deserialize(&payload_buf)?;

        let status_byte = {
            let mut arch = self.archipelago.write().await;
            match arch.vote(&pid, &self.local_node_id) {
                Ok(()) => 0x00u8,
                Err(_) => 0x01u8,
            }
        };

        stream.write_u8(0x11).await?;
        stream.write_all(&pid.to_wire_bytes()).await?;
        stream.write_u8(status_byte).await?;

        Ok(())
    }
}

impl Clone for RpcRouter {
    fn clone(&self) -> Self {
        Self {
            ingestion: self.ingestion.clone(),
            metadata: self.metadata.clone(),
            block_manager: self.block_manager.clone(),
            cluster_map: self.cluster_map.clone(),
            local_node_id: self.local_node_id.clone(),
            archipelago: self.archipelago.clone(),
            sequence: self.sequence.clone(), // Arc clone — all connections share one counter
            heal_daemon: self.heal_daemon.clone(),
            peer_pool: self.peer_pool.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::crush::Node;
    use async_trait::async_trait;

    struct DummyMetadataEngine;
    #[async_trait]
    impl MetadataEngine for DummyMetadataEngine {
        async fn init(_config: &crate::VfsConfig) -> Result<Self>
        where
            Self: Sized,
        {
            Ok(DummyMetadataEngine)
        }
        async fn append_extent(&self, _path: &str, _extent: Extent) -> Result<()> {
            Ok(())
        }
        async fn get_file(&self, _path: &str) -> Result<Vec<Extent>> {
            Ok(vec![])
        }
        async fn delete_file(&self, _path: &str) -> Result<()> {
            Ok(())
        }
    }

    struct DummyBlockManager;
    #[async_trait]
    impl BlockManager for DummyBlockManager {
        async fn init(_config: &crate::VfsConfig) -> Result<Self>
        where
            Self: Sized,
        {
            Ok(DummyBlockManager)
        }
        async fn append(&self, _data: &[u8]) -> Result<Vec<Extent>> {
            Ok(vec![])
        }
        async fn read_extent(&self, _extent: &Extent) -> Result<Vec<u8>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn test_rpc_router_forwarding_decision() {
        let mut cluster_map = ClusterMap::new();
        cluster_map.add_node(Node {
            id: "node1".into(),
            weight: 1.0,
            ip: "127.0.0.1:4001".into(),
            port: 0,
        });
        cluster_map.add_node(Node {
            id: "node2".into(),
            weight: 1.0,
            ip: "127.0.0.2:4001".into(),
            port: 0,
        });
        let cluster_map = Arc::new(cluster_map);

        let metadata = Arc::new(DummyMetadataEngine);
        let block_manager = Arc::new(DummyBlockManager);
        let archipelago = Arc::new(tokio::sync::RwLock::new(
            crate::consensus::archipelago::ArchipelagoState::new(2),
        ));

        let router = RpcRouter::new(
            Arc::new(IngestionBuffer::new(
                crate::VfsConfig {
                    local_cache_dir: std::path::PathBuf::from("/tmp/test_router_cache"),
                    metadata_db_path: std::path::PathBuf::from("/tmp/test_router_meta.redb"),
                    block_size_bytes: 1024,
                    max_ingestion_buffer_bytes: 1024,
                    node_ip: "127.0.0.1".into(),
                    storage_backend: "memory".into(),
                    storage_config: std::collections::HashMap::new(),
                    metadata_hash_algo: Default::default(),
                },
                block_manager.clone(),
                metadata.clone(),
            )),
            metadata,
            block_manager,
            cluster_map.clone(),
            "node1".into(),
            archipelago.clone(),
            "".into(),
        )
        .unwrap();

        assert_eq!(router.local_node_id, "node1");
        assert_eq!(router.cluster_map.nodes.len(), 2);

        let owner = router.cluster_map.get_owner("/test/file.bin").unwrap();
        let decision = router.routing_decision("/test/file.bin").unwrap();
        if owner.id == "node1" {
            assert_eq!(decision, RoutingDecision::Local);
        } else {
            assert_eq!(decision, RoutingDecision::Forward(owner.clone()));
        }

        let other_node_id = if owner.id == "node1" { "node2" } else { "node1" };
        let router_other = RpcRouter::new(
            router.ingestion.clone(),
            router.metadata.clone(),
            router.block_manager.clone(),
            router.cluster_map.clone(),
            other_node_id.into(),
            archipelago.clone(),
            "".into(),
        )
        .unwrap();
        let decision_other = router_other.routing_decision("/test/file.bin").unwrap();
        assert_eq!(decision_other, RoutingDecision::Forward(owner));
    }
}
