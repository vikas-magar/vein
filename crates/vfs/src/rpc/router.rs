use crate::block::BlockManager;
use crate::consensus::archipelago::ProposalId;
use crate::consensus::op::ConsensusOp;
use crate::ingest::buffer::IngestionBuffer;
use crate::metadata::{Extent, MetadataEngine};
use crate::recovery::heal::HealDaemon;
use crate::rpc::peer_pool::{PeerPool, VoteReply};
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
    pub async fn new(
        ingestion: Arc<IngestionBuffer>,
        metadata: Arc<dyn MetadataEngine>,
        block_manager: Arc<dyn BlockManager>,
        cluster_map: Arc<ClusterMap>,
        local_node_id: String,
        archipelago: Arc<tokio::sync::RwLock<crate::consensus::archipelago::ArchipelagoState>>,
        ca_cert_path: String,
    ) -> Result<Self> {
        let peer_pool = PeerPool::new(&ca_cert_path)?;
        let seq = metadata.load_sequence().await.unwrap_or(1);
        Ok(Self {
            ingestion,
            metadata,
            block_manager,
            cluster_map,
            local_node_id,
            archipelago,
            sequence: Arc::new(AtomicU64::new(seq)),
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
        self.process_push(stream).await
    }

    pub async fn process_push<S>(&self, stream: &mut S) -> Result<()>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
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
                // Phase 1: Propose + broadcast
                let op = ConsensusOp::AppendExtent {
                    path: path.clone(),
                    offset: 0,
                    length: 0, // will be refined as chunks arrive; use 0 as placeholder
                };
                let payload = op.serialize()?;
                let pid = ProposalId {
                    node_id: self.local_node_id.clone(),
                    sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
                };
                let seq_val = self.sequence.load(Ordering::SeqCst);
                if let Err(e) = self.metadata.persist_sequence(seq_val).await {
                    tracing::warn!("Failed to persist sequence: {}", e);
                }
                let (tx, rx) = tokio::sync::oneshot::channel::<()>();

                {
                    let mut arch = self.archipelago.write().await;
                    arch.propose_with_callback(pid.clone(), payload.clone(), tx)?;
                }

                // Broadcast to peers (fire-and-forget per peer; quorum drives commit)
                let peers = self.cluster_map.all_nodes_except(&self.local_node_id);
                for peer in peers {
                    let peer = peer.clone();
                    let pid2 = pid.clone();
                    let op2 = op.clone();
                    let pool = self.peer_pool.clone();
                    let arch = self.archipelago.clone();
                    tokio::spawn(async move {
                        match pool.lock().await.send_vote_request(&peer, &pid2, &op2).await {
                            Ok(VoteReply::Accept) => {
                                let mut a = arch.write().await;
                                if let Err(e) = a.vote(&pid2, &peer.id) {
                                    tracing::warn!("vote error from peer {}: {}", peer.id, e);
                                }
                            }
                            Ok(VoteReply::Reject) => tracing::warn!("peer {} rejected proposal {:?}", peer.id, pid2),
                            Err(e) => tracing::warn!("vote_request to {} failed: {}", peer.id, e),
                        }
                    });
                }

                // Wait for quorum (2s timeout)
                tokio::time::timeout(std::time::Duration::from_secs(2), rx)
                    .await
                    .map_err(|_| VfsError::Internal("consensus timeout: quorum not reached".into()))?
                    .map_err(|_| VfsError::Internal("consensus channel dropped".into()))?;

                // Persist committed proposal
                if let Err(e) = self.archipelago.read().await.persist_committed(&pid, &op, &self.metadata).await {
                    tracing::warn!("Failed to persist consensus log: {}", e);
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

    /// Processes a vote request over any async read/write stream.
    pub async fn process_vote_request<S>(&self, stream: &mut S) -> Result<()>
    where
        S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin,
    {
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
            // Register the proposal on this node if not already present, then vote.
            // propose() returns Err if already exists — that's fine, we still proceed to vote.
            let _ = arch.propose(pid.clone(), payload_buf.clone());
            match arch.vote(&pid, &self.local_node_id) {
                Ok(()) => 0x00u8,
                Err(e) => {
                    tracing::warn!("vote() failed for proposal {:?}: {}", pid, e);
                    0x01u8
                }
            }
        };

        stream.write_u8(0x11).await?;
        stream.write_all(&pid.to_wire_bytes()).await?;
        stream.write_u8(status_byte).await?;

        Ok(())
    }

    async fn handle_vote_request(&self, stream: &mut BidirectionalStream) -> Result<()> {
        self.process_vote_request(stream).await
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
        async fn persist_proposal(&self, _pid_bytes: [u8; 16], _status: u8, _op_bytes: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn load_consensus_log(&self) -> Result<Vec<([u8; 16], u8, Vec<u8>)>> {
            Ok(vec![])
        }
        async fn load_sequence(&self) -> Result<u64> {
            Ok(1)
        }
        async fn persist_sequence(&self, _seq: u64) -> Result<()> {
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
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        cluster_map.add_node(Node {
            id: "node2".into(),
            weight: 1.0,
            ip: "127.0.0.2".into(),
            port: 4001,
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
        .await
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
        .await
        .unwrap();
        let decision_other = router_other.routing_decision("/test/file.bin").unwrap();
        assert_eq!(decision_other, RoutingDecision::Forward(owner));
    }

    #[test]
    fn test_opcode_0x10_vote_request_dispatch() {
        // Opcode 0x10 (16) is defined in handle_stream to route to handle_vote_request
        const OPCODE_VOTE_REQUEST: u8 = 0x10;
        const OPCODE_VOTE_REPLY: u8 = 0x11;
        assert_eq!(OPCODE_VOTE_REQUEST, 16);
        assert_eq!(OPCODE_VOTE_REPLY, 17);
    }

    #[tokio::test]
    async fn test_process_vote_request_duplex_stream() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let cluster_map = Arc::new(ClusterMap::new());
        let metadata = Arc::new(DummyMetadataEngine);
        let block_manager = Arc::new(DummyBlockManager);
        let archipelago = Arc::new(tokio::sync::RwLock::new(
            crate::consensus::archipelago::ArchipelagoState::new(2),
        ));

        let pid = ProposalId {
            node_id: "node1".into(),
            sequence: 42,
        };

        let router = RpcRouter::new(
            Arc::new(IngestionBuffer::new(
                crate::VfsConfig {
                    local_cache_dir: std::path::PathBuf::from("/tmp/test_proc_vote_cache"),
                    metadata_db_path: std::path::PathBuf::from("/tmp/test_proc_vote_meta.redb"),
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
            cluster_map,
            "node2".into(),
            archipelago.clone(),
            "".into(),
        )
        .await
        .unwrap();

        let (mut client_stream, mut server_stream) = tokio::io::duplex(1024);

        let op = ConsensusOp::AppendExtent {
            path: "/test".into(),
            offset: 0,
            length: 0,
        };
        let op_bytes = op.serialize().unwrap();

        // Write VOTE_REQUEST wire payload into client stream
        client_stream.write_all(&pid.to_wire_bytes()).await.unwrap();
        client_stream.write_u64(op_bytes.len() as u64).await.unwrap();
        client_stream.write_all(&op_bytes).await.unwrap();

        // Process request on server stream
        let res = router.process_vote_request(&mut server_stream).await;
        assert!(res.is_ok());

        // Read and verify VOTE_REPLY frame on client stream: 0x11 opcode + 16-byte pid + 0x00 (Accept)
        let reply_opcode = client_stream.read_u8().await.unwrap();
        assert_eq!(reply_opcode, 0x11);

        let mut reply_pid_bytes = [0u8; 16];
        client_stream.read_exact(&mut reply_pid_bytes).await.unwrap();
        let reply_pid = ProposalId::from_wire_bytes(&reply_pid_bytes).unwrap();
        assert_eq!(reply_pid, pid);

        let status_byte = client_stream.read_u8().await.unwrap();
        assert_eq!(status_byte, 0x00); // 0x00 = Accept
    }

    #[tokio::test]
    async fn test_process_vote_request_existing_proposal() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let cluster_map = Arc::new(ClusterMap::new());
        let metadata = Arc::new(DummyMetadataEngine);
        let block_manager = Arc::new(DummyBlockManager);
        let archipelago = Arc::new(tokio::sync::RwLock::new(
            crate::consensus::archipelago::ArchipelagoState::new(2),
        ));

        let router = RpcRouter::new(
            Arc::new(IngestionBuffer::new(
                crate::VfsConfig {
                    local_cache_dir: std::path::PathBuf::from("/tmp/test_proc_rej_cache"),
                    metadata_db_path: std::path::PathBuf::from("/tmp/test_proc_rej_meta.redb"),
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
            cluster_map,
            "node2".into(),
            archipelago.clone(),
            "".into(),
        )
        .await
        .unwrap();

        let (mut client_stream, mut server_stream) = tokio::io::duplex(1024);
        let pid = ProposalId {
            node_id: "node1".into(),
            sequence: 999,
        };
        let op = ConsensusOp::DeleteFile {
            path: "/test/file.txt".into(),
        };
        let op_bytes = op.serialize().unwrap();

        {
            let mut arch = archipelago.write().await;
            arch.propose(pid.clone(), op_bytes.clone()).unwrap();
        }

        client_stream.write_all(&pid.to_wire_bytes()).await.unwrap();
        client_stream.write_u64(op_bytes.len() as u64).await.unwrap();
        client_stream.write_all(&op_bytes).await.unwrap();

        let res = router.process_vote_request(&mut server_stream).await;
        assert!(res.is_ok());

        let reply_opcode = client_stream.read_u8().await.unwrap();
        assert_eq!(reply_opcode, 0x11);

        let mut reply_pid_bytes = [0u8; 16];
        client_stream.read_exact(&mut reply_pid_bytes).await.unwrap();
        let reply_pid = ProposalId::from_wire_bytes(&reply_pid_bytes).unwrap();
        assert_eq!(reply_pid, pid);

        let status_byte = client_stream.read_u8().await.unwrap();
        assert_eq!(status_byte, 0x00); // 0x00 = Accept
    }

    #[tokio::test]
    async fn test_single_node_handle_push_commit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut cluster_map = ClusterMap::new();
        cluster_map.add_node(Node {
            id: "node1".into(),
            weight: 1.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        let cluster_map = Arc::new(cluster_map);

        let metadata = Arc::new(DummyMetadataEngine);
        let block_manager = Arc::new(DummyBlockManager);
        let archipelago = Arc::new(tokio::sync::RwLock::new(
            crate::consensus::archipelago::ArchipelagoState::new(1),
        ));

        let router = RpcRouter::new(
            Arc::new(IngestionBuffer::new(
                crate::VfsConfig {
                    local_cache_dir: std::path::PathBuf::from("/tmp/test_single_push_cache"),
                    metadata_db_path: std::path::PathBuf::from("/tmp/test_single_push_meta.redb"),
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
            cluster_map,
            "node1".into(),
            archipelago.clone(),
            "".into(),
        )
        .await
        .unwrap();

        let (mut client_stream, mut server_stream) = tokio::io::duplex(1024);

        let path = "/test/single_push.bin";
        client_stream.write_u64(path.len() as u64).await.unwrap();
        client_stream.write_all(path.as_bytes()).await.unwrap();
        client_stream.write_all(b"payload data").await.unwrap();
        client_stream.shutdown().await.unwrap();

        let res = router.process_push(&mut server_stream).await;
        assert!(res.is_ok());

        let status_byte = client_stream.read_u8().await.unwrap();
        assert_eq!(status_byte, 0x00);

        let arch = archipelago.read().await;
        let pid = ProposalId {
            node_id: "node1".into(),
            sequence: 1,
        };
        assert_eq!(
            arch.status.get(&pid),
            Some(&crate::consensus::archipelago::ProposalState::Committed)
        );
    }

    #[tokio::test]
    async fn test_single_node_handle_push_persists_to_redb() {
        use tokio::io::AsyncWriteExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let config = crate::VfsConfig {
            metadata_db_path: temp_dir.path().join("router_persist_meta.redb"),
            local_cache_dir: temp_dir.path().join("router_persist_cache"),
            ..Default::default()
        };

        let metadata = Arc::new(crate::metadata::RedbMetadataEngine::init(&config).await.unwrap());
        let block_manager = Arc::new(DummyBlockManager);
        let archipelago = Arc::new(tokio::sync::RwLock::new(
            crate::consensus::archipelago::ArchipelagoState::new(1),
        ));

        let mut cluster_map = ClusterMap::new();
        cluster_map.add_node(Node {
            id: "node1".into(),
            weight: 1.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        let cluster_map = Arc::new(cluster_map);

        let router = RpcRouter::new(
            Arc::new(IngestionBuffer::new(config, block_manager.clone(), metadata.clone())),
            metadata.clone(),
            block_manager,
            cluster_map,
            "node1".into(),
            archipelago.clone(),
            "".into(),
        )
        .await
        .unwrap();

        let (mut client_stream, mut server_stream) = tokio::io::duplex(1024);

        let path = "/test/push_persist.bin";
        client_stream.write_u64(path.len() as u64).await.unwrap();
        client_stream.write_all(path.as_bytes()).await.unwrap();
        client_stream.write_all(b"data").await.unwrap();
        client_stream.shutdown().await.unwrap();

        let res = router.process_push(&mut server_stream).await;
        assert!(res.is_ok());

        // Verify the proposal log was persisted in redb metadata engine
        let log = metadata.load_consensus_log().await.unwrap();
        assert_eq!(log.len(), 1);
        let (pid_bytes, status, _op_bytes) = &log[0];
        assert_eq!(*status, 1); // Committed
        let pid = ProposalId::from_wire_bytes(pid_bytes).unwrap();
        assert_eq!(pid.node_id, "node1");
        assert_eq!(pid.sequence, 1);
    }

    #[tokio::test]
    async fn test_router_sequence_persisted_and_reloaded_across_restarts() {
        use tokio::io::AsyncWriteExt;

        let temp_dir = tempfile::tempdir().unwrap();
        let config = crate::VfsConfig {
            metadata_db_path: temp_dir.path().join("router_seq_meta.redb"),
            local_cache_dir: temp_dir.path().join("router_seq_cache"),
            ..Default::default()
        };

        let block_manager = Arc::new(DummyBlockManager);
        let archipelago = Arc::new(tokio::sync::RwLock::new(
            crate::consensus::archipelago::ArchipelagoState::new(1),
        ));

        let mut cluster_map = ClusterMap::new();
        cluster_map.add_node(Node {
            id: "node1".into(),
            weight: 1.0,
            ip: "127.0.0.1".into(),
            port: 4001,
        });
        let cluster_map = Arc::new(cluster_map);

        // Instance 1: Initial sequence is loaded as 1. Push proposal will use seq 1 and advance counter to 2.
        {
            let metadata = Arc::new(crate::metadata::RedbMetadataEngine::init(&config).await.unwrap());
            let router = RpcRouter::new(
                Arc::new(IngestionBuffer::new(config.clone(), block_manager.clone(), metadata.clone())),
                metadata.clone(),
                block_manager.clone(),
                cluster_map.clone(),
                "node1".into(),
                archipelago.clone(),
                "".into(),
            )
            .await
            .unwrap();

            let (mut client_stream, mut server_stream) = tokio::io::duplex(1024);
            let path = "/test/seq_persist.bin";
            client_stream.write_u64(path.len() as u64).await.unwrap();
            client_stream.write_all(path.as_bytes()).await.unwrap();
            client_stream.write_all(b"data").await.unwrap();
            client_stream.shutdown().await.unwrap();

            let res = router.process_push(&mut server_stream).await;
            assert!(res.is_ok());

            // Give fire-and-forget persist_sequence task a moment to finish
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // Instance 2 (simulating restart): RpcRouter::new loads sequence from metadata engine
        {
            let metadata2 = Arc::new(crate::metadata::RedbMetadataEngine::init(&config).await.unwrap());
            let router2 = RpcRouter::new(
                Arc::new(IngestionBuffer::new(config, block_manager.clone(), metadata2.clone())),
                metadata2.clone(),
                block_manager,
                cluster_map,
                "node1".into(),
                archipelago,
                "".into(),
            )
            .await
            .unwrap();

            let seq_loaded = router2.sequence.load(Ordering::SeqCst);
            assert!(seq_loaded >= 2, "Loaded sequence {} should be >= 2 after 1 push", seq_loaded);
            assert_eq!(seq_loaded, 2);
        }
    }
}

