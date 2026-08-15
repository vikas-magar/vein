//! Peer connection pool for Archipelago consensus.

use common::transport::{ConnectionManager, QuicClient, build_quic_client};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::VfsError;
use crate::consensus::archipelago::ProposalId;
use crate::consensus::op::ConsensusOp;
use crate::topology::crush::Node;

/// Outcome of a consensus vote request.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VoteReply {
    /// Proposal vote accepted by peer.
    Accept,
    /// Proposal vote rejected by peer.
    Reject,
}

/// Peer connection pool holding client transport configuration.
pub struct PeerPool {
    ca_cert_path: String,
    client: Option<QuicClient>,
}

impl PeerPool {
    /// Creates a new `PeerPool` with the specified CA certificate path.
    pub fn new(ca_cert_path: &str) -> Result<Self, VfsError> {
        Ok(Self {
            ca_cert_path: ca_cert_path.to_string(),
            client: None,
        })
    }

    /// Returns the CA certificate path stored in the pool.
    pub fn ca_cert_path(&self) -> &str {
        &self.ca_cert_path
    }

    /// Sends a vote request RPC to a peer node for a given proposal and consensus operation.
    pub async fn send_vote_request(
        &mut self,
        node: &Node,
        pid: &ProposalId,
        op: &ConsensusOp,
    ) -> Result<VoteReply, VfsError> {
        if self.client.is_none() {
            let raw_client = build_quic_client(&self.ca_cert_path)
                .map_err(|e| VfsError::Internal(format!("Failed to build quic client: {}", e)))?;
            self.client = Some(QuicClient::new(raw_client));
        }

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| VfsError::Internal("QuicClient not initialized".to_string()))?;
        let mut stream = client.open_stream(&node.addr()).await.map_err(|e| {
            VfsError::Internal(format!("Failed to open stream to {}: {}", node.addr(), e))
        })?;

        // Send opcode 0x10
        stream
            .write_u8(0x10)
            .await
            .map_err(|e| VfsError::Internal(format!("Failed to write opcode: {}", e)))?;

        // Send pid
        stream
            .write_all(&pid.to_wire_bytes())
            .await
            .map_err(|e| VfsError::Internal(format!("Failed to write pid: {}", e)))?;

        // Send op
        let payload = op.serialize()?;
        stream
            .write_u64(payload.len() as u64)
            .await
            .map_err(|e| VfsError::Internal(format!("Failed to write payload len: {}", e)))?;
        stream
            .write_all(&payload)
            .await
            .map_err(|e| VfsError::Internal(format!("Failed to write payload: {}", e)))?;

        // Read reply
        let reply_opcode = stream
            .read_u8()
            .await
            .map_err(|e| VfsError::Internal(format!("Failed to read reply opcode: {}", e)))?;
        if reply_opcode != 0x11 {
            return Err(VfsError::Internal(format!(
                "Unexpected reply opcode: {}",
                reply_opcode
            )));
        }

        let mut pid_buf = [0u8; 16];
        stream
            .read_exact(&mut pid_buf)
            .await
            .map_err(|e| VfsError::Internal(format!("Failed to read reply pid: {}", e)))?;

        let status = stream
            .read_u8()
            .await
            .map_err(|e| VfsError::Internal(format!("Failed to read reply status: {}", e)))?;

        match status {
            0x00 => Ok(VoteReply::Accept),
            0x01 => Ok(VoteReply::Reject),
            _ => Err(VfsError::Internal(format!(
                "Unknown reply status: {}",
                status
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_pool_constructs() {
        let pool = PeerPool::new("/dev/null");
        assert!(pool.is_ok());
        let pool = pool.unwrap();
        assert_eq!(pool.ca_cert_path(), "/dev/null");
        assert!(pool.client.is_none());
    }

    #[test]
    fn test_vote_reply_equality_and_serde() {
        assert_eq!(VoteReply::Accept, VoteReply::Accept);
        assert_eq!(VoteReply::Reject, VoteReply::Reject);
        assert_ne!(VoteReply::Accept, VoteReply::Reject);

        let serialized = bincode::serialize(&VoteReply::Accept).unwrap();
        let deserialized: VoteReply = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, VoteReply::Accept);
    }
}
