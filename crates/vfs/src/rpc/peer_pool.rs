//! Peer connection pool for Archipelago consensus.

use crate::consensus::archipelago::ProposalId;
use crate::consensus::op::ConsensusOp;
use crate::topology::crush::Node;
use crate::VfsError;

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
}

impl PeerPool {
    /// Creates a new `PeerPool` with the specified CA certificate path.
    pub fn new(ca_cert_path: &str) -> Result<Self, VfsError> {
        Ok(Self {
            ca_cert_path: ca_cert_path.to_string(),
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
        _pid: &ProposalId,
        _op: &ConsensusOp,
    ) -> Result<VoteReply, VfsError> {
        Err(VfsError::Internal(format!(
            "PeerPool: not connected to {}",
            node.addr()
        )))
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
