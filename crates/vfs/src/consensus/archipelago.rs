//! Archipelago leaderless SMR protocol state and types.

use std::collections::{HashMap, HashSet};
use crate::VfsError;

/// Unique identifier for a proposal in Archipelago consensus.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProposalId {
    /// ID of the node originating the proposal.
    pub node_id: String,
    /// Sequence number assigned by the proposing node.
    pub sequence: u64,
}

impl ProposalId {
    /// Serializes `ProposalId` into a 16-byte wire representation.
    ///
    /// The first 8 bytes contain the UTF-8 bytes of `node_id`.
    /// Note: Node IDs longer than 8 bytes are truncated to the first 8 bytes.
    /// The remaining 8 bytes contain `sequence` in big-endian byte order.
    pub fn to_wire_bytes(&self) -> [u8; 16] {
        debug_assert!(
            self.node_id.len() <= 8,
            "Node ID '{}' exceeds 8-byte wire representation limit and will be truncated",
            self.node_id
        );
        let mut buf = [0u8; 16];
        let node_bytes = self.node_id.as_bytes();
        let len = node_bytes.len().min(8);
        buf[..len].copy_from_slice(&node_bytes[..len]);
        buf[8..16].copy_from_slice(&self.sequence.to_be_bytes());
        buf
    }

    /// Deserializes a 16-byte wire representation into a `ProposalId`.
    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self, VfsError> {
        if bytes.len() < 16 {
            return Err(VfsError::Internal(
                "Wire bytes too short for ProposalId".into(),
            ));
        }
        let node_id_raw = &bytes[..8];
        let null_pos = node_id_raw.iter().position(|&b| b == 0).unwrap_or(8);
        let node_id = String::from_utf8(node_id_raw[..null_pos].to_vec())
            .map_err(|_| VfsError::Internal("Invalid UTF-8 in ProposalId node_id".into()))?;
        let sequence = u64::from_be_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| VfsError::Internal("Invalid ProposalId sequence".into()))?,
        );
        Ok(Self { node_id, sequence })
    }
}

/// State of a proposal in the Archipelago consensus pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalState {
    /// Proposal has been broadcast to nodes.
    Proposed,
    /// Proposal has achieved quorum and is committed.
    Committed,
}

/// State machine manager for Archipelago consensus.
#[derive(Debug)]
pub struct ArchipelagoState {
    /// Total number of nodes in the consensus cluster.
    pub total_nodes: usize,
    /// Map of proposal IDs to their binary payload.
    pub proposals: HashMap<ProposalId, Vec<u8>>,
    /// Current status of each proposal.
    pub status: HashMap<ProposalId, ProposalState>,
    /// Set of voter node IDs for each proposal.
    pub votes: HashMap<ProposalId, HashSet<String>>,
    /// Oneshot notification channels for proposals waiting for consensus commit.
    pub callbacks: HashMap<ProposalId, tokio::sync::oneshot::Sender<()>>,
}

impl ArchipelagoState {
    /// Creates a new Archipelago state instance for a cluster of `total_nodes`.
    pub fn new(total_nodes: usize) -> Self {
        Self {
            total_nodes,
            proposals: HashMap::new(),
            status: HashMap::new(),
            votes: HashMap::new(),
            callbacks: HashMap::new(),
        }
    }

    /// Returns the fast path quorum size required for instant commit.
    pub fn fast_path_quorum(&self) -> usize {
        (self.total_nodes / 2) + 1
    }

    /// Internal helper to check quorum and fire oneshot callback upon commit.
    fn check_quorum(&mut self, pid: &ProposalId) {
        if let Some(votes) = self.votes.get(pid) {
            if votes.len() >= self.fast_path_quorum() {
                self.status.insert(pid.clone(), ProposalState::Committed);
                if let Some(tx) = self.callbacks.remove(pid) {
                    let _ = tx.send(()); // best-effort; receiver may have timed out
                }
            }
        }
    }

    /// Proposes a new payload for consensus.
    ///
    /// Inserts the proposal payload, records the implicit vote from the proposer (`pid.node_id`),
    /// sets initial state to `Proposed`, and advances to `Committed` if quorum is reached.
    pub fn propose(&mut self, pid: ProposalId, payload: Vec<u8>) -> Result<(), VfsError> {
        if self.proposals.contains_key(&pid) {
            return Err(VfsError::Internal("Proposal already exists".into()));
        }
        let proposer_id = pid.node_id.clone();
        self.proposals.insert(pid.clone(), payload);
        self.status.insert(pid.clone(), ProposalState::Proposed);

        let mut node_votes = HashSet::new();
        node_votes.insert(proposer_id);
        self.votes.insert(pid.clone(), node_votes);

        self.check_quorum(&pid);
        Ok(())
    }

    /// Proposes a new payload for consensus with a oneshot notification callback.
    ///
    /// The `notify` oneshot channel will be triggered when the proposal reaches quorum and enters `Committed` state.
    pub fn propose_with_callback(
        &mut self,
        pid: ProposalId,
        payload: Vec<u8>,
        notify: tokio::sync::oneshot::Sender<()>,
    ) -> Result<(), VfsError> {
        if self.proposals.contains_key(&pid) {
            return Err(VfsError::Internal("Proposal already exists".into()));
        }
        self.callbacks.insert(pid.clone(), notify);
        self.propose(pid, payload)
    }

    /// Casts a vote for an existing proposal by `voter_id`.
    ///
    /// If the vote count reaches or exceeds `fast_path_quorum()`, updates status to `Committed`
    /// and fires any registered oneshot callback.
    pub fn vote(&mut self, pid: &ProposalId, voter_id: &str) -> Result<(), VfsError> {
        let node_votes = self
            .votes
            .get_mut(pid)
            .ok_or_else(|| VfsError::NotFound(format!("Proposal not found: {:?}", pid)))?;

        node_votes.insert(voter_id.to_string());
        self.check_quorum(pid);

        Ok(())
    }

    /// Returns the current consensus status of a proposal, if present.
    pub fn get_status(&self, pid: &ProposalId) -> Option<ProposalState> {
        self.status.get(pid).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let state = ArchipelagoState::new(3); // 3 nodes
        assert_eq!(state.fast_path_quorum(), 2);
    }

    #[test]
    fn test_proposal_lifecycle() {
        let mut state = ArchipelagoState::new(3);
        let pid = ProposalId { node_id: "node1".into(), sequence: 1 };

        state.propose(pid.clone(), b"metadata_payload".to_vec()).unwrap();
        assert_eq!(state.get_status(&pid), Some(ProposalState::Proposed));

        state.vote(&pid, "node2").unwrap(); // 2nd vote (node1 implicitly voted when proposing)
        assert_eq!(state.get_status(&pid), Some(ProposalState::Committed)); // 2/3 quorum reached
    }

    #[test]
    fn test_single_node_quorum_instant_commit() {
        let mut state = ArchipelagoState::new(1);
        let pid = ProposalId { node_id: "node1".into(), sequence: 1 };

        state.propose(pid.clone(), b"single_node".to_vec()).unwrap();
        assert_eq!(state.get_status(&pid), Some(ProposalState::Committed));
    }

    #[test]
    fn test_vote_unknown_proposal_returns_not_found() {
        let mut state = ArchipelagoState::new(3);
        let pid = ProposalId { node_id: "node1".into(), sequence: 999 };

        let res = state.vote(&pid, "node2");
        assert!(matches!(res, Err(VfsError::NotFound(_))));
    }

    #[test]
    fn test_duplicate_vote_does_not_double_count() {
        let mut state = ArchipelagoState::new(3);
        let pid = ProposalId { node_id: "node1".into(), sequence: 1 };

        state.propose(pid.clone(), b"test".to_vec()).unwrap();
        assert_eq!(state.get_status(&pid), Some(ProposalState::Proposed));

        // Voting twice from node1 (proposer) should not increase vote set size
        state.vote(&pid, "node1").unwrap();
        assert_eq!(state.get_status(&pid), Some(ProposalState::Proposed));
    }

    #[test]
    fn test_duplicate_proposal_returns_error() {
        let mut state = ArchipelagoState::new(3);
        let pid = ProposalId { node_id: "node1".into(), sequence: 1 };

        state.propose(pid.clone(), b"payload1".to_vec()).unwrap();
        let res = state.propose(pid.clone(), b"payload2".to_vec());
        assert!(matches!(res, Err(VfsError::Internal(_))));
    }

    #[test]
    fn test_proposal_id_wire_bytes_roundtrip() {
        let pid = ProposalId { node_id: "node1".into(), sequence: 42 };
        let bytes = pid.to_wire_bytes();
        assert_eq!(bytes.len(), 16);
        let parsed = ProposalId::from_wire_bytes(&bytes).unwrap();
        assert_eq!(parsed, pid);
    }

    #[test]
    fn test_propose_with_callback_fires_oneshot_on_quorum() {
        let mut state = ArchipelagoState::new(3);
        let pid = ProposalId { node_id: "node1".into(), sequence: 1 };
        let (tx, mut rx) = tokio::sync::oneshot::channel();

        state
            .propose_with_callback(pid.clone(), b"payload".to_vec(), tx)
            .unwrap();

        // 1 vote out of 3, quorum is 2 -> callback not fired yet
        assert_eq!(rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty));
        assert_eq!(state.get_status(&pid), Some(ProposalState::Proposed));

        // 2nd vote reaches quorum -> callback fires
        state.vote(&pid, "node2").unwrap();
        assert_eq!(rx.try_recv(), Ok(()));
        assert_eq!(state.get_status(&pid), Some(ProposalState::Committed));
    }

    #[test]
    fn test_propose_with_callback_does_not_fire_without_quorum() {
        let mut state = ArchipelagoState::new(3);
        let pid = ProposalId { node_id: "node1".into(), sequence: 1 };
        let (tx, mut rx) = tokio::sync::oneshot::channel();

        state
            .propose_with_callback(pid.clone(), b"payload".to_vec(), tx)
            .unwrap();

        // Quorum is 2, only 1 vote exists -> callback does not fire
        assert_eq!(rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty));
        assert_eq!(state.get_status(&pid), Some(ProposalState::Proposed));
    }

    #[test]
    fn test_propose_with_callback_timed_out_receiver_no_panic() {
        let mut state = ArchipelagoState::new(3);
        let pid = ProposalId { node_id: "node1".into(), sequence: 1 };
        let (tx, rx) = tokio::sync::oneshot::channel();

        state
            .propose_with_callback(pid.clone(), b"payload".to_vec(), tx)
            .unwrap();

        // Drop receiver (simulating timeout or cancelled caller)
        drop(rx);

        // Voting to reach quorum should handle closed channel gracefully without panic
        assert!(state.vote(&pid, "node2").is_ok());
        assert_eq!(state.get_status(&pid), Some(ProposalState::Committed));
    }
}
