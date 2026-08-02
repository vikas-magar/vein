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

/// State of a proposal in the Archipelago consensus pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalState {
    /// Proposal has been broadcast to nodes.
    Proposed,
    /// Proposal has achieved quorum and is committed.
    Committed,
}

/// State machine manager for Archipelago consensus.
#[derive(Debug, Clone)]
pub struct ArchipelagoState {
    /// Total number of nodes in the consensus cluster.
    pub total_nodes: usize,
    /// Map of proposal IDs to their binary payload.
    pub proposals: HashMap<ProposalId, Vec<u8>>,
    /// Current status of each proposal.
    pub status: HashMap<ProposalId, ProposalState>,
    /// Set of voter node IDs for each proposal.
    pub votes: HashMap<ProposalId, HashSet<String>>,
}

impl ArchipelagoState {
    /// Creates a new Archipelago state instance for a cluster of `total_nodes`.
    pub fn new(total_nodes: usize) -> Self {
        Self {
            total_nodes,
            proposals: HashMap::new(),
            status: HashMap::new(),
            votes: HashMap::new(),
        }
    }

    /// Returns the fast path quorum size required for instant commit.
    pub fn fast_path_quorum(&self) -> usize {
        (self.total_nodes / 2) + 1
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

        if node_votes.len() >= self.fast_path_quorum() {
            self.status.insert(pid.clone(), ProposalState::Committed);
        }

        self.votes.insert(pid, node_votes);
        Ok(())
    }

    /// Casts a vote for an existing proposal by `voter_id`.
    ///
    /// If the vote count reaches or exceeds `fast_path_quorum()`, updates status to `Committed`.
    pub fn vote(&mut self, pid: &ProposalId, voter_id: &str) -> Result<(), VfsError> {
        let node_votes = self
            .votes
            .get_mut(pid)
            .ok_or_else(|| VfsError::NotFound(format!("Proposal not found: {:?}", pid)))?;

        node_votes.insert(voter_id.to_string());

        if node_votes.len() >= self.fast_path_quorum() {
            self.status.insert(pid.clone(), ProposalState::Committed);
        }

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
}
