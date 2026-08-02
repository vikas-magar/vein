//! Archipelago leaderless SMR protocol state and types.

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
}

impl ArchipelagoState {
    /// Creates a new Archipelago state instance for a cluster of `total_nodes`.
    pub fn new(total_nodes: usize) -> Self {
        Self { total_nodes }
    }

    /// Returns the fast path quorum size required for instant commit.
    pub fn fast_path_quorum(&self) -> usize {
        (self.total_nodes / 2) + 1
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
}
