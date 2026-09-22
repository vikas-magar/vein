//! OpenRaft type configuration for Vein metadata replication.

use std::io::Cursor;

use openraft::{BasicNode, Entry, Raft, TokioRuntime, declare_raft_types};

use crate::{CommitReceipt, MetadataCommand};

declare_raft_types!(
    pub VeinRaftConfig:
        D = MetadataCommand,
        R = CommitReceipt,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<VeinRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime
);

pub type VeinRaft = Raft<VeinRaftConfig>;

/// Starts a Vein metadata Raft node using the durable storage implementation.
///
/// The caller supplies the transport factory so the distributed crate stays
/// independent of Vein's RPC protocol and deployment topology.
pub async fn start_raft_node<N>(
    node_id: u64,
    config: std::sync::Arc<openraft::Config>,
    network: N,
    storage: crate::DurableRaftStorage,
) -> std::result::Result<VeinRaft, openraft::error::Fatal<u64>>
where
    N: openraft::network::RaftNetworkFactory<VeinRaftConfig>,
{
    let (log_store, state_machine) = openraft::storage::Adaptor::new(storage);
    VeinRaft::new(node_id, config, network, log_store, state_machine).await
}

#[cfg(test)]
mod tests {
    use openraft::RaftTypeConfig;

    use super::*;

    #[test]
    fn config_uses_metadata_commands_and_receipts() {
        fn assert_type_config<C: RaftTypeConfig<NodeId = u64>>() {}
        assert_type_config::<VeinRaftConfig>();
    }
}
