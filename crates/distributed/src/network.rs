//! OpenRaft network integration over Vein's framed QUIC control plane.

use std::future::Future;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use common::transport::{
    ConnectionManager, QuicClient, extract_control_payload, frame_control_message,
};
use common::protocol::{RaftRpcEnvelope, RaftRpcKind, VeinControlEvent};
use openraft::{
    BasicNode, RaftNetwork, RaftNetworkFactory,
    raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse},
    error::{InstallSnapshotError, RPCError, RaftError},
    network::RPCOption,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::VeinRaftConfig;

const RPC_FRAME_LIMIT: usize = 64 * 1024 * 1024;

/// Shared request-id allocator for one transport process.
#[derive(Clone, Default)]
pub struct RaftRpcIdGenerator {
    next: Arc<AtomicU64>,
}

impl RaftRpcIdGenerator {
    /// Creates a generator starting at one.
    pub fn new() -> Self {
        Self {
            next: Arc::new(AtomicU64::new(1)),
        }
    }

    fn next_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

/// Creates OpenRaft network clients backed by the shared QUIC client.
#[derive(Clone)]
pub struct QuicRaftNetworkFactory {
    client: Arc<QuicClient>,
    ids: RaftRpcIdGenerator,
}

impl QuicRaftNetworkFactory {
    /// Creates a factory using an existing pooled QUIC client.
    pub fn new(client: Arc<QuicClient>) -> Self {
        Self {
            client,
            ids: RaftRpcIdGenerator::new(),
        }
    }
}

/// One OpenRaft peer connection. Connections are opened lazily per RPC.
pub struct QuicRaftNetwork {
    target_addr: String,
    client: Arc<QuicClient>,
    ids: RaftRpcIdGenerator,
}

impl QuicRaftNetwork {
    async fn call<Req, Resp, E>(
        &mut self,
        kind: RaftRpcKind,
        request: Req,
        option: &RPCOption,
    ) -> Result<Resp, RPCError<u64, BasicNode, RaftError<u64, E>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error,
    {
        let payload = serde_json::to_vec(&request).map_err(|error| network_error::<E>(error))?;
        if payload.len() > RPC_FRAME_LIMIT {
            return Err(network_error_message::<E>("raft request exceeds control frame limit"));
        }

        let request_id = self.ids.next_id();
        let event = VeinControlEvent::RaftRpcRequest(RaftRpcEnvelope {
            request_id,
            kind,
            payload,
        });
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
            .map_err(|error| network_error_message::<E>(format!("encode raft request: {error}")))?;

        let mut stream = self
            .client
            .open_stream(&self.target_addr)
            .await
            .map_err(|error| unreachable_error::<E>(error.to_string()))?;

        stream
            .send(frame_control_message(&encoded))
            .await
            .map_err(|error| unreachable_error::<E>(error.to_string()))?;
        stream
            .finish()
            .map_err(|error| unreachable_error::<E>(error.to_string()))?;

        let response_bytes = tokio::time::timeout(option.hard_ttl(), async {
            let mut buffer = Vec::new();
            while let Some(chunk) = stream
                .receive()
                .await
                .map_err(|error| io::Error::other(error.to_string()))?
            {
                buffer.extend_from_slice(&chunk);
                if buffer.len() > RPC_FRAME_LIMIT {
                    return Err(io::Error::other("raft response exceeds control frame limit"));
                }
            }
            Ok::<_, io::Error>(buffer)
        })
        .await
        .map_err(|_| timeout_error::<E>(kind, option.hard_ttl()))?
        .map_err(|error| unreachable_error::<E>(error.to_string()))?;

        let response_payload = extract_control_payload(&response_bytes)
            .ok_or_else(|| network_error_message::<E>("missing raft response frame"))?;
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(response_payload);
        let response = rkyv::from_bytes::<VeinControlEvent, rkyv::rancor::Error>(&aligned)
            .map_err(|error| network_error_message::<E>(format!("decode raft response: {error}")))?;

        let (envelope, error) = match response {
            VeinControlEvent::RaftRpcResponse { envelope, error } => (envelope, error),
            _ => return Err(network_error_message::<E>("unexpected control event for raft response")),
        };

        if envelope.request_id != request_id || envelope.kind != kind {
            return Err(network_error_message::<E>("raft response correlation mismatch"));
        }
        if let Some(error) = error {
            return Err(unreachable_error::<E>(error));
        }

        serde_json::from_slice(&envelope.payload)
            .map_err(|error| network_error_message::<E>(format!("decode raft payload: {error}")))
    }
}

impl RaftNetworkFactory<VeinRaftConfig> for QuicRaftNetworkFactory {
    type Network = QuicRaftNetwork;

    fn new_client(
        &mut self,
        _target: u64,
        node: &BasicNode,
    ) -> impl Future<Output = Self::Network> + Send {
        let network = QuicRaftNetwork {
            target_addr: node.addr.clone(),
            client: self.client.clone(),
            ids: self.ids.clone(),
        };
        async move { network }
    }
}

impl RaftNetwork<VeinRaftConfig> for QuicRaftNetwork {
    fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<VeinRaftConfig>,
        option: RPCOption,
    ) -> impl Future<
        Output = Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>,
    > + Send {
        async move { self.call(RaftRpcKind::AppendEntries, rpc, &option).await }
    }

    fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<VeinRaftConfig>,
        option: RPCOption,
    ) -> impl Future<
        Output = Result<InstallSnapshotResponse<u64>, RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>>,
    > + Send {
        async move {
            self.call::<_, _, InstallSnapshotError>(RaftRpcKind::InstallSnapshot, rpc, &option)
                .await
        }
    }

    fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> impl Future<
        Output = Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>,
    > + Send {
        async move { self.call(RaftRpcKind::Vote, rpc, &option).await }
    }
}

fn network_error<E>(error: impl std::fmt::Display) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error,
{
    network_error_message::<E>(error.to_string())
}

fn network_error_message<E>(message: impl Into<String>) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error,
{
    RPCError::Network(openraft::error::NetworkError::new(&io::Error::other(message.into())))
}

fn unreachable_error<E>(message: impl Into<String>) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error,
{
    RPCError::Unreachable(openraft::error::Unreachable::new(&io::Error::other(message.into())))
}

fn timeout_error<E>(
    kind: RaftRpcKind,
    duration: std::time::Duration,
) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error,
{
    let action = match kind {
        RaftRpcKind::Vote => openraft::network::RPCTypes::Vote,
        RaftRpcKind::AppendEntries => openraft::network::RPCTypes::AppendEntries,
        RaftRpcKind::InstallSnapshot => openraft::network::RPCTypes::InstallSnapshot,
    };
    RPCError::Timeout(openraft::error::Timeout {
        action,
        id: 0,
        target: 0,
        timeout: duration,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_are_monotonic() {
        let ids = RaftRpcIdGenerator::new();
        assert_eq!(ids.next_id(), 1);
        assert_eq!(ids.next_id(), 2);
    }
}
