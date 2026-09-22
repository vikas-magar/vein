//! Server-side OpenRaft RPC dispatch for the shared control plane.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use common::protocol::{RaftRpcEnvelope, RaftRpcKind, VeinControlEvent};
use openraft::{
    error::{InstallSnapshotError, RaftError},
    raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest},
};
use serde::de::DeserializeOwned;

use crate::VeinRaft;

/// A transport-neutral asynchronous handler for one Raft RPC.
pub trait RaftRpcHandler: Send + Sync {
    /// Dispatches a decoded request and returns its correlated control response.
    fn handle(
        &self,
        request: RaftRpcEnvelope,
    ) -> Pin<Box<dyn Future<Output = VeinControlEvent> + Send>>;
}

/// OpenRaft-backed implementation of RaftRpcHandler.
#[derive(Clone)]
pub struct OpenRaftRpcHandler {
    raft: Arc<VeinRaft>,
}

impl OpenRaftRpcHandler {
    /// Creates a handler for a running Raft node.
    pub fn new(raft: Arc<VeinRaft>) -> Self {
        Self { raft }
    }

    async fn dispatch(&self, request: RaftRpcEnvelope) -> VeinControlEvent {
        let request_id = request.request_id;
        let kind = request.kind;
        let result = match kind {
            RaftRpcKind::Vote => self.dispatch_vote(request.payload).await,
            RaftRpcKind::AppendEntries => self.dispatch_append_entries(request.payload).await,
            RaftRpcKind::InstallSnapshot => self.dispatch_install_snapshot(request.payload).await,
        };

        match result {
            Ok(payload) => VeinControlEvent::RaftRpcResponse {
                envelope: RaftRpcEnvelope {
                    request_id,
                    kind,
                    payload,
                },
                error: None,
            },
            Err(error) => VeinControlEvent::RaftRpcResponse {
                envelope: RaftRpcEnvelope {
                    request_id,
                    kind,
                    payload: Vec::new(),
                },
                error: Some(error),
            },
        }
    }

    async fn dispatch_vote(&self, payload: Vec<u8>) -> Result<Vec<u8>, String> {
        let request: VoteRequest<u64> = decode(payload)?;
        let response = self.raft.vote(request).await.map_err(error_text)?;
        encode(response)
    }

    async fn dispatch_append_entries(&self, payload: Vec<u8>) -> Result<Vec<u8>, String> {
        let request: AppendEntriesRequest<crate::VeinRaftConfig> = decode(payload)?;
        let response = self.raft.append_entries(request).await.map_err(error_text)?;
        encode(response)
    }

    async fn dispatch_install_snapshot(&self, payload: Vec<u8>) -> Result<Vec<u8>, String> {
        let request: InstallSnapshotRequest<crate::VeinRaftConfig> = decode(payload)?;
        let response: Result<_, RaftError<u64, InstallSnapshotError>> =
            self.raft.install_snapshot(request).await;
        encode(response.map_err(error_text)?)
    }
}

impl RaftRpcHandler for OpenRaftRpcHandler {
    fn handle(
        &self,
        request: RaftRpcEnvelope,
    ) -> Pin<Box<dyn Future<Output = VeinControlEvent> + Send>> {
        let handler = self.clone();
        Box::pin(async move { handler.dispatch(request).await })
    }
}

fn decode<T: DeserializeOwned>(payload: Vec<u8>) -> Result<T, String> {
    serde_json::from_slice(&payload).map_err(|error| format!("invalid Raft request: {error}"))
}

fn encode<T: serde::Serialize>(value: T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&value).map_err(|error| format!("encode Raft response: {error}"))
}

fn error_text<E: std::fmt::Display>(error: E) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_payload_is_rejected_before_dispatch() {
        let result: Result<VoteRequest<u64>, _> = decode(vec![0xff]);
        assert!(result.is_err());
    }
}
