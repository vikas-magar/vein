//! Client for the VCS-owned exchange metadata service.

use std::path::Path;

use crate::identifiers::{PartitionId, TaskId};
use crate::protocol::{
    ExchangeFragment, ExchangeKey, ExchangeManifest, VcsExchangeRequest, VcsExchangeResponse,
};
use crate::transport::{
    build_quic_client, extract_control_payload, frame_control_message, ConnectionManager,
    QuicClient,
};
use crate::Result;
use error::VeinError;

/// A pooled QUIC client for exchange metadata operations.
#[derive(Clone)]
pub struct VcsExchangeClient {
    target: String,
    client: QuicClient,
}

impl VcsExchangeClient {
    /// Connects to a VCS endpoint using the supplied CA certificate.
    pub fn connect(
        target: impl Into<String>,
        ca_cert_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let quic = build_quic_client(ca_cert_path)?;
        Ok(Self {
            target: target.into(),
            client: QuicClient::new(quic),
        })
    }

    /// Creates a client from an already initialized pooled QUIC client.
    pub fn with_client(target: impl Into<String>, client: QuicClient) -> Self {
        Self {
            target: target.into(),
            client,
        }
    }

    /// Returns the configured VCS endpoint.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Creates or reopens an exchange.
    pub async fn create_exchange(
        &self,
        key: ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<TaskId>,
        schema_fingerprint: String,
    ) -> Result<ExchangeManifest> {
        let response = self
            .request(VcsExchangeRequest::Create {
                key,
                partition_count,
                expected_tasks,
                schema_fingerprint,
            })
            .await?;
        response_manifest(response)
    }

    /// Publishes one immutable exchange fragment.
    pub async fn publish_fragment(
        &self,
        key: ExchangeKey,
        fragment: ExchangeFragment,
    ) -> Result<()> {
        match self
            .request(VcsExchangeRequest::Publish { key, fragment })
            .await?
        {
            VcsExchangeResponse::Ack => Ok(()),
            other => Err(unexpected_response("publish", other)),
        }
    }

    /// Seals an exchange after all expected producers publish.
    pub async fn seal_exchange(&self, key: ExchangeKey) -> Result<ExchangeManifest> {
        response_manifest(self.request(VcsExchangeRequest::Seal { key }).await?)
    }

    /// Reads a sealed exchange manifest.
    pub async fn get_manifest(&self, key: ExchangeKey) -> Result<ExchangeManifest> {
        response_manifest(self.request(VcsExchangeRequest::GetManifest { key }).await?)
    }

    /// Reads the fragments assigned to one reducer partition.
    pub async fn get_partition(
        &self,
        key: ExchangeKey,
        partition_id: PartitionId,
    ) -> Result<Vec<ExchangeFragment>> {
        match self
            .request(VcsExchangeRequest::GetPartition { key, partition_id })
            .await?
        {
            VcsExchangeResponse::Partition { fragments } => Ok(fragments),
            other => Err(unexpected_response("partition lookup", other)),
        }
    }

    /// Deletes exchange metadata after retention cleanup.
    pub async fn delete_exchange(&self, key: ExchangeKey) -> Result<()> {
        match self
            .request(VcsExchangeRequest::Delete { key })
            .await?
        {
            VcsExchangeResponse::Ack => Ok(()),
            other => Err(unexpected_response("delete", other)),
        }
    }

    async fn request(&self, request: VcsExchangeRequest) -> Result<VcsExchangeResponse> {
        let mut stream = self.client.open_stream(&self.target).await?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request)
            .map_err(|error| VeinError::serialization(format!("encode VCS request: {error}")))?;
        stream
            .send(frame_control_message(&bytes))
            .await
            .map_err(|error| VeinError::quic_send(error.to_string()))?;
        stream
            .finish()
            .map_err(|error| VeinError::quic_send(error.to_string()))?;

        let mut buffer = Vec::new();
        while let Ok(Some(chunk)) = stream.receive().await {
            buffer.extend_from_slice(&chunk);
        }
        let payload = extract_control_payload(&buffer)
            .ok_or_else(|| VeinError::serialization("invalid VCS response frame"))?;
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(payload);
        rkyv::from_bytes::<VcsExchangeResponse, rkyv::rancor::Error>(&aligned)
            .map_err(|error| VeinError::serialization(format!("decode VCS response: {error}")))
    }
}

fn response_manifest(response: VcsExchangeResponse) -> Result<ExchangeManifest> {
    match response {
        VcsExchangeResponse::Manifest { manifest } => Ok(manifest),
        other => Err(unexpected_response("manifest", other)),
    }
}

fn unexpected_response(operation: &str, response: VcsExchangeResponse) -> VeinError {
    match response {
        VcsExchangeResponse::Error { message } => {
            VeinError::shuffle(format!("VCS {operation} rejected: {message}"))
        }
        _ => VeinError::serialization(format!(
            "VCS returned an unexpected response for {operation}"
        )),
    }
}
