//! Remote catalog client used by the master coordinator.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use common::transport::{
    ConnectionManager, QuicClient, build_quic_client, extract_control_payload,
    frame_control_message,
};
use distributed::{
    CommitReceipt, TableId as DistributedTableId, TableSnapshot as DistributedTableSnapshot,
    TableWriteIntent, TableWriteMode, TableWritePart, WriteId,
};
use error::{Result, VeinError};
use plan::TableMetadata;

use crate::catalog_rpc::{VcsCatalogRequest, VcsCatalogResponse};
use crate::{CatalogReader, CatalogRevision, CatalogWriter, PlanningSession, TableWriteCatalog};

/// Remote VCS catalog client.
#[derive(Clone)]
pub struct RemoteVcsCatalog {
    target: String,
    client: QuicClient,
}

impl RemoteVcsCatalog {
    /// Connects to a VCS catalog endpoint using a trusted CA certificate.
    pub fn connect(target: impl Into<String>, ca_cert_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            target: target.into(),
            client: QuicClient::new(build_quic_client(ca_cert_path)?),
        })
    }

    /// Creates a client from an existing QUIC client.
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

    async fn request(&self, request: VcsCatalogRequest) -> Result<VcsCatalogResponse> {
        let mut stream = self.client.open_stream(&self.target).await?;
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&request)
            .map_err(|error| VeinError::serialization(format!("encode VCS catalog request: {error}")))?;
        stream
            .send(frame_control_message(&payload))
            .await
            .map_err(|error| VeinError::quic_send(error.to_string()))?;
        stream
            .finish()
            .map_err(|error| VeinError::quic_send(error.to_string()))?;

        let mut buffer = Vec::new();
        while let Ok(Some(chunk)) = stream.receive().await {
            buffer.extend_from_slice(&chunk);
        }
        let response_payload = extract_control_payload(&buffer)
            .ok_or_else(|| VeinError::serialization("invalid VCS catalog response frame"))?;
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(response_payload);
        let response = rkyv::from_bytes::<VcsCatalogResponse, rkyv::rancor::Error>(&aligned)
            .map_err(|error| VeinError::serialization(format!("decode VCS catalog response: {error}")))?;
        match response {
            VcsCatalogResponse::Error { message } => {
                Err(VeinError::internal(format!("VCS catalog rejected request: {message}")))
            }
            response => Ok(response),
        }
    }

    async fn json<T: serde::Serialize>(value: &T, label: &str) -> Result<Vec<u8>> {
        serde_json::to_vec(value)
            .map_err(|error| VeinError::serialization(format!("encode {label}: {error}")))
    }

    fn decode_json<T: serde::de::DeserializeOwned>(payload: &[u8], label: &str) -> Result<T> {
        serde_json::from_slice(payload)
            .map_err(|error| VeinError::serialization(format!("decode {label}: {error}")))
    }
}

#[async_trait]
impl CatalogReader for RemoteVcsCatalog {
    async fn begin_planning_session(&self) -> Result<PlanningSession> {
        match self.request(VcsCatalogRequest::BeginPlanning).await? {
            VcsCatalogResponse::Revision { revision } => {
                Ok(PlanningSession::new(CatalogRevision::new(revision)))
            }
            _ => Err(VeinError::serialization("unexpected VCS revision response")),
        }
    }

    async fn resolve_table(
        &self,
        session: &mut PlanningSession,
        name: &str,
    ) -> Result<Arc<TableMetadata>> {
        let response = self
            .request(VcsCatalogRequest::ResolveTable {
                revision: session.catalog_revision.as_str().to_string(),
                name: name.to_string(),
            })
            .await?;
        let VcsCatalogResponse::Table { revision, metadata } = response else {
            return Err(VeinError::serialization("unexpected VCS table response"));
        };
        let revision = CatalogRevision::new(revision);
        if revision != session.catalog_revision {
            return Err(VeinError::internal(format!(
                "VCS returned table revision '{}' for planning revision '{}'",
                revision.as_str(),
                session.catalog_revision.as_str()
            )));
        }
        let metadata: Arc<TableMetadata> = Arc::new(Self::decode_json(&metadata, "table metadata")?);
        let snapshot = Arc::new(crate::TableSnapshot::from_metadata(
            metadata.clone(),
            revision,
        ));
        session
            .insert(snapshot)
            .map_err(|error| VeinError::internal(error.to_string()))?;
        Ok(metadata)
    }
}

#[async_trait]
impl CatalogWriter for RemoteVcsCatalog {
    async fn register_table(&self, metadata: TableMetadata) -> Result<()> {
        let revision = self.begin_planning_session().await?.catalog_revision;
        self.register_table_at(&revision, metadata).await
    }

    async fn register_table_at(
        &self,
        expected_revision: &CatalogRevision,
        metadata: TableMetadata,
    ) -> Result<()> {
        let payload = Self::json(&metadata, "table metadata").await?;
        match self
            .request(VcsCatalogRequest::RegisterTable {
                expected_revision: expected_revision.as_str().to_string(),
                metadata: payload,
            })
            .await?
        {
            VcsCatalogResponse::Ack => Ok(()),
            _ => Err(VeinError::serialization("unexpected VCS register response")),
        }
    }
}

#[async_trait]
impl TableWriteCatalog for RemoteVcsCatalog {
    async fn prepare_table_write(
        &self,
        write_id: WriteId,
        table_id: DistributedTableId,
        mode: TableWriteMode,
        expected_parts: u32,
    ) -> Result<TableWriteIntent> {
        let response = self
            .request(VcsCatalogRequest::PrepareTableWrite {
                write_id: write_id.to_string(),
                table_id: table_id.to_string(),
                mode: match mode {
                    TableWriteMode::Overwrite => "overwrite".to_string(),
                    TableWriteMode::Append => "append".to_string(),
                },
                expected_parts,
            })
            .await?;
        match response {
            VcsCatalogResponse::WriteIntent { intent } => {
                Self::decode_json(&intent, "table write intent")
            }
            _ => Err(VeinError::serialization("unexpected VCS prepare response")),
        }
    }

    async fn publish_table_write_part(
        &self,
        write_id: WriteId,
        part: TableWritePart,
    ) -> Result<CommitReceipt> {
        let payload = Self::json(&part, "table write part").await?;
        let response = self
            .request(VcsCatalogRequest::PublishTableWritePart {
                write_id: write_id.to_string(),
                part: payload,
            })
            .await?;
        match response {
            VcsCatalogResponse::Receipt { receipt } => {
                Self::decode_json(&receipt, "commit receipt")
            }
            _ => Err(VeinError::serialization("unexpected VCS part response")),
        }
    }

    async fn commit_table_write(
        &self,
        write_id: WriteId,
        snapshot: DistributedTableSnapshot,
    ) -> Result<CommitReceipt> {
        let payload = Self::json(&snapshot, "table snapshot").await?;
        let response = self
            .request(VcsCatalogRequest::CommitTableWrite {
                write_id: write_id.to_string(),
                snapshot: payload,
            })
            .await?;
        match response {
            VcsCatalogResponse::Receipt { receipt } => {
                Self::decode_json(&receipt, "commit receipt")
            }
            _ => Err(VeinError::serialization("unexpected VCS commit response")),
        }
    }

    async fn abort_table_write(&self, write_id: WriteId) -> Result<CommitReceipt> {
        let response = self
            .request(VcsCatalogRequest::AbortTableWrite {
                write_id: write_id.to_string(),
            })
            .await?;
        match response {
            VcsCatalogResponse::Receipt { receipt } => {
                Self::decode_json(&receipt, "commit receipt")
            }
            _ => Err(VeinError::serialization("unexpected VCS abort response")),
        }
    }
}
