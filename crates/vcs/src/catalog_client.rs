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

use crate::catalog_rpc::{
    VcsCatalogRequest, VcsCatalogResponse, decode_payload, decode_response, encode_payload,
    encode_request,
};
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

    /// Verifies that the remote VCS is reachable and serving catalog requests.
    pub async fn health_check(&self) -> Result<CatalogRevision> {
        Ok(self.begin_planning_session().await?.catalog_revision)
    }

    async fn request(&self, request: VcsCatalogRequest) -> Result<VcsCatalogResponse> {
        let mut stream = self.client.open_stream(&self.target).await?;
        let payload = encode_request(&request).map_err(VeinError::serialization)?;
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
        let response = decode_response(response_payload).map_err(VeinError::serialization)?;
        match response {
            VcsCatalogResponse::Error { message } => Err(VeinError::internal(format!(
                "VCS catalog rejected request: {message}"
            ))),
            response => Ok(response),
        }
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
        let VcsCatalogResponse::Table {
            revision,
            metadata,
            snapshot: token,
        } = response
        else {
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
        let metadata: Arc<TableMetadata> = Arc::new(
            decode_payload(&metadata, "table metadata").map_err(VeinError::serialization)?,
        );
        let mut snapshot = crate::TableSnapshot::from_metadata(metadata.clone(), revision);
        snapshot.table_id = crate::TableId::new(token.table_id.clone());
        snapshot.snapshot_id = crate::SnapshotId::new(token.snapshot_id.clone());
        snapshot.schema_version = crate::SchemaVersion::new(token.schema_version);
        session
            .insert(Arc::new(snapshot))
            .map_err(|error| VeinError::internal(error.to_string()))?;
        Ok(metadata)
    }

    async fn resolve_table_snapshot(
        &self,
        session: &mut PlanningSession,
        name: &str,
    ) -> Result<Arc<crate::TableSnapshot>> {
        self.resolve_table(session, name).await?;
        session
            .table(name)
            .ok_or_else(|| VeinError::internal("remote VCS did not return a table snapshot"))
    }

    async fn validate_snapshot(
        &self,
        token: &common::protocol::CatalogSnapshotToken,
    ) -> Result<()> {
        match self
            .request(VcsCatalogRequest::ValidateSnapshot {
                snapshot: token.clone(),
            })
            .await?
        {
            VcsCatalogResponse::SnapshotValidated => Ok(()),
            _ => Err(VeinError::serialization(
                "unexpected VCS snapshot validation response",
            )),
        }
    }
}

#[async_trait]
impl CatalogWriter for RemoteVcsCatalog {
    async fn register_table(&self, metadata: TableMetadata) -> Result<()> {
        let revision = self.begin_planning_session().await?.catalog_revision;
        self.register_table_at(&revision, metadata).await
    }

    async fn register_external_source(&self, source: &str) -> Result<Arc<TableMetadata>> {
        match self
            .request(VcsCatalogRequest::RegisterExternalSource {
                source: source.to_owned(),
            })
            .await?
        {
            VcsCatalogResponse::Table { metadata, .. } => Ok(Arc::new(
                decode_payload(&metadata, "table metadata").map_err(VeinError::serialization)?,
            )),
            _ => Err(VeinError::serialization(
                "unexpected VCS external-source response",
            )),
        }
    }

    async fn register_table_at(
        &self,
        expected_revision: &CatalogRevision,
        metadata: TableMetadata,
    ) -> Result<()> {
        let payload =
            encode_payload(&metadata, "table metadata").map_err(VeinError::serialization)?;
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
                decode_payload(&intent, "table write intent").map_err(VeinError::serialization)
            }
            _ => Err(VeinError::serialization("unexpected VCS prepare response")),
        }
    }

    async fn publish_table_write_part(
        &self,
        write_id: WriteId,
        part: TableWritePart,
    ) -> Result<CommitReceipt> {
        let payload =
            encode_payload(&part, "table write part").map_err(VeinError::serialization)?;
        let response = self
            .request(VcsCatalogRequest::PublishTableWritePart {
                write_id: write_id.to_string(),
                part: payload,
            })
            .await?;
        match response {
            VcsCatalogResponse::Receipt { receipt } => {
                decode_payload(&receipt, "commit receipt").map_err(VeinError::serialization)
            }
            _ => Err(VeinError::serialization("unexpected VCS part response")),
        }
    }

    async fn commit_table_write(
        &self,
        write_id: WriteId,
        snapshot: DistributedTableSnapshot,
    ) -> Result<CommitReceipt> {
        let payload =
            encode_payload(&snapshot, "table snapshot").map_err(VeinError::serialization)?;
        let response = self
            .request(VcsCatalogRequest::CommitTableWrite {
                write_id: write_id.to_string(),
                snapshot: payload,
            })
            .await?;
        match response {
            VcsCatalogResponse::Receipt { receipt } => {
                decode_payload(&receipt, "commit receipt").map_err(VeinError::serialization)
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
                decode_payload(&receipt, "commit receipt").map_err(VeinError::serialization)
            }
            _ => Err(VeinError::serialization("unexpected VCS abort response")),
        }
    }
}
