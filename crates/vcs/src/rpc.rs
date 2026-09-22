//! QUIC service for the VCS-owned exchange catalog.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::catalog_rpc::{
    VcsCatalogRequest, VcsCatalogResponse, decode_payload,
    decode_request as decode_catalog_request, encode_payload, encode_response,
};
use crate::{CatalogReader, CatalogWriter, TableWriteCatalog};
use common::protocol::{VcsExchangeRequest, VcsExchangeResponse, VeinControlEvent};
use common::transport::{build_quic_server, extract_control_payload, frame_control_message};
use distributed::RaftRpcHandler;
use distributed::{TableId, TableSnapshot, TableWriteMode, TableWritePart, WriteId};
use error::{Result, VeinError};
use tracing::{debug, info, warn};

use crate::ExchangeCatalog;

// Keep the listener limit aligned with the bounded Raft RPC transport limit.
// Snapshot chunking remains a separate concern; this prevents the VCS endpoint
// from rejecting valid bounded control frames before OpenRaft sees them.
const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// VCS exchange metadata service.
///
/// Executors and coordinators use this service to publish and discover shuffle
/// fragments. The service owns no data bytes; it only authoritatively records
/// immutable fragment descriptors and exchange lifecycle state.
pub struct VcsExchangeServer {
    catalog: Arc<dyn ExchangeCatalog>,
    raft_rpc_handler: Option<Arc<dyn RaftRpcHandler>>,
    catalog_reader: Option<Arc<dyn CatalogReader>>,
    catalog_writer: Option<Arc<dyn CatalogWriter>>,
    table_write_catalog: Option<Arc<dyn TableWriteCatalog>>,
}

impl VcsExchangeServer {
    /// Creates a service backed by the supplied VCS exchange catalog.
    pub fn new(catalog: Arc<dyn ExchangeCatalog>) -> Self {
        Self {
            catalog,
            raft_rpc_handler: None,
            catalog_reader: None,
            catalog_writer: None,
            table_write_catalog: None,
        }
    }

    /// Attaches an OpenRaft RPC handler to the shared VCS control-plane listener.
    pub fn with_raft_rpc_handler(mut self, handler: Arc<dyn RaftRpcHandler>) -> Self {
        self.raft_rpc_handler = Some(handler);
        self
    }

    /// Attaches the authoritative table catalog contracts to this listener.
    pub fn with_catalog_authority(
        mut self,
        reader: Arc<dyn CatalogReader>,
        writer: Arc<dyn CatalogWriter>,
        table_writes: Arc<dyn TableWriteCatalog>,
    ) -> Self {
        self.catalog_reader = Some(reader);
        self.catalog_writer = Some(writer);
        self.table_write_catalog = Some(table_writes);
        self
    }

    /// Runs the QUIC listener until it is stopped by the caller or the server
    /// encounters a bind/accept error.
    pub async fn run(&self, bind_addr: SocketAddr, cert_path: &str, key_path: &str) -> Result<()> {
        let mut server = build_quic_server(bind_addr, cert_path, key_path)?;
        info!(target: "vein_vcs", %bind_addr, "VCS exchange service listening");

        while let Some(mut connection) = server.accept().await {
            let catalog = self.catalog.clone();
            let raft_rpc_handler = self.raft_rpc_handler.clone();
            let catalog_reader = self.catalog_reader.clone();
            let catalog_writer = self.catalog_writer.clone();
            let table_write_catalog = self.table_write_catalog.clone();
            tokio::spawn(async move {
                while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let catalog = catalog.clone();
                    let raft_rpc_handler = raft_rpc_handler.clone();
                    let catalog_reader = catalog_reader.clone();
                    let catalog_writer = catalog_writer.clone();
                    let table_write_catalog = table_write_catalog.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_stream(
                            &mut stream,
                            catalog,
                            raft_rpc_handler,
                            catalog_reader,
                            catalog_writer,
                            table_write_catalog,
                        )
                        .await
                        {
                            warn!(target: "vein_vcs", %error, "VCS exchange request failed");
                        }
                    });
                }
            });
        }

        Ok(())
    }
}

/// Handles one length-prefixed VCS request and returns one typed response.
async fn handle_stream(
    stream: &mut s2n_quic::stream::BidirectionalStream,
    catalog: Arc<dyn ExchangeCatalog>,
    raft_rpc_handler: Option<Arc<dyn RaftRpcHandler>>,
    catalog_reader: Option<Arc<dyn CatalogReader>>,
    catalog_writer: Option<Arc<dyn CatalogWriter>>,
    table_write_catalog: Option<Arc<dyn TableWriteCatalog>>,
) -> Result<()> {
    let mut buffer = Vec::new();
    while let Ok(Some(chunk)) = stream.receive().await {
        buffer.extend_from_slice(&chunk);
        if buffer.len() > MAX_REQUEST_BYTES {
            let response = VcsExchangeResponse::Error {
                message: "VCS request exceeds the maximum allowed size".to_string(),
            };
            send_response(stream, &response).await?;
            return Ok(());
        }
    }

    if buffer.is_empty() {
        return Err(VeinError::connection_failed(
            "VCS client closed stream without a request",
        ));
    }

    let payload = extract_control_payload(&buffer)
        .ok_or_else(|| VeinError::serialization("invalid VCS request frame"))?;
    if let Some(handler) = raft_rpc_handler
        && let Ok(VeinControlEvent::RaftRpcRequest(request)) = decode_control_event(payload)
    {
        let response = handler.handle(request).await;
        send_control_response(stream, &response).await?;
        return Ok(());
    }

    if let Ok(request) = decode_catalog_request(payload) {
        let response =
            execute_catalog_request(request, catalog_reader, catalog_writer, table_write_catalog)
                .await;
        return send_catalog_response(stream, &response).await;
    }

    let request = decode_request(payload)?;
    let response = execute_request(catalog, request).await;
    send_response(stream, &response).await
}

fn decode_control_event(payload: &[u8]) -> Result<VeinControlEvent> {
    let mut aligned = rkyv::util::AlignedVec::<16>::new();
    aligned.extend_from_slice(payload);
    rkyv::from_bytes::<VeinControlEvent, rkyv::rancor::Error>(&aligned)
        .map_err(|error| VeinError::serialization(format!("invalid control event: {error}")))
}
fn decode_request(payload: &[u8]) -> Result<VcsExchangeRequest> {
    let mut aligned = rkyv::util::AlignedVec::<16>::new();
    aligned.extend_from_slice(payload);
    rkyv::from_bytes::<VcsExchangeRequest, rkyv::rancor::Error>(&aligned)
        .map_err(|error| VeinError::serialization(format!("invalid VCS request: {error}")))
}

async fn execute_request(
    catalog: Arc<dyn ExchangeCatalog>,
    request: VcsExchangeRequest,
) -> VcsExchangeResponse {
    let result = match request {
        VcsExchangeRequest::Create {
            key,
            partition_count,
            expected_tasks,
            schema_fingerprint,
        } => catalog
            .create_exchange(key, partition_count, expected_tasks, schema_fingerprint)
            .await
            .map(|manifest| VcsExchangeResponse::Manifest { manifest }),
        VcsExchangeRequest::Publish { key, fragment } => catalog
            .publish_fragment(key, fragment)
            .await
            .map(|_| VcsExchangeResponse::Ack),
        VcsExchangeRequest::Seal { key } => catalog
            .seal_exchange(key)
            .await
            .map(|manifest| VcsExchangeResponse::Manifest { manifest }),
        VcsExchangeRequest::GetManifest { key } => catalog
            .get_manifest(key)
            .await
            .map(|manifest| VcsExchangeResponse::Manifest { manifest }),
        VcsExchangeRequest::GetPartition { key, partition_id } => catalog
            .get_partition(key, partition_id.into_raw() as u32)
            .await
            .map(|fragments| VcsExchangeResponse::Partition { fragments }),
        VcsExchangeRequest::Delete { key } => catalog
            .delete_exchange(key)
            .await
            .map(|_| VcsExchangeResponse::Ack),
        VcsExchangeRequest::RegisterSpill { spill } => catalog
            .register_spill(spill)
            .await
            .map(|_| VcsExchangeResponse::Ack),
        VcsExchangeRequest::GetSpills {
            key,
            task_id,
            attempt,
        } => catalog
            .get_spills(key, task_id, attempt)
            .await
            .map(|spills| VcsExchangeResponse::Spills { spills }),
        VcsExchangeRequest::SealSpill { storage_path } => catalog
            .seal_spill(&storage_path)
            .await
            .map(|_| VcsExchangeResponse::Ack),
        VcsExchangeRequest::AbortSpill { storage_path } => catalog
            .abort_spill(&storage_path)
            .await
            .map(|_| VcsExchangeResponse::Ack),
        VcsExchangeRequest::DeleteSpill { storage_path } => catalog
            .delete_spill(&storage_path)
            .await
            .map(|_| VcsExchangeResponse::Ack),
    };

    result.unwrap_or_else(|error| VcsExchangeResponse::Error {
        message: error.to_string(),
    })
}

async fn execute_catalog_request(
    request: VcsCatalogRequest,
    reader: Option<Arc<dyn CatalogReader>>,
    writer: Option<Arc<dyn CatalogWriter>>,
    table_writes: Option<Arc<dyn TableWriteCatalog>>,
) -> VcsCatalogResponse {
    let result = async {
        match request {
            VcsCatalogRequest::BeginPlanning => {
                let reader = reader
                    .as_ref()
                    .ok_or_else(|| VeinError::internal("VCS catalog reader is not configured"))?;
                let session = reader.begin_planning_session().await?;
                Ok(VcsCatalogResponse::Revision {
                    revision: session.catalog_revision.as_str().to_string(),
                })
            }
            VcsCatalogRequest::ResolveTable { revision, name } => {
                let reader = reader
                    .as_ref()
                    .ok_or_else(|| VeinError::internal("VCS catalog reader is not configured"))?;
                let mut session =
                    crate::PlanningSession::new(crate::CatalogRevision::new(revision));
                let snapshot = reader.resolve_table_snapshot(&mut session, &name).await?;
                let payload = encode_payload(snapshot.metadata.as_ref(), "table metadata")
                    .map_err(VeinError::serialization)?;
                let token = common::protocol::CatalogSnapshotToken::new(
                    snapshot.table_id.as_str(),
                    snapshot.catalog_revision.as_str(),
                    snapshot.snapshot_id.as_str(),
                    snapshot.schema_version.value(),
                );
                Ok(VcsCatalogResponse::Table {
                    revision: session.catalog_revision.as_str().to_string(),
                    metadata: payload,
                    snapshot: token,
                })
            }
            VcsCatalogRequest::ValidateSnapshot { snapshot } => {
                let reader = reader
                    .as_ref()
                    .ok_or_else(|| VeinError::internal("VCS catalog reader is not configured"))?;
                reader.validate_snapshot(&snapshot).await?;
                Ok(VcsCatalogResponse::SnapshotValidated)
            }
            VcsCatalogRequest::RegisterTable {
                expected_revision,
                metadata,
            } => {
                let writer = writer
                    .as_ref()
                    .ok_or_else(|| VeinError::internal("VCS catalog writer is not configured"))?;
                let metadata: plan::TableMetadata = decode_payload(&metadata, "table metadata")
                    .map_err(VeinError::serialization)?;
                writer
                    .register_table_at(&crate::CatalogRevision::new(expected_revision), metadata)
                    .await?;
                Ok(VcsCatalogResponse::Ack)
            }
            VcsCatalogRequest::PrepareTableWrite {
                write_id,
                table_id,
                mode,
                expected_parts,
            } => {
                let table_writes = table_writes.as_ref().ok_or_else(|| {
                    VeinError::internal("VCS table-write catalog is not configured")
                })?;
                let mode = match mode.as_str() {
                    "overwrite" => TableWriteMode::Overwrite,
                    "append" => TableWriteMode::Append,
                    value => {
                        return Err(VeinError::internal(format!(
                            "invalid table write mode '{value}'"
                        )));
                    }
                };
                let intent = table_writes
                    .prepare_table_write(
                        WriteId::from(write_id),
                        TableId::from(table_id),
                        mode,
                        expected_parts,
                    )
                    .await?;
                Ok(VcsCatalogResponse::WriteIntent {
                    intent: encode_payload(&intent, "table write intent")
                        .map_err(VeinError::serialization)?,
                })
            }
            VcsCatalogRequest::PublishTableWritePart { write_id, part } => {
                let table_writes = table_writes.as_ref().ok_or_else(|| {
                    VeinError::internal("VCS table-write catalog is not configured")
                })?;
                let part: TableWritePart =
                    decode_payload(&part, "table write part").map_err(VeinError::serialization)?;
                let receipt = table_writes
                    .publish_table_write_part(WriteId::from(write_id), part)
                    .await?;
                Ok(VcsCatalogResponse::Receipt {
                    receipt: encode_payload(&receipt, "commit receipt")
                        .map_err(VeinError::serialization)?,
                })
            }
            VcsCatalogRequest::CommitTableWrite { write_id, snapshot } => {
                let table_writes = table_writes.as_ref().ok_or_else(|| {
                    VeinError::internal("VCS table-write catalog is not configured")
                })?;
                let snapshot: TableSnapshot = decode_payload(&snapshot, "table snapshot")
                    .map_err(VeinError::serialization)?;
                let receipt = table_writes
                    .commit_table_write(WriteId::from(write_id), snapshot)
                    .await?;
                Ok(VcsCatalogResponse::Receipt {
                    receipt: encode_payload(&receipt, "commit receipt")
                        .map_err(VeinError::serialization)?,
                })
            }
            VcsCatalogRequest::AbortTableWrite { write_id } => {
                let table_writes = table_writes.as_ref().ok_or_else(|| {
                    VeinError::internal("VCS table-write catalog is not configured")
                })?;
                let receipt = table_writes
                    .abort_table_write(WriteId::from(write_id))
                    .await?;
                Ok(VcsCatalogResponse::Receipt {
                    receipt: encode_payload(&receipt, "commit receipt")
                        .map_err(VeinError::serialization)?,
                })
            }
        }
    }
    .await;

    result.unwrap_or_else(|error: VeinError| VcsCatalogResponse::Error {
        message: error.to_string(),
    })
}

async fn send_catalog_response(
    stream: &mut s2n_quic::stream::BidirectionalStream,
    response: &VcsCatalogResponse,
) -> Result<()> {
    let bytes = encode_response(response).map_err(VeinError::serialization)?;
    stream
        .send(frame_control_message(&bytes))
        .await
        .map_err(|error| VeinError::quic_send(error.to_string()))?;
    stream
        .finish()
        .map_err(|error| VeinError::quic_send(error.to_string()))?;
    Ok(())
}

async fn send_control_response(
    stream: &mut s2n_quic::stream::BidirectionalStream,
    response: &VeinControlEvent,
) -> Result<()> {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(response).map_err(|error| {
        VeinError::serialization(format!("failed to encode Raft response: {error}"))
    })?;
    stream
        .send(frame_control_message(&bytes))
        .await
        .map_err(|error| VeinError::quic_send(error.to_string()))?;
    stream
        .finish()
        .map_err(|error| VeinError::quic_send(error.to_string()))?;
    Ok(())
}

async fn send_response(
    stream: &mut s2n_quic::stream::BidirectionalStream,
    response: &VcsExchangeResponse,
) -> Result<()> {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(response).map_err(|error| {
        VeinError::serialization(format!("failed to encode VCS response: {error}"))
    })?;
    debug!(target: "vein_vcs", bytes = bytes.len(), "Sending VCS exchange response");
    stream
        .send(frame_control_message(&bytes))
        .await
        .map_err(|error| VeinError::quic_send(error.to_string()))?;
    stream
        .finish()
        .map_err(|error| VeinError::quic_send(error.to_string()))?;
    Ok(())
}
