//! QUIC service for the VCS-owned exchange catalog.

use std::net::SocketAddr;
use std::sync::Arc;

use common::protocol::{VcsExchangeRequest, VcsExchangeResponse};
use common::transport::{build_quic_server, extract_control_payload, frame_control_message};
use error::{Result, VeinError};
use tracing::{debug, info, warn};

use crate::ExchangeCatalog;

const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

/// VCS exchange metadata service.
///
/// Executors and coordinators use this service to publish and discover shuffle
/// fragments. The service owns no data bytes; it only authoritatively records
/// immutable fragment descriptors and exchange lifecycle state.
pub struct VcsExchangeServer {
    catalog: Arc<dyn ExchangeCatalog>,
}

impl VcsExchangeServer {
    /// Creates a service backed by the supplied VCS exchange catalog.
    pub fn new(catalog: Arc<dyn ExchangeCatalog>) -> Self {
        Self { catalog }
    }

    /// Runs the QUIC listener until it is stopped by the caller or the server
    /// encounters a bind/accept error.
    pub async fn run(&self, bind_addr: SocketAddr, cert_path: &str, key_path: &str) -> Result<()> {
        let mut server = build_quic_server(bind_addr, cert_path, key_path)?;
        info!(target: "vein_vcs", %bind_addr, "VCS exchange service listening");

        while let Some(mut connection) = server.accept().await {
            let catalog = self.catalog.clone();
            tokio::spawn(async move {
                while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                    let catalog = catalog.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_stream(&mut stream, catalog).await {
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
    let request = decode_request(payload)?;
    let response = execute_request(catalog, request).await;
    send_response(stream, &response).await
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
