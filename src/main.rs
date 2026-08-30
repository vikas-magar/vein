use common::protocol::{ArchivedProtocolResponse, ProtocolRequest};
use common::transport::build_quic_client;
use s2n_quic::client::Connect;
use std::net::SocketAddr;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

const MASTER_ADDR: &str = "127.0.0.1:8080";
const CERT_PATH: &str = "certs/server.crt";
const SERVER_NAME: &str = "localhost";
const DEFAULT_QUERY: &str = "FROM users |> WHERE age > 18 + 2 |> SELECT id, name, age";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let query = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_QUERY.to_string());

    let master_addr: SocketAddr = MASTER_ADDR
        .parse()
        .map_err(|e| format!("Failed to parse Master address '{MASTER_ADDR}': {e}"))?;

    info!(target: "vein_client", %master_addr, tls_sni = SERVER_NAME, "Connecting to Vein Master over QUIC");
    debug!(target: "vein_client", cert_path = CERT_PATH, "Loading TLS certificate");

    let client = build_quic_client(CERT_PATH)?;
    let connect = Connect::new(master_addr).with_server_name(SERVER_NAME);
    let mut connection = client
        .connect(connect)
        .await
        .map_err(|e| format!("Failed to establish QUIC connection to Master at {master_addr}: {e}"))?;

    connection
        .keep_alive(true)
        .map_err(|e| format!("Failed to enable keep_alive on QUIC connection: {e}"))?;

    debug!(target: "vein_client", "Opening bidirectional QUIC stream");
    let stream = connection
        .open_bidirectional_stream()
        .await
        .map_err(|e| format!("Failed to open bidirectional stream: {e}"))?;

    let (mut recv, mut send) = stream.split();

    let request = ProtocolRequest::ExecuteSql { sql: query.clone() };
    debug!(target: "vein_client", "Serializing ProtocolRequest via rkyv");
    let req_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request)
        .map_err(|e| format!("Failed to serialize ProtocolRequest via rkyv: {e}"))?;

    info!(target: "vein_client", query = %query, bytes_len = req_bytes.len(), "Submitting rkyv query payload");
    send.send(bytes::Bytes::from(req_bytes.into_vec()))
        .await
        .map_err(|e| format!("Failed to send query bytes over QUIC: {e}"))?;

    send.finish()
        .map_err(|e| format!("Failed to finish send stream: {e}"))?;

    debug!(target: "vein_client", "Awaiting ProtocolResponse from Master");
    let mut response_bytes = Vec::new();
    while let Some(chunk) = recv
        .receive()
        .await
        .map_err(|e| format!("Error receiving response chunk from Master: {e}"))?
    {
        debug!(target: "vein_client", chunk_size = chunk.len(), "Received chunk from Master");
        response_bytes.extend_from_slice(&chunk);
    }

    if response_bytes.is_empty() {
        warn!(target: "vein_client", "Received empty response from Master");
        return Ok(());
    }

    debug!(target: "vein_client", total_bytes = response_bytes.len(), "Accessing ProtocolResponse zero-copy via rkyv");
    let response = rkyv::access::<ArchivedProtocolResponse, rkyv::rancor::Error>(&response_bytes)
        .map_err(|e| format!("Failed to access ProtocolResponse via rkyv: {e}"))?;

    match response {
        ArchivedProtocolResponse::Plan { text } => {
            info!(target: "vein_client", "Query plan response received successfully");
            println!("{text}");
        }
        ArchivedProtocolResponse::Data { batch } => {
            info!(target: "vein_client", batch_bytes = batch.len(), "Data batch response received successfully");
        }
        ArchivedProtocolResponse::QueryCompleted { row_count } => {
            info!(target: "vein_client", row_count = row_count.to_native(), "Query execution finished successfully");
        }
        ArchivedProtocolResponse::QuerySubmitted { query_id } => {
            info!(target: "vein_client", %query_id, "Query submitted successfully");
        }
        ArchivedProtocolResponse::QueryCancelled { query_id } => {
            info!(target: "vein_client", %query_id, "Query cancelled successfully");
        }
        ArchivedProtocolResponse::Error { message } => {
            error!(target: "vein_client", %message, "Master returned an error");
            eprintln!("Error: {message}");
        }
    }

    Ok(())
}