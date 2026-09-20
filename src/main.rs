use common::protocol::{ArchivedProtocolResponse, ProtocolRequest};
use common::transport::{ControlFrameDecoder, build_quic_client};
use s2n_quic::client::Connect;
use std::net::SocketAddr;
use common::logging;
use tracing::{debug, error, info};

const MASTER_ADDR: &str = "127.0.0.1:8080";
const CERT_PATH: &str = "certs/server.crt";
const SERVER_NAME: &str = "localhost";
const DEFAULT_QUERY: &str = "FROM users |> WHERE age > 18 + 2 |> SELECT id, name, age";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logging::init("vein-client")?;

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
    let mut connection = client.connect(connect).await.map_err(|e| {
        format!("Failed to establish QUIC connection to Master at {master_addr}: {e}")
    })?;

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

    debug!(target: "vein_client", "Awaiting streamed ProtocolResponse frames from Master");
    let mut frame_decoder = ControlFrameDecoder::new();
    let mut result_query_id = None;
    let mut saw_batch = false;

    while let Some(chunk) = recv
        .receive()
        .await
        .map_err(|e| format!("Error receiving response chunk from Master: {e}"))?
    {
        for payload in frame_decoder
            .push_chunk(&chunk)
            .map_err(|e| format!("Invalid response frame: {e}"))?
        {
            let mut aligned = rkyv::util::AlignedVec::<16>::new();
            aligned.extend_from_slice(&payload);
            let response = rkyv::access::<ArchivedProtocolResponse, rkyv::rancor::Error>(&aligned)
                .map_err(|e| format!("Failed to access ProtocolResponse: {e}"))?;

            match response {
                ArchivedProtocolResponse::Plan { text } => {
                    info!(target: "vein_client", "Query plan response received successfully");
                    println!("{text}");
                }
                ArchivedProtocolResponse::QueryResultStart { query_id } => {
                    result_query_id = Some(query_id.0.to_native());
                    info!(target: "vein_client", %query_id, "Query result stream started");
                }
                ArchivedProtocolResponse::QueryResultBatch {
                    query_id,
                    sequence,
                    batch,
                } => {
                    if result_query_id != Some(query_id.0.to_native()) {
                        return Err("result batch query ID mismatch".into());
                    }
                    let cursor = std::io::Cursor::new(batch.as_slice());
                    let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
                        .map_err(|e| format!("Failed to initialize Arrow IPC reader: {e}"))?;
                    while let Some(batch_result) = reader.next() {
                        let record_batch = batch_result
                            .map_err(|e| format!("Failed to decode result batch: {e}"))?;
                        saw_batch = true;
                        arrow::util::pretty::print_batches(&[record_batch])
                            .map_err(|e| format!("Failed to print result batch: {e}"))?;
                    }
                    debug!(target: "vein_client", sequence = sequence.to_native(), "Received result batch");
                }
                ArchivedProtocolResponse::QueryResultEnd {
                    query_id,
                    row_count,
                } => {
                    if result_query_id != Some(query_id.0.to_native()) {
                        return Err("result end query ID mismatch".into());
                    }
                    info!(target: "vein_client", row_count = row_count.to_native(), "Query execution finished successfully");
                }
                ArchivedProtocolResponse::WriteCompleted { table, row_count } => {
                    println!(
                        "WRITE OK: table '{}' ({} rows)",
                        table,
                        row_count.to_native()
                    );
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
        }
    }

    if !saw_batch {
        debug!(target: "vein_client", "Query returned no result batches");
    }

    Ok(())
}
