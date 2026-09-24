use common::protocol::{ArchivedProtocolResponse, ProtocolRequest};
use common::transport::{ControlFrameDecoder, build_quic_client};
use s2n_quic::client::Connect;
use std::io::Write;
use std::net::SocketAddr;
use std::time::Instant;

use common::logging;
use tracing::{debug, error, info};

const MASTER_ADDR: &str = "127.0.0.1:8080";
const CERT_PATH: &str = "certs/server.crt";
const SERVER_NAME: &str = "localhost";
const DEFAULT_QUERY: &str = "FROM users |> WHERE age > 18 + 2 |> SELECT id, name, age";

fn print_streamed_batch(
    batch: &arrow::record_batch::RecordBatch,
    printed_header: &mut bool,
    table_footer: &mut Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let formatted = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(batch))
        .map_err(|e| format!("Failed to format result batch: {e}"))?
        .to_string();
    let lines: Vec<&str> = formatted.lines().collect();

    if !*printed_header {
        if let Some(footer) = lines.last() {
            *table_footer = Some((*footer).to_string());
        }
        for line in lines.iter().take(lines.len().saturating_sub(1)) {
            println!("{line}");
        }
        *printed_header = true;
        return Ok(());
    }

    // Arrow's formatter emits a complete table for each batch. For subsequent
    // batches, retain only data rows so the client presents one logical result.
    for line in lines.iter().skip(3).take(lines.len().saturating_sub(4)) {
        if line.starts_with('|') {
            println!("{line}");
        }
    }

    Ok(())
}

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
    let query_started = Instant::now();
    let mut result_query_id = None;
    let mut saw_batch = false;
    let mut printed_header = false;
    let mut table_footer = None;

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
                        print_streamed_batch(
                            &record_batch,
                            &mut printed_header,
                            &mut table_footer,
                        )?;
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
                    if let Some(table_footer) = table_footer.as_deref() {
                        println!("{table_footer}");
                        std::io::stdout()
                            .flush()
                            .map_err(|e| format!("Failed to flush rendered result: {e}"))?;
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
    info!(
        target: "vein_client",
        elapsed_ms = query_started.elapsed().as_millis() as u64,
        "Query client stream completed"
    );

    Ok(())
}
