use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use common::arrow::AsyncIpcDecoder;
use common::identifiers::{QueryId, TaskId};
use common::protocol::{ArchivedVeinControlEvent, ExecuteTask, VeinControlEvent};
use common::transport::{
    ConnectionManager, ControlFrameDecoder, QuicClient, build_quic_client, frame_control_message,
};
use common::{
    ResultBatch, ResultChannelSender, ResultEvent, SendableResultStream, bounded_result_channel,
};
use error::VeinError;

use crate::executor::NodeInfo;
use crate::traits::{TaskCompletion, TaskDispatcher};

const DEFAULT_CERT_PATH: &str = "certs/server.crt";

struct SharedArchivedSlice {
    owner: Arc<rkyv::util::AlignedVec<16>>,
    range: Range<usize>,
}

impl AsRef<[u8]> for SharedArchivedSlice {
    fn as_ref(&self) -> &[u8] {
        &self.owner[self.range.clone()]
    }
}

fn archived_slice_range(owner: &[u8], slice: &[u8]) -> Result<Range<usize>, VeinError> {
    let owner_start = owner.as_ptr() as usize;
    let slice_start = slice.as_ptr() as usize;
    let start = slice_start
        .checked_sub(owner_start)
        .ok_or_else(|| VeinError::internal("result IPC payload is outside its archived frame"))?;
    let end = start
        .checked_add(slice.len())
        .filter(|&end| end <= owner.len())
        .ok_or_else(|| VeinError::internal("result IPC payload exceeds its archived frame"))?;
    Ok(start..end)
}

async fn send_result_batch(
    schema_state: &tokio::sync::Mutex<Option<SchemaRef>>,
    sender: &ResultChannelSender,
    task_id: TaskId,
    attempt: u32,
    sequence: u64,
    batch: arrow::record_batch::RecordBatch,
) -> Result<(), VeinError> {
    let schema = batch.schema();
    let mut current_schema = schema_state.lock().await;
    match current_schema.as_ref() {
        Some(expected) if expected.as_ref() != schema.as_ref() => {
            return Err(VeinError::internal(
                "task result schema differs from the query result schema",
            ));
        }
        Some(_) => {}
        None => {
            sender
                .send(Ok(ResultEvent::Start {
                    schema: schema.clone(),
                }))
                .await
                .map_err(|_| VeinError::internal("result stream consumer dropped"))?;
            *current_schema = Some(schema);
        }
    }
    sender
        .send(Ok(ResultEvent::Batch(ResultBatch::new(
            task_id, attempt, sequence, batch,
        ))))
        .await
        .map_err(|_| VeinError::internal("result stream consumer dropped"))
}

fn validate_result_identity(
    event_task_id: TaskId,
    event_query_id: QueryId,
    event_attempt: u32,
    expected_task_id: TaskId,
    expected_query_id: QueryId,
    expected_attempt: u32,
) -> Result<(), VeinError> {
    if (event_task_id, event_query_id, event_attempt)
        != (expected_task_id, expected_query_id, expected_attempt)
    {
        return Err(VeinError::internal(
            "executor result identity does not match dispatched task",
        ));
    }

    Ok(())
}

/// The QUIC Task Dispatcher.
///
/// Responsible for serializing `ExecuteTask`s and streaming them to the appropriate
/// Executor node via a highly concurrent, multiplexed s2n-quic stream.
pub struct QuicTaskDispatcher {
    cert_path: String,
    client: Mutex<Option<Arc<QuicClient>>>,
    completions: Arc<
        Mutex<
            HashMap<
                (QueryId, TaskId, u32),
                tokio::sync::oneshot::Receiver<Result<TaskCompletion, VeinError>>,
            >,
        >,
    >,
    result_senders: Arc<Mutex<HashMap<QueryId, ResultChannelSender>>>,
    result_schemas: Arc<Mutex<HashMap<QueryId, Arc<tokio::sync::Mutex<Option<SchemaRef>>>>>>,
}

impl Default for QuicTaskDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl QuicTaskDispatcher {
    /// Create a new QUIC task dispatcher with the default certificate path.
    pub fn new() -> Self {
        Self::with_cert(DEFAULT_CERT_PATH)
    }

    /// Create a new QUIC task dispatcher using a custom TLS certificate path.
    pub fn with_cert(cert_path: impl Into<String>) -> Self {
        Self {
            cert_path: cert_path.into(),
            client: Mutex::new(None),
            completions: Arc::new(Mutex::new(HashMap::new())),
            result_senders: Arc::new(Mutex::new(HashMap::new())),
            result_schemas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new QUIC task dispatcher with a pre-configured [`QuicClient`].
    pub fn with_client(client: Arc<QuicClient>) -> Self {
        Self {
            cert_path: DEFAULT_CERT_PATH.to_string(),
            client: Mutex::new(Some(client)),
            completions: Arc::new(Mutex::new(HashMap::new())),
            result_senders: Arc::new(Mutex::new(HashMap::new())),
            result_schemas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Open a bounded result stream before query tasks are dispatched.
    pub fn open_result_stream(&self, query_id: QueryId) -> SendableResultStream {
        let (sender, stream) = bounded_result_channel(32);
        if let Ok(mut senders) = self.result_senders.lock() {
            senders.insert(query_id, sender);
        }
        if let Ok(mut schemas) = self.result_schemas.lock() {
            schemas.insert(query_id, Arc::new(tokio::sync::Mutex::new(None)));
        }
        stream
    }

    /// Close a query result stream after all task readers have finished.
    pub fn close_result_stream(&self, query_id: QueryId) {
        if let Ok(mut senders) = self.result_senders.lock() {
            senders.remove(&query_id);
        }
        if let Ok(mut schemas) = self.result_schemas.lock() {
            schemas.remove(&query_id);
        }
    }

    /// Get or lazily initialize the shared [`QuicClient`].
    fn get_or_init_client(&self) -> Result<Arc<QuicClient>, VeinError> {
        let mut guard = self
            .client
            .lock()
            .map_err(|_| VeinError::internal("QuicTaskDispatcher client mutex poisoned"))?;

        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let quic_client_raw = build_quic_client(&self.cert_path).map_err(|e| {
            VeinError::internal(format!("Failed to build QUIC client for dispatcher: {e}"))
        })?;
        let client = Arc::new(QuicClient::new(quic_client_raw));
        *guard = Some(client.clone());
        Ok(client)
    }
}

#[async_trait]
impl TaskDispatcher for QuicTaskDispatcher {
    async fn dispatch(&self, executor: &NodeInfo, task: ExecuteTask) -> Result<(), VeinError> {
        let client = self.get_or_init_client()?;
        let target_addr = executor.addr.to_string();
        let request_id = task.request_id;
        let query_id = task.query_id;
        let task_id = task.task_id;
        let expected_attempt = task.attempt;
        let rpc_started = Instant::now();

        tracing::debug!(
            target: "vein_dispatcher",
            %request_id,
            %query_id,
            %task_id,
            attempt = expected_attempt,
            executor_addr = %target_addr,
            "Opening QUIC stream to dispatch task"
        );

        let mut stream = client.open_stream(&target_addr).await.map_err(|e| {
            VeinError::internal(format!(
                "Failed to open stream to executor at {target_addr}: {e}"
            ))
        })?;

        // 1. Serialize DispatchTask control event via rkyv
        let event = VeinControlEvent::DispatchTask(task);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event).map_err(|e| {
            VeinError::internal(format!("Failed to serialize DispatchTask via rkyv: {e}"))
        })?;

        // 2. Send opcode (0x02) + length-prefixed payload
        let wire = frame_control_message(&bytes);
        let wire_len = wire.len();
        stream.send(wire).await.map_err(|e| {
            VeinError::internal(format!("Failed to send task payload to executor: {e}"))
        })?;

        stream
            .finish()
            .map_err(|e| VeinError::internal(format!("Failed to finish send stream: {e}")))?;

        tracing::info!(
            target: "vein.audit",
            event = "rpc_task_dispatch_sent",
            %request_id,
            %query_id,
            %task_id,
            attempt = expected_attempt,
            executor_addr = %target_addr,
            bytes_sent = wire_len,
            duration_ms = rpc_started.elapsed().as_millis() as u64,
            "rpc_task_dispatch_sent"
        );

        // 3. Setup completion oneshot channel and spawn reader task
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut completions = self
                .completions
                .lock()
                .map_err(|_| VeinError::internal("completions mutex poisoned"))?;
            completions.insert((query_id, task_id, expected_attempt), rx);
        }

        let result_senders = self.result_senders.clone();
        let result_schemas = self.result_schemas.clone();
        tokio::spawn(async move {
            let mut frame_decoder = ControlFrameDecoder::new();
            let mut saw_frame = false;
            let mut saw_start = false;
            let mut sequence = 0u64;
            let mut received_rows = 0u64;
            let mut response_bytes = 0u64;

            loop {
                let chunk = match stream.receive().await {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = tx.send(Err(VeinError::TaskFailed {
                            task_id: task_id.to_string(),
                            message: format!("executor result stream failed: {error}"),
                        }));
                        return;
                    }
                };
                response_bytes += chunk.len() as u64;
                let frames = match frame_decoder.push_chunk(&chunk) {
                    Ok(frames) => frames,
                    Err(error) => {
                        let _ = tx.send(Err(error));
                        return;
                    }
                };

                for payload in frames {
                    saw_frame = true;
                    let mut aligned = rkyv::util::AlignedVec::<16>::new();
                    aligned.extend_from_slice(&payload);
                    let aligned = Arc::new(aligned);
                    let archived = match rkyv::access::<ArchivedVeinControlEvent, rkyv::rancor::Error>(
                        &aligned[..],
                    ) {
                        Ok(event) => event,
                        Err(error) => {
                            let _ = tx.send(Err(VeinError::internal(format!(
                                "failed to decode executor result frame: {error}"
                            ))));
                            return;
                        }
                    };

                    match archived {
                        ArchivedVeinControlEvent::TaskResultStart {
                            task_id: event_task_id,
                            query_id: event_query_id,
                            attempt,
                        } => {
                            if let Err(error) = validate_result_identity(
                                TaskId::from_raw(event_task_id.0.to_native()),
                                QueryId::from_raw(event_query_id.0.to_native()),
                                attempt.to_native(),
                                task_id,
                                query_id,
                                expected_attempt,
                            ) {
                                let _ = tx.send(Err(error));
                                return;
                            }
                            if saw_start {
                                let _ = tx
                                    .send(Err(VeinError::internal("duplicate result start frame")));
                                return;
                            }
                            saw_start = true;
                        }
                        ArchivedVeinControlEvent::TaskResultBatch {
                            task_id: event_task_id,
                            query_id: event_query_id,
                            result_bytes,
                            attempt,
                            sequence: frame_sequence,
                            ..
                        } => {
                            if let Err(error) = validate_result_identity(
                                TaskId::from_raw(event_task_id.0.to_native()),
                                QueryId::from_raw(event_query_id.0.to_native()),
                                attempt.to_native(),
                                task_id,
                                query_id,
                                expected_attempt,
                            ) {
                                let _ = tx.send(Err(error));
                                return;
                            }
                            if !saw_start {
                                let _ = tx.send(Err(VeinError::internal(
                                    "result batch arrived outside the result stream lifecycle",
                                )));
                                return;
                            }
                            let frame_sequence = frame_sequence.to_native();
                            if frame_sequence != sequence {
                                let _ = tx.send(Err(VeinError::internal(
                                    "result batch sequence is not monotonic",
                                )));
                                return;
                            }
                            let sender = match result_senders.lock() {
                                Ok(senders) => senders.get(&query_id).cloned(),
                                Err(_) => {
                                    let _ = tx.send(Err(VeinError::internal(
                                        "result sender registry mutex poisoned",
                                    )));
                                    return;
                                }
                            };
                            let Some(sender) = sender else {
                                let _ = tx.send(Err(VeinError::internal(
                                    "query result stream was not opened before task dispatch",
                                )));
                                return;
                            };

                            let ipc_range = match archived_slice_range(&aligned, result_bytes) {
                                Ok(range) => range,
                                Err(error) => {
                                    let _ = tx.send(Err(error));
                                    return;
                                }
                            };
                            let ipc_payload = Bytes::from_owner(SharedArchivedSlice {
                                owner: aligned.clone(),
                                range: ipc_range,
                            });
                            let mut decoder = AsyncIpcDecoder::new();
                            let batches = match decoder.push_chunk(ipc_payload) {
                                Ok(batches) if batches.len() == 1 => batches,
                                Ok(_) => {
                                    let _ = tx.send(Err(VeinError::internal(
                                        "result batch frame must contain exactly one record batch",
                                    )));
                                    return;
                                }
                                Err(error) => {
                                    let _ = tx.send(Err(error));
                                    return;
                                }
                            };
                            let Some(batch) = batches.into_iter().next() else {
                                let _ = tx.send(Err(VeinError::internal(
                                    "result batch decoder returned no record batch",
                                )));
                                return;
                            };
                            received_rows = received_rows.saturating_add(batch.num_rows() as u64);
                            let schema_state = result_schemas
                                .lock()
                                .map_err(|_| VeinError::internal("result schema mutex poisoned"))
                                .and_then(|schemas| {
                                    schemas.get(&query_id).cloned().ok_or_else(|| {
                                        VeinError::internal("query result schema state is missing")
                                    })
                                });
                            let schema_state = match schema_state {
                                Ok(state) => state,
                                Err(error) => {
                                    let _ = tx.send(Err(error));
                                    return;
                                }
                            };
                            if let Err(error) = send_result_batch(
                                &schema_state,
                                &sender,
                                task_id,
                                expected_attempt,
                                frame_sequence,
                                batch,
                            )
                            .await
                            {
                                let _ = tx.send(Err(error));
                                return;
                            }
                            sequence += 1;
                        }
                        ArchivedVeinControlEvent::TaskResultEnd {
                            task_id: event_task_id,
                            query_id: event_query_id,
                            row_count,
                            bytes_read,
                            duration_ns,
                            attempt,
                        } => {
                            if let Err(error) = validate_result_identity(
                                TaskId::from_raw(event_task_id.0.to_native()),
                                QueryId::from_raw(event_query_id.0.to_native()),
                                attempt.to_native(),
                                task_id,
                                query_id,
                                expected_attempt,
                            ) {
                                let _ = tx.send(Err(error));
                                return;
                            }
                            if !saw_start {
                                let _ = tx.send(Err(VeinError::internal(
                                    "result end arrived outside the result stream lifecycle",
                                )));
                                return;
                            }
                            let row_count = row_count.to_native();
                            if received_rows != row_count {
                                let _ = tx.send(Err(VeinError::internal(format!(
                                    "result row count mismatch: received {received_rows}, declared {row_count}",
                                ))));
                                return;
                            }
                            tracing::info!(
                                target: "vein.audit",
                                event = "rpc_task_result_received",
                                %request_id,
                                %query_id,
                                %task_id,
                                attempt = expected_attempt,
                                rows = row_count,
                                bytes_received = response_bytes,
                                duration_ms = rpc_started.elapsed().as_millis() as u64,
                                "rpc_task_result_received"
                            );
                            let _ = tx.send(Ok(TaskCompletion {
                                row_count,
                                bytes_read: bytes_read.to_native(),
                                duration_ns: duration_ns.to_native(),
                                write_parts: Vec::new(),
                            }));
                            return;
                        }
                        ArchivedVeinControlEvent::TaskCompleted {
                            task_id: event_task_id,
                            query_id: event_query_id,
                            row_count,
                            bytes_read,
                            duration_ns,
                            result_bytes,
                            write_parts,
                            attempt,
                            ..
                        } => {
                            if let Err(error) = validate_result_identity(
                                TaskId::from_raw(event_task_id.0.to_native()),
                                QueryId::from_raw(event_query_id.0.to_native()),
                                attempt.to_native(),
                                task_id,
                                query_id,
                                expected_attempt,
                            ) {
                                let _ = tx.send(Err(error));
                                return;
                            }
                            let has_result_bytes =
                                matches!(&result_bytes, rkyv::option::ArchivedOption::Some(_));
                            let mut decoded_rows = 0u64;
                            let sender = match result_senders.lock() {
                                Ok(senders) => senders.get(&query_id).cloned(),
                                Err(_) => {
                                    let _ = tx.send(Err(VeinError::internal(
                                        "result sender registry mutex poisoned",
                                    )));
                                    return;
                                }
                            };
                            let Some(sender) = sender else {
                                let _ = tx.send(Err(VeinError::internal(
                                    "query result stream was not opened before task dispatch",
                                )));
                                return;
                            };
                            if let rkyv::option::ArchivedOption::Some(batch_bytes) = result_bytes {
                                let ipc_range = match archived_slice_range(&aligned, batch_bytes) {
                                    Ok(range) => range,
                                    Err(error) => {
                                        let _ = tx.send(Err(error));
                                        return;
                                    }
                                };
                                let ipc_payload = Bytes::from_owner(SharedArchivedSlice {
                                    owner: aligned.clone(),
                                    range: ipc_range,
                                });
                                let mut decoder = AsyncIpcDecoder::new();
                                let batches = match decoder.push_chunk(ipc_payload) {
                                    Ok(batches) => batches,
                                    Err(error) => {
                                        let _ = tx.send(Err(error));
                                        return;
                                    }
                                };
                                for batch in batches {
                                    decoded_rows =
                                        decoded_rows.saturating_add(batch.num_rows() as u64);
                                    let schema_state = result_schemas
                                        .lock()
                                        .map_err(|_| {
                                            VeinError::internal("result schema mutex poisoned")
                                        })
                                        .and_then(|schemas| {
                                            schemas.get(&query_id).cloned().ok_or_else(|| {
                                                VeinError::internal(
                                                    "query result schema state is missing",
                                                )
                                            })
                                        });
                                    let schema_state = match schema_state {
                                        Ok(state) => state,
                                        Err(error) => {
                                            let _ = tx.send(Err(error));
                                            return;
                                        }
                                    };
                                    if let Err(error) = send_result_batch(
                                        &schema_state,
                                        &sender,
                                        task_id,
                                        expected_attempt,
                                        sequence,
                                        batch,
                                    )
                                    .await
                                    {
                                        let _ = tx.send(Err(error));
                                        return;
                                    }
                                    sequence += 1;
                                }
                            }
                            let declared_row_count = row_count.to_native();
                            if has_result_bytes && decoded_rows != declared_row_count {
                                let _ = tx.send(Err(VeinError::internal(format!(
                                    "task result row count mismatch: decoded {decoded_rows}, declared {declared_row_count}",
                                ))));
                                return;
                            }
                            let _ = tx.send(Ok(TaskCompletion {
                                row_count: declared_row_count,
                                bytes_read: bytes_read.to_native(),
                                duration_ns: duration_ns.to_native(),
                                write_parts: write_parts
                                    .iter()
                                    .map(|part| distributed::TableWritePart {
                                        partition_id: part.partition_id.to_native(),
                                        artifact: distributed::ArtifactRef {
                                            id: distributed::ArtifactId::from(
                                                part.artifact_id.as_str(),
                                            ),
                                            uri: part.artifact_uri.as_str().to_owned(),
                                            checksum: part.checksum.as_str().to_owned(),
                                            size_bytes: part.size_bytes.to_native(),
                                        },
                                        row_count: part.row_count.to_native(),
                                    })
                                    .collect(),
                            }));
                            return;
                        }
                        ArchivedVeinControlEvent::TaskFailed {
                            task_id: event_task_id,
                            query_id: event_query_id,
                            error,
                            attempt,
                        } => {
                            if let Err(error) = validate_result_identity(
                                TaskId::from_raw(event_task_id.0.to_native()),
                                QueryId::from_raw(event_query_id.0.to_native()),
                                attempt.to_native(),
                                task_id,
                                query_id,
                                expected_attempt,
                            ) {
                                let _ = tx.send(Err(error));
                                return;
                            }
                            tracing::warn!(
                                target: "vein_dispatcher",
                                %query_id,
                                %task_id,
                                error = %error,
                                "Task failed on executor"
                            );
                            let _ = tx.send(Err(VeinError::TaskFailed {
                                task_id: task_id.to_string(),
                                message: error.as_str().to_string(),
                            }));
                            return;
                        }
                        _ => {
                            let _ = tx.send(Err(VeinError::internal(
                                "unexpected event received from executor",
                            )));
                            return;
                        }
                    }
                }
            }

            {
                let message = if !saw_frame || frame_decoder.has_pending_data() {
                    "executor closed stream before sending a complete result"
                } else {
                    "executor closed result stream without an end frame"
                };
                let _ = tx.send(Err(VeinError::TaskFailed {
                    task_id: task_id.to_string(),
                    message: message.to_string(),
                }));
            }
        });

        Ok(())
    }

    async fn wait_for_completion(
        &self,
        query_id: QueryId,
        task_id: TaskId,
        attempt: u32,
    ) -> std::result::Result<TaskCompletion, VeinError> {
        let rx = {
            let mut completions = self
                .completions
                .lock()
                .map_err(|_| VeinError::internal("completions mutex poisoned"))?;
            completions.remove(&(query_id, task_id, attempt))
        };

        match rx {
            Some(receiver) => receiver.await.map_err(|_| {
                VeinError::internal("Task completion channel was dropped prematurely")
            })?,
            None => Err(VeinError::internal(format!(
                "No pending completion channel found for Task {task_id} attempt {attempt}"
            ))),
        }
    }

    async fn cancel_task(
        &self,
        executor: &NodeInfo,
        query_id: QueryId,
        task_id: TaskId,
        attempt: u32,
    ) -> std::result::Result<(), VeinError> {
        let client = self.get_or_init_client()?;
        let target_addr = executor.addr.to_string();

        let mut stream = client.open_stream(&target_addr).await.map_err(|e| {
            VeinError::internal(format!(
                "Failed to open stream to executor for cancellation: {e}"
            ))
        })?;

        let event = VeinControlEvent::CancelTask {
            task_id,
            query_id,
            attempt,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
            .map_err(|e| VeinError::internal(format!("Failed to serialize CancelTask: {e}")))?;

        let wire = frame_control_message(&bytes);
        stream.send(wire).await.map_err(|error| {
            VeinError::internal(format!("Failed to send task cancellation: {error}"))
        })?;
        stream
            .finish()
            .map_err(|error| VeinError::internal(format!("Failed to finish cancellation stream: {error}")))?;

        Ok(())
    }

    fn open_result_stream(&self, query_id: QueryId) -> SendableResultStream {
        QuicTaskDispatcher::open_result_stream(self, query_id)
    }

    fn close_result_stream(&self, query_id: QueryId) {
        QuicTaskDispatcher::close_result_stream(self, query_id)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SharedArchivedSlice, archived_slice_range, send_result_batch, validate_result_identity,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use common::ResultEvent;
    use common::identifiers::{QueryId, TaskId};
    use futures::StreamExt;
    use std::sync::Arc;

    #[test]
    fn archived_ipc_slice_keeps_frame_allocation_without_copying() {
        let mut frame = rkyv::util::AlignedVec::<16>::new();
        frame.extend_from_slice(b"header-arrow-payload-tail");
        let payload_range = archived_slice_range(&frame, &frame[7..20]).unwrap();
        let owner = Arc::new(frame);
        let payload = Bytes::from_owner(SharedArchivedSlice {
            owner: owner.clone(),
            range: payload_range,
        });

        assert_eq!(&payload[..], b"arrow-payload");
        assert!(Arc::strong_count(&owner) > 1);
    }

    #[test]
    fn archived_ipc_slice_rejects_foreign_memory() {
        let frame = rkyv::util::AlignedVec::<16>::new();
        assert!(archived_slice_range(&frame, b"not in frame").is_err());
    }

    #[tokio::test]
    async fn result_batches_emit_one_ordered_schema_and_reject_schema_drift() {
        let (sender, mut stream) = common::bounded_result_channel(4);
        let state = tokio::sync::Mutex::new(None);
        let task = TaskId::from_raw(5);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let first = RecordBatch::new_empty(schema.clone());
        send_result_batch(&state, &sender, task, 0, 0, first)
            .await
            .expect("first result batch");
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ResultEvent::Start { .. }
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ResultEvent::Batch(_)
        ));

        let second = RecordBatch::new_empty(schema);
        send_result_batch(&state, &sender, task, 0, 1, second)
            .await
            .expect("matching schema accepted");
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            ResultEvent::Batch(_)
        ));

        let changed_schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let changed = RecordBatch::new_empty(changed_schema);
        assert!(
            send_result_batch(&state, &sender, task, 0, 2, changed)
                .await
                .is_err()
        );
    }

    #[test]
    fn accepts_matching_result_identity() {
        let task_id = TaskId::from_raw(11);
        let query_id = QueryId::from_raw(22);
        assert!(validate_result_identity(task_id, query_id, 3, task_id, query_id, 3).is_ok());
    }

    #[test]
    fn rejects_mismatched_result_identity() {
        let task_id = TaskId::from_raw(11);
        let query_id = QueryId::from_raw(22);
        assert!(
            validate_result_identity(TaskId::from_raw(12), query_id, 3, task_id, query_id, 3,)
                .is_err()
        );
        assert!(validate_result_identity(task_id, query_id, 4, task_id, query_id, 3,).is_err());
    }
}
