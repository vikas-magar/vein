use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

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
use crate::traits::TaskDispatcher;

const DEFAULT_CERT_PATH: &str = "certs/server.crt";

/// The QUIC Task Dispatcher.
///
/// Responsible for serializing `ExecuteTask`s and streaming them to the appropriate
/// Executor node via a highly concurrent, multiplexed s2n-quic stream.
pub struct QuicTaskDispatcher {
    cert_path: String,
    client: Mutex<Option<Arc<QuicClient>>>,
    completions: Arc<
        Mutex<HashMap<(QueryId, TaskId), tokio::sync::oneshot::Receiver<Result<(), VeinError>>>>,
    >,
    result_senders: Arc<Mutex<HashMap<QueryId, ResultChannelSender>>>,
    result_started: Arc<Mutex<HashSet<QueryId>>>,
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
            result_started: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Create a new QUIC task dispatcher with a pre-configured [`QuicClient`].
    pub fn with_client(client: Arc<QuicClient>) -> Self {
        Self {
            cert_path: DEFAULT_CERT_PATH.to_string(),
            client: Mutex::new(Some(client)),
            completions: Arc::new(Mutex::new(HashMap::new())),
            result_senders: Arc::new(Mutex::new(HashMap::new())),
            result_started: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Open a bounded result stream before query tasks are dispatched.
    pub fn open_result_stream(&self, query_id: QueryId) -> SendableResultStream {
        let (sender, stream) = bounded_result_channel(32);
        if let Ok(mut senders) = self.result_senders.lock() {
            senders.insert(query_id, sender);
        }
        if let Ok(mut started) = self.result_started.lock() {
            started.remove(&query_id);
        }
        stream
    }

    /// Close a query result stream after all task readers have finished.
    pub fn close_result_stream(&self, query_id: QueryId) {
        if let Ok(mut senders) = self.result_senders.lock() {
            senders.remove(&query_id);
        }
        if let Ok(mut started) = self.result_started.lock() {
            started.remove(&query_id);
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
        let query_id = task.query_id;
        let task_id = task.task_id;
        let expected_attempt = task.attempt;

        tracing::debug!(
            target: "vein_dispatcher",
            %query_id,
            %task_id,
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
        stream.send(wire).await.map_err(|e| {
            VeinError::internal(format!("Failed to send task payload to executor: {e}"))
        })?;

        stream
            .finish()
            .map_err(|e| VeinError::internal(format!("Failed to finish send stream: {e}")))?;

        // 3. Setup completion oneshot channel and spawn reader task
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut completions = self
                .completions
                .lock()
                .map_err(|_| VeinError::internal("completions mutex poisoned"))?;
            completions.insert((query_id, task_id), rx);
        }

        let result_senders = self.result_senders.clone();
        let result_started = self.result_started.clone();
        tokio::spawn(async move {
            let mut frame_decoder = ControlFrameDecoder::new();
            let mut saw_frame = false;
            let mut sequence = 0u64;
            let mut seen_sequences = HashSet::new();

            while let Ok(Some(chunk)) = stream.receive().await {
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
                    let archived = match rkyv::access::<ArchivedVeinControlEvent, rkyv::rancor::Error>(
                        &aligned,
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
                        ArchivedVeinControlEvent::TaskResultStart { attempt, .. } => {
                            if attempt.to_native() != expected_attempt {
                                continue;
                            }
                        }
                        ArchivedVeinControlEvent::TaskResultBatch {
                            result_bytes,
                            attempt,
                            sequence: frame_sequence,
                            ..
                        } => {
                            if attempt.to_native() != expected_attempt {
                                continue;
                            }
                            let frame_sequence = frame_sequence.to_native();
                            if !seen_sequences.insert(frame_sequence) {
                                continue;
                            }
                            if frame_sequence != sequence {
                                if frame_sequence < sequence {
                                    continue;
                                }
                                let _ = tx.send(Err(VeinError::internal(
                                    "result batch sequence is not monotonic",
                                )));
                                return;
                            }
                            let sender = result_senders
                                .lock()
                                .ok()
                                .and_then(|senders| senders.get(&query_id).cloned());
                            let Some(sender) = sender else {
                                let _ = tx.send(Err(VeinError::internal(
                                    "query result stream was not opened before task dispatch",
                                )));
                                return;
                            };

                            let mut decoder = AsyncIpcDecoder::new();
                            let batches = match decoder
                                .push_chunk(Bytes::copy_from_slice(result_bytes.as_slice()))
                            {
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
                            let batch = batches.into_iter().next().expect("checked one batch");
                            let should_start = result_started
                                .lock()
                                .map(|mut started| started.insert(query_id))
                                .unwrap_or(false);
                            if should_start
                                && sender
                                    .send(Ok(ResultEvent::Start {
                                        schema: batch.schema(),
                                    }))
                                    .await
                                    .is_err()
                            {
                                let _ = tx.send(Err(VeinError::internal(
                                    "result stream consumer dropped",
                                )));
                                return;
                            }
                            if sender
                                .send(Ok(ResultEvent::Batch(ResultBatch::new(
                                    task_id,
                                    expected_attempt,
                                    frame_sequence,
                                    batch,
                                ))))
                                .await
                                .is_err()
                            {
                                let _ = tx.send(Err(VeinError::internal(
                                    "result stream consumer dropped",
                                )));
                                return;
                            }
                            sequence += 1;
                        }
                        ArchivedVeinControlEvent::TaskResultEnd {
                            row_count, attempt, ..
                        } => {
                            if attempt.to_native() != expected_attempt {
                                continue;
                            }
                            tracing::info!(
                                target: "vein_dispatcher",
                                %query_id,
                                %task_id,
                                rows = row_count.to_native(),
                                "Task completed successfully on executor"
                            );
                            let _ = tx.send(Ok(()));
                            return;
                        }
                        ArchivedVeinControlEvent::TaskCompleted {
                            row_count: _row_count,
                            result_bytes,
                            attempt,
                            ..
                        } => {
                            if attempt.to_native() != expected_attempt {
                                continue;
                            }
                            let sender = result_senders
                                .lock()
                                .ok()
                                .and_then(|senders| senders.get(&query_id).cloned());
                            let Some(sender) = sender else {
                                let _ = tx.send(Err(VeinError::internal(
                                    "query result stream was not opened before task dispatch",
                                )));
                                return;
                            };
                            if let rkyv::option::ArchivedOption::Some(batch_bytes) = result_bytes {
                                let mut decoder = AsyncIpcDecoder::new();
                                let batches = match decoder
                                    .push_chunk(Bytes::copy_from_slice(batch_bytes.as_slice()))
                                {
                                    Ok(batches) => batches,
                                    Err(error) => {
                                        let _ = tx.send(Err(error));
                                        return;
                                    }
                                };
                                for batch in batches {
                                    let should_start = result_started
                                        .lock()
                                        .map(|mut started| started.insert(query_id))
                                        .unwrap_or(false);
                                    if should_start
                                        && sender
                                            .send(Ok(ResultEvent::Start {
                                                schema: batch.schema(),
                                            }))
                                            .await
                                            .is_err()
                                    {
                                        let _ = tx.send(Err(VeinError::internal(
                                            "result stream consumer dropped",
                                        )));
                                        return;
                                    }
                                    if sender
                                        .send(Ok(ResultEvent::Batch(ResultBatch::new(
                                            task_id,
                                            expected_attempt,
                                            sequence,
                                            batch,
                                        ))))
                                        .await
                                        .is_err()
                                    {
                                        let _ = tx.send(Err(VeinError::internal(
                                            "result stream consumer dropped",
                                        )));
                                        return;
                                    }
                                    sequence += 1;
                                }
                            }
                            let _ = tx.send(Ok(()));
                            return;
                        }
                        ArchivedVeinControlEvent::TaskFailed { error, attempt, .. } => {
                            if attempt.to_native() != expected_attempt {
                                continue;
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
    ) -> std::result::Result<(), VeinError> {
        let rx = {
            let mut completions = self
                .completions
                .lock()
                .map_err(|_| VeinError::internal("completions mutex poisoned"))?;
            completions.remove(&(query_id, task_id))
        };

        match rx {
            Some(receiver) => receiver.await.map_err(|_| {
                VeinError::internal("Task completion channel was dropped prematurely")
            })?,
            None => Err(VeinError::internal(format!(
                "No pending completion channel found for Task {task_id}"
            ))),
        }
    }

    async fn cancel_task(
        &self,
        executor: &NodeInfo,
        query_id: QueryId,
        task_id: TaskId,
    ) -> std::result::Result<(), VeinError> {
        let client = self.get_or_init_client()?;
        let target_addr = executor.addr.to_string();

        let mut stream = client.open_stream(&target_addr).await.map_err(|e| {
            VeinError::internal(format!(
                "Failed to open stream to executor for cancellation: {e}"
            ))
        })?;

        let event = VeinControlEvent::CancelTask { task_id, query_id };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
            .map_err(|e| VeinError::internal(format!("Failed to serialize CancelTask: {e}")))?;

        let wire = frame_control_message(&bytes);
        let _ = stream.send(wire).await;
        let _ = stream.finish();

        Ok(())
    }

    fn open_result_stream(&self, query_id: QueryId) -> SendableResultStream {
        QuicTaskDispatcher::open_result_stream(self, query_id)
    }

    fn close_result_stream(&self, query_id: QueryId) {
        QuicTaskDispatcher::close_result_stream(self, query_id)
    }
}
