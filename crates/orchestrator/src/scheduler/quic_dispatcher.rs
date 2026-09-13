use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use common::identifiers::{QueryId, TaskId};
use common::protocol::{
    ArchivedVeinControlEvent, ExecuteTask, VeinControlEvent,
};
use common::transport::{
    build_quic_client, extract_control_payload, frame_control_message, ConnectionManager, QuicClient,
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
    completions: Arc<Mutex<HashMap<(QueryId, TaskId), tokio::sync::oneshot::Receiver<Result<(), VeinError>>>>>,
    query_outputs: Arc<Mutex<HashMap<QueryId, Vec<u8>>>>,
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
            query_outputs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new QUIC task dispatcher with a pre-configured [`QuicClient`].
    pub fn with_client(client: Arc<QuicClient>) -> Self {
        Self {
            cert_path: DEFAULT_CERT_PATH.to_string(),
            client: Mutex::new(Some(client)),
            completions: Arc::new(Mutex::new(HashMap::new())),
            query_outputs: Arc::new(Mutex::new(HashMap::new())),
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

        let quic_client_raw = build_quic_client(&self.cert_path)
            .map_err(|e| VeinError::internal(format!("Failed to build QUIC client for dispatcher: {e}")))?;
        let client = Arc::new(QuicClient::new(quic_client_raw));
        *guard = Some(client.clone());
        Ok(client)
    }
}

#[async_trait]
impl TaskDispatcher for QuicTaskDispatcher {
    async fn dispatch(
        &self,
        executor: &NodeInfo,
        task: ExecuteTask,
    ) -> Result<(), VeinError> {
        let client = self.get_or_init_client()?;
        let target_addr = executor.addr.to_string();
        let query_id = task.query_id;
        let task_id = task.task_id;

        tracing::debug!(
            target: "vein_dispatcher",
            %query_id,
            %task_id,
            executor_addr = %target_addr,
            "Opening QUIC stream to dispatch task"
        );

        let mut stream = client
            .open_stream(&target_addr)
            .await
            .map_err(|e| VeinError::internal(format!("Failed to open stream to executor at {target_addr}: {e}")))?;

        // 1. Serialize DispatchTask control event via rkyv
        let event = VeinControlEvent::DispatchTask(task);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
            .map_err(|e| VeinError::internal(format!("Failed to serialize DispatchTask via rkyv: {e}")))?;

        // 2. Send opcode (0x02) + length-prefixed payload
        let wire = frame_control_message(&bytes);
        stream
            .send(wire)
            .await
            .map_err(|e| VeinError::internal(format!("Failed to send task payload to executor: {e}")))?;

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

        let outputs = self.query_outputs.clone();
        tokio::spawn(async move {
            let mut resp_buf = Vec::new();
            while let Ok(Some(chunk)) = stream.receive().await {
                resp_buf.extend_from_slice(&chunk);
            }

            if resp_buf.is_empty() {
                let _ = tx.send(Err(VeinError::TaskFailed {
                    task_id: task_id.to_string(),
                    message: "Executor closed stream without sending TaskCompleted event".to_string(),
                }));
                return;
            }

            let payload = match extract_control_payload(&resp_buf) {
                Some(p) => p,
                None => {
                    let _ = tx.send(Err(VeinError::TaskFailed {
                        task_id: task_id.to_string(),
                        message: "Executor returned invalid control payload".to_string(),
                    }));
                    return;
                }
            };

            let mut aligned = rkyv::util::AlignedVec::<16>::new();
            aligned.extend_from_slice(payload);

            let archived = match rkyv::access::<ArchivedVeinControlEvent, rkyv::rancor::Error>(&aligned) {
                Ok(a) => a,
                Err(e) => {
                    let _ = tx.send(Err(VeinError::internal(format!(
                        "Failed to decode TaskCompleted event from executor: {e}"
                    ))));
                    return;
                }
            };

            match archived {
                ArchivedVeinControlEvent::TaskCompleted {
                    row_count,
                    result_bytes,
                    ..
                } => {
                    tracing::info!(
                        target: "vein_dispatcher",
                        %query_id,
                        %task_id,
                        rows = row_count.to_native(),
                        "Task completed successfully on executor"
                    );
                    if let (rkyv::option::ArchivedOption::Some(batch_bytes), Ok(mut out)) =
                        (result_bytes, outputs.lock())
                    {
                        out.entry(query_id)
                            .or_default()
                            .extend_from_slice(batch_bytes.as_slice());
                    }
                    let _ = tx.send(Ok(()));
                }
                ArchivedVeinControlEvent::TaskFailed { error, .. } => {
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
                }
                _ => {
                    let _ = tx.send(Err(VeinError::internal(
                        "Unexpected event received from executor (expected TaskCompleted or TaskFailed)",
                    )));
                }
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
            Some(receiver) => receiver
                .await
                .map_err(|_| VeinError::internal("Task completion channel was dropped prematurely"))?,
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

        let mut stream = client
            .open_stream(&target_addr)
            .await
            .map_err(|e| VeinError::internal(format!("Failed to open stream to executor for cancellation: {e}")))?;

        let event = VeinControlEvent::CancelTask { task_id, query_id };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)
            .map_err(|e| VeinError::internal(format!("Failed to serialize CancelTask: {e}")))?;

        let wire = frame_control_message(&bytes);
        let _ = stream.send(wire).await;
        let _ = stream.finish();

        Ok(())
    }

    fn take_query_output(&self, query_id: QueryId) -> Option<Bytes> {
        if let Ok(mut guard) = self.query_outputs.lock() {
            guard.remove(&query_id).map(Bytes::from)
        } else {
            None
        }
    }
}
