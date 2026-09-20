//! Client-coordinator RPC requests, responses, elasticity commands, and VFS storage requests.

use serde::{Deserialize, Serialize};

use super::task::ExecuteTask;
use crate::identifiers::{QueryId, TaskId};

/// Request from Client to Master.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum ProtocolRequest {
    /// Execute a SQL query.
    ExecuteSql {
        /// SQL text.
        sql: String,
    },
    /// Cancel a running query.
    CancelQuery {
        /// Query to cancel.
        query_id: QueryId,
    },
}

/// Response from Master to Client.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum ProtocolResponse {
    /// Query accepted and assigned an ID.
    QuerySubmitted {
        /// The assigned query ID.
        query_id: QueryId,
    },
    /// A text plan response (e.g. from EXPLAIN).
    Plan {
        /// Rendered execution plan text.
        text: String,
    },
    /// Announces the beginning of a streamed query result.
    QueryResultStart {
        /// The query producing the result.
        query_id: QueryId,
    },
    /// One independently decodable Arrow IPC record batch.
    QueryResultBatch {
        /// The query producing the result.
        query_id: QueryId,
        /// Monotonically increasing batch sequence.
        sequence: u64,
        /// Serialized Arrow IPC payload for exactly one record batch.
        batch: Vec<u8>,
    },
    /// The query finished successfully.
    QueryResultEnd {
        /// The query producing the result.
        query_id: QueryId,
        /// Total rows produced.
        row_count: u64,
    },
    /// A write query completed successfully.
    WriteCompleted {
        /// Destination table name.
        table: String,
        /// Rows written when known.
        row_count: u64,
    },
    /// Query successfully cancelled.
    QueryCancelled {
        /// The cancelled query ID.
        query_id: QueryId,
    },
    /// Error processing the request.
    Error {
        /// Human-readable error message.
        message: String,
    },
}

/// Response to a preempt command.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct PreemptResponse {
    /// The task that was preempted.
    pub task_id: TaskId,
    /// Sub-tasks completed before preemption.
    pub subtasks_completed: usize,
    /// The boundary of work that the worker yielded back to the orchestrator.
    pub subtasks_yielded: usize,
}

/// A command sent from the Master to a Compute Node for elasticity and scheduling.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum MasterCommand {
    /// Dispatches a new task to the worker.
    DispatchTask(Box<ExecuteTask>),
    /// Forces the worker to pause and relinquish un-claimed sub-tasks.
    PreemptTask {
        /// The task to preempt.
        task_id: TaskId,
    },
}

/// Requests sent to the Virtual File System (VFS).
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum VfsRequest {
    /// Read a block of data, applying the provided eBPF bytecode in the kernel.
    ReadWithFilter {
        /// The file path to read.
        file_path: String,
        /// The raw eBPF ELF bytecode.
        ebpf_bytecode: Vec<u8>,
    },
}
