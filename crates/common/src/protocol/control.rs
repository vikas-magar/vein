//! Control-plane events, node types, and telemetry metrics.

use serde::{Deserialize, Serialize};

use crate::identifiers::{NodeId, QueryId, StageId, TaskId};
use super::task::ExecuteTask;

/// Kind of a Vein cluster node.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum NodeKind {
    /// Query coordinator node.
    Master,
    /// Stateless compute node.
    Executor,
    /// Catalog and metadata service.
    Vcs,
}

/// Load snapshot of a node, sent in heartbeats.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(attr(allow(missing_docs)))]
pub struct NodeLoad {
    /// Number of active tasks.
    pub active_tasks: u16,
    /// CPU utilization percent.
    pub cpu_percent: f32,
    /// Memory usage in bytes.
    pub memory_bytes: u64,
}

/// Control events sent as QUIC datagrams (<1200 bytes) or multiplexed streams.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(attr(allow(missing_docs)))]
pub enum VeinControlEvent {
    /// A table's metadata has changed; VCS is broadcasting the new snapshot.
    TableInvalidated {
        /// Namespace path (e.g. ["my_catalog", "my_schema"]).
        namespace: Vec<String>,
        /// Table name.
        table_name: String,
        /// New Iceberg snapshot ID.
        new_snapshot_id: i64,
    },
    /// VCS has finished loading all catalog data and is ready.
    CatalogReady,
    /// A task has completed on an Executor.
    TaskCompleted {
        /// The completed task.
        task_id: TaskId,
        /// Query the task belongs to.
        query_id: QueryId,
        /// Number of rows produced.
        row_count: u64,
        /// Bytes read from storage.
        bytes_read: u64,
        /// Wall-clock duration in nanoseconds.
        duration_ns: u64,
        /// Serialized Arrow IPC stream for DataSink::Client tasks.
        result_bytes: Option<Vec<u8>>,
    },
    /// A new task assigned to this Executor.
    DispatchTask(ExecuteTask),
    /// A task has failed on an Executor.
    TaskFailed {
        /// The failed task.
        task_id: TaskId,
        /// Query the task belongs to.
        query_id: QueryId,
        /// Error message.
        error: String,
    },
    /// Cancel a running task.
    CancelTask {
        /// Task to cancel.
        task_id: TaskId,
        /// Query the task belongs to.
        query_id: QueryId,
    },
    /// Explicitly drain a shuffle partition in VFS.
    ExplicitDrain {
        /// Query owning the partition.
        query_id: QueryId,
        /// Stage owning the partition.
        stage_id: StageId,
    },
    /// Periodic heartbeat from any node.
    Heartbeat {
        /// Sender node.
        node_id: NodeId,
        /// Sender node type.
        node_kind: NodeKind,
        /// Unix timestamp in milliseconds.
        timestamp_ms: u64,
        /// Current load.
        load: NodeLoad,
        /// Listening address for incoming task dispatch (e.g. "127.0.0.1:8081").
        advertise_addr: String,
    },
    /// Master acknowledgment of a heartbeat.
    HeartbeatAck,
    /// A map-stage has finished producing all data; partitions are ready in VFS.
    StageReady {
        /// Query the stage belongs to.
        query_id: QueryId,
        /// The stage that is ready.
        stage_id: StageId,
        /// How many partitions were produced.
        num_partitions: u16,
    },
    /// All reduce tasks for a stage have drained their partitions.
    DrainCompleted {
        /// Query the stage belongs to.
        query_id: QueryId,
        /// The stage that has been drained.
        stage_id: StageId,
    },
}

/// Telemetry feedback from an executed query, used to train RL agents (like ADOPT).
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct QueryTelemetry {
    /// Unique identifier for the executed query.
    pub query_id: QueryId,
    /// Total wall-clock execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Number of output rows produced.
    pub rows_produced: u64,
}

/// Streaming telemetry from a compute node to the orchestrator.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct TaskTelemetry {
    /// The task this telemetry is for.
    pub task_id: TaskId,
    /// Total sub-tasks assigned to this task.
    pub total_subtasks_assigned: usize,
    /// Sub-tasks completed so far.
    pub subtasks_completed: usize,
    /// Current processing speed.
    pub rows_processed_per_sec: f64,
}
