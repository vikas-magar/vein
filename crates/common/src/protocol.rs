use std::net::SocketAddr;
use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::identifiers::{NodeId, PartitionId, QueryId, StageId, TaskId};

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
    /// External Shuffle Service node.
    Ess,
}

/// Load snapshot of a node, sent in heartbeats.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub struct NodeLoad {
    /// Number of active tasks.
    pub active_tasks: u16,
    /// CPU utilization percent.
    pub cpu_percent: f32,
    /// Memory usage in bytes.
    pub memory_bytes: u64,
}

/// Control events sent as QUIC datagrams (<1200 bytes).

#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

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
        /// Number of rows produced.
        row_count: u64,
        /// Bytes read from storage.
        bytes_read: u64,
        /// Wall-clock duration in nanoseconds.
        duration_ns: u64,
    },
    /// A new task assigned to this Executor.
    DispatchTask(ExecuteTask),
    /// A task has failed on an Executor.
    TaskFailed {
        /// The failed task.
        task_id: TaskId,
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
    /// Explicitly drain a shuffle partition at the ESS.
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
    },
    /// A map-stage has finished producing all data; partitions are ready at ESS.
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

/// Supported file formats for storage scans.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum FileFormat {
    /// Apache Parquet
    Parquet,
    /// Comma-Separated Values
    Csv,
    /// JavaScript Object Notation
    Json,
}

/// Self-contained file scan instruction for an Executor.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct FileScanTask {
    /// List of URIs/Paths to the files.
    pub files: Vec<String>,
    /// The format of the files.
    pub format: FileFormat,
    /// Optional byte range within the file.
    pub byte_range: Option<Range<u64>>,
    /// Column IDs to read (projection pushdown).
    pub column_ids: Vec<i32>,
    /// Schema of the projected columns.
    pub projected_schema: Vec<FieldSchema>,
    /// Task identifier for reporting.
    pub task_id: TaskId,
    /// ESS address to send output to, if applicable.
    pub ess_target: Option<SocketAddr>,
}

/// Minimal field schema for protocol serialization.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub struct FieldSchema {
    /// Iceberg field ID.
    pub field_id: i32,
    /// Column name.
    pub name: String,
    /// Data type string (e.g. "INT", "STRING").
    pub data_type: String,
}

/// Operator applied by an Executor to a data stream.

#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub enum Operator {
    /// Row-level filter predicate.
    Filter {
        /// Expression string in the engine's expression language.
        expr: String,
    },
    /// Column projection.
    Project {
        /// Column IDs to keep.
        columns: Vec<i32>,
    },
    /// Partial hash aggregation (for two-phase aggregation).
    HashAggPartial {
        /// Column IDs for GROUP BY.
        group_by: Vec<i32>,
        /// Aggregate expressions.
        aggregates: Vec<AggregateExpr>,
        /// Number of shuffle partitions for the output.
        num_partitions: u16,
    },
    /// Hash join probe side.
    HashJoinProbe {
        /// Column IDs for the join key.
        join_key: Vec<i32>,
    },
    /// Sort operator.
    Sort {
        /// Sort keys and directions.
        sort_keys: Vec<SortKey>,
    },
    /// Limit operator.
    Limit {
        /// Offset (rows to skip).
        offset: u64,
        /// Maximum rows to emit.
        count: u64,
    },
    /// Top-N operator (sorted within groups).
    TopN {
        /// Grouping columns.
        group_by: Vec<i32>,
        /// Ordering columns and directions.
        order_by: Vec<SortKey>,
        /// Maximum rows per group.
        n: u64,
    },
}

/// Aggregate expression for partial hash aggregation.

#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub enum AggregateExpr {
    /// COUNT(input).
    Count {
        /// Input column ID.
        input: i32,
    },
    /// SUM(input).
    Sum {
        /// Input column ID.
        input: i32,
    },
    /// MIN(input).
    Min {
        /// Input column ID.
        input: i32,
    },
    /// MAX(input).
    Max {
        /// Input column ID.
        input: i32,
    },
    /// AVG(input).
    Avg {
        /// Input column ID.
        input: i32,
    },
}

/// Sort key with direction.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub struct SortKey {
    /// Column ID.
    pub column: i32,
    /// True for descending, false for ascending.
    pub descending: bool,
}

/// Source of data for a stage.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub enum DataSource {
    /// Read from static storage (e.g., Parquet).
    File(FileScanTask),
    /// Read from the Ephemeral Shuffle Service (Exchange).
    Exchange(ExchangeSource),
    /// Read from an infinite event stream (e.g., Kafka).
    Stream(StreamSource),
    /// A source that generates data directly (e.g. VALUES).
    Memory,
}

/// Read from ESS.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub struct ExchangeSource {
    /// The upstream stage ID that produced the data.
    pub upstream_stage_id: StageId,
}

/// Read from an infinite stream.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub struct StreamSource {
    /// E.g., Kafka topic name.
    pub topic: String,
}

/// Target for shuffle output (ESS address + partition info).
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub struct ShuffleTarget {
    /// ESS node address.
    pub ess_addr: SocketAddr,
    /// Number of shuffle partitions.
    pub num_partitions: u16,
}

/// Target for persistent storage output.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub struct StorageTarget {
    /// The path or table URI.
    pub destination_uri: String,
}

/// Sink for data produced by a stage.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub enum DataSink {
    /// Push data to the Shuffle Service for the next stage.
    Shuffle(ShuffleTarget),
    /// Stream data directly back to the Query Coordinator/Client.
    Client,
    /// Write data out to storage (e.g., Iceberg commit).
    Storage(StorageTarget),
    /// Discard the data (useful for some benchmarks or tests).
    Discard,
}

/// Mode of execution for a task.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]

pub enum ExecutionMode {
    /// Finite batch execution (e.g., reading Parquet files).
    Batch,
    /// Infinite stream execution (DBSP incremental multi-epoch).
    Streaming,
}

/// Task dispatched from Master to Executor.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]

pub struct ExecuteTask {
    /// Execution mode (Batch or Streaming).
    pub mode: ExecutionMode,
    /// Unique task identifier.
    pub task_id: TaskId,
    /// Query this task belongs to.
    pub query_id: QueryId,
    /// Stage this task is part of.
    pub stage_id: StageId,
    /// Data source for this task.
    pub source: DataSource,
    /// Operators to apply to the data stream.
    pub operators: Vec<Operator>,
    /// Where to send the output.
    pub sink: DataSink,
}

/// Payload sent from Executor to ESS.
#[derive(Debug, Clone)]
pub struct ShufflePayload {
    /// Query this payload belongs to.
    pub query_id: QueryId,
    /// Stage this payload is part of.
    pub stage_id: StageId,
    /// Target partition.
    pub partition: PartitionId,
    /// Serialized Arrow IPC data.
    pub data: bytes::Bytes,
}

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
    /// A chunk of result data (Arrow IPC).
    Data {
        /// Serialized Arrow IPC batch.
        batch: Vec<u8>,
    },
    /// The query finished successfully.
    QueryCompleted {
        /// Total rows produced (for batch queries).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_event_serde() {
        let event = VeinControlEvent::CatalogReady;
        let json = serde_json::to_string(&event).unwrap();
        let back: VeinControlEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, VeinControlEvent::CatalogReady));
    }

    #[test]
    fn test_execute_task_serde() {
        let task = ExecuteTask {
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            source: DataSource::Memory,
            operators: vec![
                Operator::Filter {
                    expr: "col > 10".into(),
                },
                Operator::Project {
                    columns: vec![0, 1, 2],
                },
            ],
            sink: DataSink::Client,
        };
        let json = serde_json::to_string(&task).unwrap();
        let back: ExecuteTask = serde_json::from_str(&json).unwrap();
        assert_eq!(back.operators.len(), 2);
    }

    #[test]
    fn test_protocol_request_serde() {
        let req = ProtocolRequest::ExecuteSql {
            sql: "SELECT * FROM t".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: ProtocolRequest = serde_json::from_str(&json).unwrap();
        match back {
            ProtocolRequest::ExecuteSql { sql } => assert_eq!(sql, "SELECT * FROM t"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_protocol_response_serde() {
        let resp = ProtocolResponse::QuerySubmitted {
            query_id: QueryId::new(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let back: ProtocolResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, ProtocolResponse::QuerySubmitted { .. }));
    }
}
