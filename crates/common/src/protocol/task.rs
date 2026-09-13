//! Physical task definitions, execution modes, sources, and sinks.

use std::ops::Range;
use serde::{Deserialize, Serialize};

use crate::identifiers::{QueryId, StageId, TaskId};
use super::ir::MicroProgram;

/// Supported file formats for storage scans.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum FileFormat {
    /// Apache Parquet
    Parquet,
    /// Comma-Separated Values
    Csv,
    /// JavaScript Object Notation
    Json,
    /// Vein Dual-Plane Columnar Storage Format (.exo)
    Exo,
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
    /// VFS path prefix to send output to, if applicable.
    pub shuffle_target_path: Option<String>,
}

/// Read from VFS Exchange.
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

/// Source of data for a stage.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum DataSource {
    /// Read from static storage (e.g., Parquet).
    File(FileScanTask),
    /// Read from VFS (Exchange).
    Exchange(ExchangeSource),
    /// Read from an infinite event stream (e.g., Kafka).
    Stream(StreamSource),
    /// A source that generates data directly (e.g. VALUES).
    Memory,
}

/// Target for shuffle output (VFS path prefix + partition info).
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct ShuffleTarget {
    /// VFS path prefix (e.g. /vein/queries/q1/stage_1).
    pub vfs_path_prefix: String,
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
    /// Push data to VFS for the next stage.
    Shuffle(ShuffleTarget),
    /// Stream data directly back to the Query Coordinator/Client.
    Client {
        /// Schema of the columns expected at the client.
        schema: Vec<FieldSchema>,
    },
    /// Write data out to storage (e.g., Iceberg commit).
    Storage(StorageTarget),
    /// Discard the data (useful for some benchmarks or tests).
    Discard,
}

/// Mode of execution for a task.
#[derive(
    Debug,
    Clone,
    Copy,
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
    /// The Micro-IR program to apply to the data stream.
    pub program: MicroProgram,
    /// Where to send the output.
    pub sink: DataSink,
}
