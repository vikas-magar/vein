//! Physical task definitions, execution modes, sources, and sinks.

use std::ops::Range;
use serde::{Deserialize, Serialize};

use crate::identifiers::{QueryId, StageId, TaskId};
use super::ir::MicroProgram;
use super::exchange::ExchangeKey;
use super::join::{JoinKeySpec, JoinSource};

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

/// A typed scalar value used by storage-level predicate pushdown.
#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum ScanPredicateValue {
    /// Boolean value.
    Boolean(bool),
    /// Signed 64-bit integer value.
    Int64(i64),
    /// IEEE-754 floating-point value.
    Float64(f64),
    /// UTF-8 string value.
    String(String),
    /// Date32 value represented as days from the Unix epoch.
    Date(i32),
    /// Timestamp value represented as microseconds from the Unix epoch.
    Timestamp(i64),
}

/// A predicate that can be evaluated by a storage format without materializing rows.
#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum ScanPredicate {
    /// A comparison against a canonical-address-backed column.
    Address {
        /// Column name to evaluate.
        column: String,
        /// Comparison operator (`eq`, `neq`, `lt`, `lte`, `gt`, or `gte`).
        operator: String,
        /// Right-hand scalar value.
        value: ScanPredicateValue,
    },
    /// A nullability check against a canonical-address-backed column.
    AddressNull {
        /// Column name to evaluate.
        column: String,
        /// Whether the predicate checks for null (`true`) or non-null (`false`).
        is_null: bool,
    },
    /// An `IN` predicate against a canonical-address-backed column.
    AddressIn {
        /// Column name to evaluate.
        column: String,
        /// Candidate values.
        values: Vec<ScanPredicateValue>,
    },
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
    /// Optional storage-level predicate extracted by the physical planner.
    pub scan_predicates: Vec<ScanPredicate>,
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
    /// Authoritative VCS exchange identity.
    pub exchange_key: ExchangeKey,
    /// The upstream stage ID that produced the data.
    pub upstream_stage_id: StageId,
    /// Canonical VFS prefix written by the producing shuffle sink.
    pub path_prefix: String,
    /// Partition assigned to this task.
    pub partition_id: u32,
    /// Total number of partitions in the exchange.
    pub partition_count: u16,
    /// Schema of the exchange rows.
    pub schema: Vec<FieldSchema>,
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
    /// Consume two partition-aligned exchange inputs with a physical join.
    Join(JoinSource),
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
    /// Authoritative VCS exchange identity.
    pub exchange_key: ExchangeKey,
    /// VFS path prefix used as the exchange data namespace.
    pub vfs_path_prefix: String,
    /// Producer task that owns the immutable fragment paths.
    pub producer_task_id: TaskId,
    /// Retry attempt for this producer task.
    pub attempt: u32,
    /// Producer tasks expected to publish fragments.
    pub expected_tasks: Vec<TaskId>,
    /// Fingerprint of the exchanged schema and key contract.
    pub schema_fingerprint: String,
    /// Number of shuffle partitions.
    pub num_partitions: u16,
    /// Keys used to compute the stable partition hash.
    pub partition_keys: Vec<JoinKeySpec>,
    /// Schema of the rows written to the exchange.
    pub schema: Vec<FieldSchema>,
}

/// Key-value option pair for storage and scan configurations.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct KeyValue {
    /// Option key name.
    pub key: String,
    /// Option value.
    pub value: String,
}

impl KeyValue {
    /// Creates a new key-value pair.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Target for persistent storage output.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct StorageTarget {
    /// The path or table URI.
    pub destination_uri: String,
    /// Storage format and execution options.
    pub options: Vec<KeyValue>,
    /// Authoritative schema of columns to write.
    pub schema: Vec<FieldSchema>,
}

impl StorageTarget {
    /// Creates a new StorageTarget.
    pub fn new(
        destination_uri: impl Into<String>,
        options: Vec<KeyValue>,
        schema: Vec<FieldSchema>,
    ) -> Self {
        Self {
            destination_uri: destination_uri.into(),
            options,
            schema,
        }
    }

    /// Helper to find an option value by key (case-insensitive).
    pub fn get_option(&self, key: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|kv| kv.key.eq_ignore_ascii_case(key))
            .map(|kv| kv.value.as_str())
    }
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
