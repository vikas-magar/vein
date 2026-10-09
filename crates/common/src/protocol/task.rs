//! Physical task definitions, execution modes, sources, and sinks.

use catalog::CatalogSnapshotToken;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use types::VeinSchemaRef;

use super::ir::{MicroProgram, RegId};
use crate::identifiers::{QueryId, RequestId, StageId, TaskId};

/// Supported file formats for storage scans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileFormat {
    /// Apache Parquet
    Parquet,
    /// Comma-Separated Values
    Csv,
    /// JavaScript Object Notation
    Json,
}

/// A predicate a source evaluates before rows are materialized.
///
/// Held as compiled Micro-IR rather than a planner expression so an executor can
/// evaluate it through the same VM that runs the residual pipeline — no planner
/// and no logical plan are involved on the reading side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPredicate {
    /// The predicate's input columns, in register order: input column `i` is
    /// mounted into VM register `i`.
    pub input: VeinSchemaRef,
    /// Program computing the predicate's boolean for one input batch.
    pub program: Arc<MicroProgram>,
    /// Register holding the boolean result.
    pub result: RegId,
}

/// Self-contained file scan instruction for an Executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileScanTask {
    /// List of URIs/Paths to the files.
    pub files: Vec<String>,
    /// The format of the files.
    pub format: FileFormat,
    /// Optional byte range within the file.
    pub byte_range: Option<Range<u64>>,
    /// The projected columns, in the engine's own schema vocabulary.
    ///
    /// Refcounted so splitting one file scan into per-file tasks copies a
    /// handle rather than the schema.
    pub projected: VeinSchemaRef,
    /// Projected column positions that may retain Parquet dictionary encoding.
    ///
    /// This is an execution hint for consumers that can use dictionary IDs
    /// directly. An empty list keeps the default decoded Arrow representation.
    #[serde(default)]
    pub dictionary_columns: Vec<u32>,
    /// Push-down predicate the source evaluates before materializing rows.
    ///
    /// Refcounted for the same reason as `projected`: splitting a scan into
    /// per-file tasks must not duplicate the predicate. `None` means the source
    /// returns every projected row and the residual program filters them, so a
    /// source that ignores this only forgoes an optimization.
    pub pushdown: Option<Arc<ScanPredicate>>,
    /// Task identifier for reporting.
    pub task_id: TaskId,
    /// Catalog snapshot pinned by the planning session.
    pub catalog_snapshot: Option<CatalogSnapshotToken>,
}

/// Read from an infinite stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSource {
    /// E.g., Kafka topic name.
    pub topic: String,
}

/// Source of data for a stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataSource {
    /// Read from static storage (e.g., Parquet).
    File(FileScanTask),
    /// Read from an infinite event stream (e.g., Kafka).
    Stream(StreamSource),
    /// A source that generates data directly (e.g. VALUES).
    Memory,
}

/// A streaming input tree used when a logical input contains chained joins.
///
/// Each pipeline leaf retains its own VM program so scan filters and projections
/// remain attached to the scan that produces them. Join nodes build their right
/// input and stream their left input through a hash join.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalInputPlan {
    /// A source and its per-batch vector program.
    Pipeline {
        /// Source that produces this input's batches.
        source: DataSource,
        /// Batch program applied before forwarding the input.
        program: Arc<MicroProgram>,
    },
    /// A row-wise VM program applied to batches from a recursive input.
    ///
    /// This carries filters and projections that sit above a join. The VM's
    /// Arrow projection retains array buffers by reference, so this boundary
    /// does not materialize intermediate columns.
    Transform {
        /// The input whose batches this program consumes.
        input: Box<PhysicalInputPlan>,
        /// The program applied to each input batch.
        program: Arc<MicroProgram>,
    },
    /// A pipeline breaker that groups a recursive input before forwarding its
    /// output batches. This is used when an aggregate feeds a join input.
    Aggregate {
        /// Input batches containing group keys followed by aggregate arguments.
        input: Box<PhysicalInputPlan>,
        /// Grouping and reduction instructions for the input batches.
        plan: AggregatePlan,
    },
    /// A hash join over two recursively executable inputs.
    Join {
        /// Input streamed through the probe side of this join.
        probe: Box<PhysicalInputPlan>,
        /// Input fully consumed to build this join's hash table.
        build: Box<PhysicalInputPlan>,
        /// Join keys, output layout, and join flavour.
        plan: JoinPlan,
    },
}

/// Key-value option pair for storage and scan configurations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageTarget {
    /// The path or table URI.
    pub destination_uri: String,
    /// Storage format and execution options.
    pub options: Vec<KeyValue>,
    /// Authoritative schema of columns to write.
    pub schema: VeinSchemaRef,
    /// Arrow IPC stream containing the complete authoritative output schema.
    /// This preserves nested types and allows sinks to initialize empty writes.
    pub schema_ipc: Vec<u8>,
}

impl StorageTarget {
    /// Creates a new StorageTarget.
    pub fn new(
        destination_uri: impl Into<String>,
        options: Vec<KeyValue>,
        schema: VeinSchemaRef,
    ) -> Self {
        Self {
            destination_uri: destination_uri.into(),
            options,
            schema,
            schema_ipc: Vec::new(),
        }
    }

    /// Sets the canonical Arrow IPC schema used by storage sinks.
    pub fn with_schema_ipc(mut self, schema_ipc: Vec<u8>) -> Self {
        self.schema_ipc = schema_ipc;
        self
    }

    /// Helper to find an option value by key (case-insensitive).
    pub fn get_option(&self, key: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|kv| kv.key.eq_ignore_ascii_case(key))
            .map(|kv| kv.value.as_str())
    }

    /// Sets an option, replacing an existing key case-insensitively.
    pub fn set_option(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        if let Some(option) = self
            .options
            .iter_mut()
            .find(|option| option.key.eq_ignore_ascii_case(&key))
        {
            option.value = value.into();
        } else {
            self.options.push(KeyValue::new(key, value));
        }
    }
}

/// Sink for data produced by a stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataSink {
    /// Stream data directly back to the Query Coordinator/Client.
    Client {
        /// Schema of the columns expected at the client.
        schema: VeinSchemaRef,
    },
    /// Write data out to storage (e.g., Iceberg commit).
    Storage(StorageTarget),
    /// Discard the data (useful for some benchmarks or tests).
    Discard,
}

/// Mode of execution for a task.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ExecutionMode {
    /// Finite batch execution (e.g., reading Parquet files).
    Batch,
    /// Infinite stream execution (DBSP incremental multi-epoch).
    Streaming,
}

/// Task dispatched from Master to Executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteTask {
    /// Stable request lineage identifier shared by coordinator, scheduler, and executors.
    pub request_id: RequestId,
    /// Execution mode (Batch or Streaming).
    pub mode: ExecutionMode,
    /// Unique task identifier.
    pub task_id: TaskId,
    /// Retry attempt for this task, starting at zero.
    pub attempt: u32,
    /// Query this task belongs to.
    pub query_id: QueryId,
    /// Stage this task is part of.
    pub stage_id: StageId,
    /// Recursive physical input for a plan with chained pipeline breakers.
    ///
    /// `None` preserves the original single-source task wire format.
    #[serde(default)]
    pub input_plan: Option<Arc<PhysicalInputPlan>>,
    /// Data source for this task.
    pub source: DataSource,
    /// The Micro-IR program to apply to the data stream.
    ///
    /// Shared so every task split from one stage refers to a single compiled
    /// program instead of deep-cloning the instruction vector per partition.
    pub program: Arc<MicroProgram>,
    /// Where to send the output.
    pub sink: Arc<DataSink>,
    /// Second input, read to completion before `source` is probed.
    ///
    /// `Some` makes this task a join: the build side is absorbed into a hash
    /// table, and only then does the probe side stream against it. `None` is a
    /// plain single-input task. The two-input shape is on the task rather than
    /// in the program because a join is a pipeline breaker — the VM sees one
    /// chunk at a time and has no second stream.
    pub build: Option<DataSource>,
    /// How the two inputs are joined. Set exactly when `build` is.
    pub join: Option<JoinPlan>,
    /// The row window to emit, when the query has a `LIMIT`.
    ///
    /// It travels with the task because a limit is cross-chunk state that also
    /// has to *stop the source*: the VM sees one chunk at a time, and dropping
    /// rows and knowing when to stop reading are one decision.
    pub limit: Option<LimitSpec>,
    /// Global row ordering, applied before the output window.
    #[serde(default)]
    pub sort: Option<SortPlan>,
    /// VM program that appends computed sort keys after the main row program.
    #[serde(default)]
    pub sort_key_program: Option<Arc<MicroProgram>>,
    /// Number of user-visible columns before helper sort keys are appended.
    #[serde(default)]
    pub sort_output_columns: Option<u32>,
    /// How to aggregate, when the query has a `GROUP BY` or a `DISTINCT`.
    pub aggregate: Option<AggregatePlan>,
    /// Vector VM program applied to aggregate output, after the aggregate has
    /// finished and before the output window and sink.
    #[serde(default)]
    pub post_aggregate_program: Option<Arc<MicroProgram>>,
}

use super::{AggOpCode, JoinKind};

/// Where one output column of a join comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinOutput {
    /// A column of the probe input.
    Probe(u32),
    /// A column of the build input.
    Build(u32),
}

/// How a task's two inputs are joined.
///
/// It travels with the task because the executor must build exactly the table
/// the planner described: key positions on both sides, join flavour, and the
/// output column layout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinPlan {
    /// Which flavour of join to perform.
    pub join_type: JoinKind,
    /// Key columns of the probe input.
    pub probe_keys: Vec<u32>,
    /// Key columns of the build input.
    pub build_keys: Vec<u32>,
    /// Output columns, in output order.
    pub outputs: Vec<JoinOutput>,
    /// The data type of every probe column the output may reference, so a null
    /// probe side can still be given a type.
    pub probe_types: Vec<types::VeinDataType>,
    /// The data type of every build column the output may reference.
    pub build_types: Vec<types::VeinDataType>,
}

/// The row window a task emits: skip `offset` rows, then emit at most `count`.
///
/// One struct rather than two loose fields so "a task has a window" is a single
/// thing to check, the way `JoinPlan` is one thing for "a task is a join".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitSpec {
    /// Rows to skip before emitting any.
    pub offset: u64,
    /// Maximum rows to emit after the offset.
    pub count: u64,
}

/// A sort key over one column produced by the task's vector program.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    /// Zero-based column position in each input batch.
    pub column: u32,
    /// Whether values are ordered from low to high.
    pub ascending: bool,
    /// Whether null values precede non-null values.
    pub nulls_first: bool,
}

/// The ordering applied to the complete task input before its output window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortPlan {
    /// Sort keys in lexicographic priority order.
    pub keys: Vec<SortKey>,
}

/// How a task aggregates its input, when the query groups or deduplicates.
///
/// The columns the executor sees are the program's output registers: the first
/// `groups` of them are the group key, and every column after them is one
/// aggregate's argument. The compiler puts them there, so an argument like
/// `sum(price * quantity)` is just another register and this plan never has to
/// describe an expression.
///
/// A `DISTINCT` is the same shape with every output column as a key and no
/// aggregates, which is why it needs no operator of its own.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregatePlan {
    /// Leading output columns that together form the group key.
    pub groups: u32,
    /// Optional planner estimate for the number of distinct groups.
    ///
    /// This is only a capacity hint; execution remains correct when it is absent
    /// or inaccurate. Older serialized plans omit it.
    #[serde(default)]
    pub estimated_groups: Option<u64>,
    /// Positions in the leading key columns that belong to each grouping set.
    /// `None` preserves the ordinary single-set layout where every key column
    /// participates in the group. `Some` carries explicit grouping sets.
    #[serde(default)]
    pub grouping_sets: Option<Vec<Vec<u32>>>,
    /// One op per column after the keys, in column order.
    pub aggs: Vec<AggOpCode>,
    /// The physical input type for each aggregate argument, in aggregate order.
    pub aggregate_input_types: Vec<types::VeinDataType>,
    /// The type of every output column: the keys first, then one per aggregate.
    pub types: Vec<types::VeinDataType>,
}

#[cfg(test)]
mod tests {
    use super::{AggOpCode, AggregatePlan, SortKey, SortPlan};

    #[test]
    fn sort_plan_preserves_key_priority_and_null_order_over_wire_format() {
        let plan = SortPlan {
            keys: vec![
                SortKey {
                    column: 3,
                    ascending: false,
                    nulls_first: true,
                },
                SortKey {
                    column: 1,
                    ascending: true,
                    nulls_first: false,
                },
            ],
        };

        let encoded = serde_json::to_vec(&plan).expect("serialize sort plan");
        let decoded: SortPlan = serde_json::from_slice(&encoded).expect("deserialize sort plan");

        assert_eq!(decoded, plan);
    }

    #[test]
    fn aggregate_estimate_round_trips_and_defaults_for_older_wire_plans() {
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: Some(12),
            grouping_sets: None,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![types::VeinDataType::Int64],
            types: vec![types::VeinDataType::Int64; 2],
        };
        let mut value = serde_json::to_value(&plan).expect("serialize aggregate plan");
        value
            .as_object_mut()
            .expect("aggregate plan object")
            .remove("estimated_groups");

        let decoded: AggregatePlan =
            serde_json::from_value(value).expect("deserialize legacy aggregate plan");

        assert_eq!(decoded.estimated_groups, None);
    }
}
