//! Physical task definitions, execution modes, sources, and sinks.

use catalog::{CatalogSnapshotToken, StatisticsPredicate};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use types::VeinSchemaRef;

use super::ir::{MicroProgram, RegId};
use crate::identifiers::{IntermediateSourceId, QueryId, RequestId, StageId, TaskId};
use crate::statistics::QueryStatisticsContext;

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

/// A source predicate plan evaluated before projected rows are materialized.
///
/// It is held as compiled Micro-IR so a source uses the same VM as the residual
/// pipeline. Stages are applied in order, allowing a selective safe predicate to
/// reduce the input to later stages without materializing conjunction arrays.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPredicate {
    /// Number of logical conjuncts in the source predicate.
    pub predicate_count: usize,
    /// Ordered predicate stages. Each stage only reads its own required columns.
    pub stages: Vec<ScanPredicateStage>,
    /// Format-neutral necessary conditions for skipping data units from exact
    /// source statistics. Unsupported logical expressions remain `Unknown`.
    #[serde(default)]
    pub statistics: Vec<StatisticsPredicate>,
}

/// One compiled conjunction evaluated as a source-level selection stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPredicateStage {
    /// Number of logical conjuncts combined in this stage.
    pub predicate_count: usize,
    /// Whether evaluating this stage in a different order is semantics-safe.
    /// A missing value defaults to false so older serialized plans stay conservative.
    #[serde(default)]
    pub reorder_safe: bool,
    /// Stage input columns, in register order: input column `i` is mounted into
    /// VM register `i`.
    pub input: VeinSchemaRef,
    /// Program computing this stage's boolean for one input batch.
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
    /// Optional range of reader-defined, independently schedulable units in a
    /// single file. For Parquet this addresses row groups; other scan adapters
    /// may define their own unit ordinals. `None` means scan the whole file.
    #[serde(default)]
    pub scan_unit_range: Option<Range<usize>>,
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
    /// Runtime synopsis collection context, when the planner identified a table scan.
    #[serde(default)]
    pub runtime_statistics: Option<RuntimeStatisticsScan>,
}

/// Stable identity and coverage information for lazy runtime statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStatisticsScan {
    /// Identity shared by all independently scheduled pieces of this scan.
    pub scan_id: u128,
    /// Source revision key used to prevent cross-snapshot statistics reuse.
    pub table_key: String,
    /// Exact row count from the catalog, when available.
    pub expected_rows: Option<u64>,
    /// Whether the plan can prove complete source-row coverage. Column
    /// coverage is independent: projected-column synopses may still be useful.
    pub row_coverage_candidate: bool,
    /// Ordinal of this independently scheduled work unit.
    pub partition_index: usize,
    /// Number of work units that must finish before the synopsis is finalized.
    pub partition_count: usize,
}

/// Read from an infinite stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSource {
    /// E.g., Kafka topic name.
    pub topic: String,
}

/// Source of data for a stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
// Physical plan nodes retain this enum behind an Arc. Keeping the file task
// inline avoids a second allocation for its already heap-backed metadata.
#[allow(clippy::large_enum_variant)]
pub enum DataSource {
    /// Read from static storage (e.g., Parquet).
    File(FileScanTask),
    /// Read from an infinite event stream (e.g., Kafka).
    Stream(StreamSource),
    /// A source that generates data directly (e.g. VALUES).
    Memory,
    /// A query-local relation produced by an earlier adaptive stage.
    ///
    /// The identifier is resolved by the in-process executor registry. The
    /// relation payload itself is deliberately not serialized into the plan.
    Intermediate(IntermediateSourceId),
}

/// Immutable, schema-aware physical plan tree shared by planning and execution.
///
/// The root is normally a [`PhysicalPlanOperator::Sink`]. Each operator node
/// owns its output schema and references its input nodes through `Arc`, so a
/// plan can be inspected or shared without copying operator subtrees. Mutable
/// execution state such as hash tables and aggregate accumulators belongs to an
/// execution, not to these nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalPlan {
    /// The root operator of the query plan.
    pub root: Arc<PhysicalPlanNode>,
}

/// The two physical fragments produced by cutting a plan at a checkpoint.
pub struct CheckpointPlanFragments {
    /// Upstream subtree that materializes the checkpoint relation.
    pub producer: PhysicalPlan,
    /// Remaining downstream operators reading the registered relation.
    pub continuation: PhysicalPlan,
}

impl PhysicalPlan {
    /// Split this canonical plan at one node for adaptive execution.
    ///
    /// The producer retains the selected subtree and writes its output through a
    /// client-shaped sink, which can be replaced by the checkpoint sink by the
    /// coordinator. The continuation preserves the surrounding immutable
    /// operator tree and replaces the selected subtree with an intermediate
    /// source. It returns `None` if the node is absent, is the output sink, or is
    /// referenced more than once in the plan.
    pub fn split_at_checkpoint(
        &self,
        target: &Arc<PhysicalPlanNode>,
        source_id: IntermediateSourceId,
    ) -> Option<CheckpointPlanFragments> {
        if Arc::ptr_eq(&self.root, target)
            || matches!(target.operator, PhysicalPlanOperator::Sink { .. })
        {
            return None;
        }

        let source = Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&target.output_schema),
            operator: PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Intermediate(source_id)),
                input_schema: Arc::clone(&target.output_schema),
                program: Arc::new(MicroProgram::empty()),
            },
        });
        let (continuation_root, replacements) = replace_plan_node(&self.root, target, &source);
        if replacements != 1 {
            return None;
        }

        let producer = PhysicalPlan {
            root: Arc::new(PhysicalPlanNode {
                output_schema: Arc::clone(&target.output_schema),
                operator: PhysicalPlanOperator::Sink {
                    input: Arc::clone(target),
                    sink: Arc::new(DataSink::Client {
                        schema: Arc::clone(&target.output_schema),
                    }),
                },
            }),
        };
        Some(CheckpointPlanFragments {
            producer,
            continuation: PhysicalPlan {
                root: continuation_root,
            },
        })
    }
}

fn replace_plan_node(
    current: &Arc<PhysicalPlanNode>,
    target: &Arc<PhysicalPlanNode>,
    replacement: &Arc<PhysicalPlanNode>,
) -> (Arc<PhysicalPlanNode>, usize) {
    if Arc::ptr_eq(current, target) {
        return (Arc::clone(replacement), 1);
    }

    let (operator, replacements) = match &current.operator {
        PhysicalPlanOperator::Pipeline {
            source,
            input_schema,
            program,
        } => (
            PhysicalPlanOperator::Pipeline {
                source: Arc::clone(source),
                input_schema: Arc::clone(input_schema),
                program: Arc::clone(program),
            },
            0,
        ),
        PhysicalPlanOperator::Transform { input, program } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Transform {
                    input,
                    program: Arc::clone(program),
                },
                count,
            )
        }
        PhysicalPlanOperator::Join { probe, build, plan } => {
            let (probe, probe_count) = replace_plan_node(probe, target, replacement);
            let (build, build_count) = replace_plan_node(build, target, replacement);
            (
                PhysicalPlanOperator::Join {
                    probe,
                    build,
                    plan: plan.clone(),
                },
                probe_count + build_count,
            )
        }
        PhysicalPlanOperator::Aggregate { input, plan } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Aggregate {
                    input,
                    plan: plan.clone(),
                },
                count,
            )
        }
        PhysicalPlanOperator::Distinct { input, plan } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Distinct {
                    input,
                    plan: plan.clone(),
                },
                count,
            )
        }
        PhysicalPlanOperator::Sort {
            input,
            plan,
            key_program,
            output_columns,
        } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Sort {
                    input,
                    plan: plan.clone(),
                    key_program: key_program.clone(),
                    output_columns: *output_columns,
                },
                count,
            )
        }
        PhysicalPlanOperator::Limit { input, spec } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (PhysicalPlanOperator::Limit { input, spec: *spec }, count)
        }
        PhysicalPlanOperator::Sink { input, sink } => {
            let (input, count) = replace_plan_node(input, target, replacement);
            (
                PhysicalPlanOperator::Sink {
                    input,
                    sink: Arc::clone(sink),
                },
                count,
            )
        }
    };

    (
        Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&current.output_schema),
            operator,
        }),
        replacements,
    )
}

/// One immutable operator node in a [`PhysicalPlan`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalPlanNode {
    /// Output columns produced by this operator. For a sink, this is the schema
    /// consumed by the sink.
    pub output_schema: VeinSchemaRef,
    /// Operator semantics and references to this node's inputs.
    pub operator: PhysicalPlanOperator,
}

/// Physical operator semantics stored in the canonical plan tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalPlanOperator {
    /// A data source and its source-local vector program.
    Pipeline {
        /// Source descriptor.
        source: Arc<DataSource>,
        /// Ordered columns emitted by the source before the program runs.
        input_schema: VeinSchemaRef,
        /// Program applied to each source batch.
        program: Arc<MicroProgram>,
    },
    /// A vector program applied to a recursively produced input.
    Transform {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Program applied to the child batches.
        program: Arc<MicroProgram>,
    },
    /// A hash join over probe and build inputs.
    Join {
        /// Input streamed through the probe side.
        probe: Arc<PhysicalPlanNode>,
        /// Input fully consumed to build the hash table.
        build: Arc<PhysicalPlanNode>,
        /// Join keys, output layout, and join flavor.
        plan: JoinPlan,
    },
    /// A grouped or scalar aggregate over one input.
    Aggregate {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Grouping and reduction instructions.
        plan: AggregatePlan,
    },
    /// Duplicate elimination by grouping every child output column.
    Distinct {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Grouping layout and output types used by the shared aggregate engine.
        plan: AggregatePlan,
    },
    /// A global ordering boundary over one input.
    Sort {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Sort keys and their ordering semantics.
        plan: SortPlan,
        /// Optional program that appends computed sort keys.
        key_program: Option<Arc<MicroProgram>>,
        /// Number of child columns to retain after removing helper keys.
        output_columns: u32,
    },
    /// A row window over one input.
    Limit {
        /// Child input.
        input: Arc<PhysicalPlanNode>,
        /// Offset and maximum output row count.
        spec: LimitSpec,
    },
    /// The output boundary for client results or storage writes.
    Sink {
        /// Child input consumed by the sink.
        input: Arc<PhysicalPlanNode>,
        /// Client, storage, or discard destination.
        sink: Arc<DataSink>,
    },
}

/// Which view inputs an operator may convert to ordinary Arrow arrays.
///
/// Conversion can share contiguous buffers or gather mapped rows. This describes
/// conversion scope, not bytes copied, memory usage, or computed allocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMaterialization {
    /// Reads row/column references without converting input arrays.
    None,
    /// Converts only columns required by kernels, keys, or residual expressions.
    KernelColumns,
    /// Requires every active input column as an ordinary Arrow array.
    WholeBatch,
    /// Client consumers choose between retaining views and requesting Arrow arrays.
    ConsumerBoundary,
}

/// How an operator represents output payloads in the current single-node runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputRepresentation {
    /// Output rows reference existing payload arrays, including NULL extension.
    References,
    /// Unchanged payloads remain references; computed columns may allocate arrays.
    ReferencesAndComputed,
    /// Produces result arrays which the runtime wraps in a view.
    ResultArrays,
    /// Consumes the batch at the client, storage, or discard boundary.
    Sink,
}

/// Derived data-flow contract of a canonical physical operator.
///
/// This is computed from operator semantics rather than serialized or stored as
/// an independent plan setting. It describes current runtime behavior; it does
/// not claim that hash keys, encoded sort keys, or aggregate state are zero-copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorDataFlow {
    /// Stable operator label used in runtime diagnostics.
    pub operator: &'static str,
    /// Scope of input view conversion required by the implementation.
    pub input_materialization: InputMaterialization,
    /// Output payload representation used by the implementation.
    pub output: OutputRepresentation,
}

impl PhysicalPlanOperator {
    /// Derives the view/materialization contract from this canonical operator.
    ///
    /// Kernel conversions are conditional: direct VM projections do not need
    /// arrays, and Cross joins with no residual do not need hash-key conversion.
    /// Sort includes any computed-key program. Aggregate and DISTINCT kernels
    /// read only their required columns; grouping keys remain reference-backed.
    /// Storage and client boundaries decide when complete arrays are required.
    pub fn data_flow(&self) -> OperatorDataFlow {
        use InputMaterialization::{ConsumerBoundary, KernelColumns, None, WholeBatch};
        use OutputRepresentation::{References, ReferencesAndComputed, ResultArrays, Sink};

        let (operator, input_materialization, output) = match self {
            Self::Pipeline { .. } => ("pipeline", KernelColumns, ReferencesAndComputed),
            Self::Transform { .. } => ("transform", KernelColumns, ReferencesAndComputed),
            Self::Join { .. } => ("join", KernelColumns, References),
            Self::Sort { .. } => ("sort", KernelColumns, References),
            Self::Limit { .. } => ("limit", None, References),
            Self::Aggregate { .. } => ("aggregate", KernelColumns, ResultArrays),
            Self::Distinct { .. } => ("distinct", KernelColumns, ResultArrays),
            Self::Sink { sink, .. } => (
                "sink",
                match sink.as_ref() {
                    DataSink::Client { .. } => ConsumerBoundary,
                    DataSink::Storage(_) => WholeBatch,
                    DataSink::Discard => None,
                },
                Sink,
            ),
        };
        OperatorDataFlow {
            operator,
            input_materialization,
            output,
        }
    }
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
    /// The authoritative physical operator tree for this task.
    pub plan: PhysicalPlan,
    /// In-process query statistics shared by planning and local execution.
    ///
    /// This is deliberately excluded from the serialized task payload. A local
    /// coordinator attaches the context after planning; remote executors receive
    /// the self-contained plan and can provide their own local context.
    #[serde(skip)]
    pub statistics_context: Option<Arc<QueryStatisticsContext>>,
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
    /// Candidate-pair predicate evaluated after hash-key matches and before
    /// unmatched rows are determined. Input columns are ordered as listed.
    #[serde(default)]
    pub residual: Option<JoinResidualPlan>,
    /// Output columns, in output order.
    pub outputs: Vec<JoinOutput>,
    /// The data type of every probe column the output may reference, so a null
    /// probe side can still be given a type.
    pub probe_types: Vec<types::VeinDataType>,
    /// The data type of every build column the output may reference.
    pub build_types: Vec<types::VeinDataType>,
}

/// Vector predicate for the non-key terms of a join condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinResidualPlan {
    /// Columns gathered from the probe and build rows for predicate evaluation.
    pub input_columns: Vec<JoinOutput>,
    /// VM program that filters candidate pairs in place through its selection.
    pub program: Arc<MicroProgram>,
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
    /// Whether an empty grouping set produces a row when the input is empty.
    ///
    /// SQL scalar aggregates emit an empty-input row, while DISTINCT over an
    /// empty projection does not. Older plans retain scalar aggregate behavior.
    #[serde(default = "default_emit_empty_group")]
    pub emit_empty_group_on_empty_input: bool,
    /// One op per column after the keys, in column order.
    pub aggs: Vec<AggOpCode>,
    /// The physical input type for each aggregate argument, in aggregate order.
    pub aggregate_input_types: Vec<types::VeinDataType>,
    /// The type of every output column: the keys first, then one per aggregate.
    pub types: Vec<types::VeinDataType>,
}

fn default_emit_empty_group() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{
        AggOpCode, AggregatePlan, DataSink, DataSource, InputMaterialization, JoinKind, JoinOutput,
        JoinPlan, LimitSpec, MicroProgram, OutputRepresentation, PhysicalPlan, PhysicalPlanNode,
        PhysicalPlanOperator, SortKey, SortPlan,
    };
    use crate::identifiers::IntermediateSourceId;
    use std::sync::Arc;
    use types::VeinSchema;

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
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Count],
            aggregate_input_types: vec![types::VeinDataType::Int64],
            types: vec![types::VeinDataType::Int64; 2],
        };
        let mut value = serde_json::to_value(&plan).expect("serialize aggregate plan");
        value
            .as_object_mut()
            .expect("aggregate plan object")
            .remove("estimated_groups");
        value
            .as_object_mut()
            .expect("aggregate plan object")
            .remove("emit_empty_group_on_empty_input");

        let decoded: AggregatePlan =
            serde_json::from_value(value).expect("deserialize legacy aggregate plan");

        assert_eq!(decoded.estimated_groups, None);
        assert!(decoded.emit_empty_group_on_empty_input);
    }

    fn node(operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
        Arc::new(PhysicalPlanNode {
            output_schema: Arc::new(VeinSchema::empty()),
            operator,
        })
    }

    #[test]
    fn sink_contracts_follow_their_consumer_boundary() {
        let input = node(PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::new(VeinSchema::empty()),
            program: Arc::new(MicroProgram::empty()),
        });
        for (sink, expected) in [
            (DataSink::Discard, InputMaterialization::None),
            (
                DataSink::Client {
                    schema: Arc::new(VeinSchema::empty()),
                },
                InputMaterialization::ConsumerBoundary,
            ),
            (
                DataSink::Storage(super::StorageTarget::new(
                    "test",
                    Vec::new(),
                    Arc::new(VeinSchema::empty()),
                )),
                InputMaterialization::WholeBatch,
            ),
        ] {
            let operator = PhysicalPlanOperator::Sink {
                input: Arc::clone(&input),
                sink: Arc::new(sink),
            };
            let contract = operator.data_flow();
            assert_eq!(contract.input_materialization, expected);
            assert_eq!(contract.output, OutputRepresentation::Sink);
        }
    }

    fn operator_names(node: &PhysicalPlanNode, names: &mut Vec<&'static str>) {
        let contract = node.operator.data_flow();
        // Exhaustive against the operator enum: a new variant requires both a
        // production contract and regression expectations, without a wildcard.
        let (input, output) = match &node.operator {
            PhysicalPlanOperator::Pipeline { .. } | PhysicalPlanOperator::Transform { .. } => (
                InputMaterialization::KernelColumns,
                OutputRepresentation::ReferencesAndComputed,
            ),
            PhysicalPlanOperator::Join { .. } | PhysicalPlanOperator::Sort { .. } => (
                InputMaterialization::KernelColumns,
                OutputRepresentation::References,
            ),
            PhysicalPlanOperator::Limit { .. } => {
                (InputMaterialization::None, OutputRepresentation::References)
            }
            PhysicalPlanOperator::Aggregate { .. } => (
                InputMaterialization::KernelColumns,
                OutputRepresentation::ResultArrays,
            ),
            PhysicalPlanOperator::Distinct { .. } => (
                InputMaterialization::KernelColumns,
                OutputRepresentation::ResultArrays,
            ),
            PhysicalPlanOperator::Sink { sink, .. } => (
                match sink.as_ref() {
                    DataSink::Client { .. } => InputMaterialization::ConsumerBoundary,
                    DataSink::Storage(_) => InputMaterialization::WholeBatch,
                    DataSink::Discard => InputMaterialization::None,
                },
                OutputRepresentation::Sink,
            ),
        };
        assert_eq!(contract.input_materialization, input);
        assert_eq!(contract.output, output);
        match &node.operator {
            PhysicalPlanOperator::Pipeline { .. } => names.push("pipeline"),
            PhysicalPlanOperator::Transform { input, .. } => {
                operator_names(input, names);
                names.push("transform");
            }
            PhysicalPlanOperator::Join { probe, build, .. } => {
                operator_names(probe, names);
                operator_names(build, names);
                names.push("join");
            }
            PhysicalPlanOperator::Aggregate { input, .. } => {
                operator_names(input, names);
                names.push("aggregate");
            }
            PhysicalPlanOperator::Distinct { input, .. } => {
                operator_names(input, names);
                names.push("distinct");
            }
            PhysicalPlanOperator::Sort { input, .. } => {
                operator_names(input, names);
                names.push("sort");
            }
            PhysicalPlanOperator::Limit { input, .. } => {
                operator_names(input, names);
                names.push("limit");
            }
            PhysicalPlanOperator::Sink { input, .. } => {
                operator_names(input, names);
                names.push("sink");
            }
        }
        assert_eq!(names.last().copied(), Some(contract.operator));
    }

    #[test]
    fn canonical_physical_plan_round_trips_every_operator_node() {
        let leaf = || {
            node(PhysicalPlanOperator::Pipeline {
                source: Arc::new(DataSource::Memory),
                input_schema: Arc::new(VeinSchema::empty()),
                program: Arc::new(MicroProgram::empty()),
            })
        };
        let join = node(PhysicalPlanOperator::Join {
            probe: leaf(),
            build: leaf(),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: vec![0],
                build_keys: vec![0],
                residual: None,
                outputs: vec![JoinOutput::Probe(0)],
                probe_types: vec![types::VeinDataType::Int64],
                build_types: vec![types::VeinDataType::Int64],
            },
        });
        let transformed = node(PhysicalPlanOperator::Transform {
            input: join,
            program: Arc::new(MicroProgram::empty()),
        });
        let aggregated = node(PhysicalPlanOperator::Aggregate {
            input: transformed,
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: None,
                emit_empty_group_on_empty_input: true,
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![types::VeinDataType::Int64],
                types: vec![types::VeinDataType::Int64],
            },
        });
        let distinct = node(PhysicalPlanOperator::Distinct {
            input: aggregated,
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![]]),
                emit_empty_group_on_empty_input: false,
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: Vec::new(),
            },
        });
        let sorted = node(PhysicalPlanOperator::Sort {
            input: distinct,
            plan: SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            key_program: None,
            output_columns: 1,
        });
        let limited = node(PhysicalPlanOperator::Limit {
            input: sorted,
            spec: LimitSpec {
                offset: 1,
                count: 10,
            },
        });
        let plan = PhysicalPlan {
            root: node(PhysicalPlanOperator::Sink {
                input: limited,
                sink: Arc::new(DataSink::Discard),
            }),
        };

        let encoded = serde_json::to_vec(&plan).expect("serialize canonical physical plan");
        let wire = std::str::from_utf8(&encoded).unwrap();
        assert!(!wire.contains("input_materialization"));
        assert!(!wire.contains("output_representation"));
        let decoded: PhysicalPlan =
            serde_json::from_slice(&encoded).expect("deserialize canonical physical plan");
        let mut names = Vec::new();
        operator_names(&decoded.root, &mut names);

        assert_eq!(
            names,
            vec![
                "pipeline",
                "pipeline",
                "join",
                "transform",
                "aggregate",
                "distinct",
                "sort",
                "limit",
                "sink",
            ]
        );
    }

    #[test]
    fn canonical_plan_clones_share_immutable_operator_nodes() {
        let child = node(PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::new(VeinSchema::empty()),
            program: Arc::new(MicroProgram::empty()),
        });
        let plan = PhysicalPlan {
            root: node(PhysicalPlanOperator::Limit {
                input: Arc::clone(&child),
                spec: LimitSpec {
                    offset: 0,
                    count: 5,
                },
            }),
        };
        let cloned = plan.clone();

        assert!(Arc::ptr_eq(&plan.root, &cloned.root));
        let (
            PhysicalPlanOperator::Limit { input, .. },
            PhysicalPlanOperator::Limit {
                input: cloned_input,
                ..
            },
        ) = (&plan.root.operator, &cloned.root.operator)
        else {
            panic!("cloned plan should retain its root node");
        };
        assert!(Arc::ptr_eq(input, cloned_input));
    }

    #[test]
    fn checkpoint_split_preserves_producer_and_replaces_only_the_continuation_input() {
        let schema = Arc::new(VeinSchema::empty());
        let source = node(PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::clone(&schema),
            program: Arc::new(MicroProgram::empty()),
        });
        let boundary = Arc::new(PhysicalPlanNode {
            output_schema: Arc::clone(&schema),
            operator: PhysicalPlanOperator::Transform {
                input: source,
                program: Arc::new(MicroProgram::empty()),
            },
        });
        let plan = PhysicalPlan {
            root: Arc::new(PhysicalPlanNode {
                output_schema: Arc::clone(&schema),
                operator: PhysicalPlanOperator::Sink {
                    input: Arc::clone(&boundary),
                    sink: Arc::new(DataSink::Client {
                        schema: Arc::clone(&schema),
                    }),
                },
            }),
        };
        let source_id = IntermediateSourceId::new();

        let fragments = plan
            .split_at_checkpoint(&boundary, source_id)
            .expect("selected subplan should split into two fragments");
        let PhysicalPlanOperator::Sink {
            input: producer_input,
            ..
        } = &fragments.producer.root.operator
        else {
            panic!("producer fragment must end at a checkpoint sink");
        };
        assert!(Arc::ptr_eq(producer_input, &boundary));

        let PhysicalPlanOperator::Sink {
            input: continuation_input,
            ..
        } = &fragments.continuation.root.operator
        else {
            panic!("continuation must preserve the original output sink");
        };
        // The selected transform runs in the producer. Its entire subtree is
        // replaced in the continuation so it cannot be executed a second time.
        let PhysicalPlanOperator::Pipeline { source, .. } = &continuation_input.operator
        else {
            panic!("boundary should be replaced by an intermediate source");
        };
        let DataSource::Intermediate(actual) = source.as_ref() else {
            panic!("continuation must read the checkpoint relation");
        };
        assert_eq!(*actual, source_id);

        assert!(
            plan.split_at_checkpoint(&plan.root, source_id).is_none(),
            "the terminal sink cannot be used as a checkpoint boundary"
        );
    }
}
