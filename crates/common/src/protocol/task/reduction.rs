use super::*;

use crate::protocol::{AggOpCode, JoinKind};

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
    /// Typed expression steps that filter candidate pairs through a selection.
    pub program: Arc<ExpressionProgram>,
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
