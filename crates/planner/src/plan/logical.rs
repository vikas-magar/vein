use crate::schema::{ColumnId, ResolvedColumn, VeinSchema};
use crate::expr::LogicalExpr;
use parser::ast::query::SetOpKind;
use super::types::SortKey;

/// A node in the logical query plan tree
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    // ── Sources ─────────────────────────────────────────────
    /// A table scan
    Scan {
        /// The name of the table
        table_name: String,
        /// The projection of column IDs to read
        projection: Option<Vec<ColumnId>>,
        /// The filters to push down
        filters: Vec<LogicalExpr>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },

    // ── Relational Core ─────────────────────────────────────
    /// A filter operation
    Filter {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The filter predicate
        predicate: LogicalExpr,
    },
    /// A projection operation
    Project {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The projection expressions
        exprs: Vec<LogicalExpr>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },

    /// An aggregation operation
    Aggregate {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The multidimensional grouping sets (for ROLLUP/CUBE)
        grouping_sets: Vec<Vec<LogicalExpr>>,
        /// The aggregate expressions
        aggs: Vec<LogicalExpr>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },

    // ── Ordering/Limits ─────────────────────────────────────
    /// A sort operation
    Sort {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The sort keys
        sort_keys: Vec<SortKey>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A limit/offset operation
    Limit {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The offset to skip
        offset: u64,
        /// The number of rows to return
        count: u64,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A distinct operation
    Distinct {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },

    // ── Structure ───────────────────────────────────────────
    /// A subquery alias
    SubqueryAlias {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The alias name
        alias: String,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A set operation (Union/Intersect/Except)
    SetOp {
        /// The kind of set operation
        kind: SetOpKind,
        /// Whether to retain duplicates (ALL)
        all: bool,
        /// The left input plan
        left: Box<LogicalPlan>,
        /// The right input plan
        right: Box<LogicalPlan>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A sampling operation
    Sample {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The sampling method
        method: String,
        /// The percentage to sample
        percent: f64,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// An assertion operation
    Assert {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The condition expression
        condition: String,
        /// The assertion message
        message: Option<String>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A fork operation splitting a plan into branches
    Fork {
        /// The branch plans
        branches: Vec<Box<LogicalPlan>>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A tee operation multiplexing an output
    Tee {
        /// The branch plans
        branches: Vec<Box<LogicalPlan>>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },

    // ── Semantic Layer ──────────────────────────────────────
    /// An LLM-evaluated semantic filter
    SemanticFilter {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The natural language predicate
        predicate: String,
        /// The target features passed to the LLM
        targets: Vec<ResolvedColumn>,
        /// The conformal prediction confidence bound
        alpha: f64,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// An LLM-evaluated semantic join
    SemanticJoin {
        /// The left input plan
        left: Box<LogicalPlan>,
        /// The right input plan
        right: Box<LogicalPlan>,
        /// The natural language join condition
        condition: String,
        /// The bounding equi-join condition to prevent Cartesian explosion
        bounding_equi: Vec<(ResolvedColumn, ResolvedColumn)>,
        /// The dependent columns passed to the LLM
        depends_on: (Vec<ResolvedColumn>, Vec<ResolvedColumn>),
        /// The conformal prediction confidence bound
        alpha: f64,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A conformal calibration operator
    SemanticCalibrate {
        /// The input plan containing features and ground truth labels
        input: Box<LogicalPlan>,
        /// The target features used by the reference model
        target_features: Vec<ResolvedColumn>,
        /// The ground truth label column
        ground_truth_label: ResolvedColumn,
        /// The target alpha level
        alpha: f64,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },

    // ── Output ──────────────────────────────────────────────
    /// A write operator to a sink
    Write {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The target URI (e.g. api://, kafka://)
        target: String,
        /// Additional options
        options: Vec<(String, String)>,
        /// The resulting schema (usually identical to input)
        schema: Box<VeinSchema>,
    },
}
