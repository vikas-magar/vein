use super::types::SortKey;
use crate::expr::logical::LogicalExpr;
use crate::schema::{ColumnId, ResolvedColumnRef, VeinSchema};
use parser::ast::pipe_op::JoinType;
use parser::ast::query::SetOpKind;

/// A computed window expression: the resolved call together with the output
/// column the surrounding projection references (via a placeholder `Column`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowExpr {
    /// The bound window function call (`fn(args) OVER (...)`).
    pub call: LogicalExpr,
    /// The output column holding this window expression's result.
    pub output_column: ResolvedColumnRef,
}

/// A node in the logical query plan tree
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LogicalPlan {
    // ── Sources ─────────────────────────────────────────────
    /// A table scan
    Scan {
        /// The authoritative table metadata from the catalog.
        table: std::sync::Arc<catalog::TableMetadata>,
        /// The projection of column IDs to read
        projection: Option<Vec<ColumnId>>,
        /// The filters to push down
        filters: Vec<LogicalExpr>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A scan of an external object store (`FROM 'url' WITH (...)`).
    ExternalScan {
        /// The object store URL.
        url: String,
        /// Inline options (e.g. format).
        options: Vec<(String, String)>,
        /// Physical files to scan from the catalog.
        files: Vec<String>,
        /// Catalog metadata for this source when discovery resolved it.
        /// Retaining the shared handle keeps its statistics and revision
        /// available to planning and the query-scoped statistics context.
        source_metadata: Option<std::sync::Arc<catalog::TableMetadata>>,
        /// The resulting schema.
        schema: Box<VeinSchema>,
    },
    /// A query-local relation produced by an earlier adaptive execution stage.
    ///
    /// Unlike a table scan, this source has no catalog identity or storage
    /// revision. Its row count is an observation of this query's materialized
    /// relation and is used only as a planning estimate.
    Intermediate {
        /// Query-local handle resolved by the execution source registry.
        source_id: u128,
        /// Rows observed when the producer stage sealed the relation.
        row_count: u64,
        /// Stable columns exposed to remaining logical operators.
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
    /// A windowed aggregation computed over partitions of the input, produced
    /// by SELECT/EXTEND pipe operators that reference window functions
    /// (paper §4.1.1). The projection consuming it references the window
    /// output columns through placeholders.
    Window {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The PARTITION BY expressions (shared by all calls in this node)
        partition_by: Vec<LogicalExpr>,
        /// The ORDER BY keys inside OVER (drives RANK etc.)
        order_by: Vec<SortKey>,
        /// The optional frame — reuses `parser::ast::pipe_op::WindowFrame`
        /// (already `Clone/Debug/PartialEq/serde`); defaults to the full
        /// partition when `None`
        frame: Option<parser::ast::pipe_op::WindowFrame>,
        /// The computed window expressions with their output columns
        exprs: Vec<WindowExpr>,
        /// The resulting schema (input columns + new window columns)
        schema: Box<VeinSchema>,
    },

    // ── Relational Core ─────────────────────────────────────
    /// A relational join combining two input plans.
    Join {
        /// The join flavor (INNER/LEFT/RIGHT/FULL/CROSS).
        join_type: JoinType,
        /// The left input plan.
        left: Box<LogicalPlan>,
        /// The right input plan.
        right: Box<LogicalPlan>,
        /// The join predicate (`None` for a CROSS join).
        on: Option<LogicalExpr>,
        /// The `USING` columns, kept for schema de-duplication and display.
        using: Vec<ResolvedColumnRef>,
        /// The resulting schema (left ++ right; `USING` merges the join column).
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
    /// A plan-requested-by-`EXPLAIN`: bind and display the inner plan
    /// without executing it. The schema mirrors the inner plan's schema.
    Explain {
        /// The inner plan to display
        input: Box<LogicalPlan>,
        /// The resulting schema (identical to the input's)
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
        condition: LogicalExpr,
        /// The assertion message
        message: Option<String>,
        /// The resulting schema
        schema: Box<VeinSchema>,
    },
    /// A pivot: reshapes a column's distinct values into new columns.
    Pivot {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The bound aggregate expression.
        aggregate: LogicalExpr,
        /// The optional output alias for the pivoted column(s).
        alias: Option<String>,
        /// The column whose distinct values become new columns.
        value_column: ResolvedColumnRef,
        /// The explicit pivot values.
        values: Vec<LogicalExpr>,
        /// The resulting schema.
        schema: Box<VeinSchema>,
    },
    /// An unpivot: turns multiple columns into two columns (`name`, `value`).
    Unpivot {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The generated value column name.
        value_name: String,
        /// The generated name column name.
        name_name: String,
        /// The columns to unpivot.
        columns: Vec<ResolvedColumnRef>,
        /// Whether nulls are retained (`ALL`).
        include_nulls: bool,
        /// The resulting schema.
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
        targets: Vec<ResolvedColumnRef>,
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
        bounding_equi: Vec<(ResolvedColumnRef, ResolvedColumnRef)>,
        /// The dependent columns passed to the LLM
        depends_on: (Vec<ResolvedColumnRef>, Vec<ResolvedColumnRef>),
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
        target_features: Vec<ResolvedColumnRef>,
        /// The ground truth label column
        ground_truth_label: ResolvedColumnRef,
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
    /// Evaluates a function call over the input stream (`|> CALL name(args)`).
    Call {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The called function name
        name: String,
        /// The bound function arguments
        args: Vec<LogicalExpr>,
        /// The resulting schema (mirrors the input)
        schema: Box<VeinSchema>,
    },
}

impl LogicalPlan {
    /// Returns the stable name of this logical operator without formatting its
    /// inputs, schemas, or other potentially large plan metadata.
    pub fn operator_name(&self) -> &'static str {
        match self {
            LogicalPlan::Scan { .. } => "Scan",
            LogicalPlan::ExternalScan { .. } => "ExternalScan",
            LogicalPlan::Intermediate { .. } => "Intermediate",
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Project { .. } => "Project",
            LogicalPlan::Aggregate { .. } => "Aggregate",
            LogicalPlan::Window { .. } => "Window",
            LogicalPlan::Join { .. } => "Join",
            LogicalPlan::Sort { .. } => "Sort",
            LogicalPlan::Limit { .. } => "Limit",
            LogicalPlan::Distinct { .. } => "Distinct",
            LogicalPlan::SubqueryAlias { .. } => "SubqueryAlias",
            LogicalPlan::Explain { .. } => "Explain",
            LogicalPlan::SetOp { .. } => "SetOp",
            LogicalPlan::Sample { .. } => "Sample",
            LogicalPlan::Assert { .. } => "Assert",
            LogicalPlan::Pivot { .. } => "Pivot",
            LogicalPlan::Unpivot { .. } => "Unpivot",
            LogicalPlan::Fork { .. } => "Fork",
            LogicalPlan::Tee { .. } => "Tee",
            LogicalPlan::SemanticFilter { .. } => "SemanticFilter",
            LogicalPlan::SemanticJoin { .. } => "SemanticJoin",
            LogicalPlan::SemanticCalibrate { .. } => "SemanticCalibrate",
            LogicalPlan::Write { .. } => "Write",
            LogicalPlan::Call { .. } => "Call",
        }
    }

    /// Returns the table name if this plan node is a table `Scan`.
    pub fn table_name(&self) -> Option<&str> {
        match self {
            LogicalPlan::Scan { table, .. } => Some(&table.name),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
