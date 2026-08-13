use crate::schema::{ColumnId, ResolvedColumn, VeinSchema};
use crate::expr::LogicalExpr;

/// The type of join to perform
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    /// Inner join
    Inner,
    /// Left outer join
    Left,
    /// Right outer join
    Right,
    /// Full outer join
    Full,
    /// Cartesian cross join
    Cross,
}

/// The kind of set operation
#[derive(Debug, Clone, PartialEq)]
pub enum SetOpKind {
    /// Union of two sets
    Union,
    /// Intersection of two sets
    Intersect,
    /// Difference of two sets
    Except,
}

/// A specific version or state of a table for Time Travel
#[derive(Debug, Clone, PartialEq)]
pub enum TableVersion {
    /// Unix timestamp in milliseconds
    Timestamp(i64),
    /// A named branch in the VCS
    Branch(String),
    /// A specific commit hash in the VCS
    Commit(String),
}

/// The network partitioning scheme for an Exchange node
#[derive(Debug, Clone, PartialEq)]
pub enum Partitioning {
    /// Hash partition by a set of expressions
    Hash(Vec<LogicalExpr>),
    /// Round robin distribution
    RoundRobin,
    /// Single partition (gather all to one node)
    Single,
}

/// Defines sorting order for a specific key
#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    /// The expression to sort by
    pub expr: LogicalExpr,
    /// Whether to sort in ascending order
    pub asc: bool,
    /// Whether nulls appear first
    pub nulls_first: bool,
}

/// A node in the logical query plan tree
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    // ── Sources ─────────────────────────────────────────────
    /// A table scan
    Scan {
        /// The name of the table
        table_name: String,
        /// The optional version for time travel
        version: Option<TableVersion>,
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
    /// A join operation
    Join {
        /// The left input plan
        left: Box<LogicalPlan>,
        /// The right input plan
        right: Box<LogicalPlan>,
        /// The equi-join conditions
        equi: Vec<(ResolvedColumn, ResolvedColumn)>,
        /// The join type
        join_type: JoinType,
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

    // ── Distributed Topology ────────────────────────────────
    /// A network exchange operation (Shuffle)
    Exchange {
        /// The input plan
        input: Box<LogicalPlan>,
        /// The partitioning scheme
        partition_scheme: Partitioning,
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
}

impl LogicalPlan {
    /// Returns the schema of the plan node
    pub fn schema(&self) -> &VeinSchema {
        todo!()
    }

    /// Returns a list of child plans for traversal
    pub fn children(&self) -> Vec<&LogicalPlan> {
        todo!()
    }

    /// Maps children using a mutable function
    pub fn map_children<F>(&self, _f: &mut F) -> Result<LogicalPlan, error::VeinError>
    where
        F: FnMut(Box<LogicalPlan>) -> Result<Box<LogicalPlan>, error::VeinError>,
    {
        todo!()
    }

    /// Checks if the plan contains semantic nodes (which blocks relational sharing rules)
    pub fn contains_semantic(&self) -> bool {
        todo!()
    }
}
