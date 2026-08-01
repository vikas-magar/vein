use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use crate::expr::{LogicalExpr, ResolvedColumn};

/// Defines the type of join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    /// Inner join.
    Inner,
    /// Left outer join.
    Left,
    /// Right outer join.
    Right,
    /// Full outer join.
    Full,
}

/// A node in the logical query plan tree.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Reads data from a registered table or external source.
    Scan {
        /// The name of the table being scanned.
        table_name: String,
        /// Optional list of column indices to project (pushdown).
        projection: Option<Vec<usize>>,
        /// Optional list of filters applied directly during the scan (pushdown).
        filters: Vec<LogicalExpr>,
        /// The schema produced by this scan.
        schema: SchemaRef,
    },
    
    /// Filters rows based on a predicate.
    Filter {
        /// The input plan node.
        input: Arc<LogicalPlan>,
        /// The filtering predicate.
        predicate: LogicalExpr,
    },
    
    /// Computes new columns or passes existing ones through.
    Project {
        /// The input plan node.
        input: Arc<LogicalPlan>,
        /// The expressions to project.
        exprs: Vec<LogicalExpr>,
        /// The schema produced by this projection.
        schema: SchemaRef,
    },
    
    /// Joins two inputs together.
    Join {
        /// The left input.
        left: Arc<LogicalPlan>,
        /// The right input.
        right: Arc<LogicalPlan>,
        /// Equi-join conditions (left_column = right_column).
        on: Vec<(ResolvedColumn, ResolvedColumn)>,
        /// The type of join.
        join_type: JoinType,
        /// The schema produced by this join.
        schema: SchemaRef,
    },
    
    /// Aggregates data by groups.
    Aggregate {
        /// The input plan node.
        input: Arc<LogicalPlan>,
        /// The grouping expressions.
        group_by: Vec<LogicalExpr>,
        /// The aggregate expressions.
        aggs: Vec<LogicalExpr>,
        /// The schema produced by this aggregation.
        schema: SchemaRef,
    },
}

impl LogicalPlan {
    /// Returns the schema of the data produced by this logical plan node.
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { schema, .. } => schema.clone(),
            LogicalPlan::Join { schema, .. } => schema.clone(),
            LogicalPlan::Aggregate { schema, .. } => schema.clone(),
        }
    }
}
