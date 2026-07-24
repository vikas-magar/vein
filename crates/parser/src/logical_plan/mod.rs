pub mod builder;
pub mod error;
pub mod tree_node;

pub use builder::PlanBuilder;
pub use error::PlanError;
pub use tree_node::{Transformed, TreeNode, TreeNodeRecursion};

use crate::ast::expr::Expr;
use crate::ast::pipe_op::OrderByExpr;

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan {
        source: ScanSource,
        projection: Option<Vec<String>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    },
    Filter { predicate: Expr, input: Box<LogicalPlan> },
    Projection { expr: Vec<Expr>, input: Box<LogicalPlan> },
    Aggregate { group_expr: Vec<Expr>, aggregate_expr: Vec<Expr>, input: Box<LogicalPlan> },
    Sort { order_by: Vec<OrderByExpr>, input: Box<LogicalPlan> },
    Limit { limit: usize, offset: usize, input: Box<LogicalPlan> },
    Distinct { input: Box<LogicalPlan> },
    Join { join_type: JoinType, left: Box<LogicalPlan>, right: Box<LogicalPlan>, predicate: Option<Expr>, using: Vec<String> },
    TableAlias { alias: String, input: Box<LogicalPlan> },
    SubqueryAlias { alias: String, input: Box<LogicalPlan> },
    SetOp { kind: SetOpType, all: bool, left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    Extend { items: Vec<Expr>, input: Box<LogicalPlan> },
    Drop { columns: Vec<String>, input: Box<LogicalPlan> },
    Assert { condition: String, message: Option<String>, input: Box<LogicalPlan> },
    Call { name: String, args: Vec<Expr> },
    Write { target: crate::ast::pipe_op::WriteTarget, input: Box<LogicalPlan> },
    With { ctes: Vec<crate::ast::pipe_op::CommonTableExpr>, input: Box<LogicalPlan> },
    Empty,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScanSource {
    Table(String),
    ObjectStore { url: String, options: Vec<(String, String)> },
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}
