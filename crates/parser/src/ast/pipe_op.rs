use crate::ast::expr::Expr;
use crate::ast::query::{Query, SetOpKind};
use crate::logical_plan::JoinType;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PipeOperator {
    Select { items: Vec<SelectItem> },
    Where { predicate: Expr },
    Aggregate { group_by: Vec<Expr>, aggregates: Vec<AggregateExpr> },
    OrderBy { order_bys: Vec<OrderByExpr> },
    Limit { limit: u64, offset: u64 },
    Extend { items: Vec<SelectItem> },
    Set { assignments: Vec<(String, Expr)> },
    Drop { columns: Vec<String> },
    Rename { renames: Vec<(String, String)> },
    As { alias: String },
    Distinct,
    Join { join: JoinClause },
    Call { name: String, args: Vec<Expr> },
    TableSample { method: String, percent: f64 },
    Assert { condition: String, message: Option<String> },
    SetOp { kind: SetOpKind, right: Box<Query>, all: bool },
    Pivot,
    Unpivot,
    With { ctes: Vec<CommonTableExpr> },
    Write { target: WriteTarget },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateExpr {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowFrame {
    pub kind: WindowFrameKind,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowFrameKind {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub right: Box<Query>,
    pub predicate: Option<Expr>,
    pub using: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WriteTarget {
    pub table: String,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CommonTableExpr {
    pub alias: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<Query>,
}
