use crate::ast::expr::SpannedExpr;
use crate::ast::query::{Query, SetOpKind};
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PipeOperator<'a> {
    Select {
        #[serde(borrow)]
        items: Vec<SelectItem<'a>>,
    },
    Where {
        #[serde(borrow)]
        predicate: SpannedExpr<'a>,
    },
    Aggregate {
        #[serde(borrow)]
        group_by: Vec<OrderByExpr<'a>>,
        #[serde(borrow)]
        aggregates: Vec<AggregateExpr<'a>>,
    },
    OrderBy {
        #[serde(borrow)]
        order_bys: Vec<OrderByExpr<'a>>,
    },
    Limit { limit: u64, offset: u64 },
    Extend {
        #[serde(borrow)]
        items: Vec<SelectItem<'a>>,
    },
    Set {
        #[serde(borrow)]
        assignments: Vec<(&'a str, SpannedExpr<'a>)>,
    },
    Drop {
        #[serde(borrow)]
        columns: Vec<&'a str>,
    },
    Rename {
        #[serde(borrow)]
        renames: Vec<(&'a str, &'a str)>,
    },
    As {
        #[serde(borrow)]
        alias: &'a str,
    },
    Distinct,
    Join {
        #[serde(borrow)]
        join: JoinClause<'a>,
    },
    Call {
        #[serde(borrow)]
        name: &'a str,
        #[serde(borrow)]
        args: Vec<SpannedExpr<'a>>,
    },
    TableSample {
        #[serde(borrow)]
        method: &'a str,
        percent: f64,
    },
    Assert {
        #[serde(borrow)]
        condition: &'a str,
        #[serde(borrow)]
        message: Option<&'a str>,
    },
    SetOp {
        kind: SetOpKind,
        #[serde(borrow)]
        right: Box<Query<'a>>,
        all: bool,
    },
    Pivot,
    Unpivot,
    With {
        #[serde(borrow)]
        ctes: Vec<CommonTableExpr<'a>>,
    },
    Write {
        #[serde(borrow)]
        target: WriteTarget<'a>,
    },
    Fork {
        #[serde(borrow)]
        pipelines: Vec<Vec<PipeOperator<'a>>>,
    },
    Tee {
        #[serde(borrow)]
        pipelines: Vec<Vec<PipeOperator<'a>>>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SelectItem<'a> {
    #[serde(borrow)]
    pub expr: SpannedExpr<'a>,
    #[serde(borrow)]
    pub alias: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateExpr<'a> {
    #[serde(borrow)]
    pub expr: SpannedExpr<'a>,
    #[serde(borrow)]
    pub alias: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderByExpr<'a> {
    #[serde(borrow)]
    pub expr: SpannedExpr<'a>,
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
pub struct JoinClause<'a> {
    pub join_type: JoinType,
    #[serde(borrow)]
    pub right: Box<Query<'a>>,
    #[serde(borrow)]
    pub predicate: Option<SpannedExpr<'a>>,
    #[serde(borrow)]
    pub using: Vec<&'a str>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WriteTarget<'a> {
    #[serde(borrow)]
    pub table: &'a str,
    #[serde(borrow)]
    pub options: Vec<(&'a str, &'a str)>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CommonTableExpr<'a> {
    #[serde(borrow)]
    pub alias: &'a str,
    #[serde(borrow)]
    pub columns: Option<Vec<&'a str>>,
    #[serde(borrow)]
    pub query: Box<Query<'a>>,
}
