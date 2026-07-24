use ordered_float::OrderedFloat;
use crate::ast::data_type::DataType;
use crate::ast::query::Query;
use crate::token::Span;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

pub type SpannedExpr = Spanned<Expr>;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Column { table: Option<String>, column: String },
    Wildcard { table: Option<String> },
    Literal(Literal),
    BinaryOp { left: Box<SpannedExpr>, op: BinaryOp, right: Box<SpannedExpr> },
    UnaryOp { op: UnaryOp, expr: Box<SpannedExpr> },
    Function { name: String, args: Vec<SpannedExpr>, distinct: bool },
    WindowFunction {
        name: String,
        args: Vec<SpannedExpr>,
        distinct: bool,
        partition_by: Vec<SpannedExpr>,
        order_by: Vec<super::pipe_op::OrderByExpr>,
        window_frame: Option<super::pipe_op::WindowFrame>,
    },
    Case { operands: Vec<SpannedExpr>, results: Vec<SpannedExpr>, else_result: Option<Box<SpannedExpr>> },
    Cast { expr: Box<SpannedExpr>, data_type: DataType },
    Subquery(Box<Query>),
    Alias { expr: Box<SpannedExpr>, alias: String },
    Between { expr: Box<SpannedExpr>, low: Box<SpannedExpr>, high: Box<SpannedExpr>, negated: bool },
    InList { expr: Box<SpannedExpr>, list: Vec<SpannedExpr>, negated: bool },
    Like { expr: Box<SpannedExpr>, pattern: Box<SpannedExpr>, negated: bool },
    Exists { subquery: Box<Query>, negated: bool },
    ScalarSubquery(Box<Query>),
    TypedString { data_type: DataType, value: String },
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BinaryOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Concat,
    Eq,
    Neq,
    Lt,
    Gt,
    Leq,
    Geq,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum UnaryOp {
    Not,
    Negate,
    Plus,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Literal {
    Null,
    Boolean(bool),
    Int(i64),
    Float(OrderedFloat<f64>),
    String(String),
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Float(a), Literal::Float(b)) => a.0.is_nan() && b.0.is_nan() || a.0 == b.0,
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }
}
