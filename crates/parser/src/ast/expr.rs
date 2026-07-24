use ordered_float::OrderedFloat;
use crate::ast::data_type::DataType;
use crate::ast::query::Query;
use crate::token::Span;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Column { table: Option<String>, column: String, span: Span },
    Wildcard { table: Option<String>, span: Span },
    Literal(Literal, Span),
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr>, span: Span },
    UnaryOp { op: UnaryOp, expr: Box<Expr>, span: Span },
    Function { name: String, args: Vec<Expr>, distinct: bool, span: Span },
    WindowFunction {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
        partition_by: Vec<Expr>,
        order_by: Vec<super::pipe_op::OrderByExpr>,
        window_frame: Option<super::pipe_op::WindowFrame>,
        span: Span,
    },
    Case { operands: Vec<Expr>, results: Vec<Expr>, else_result: Option<Box<Expr>>, span: Span },
    Cast { expr: Box<Expr>, data_type: DataType, span: Span },
    Subquery(Box<Query>, Span),
    Alias { expr: Box<Expr>, alias: String, span: Span },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool, span: Span },
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool, span: Span },
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool, span: Span },
    Exists { subquery: Box<Query>, negated: bool, span: Span },
    ScalarSubquery(Box<Query>, Span),
    TypedString { data_type: DataType, value: String, span: Span },
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Self::Column { span, .. } => *span,
            Self::Wildcard { span, .. } => *span,
            Self::Literal(_, span) => *span,
            Self::BinaryOp { span, .. } => *span,
            Self::UnaryOp { span, .. } => *span,
            Self::Function { span, .. } => *span,
            Self::WindowFunction { span, .. } => *span,
            Self::Case { span, .. } => *span,
            Self::Cast { span, .. } => *span,
            Self::Subquery(_, span) => *span,
            Self::Alias { span, .. } => *span,
            Self::Between { span, .. } => *span,
            Self::InList { span, .. } => *span,
            Self::Like { span, .. } => *span,
            Self::Exists { span, .. } => *span,
            Self::ScalarSubquery(_, span) => *span,
            Self::TypedString { span, .. } => *span,
        }
    }
}

impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Literal(a, _), Self::Literal(b, _)) => a == b,
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }
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
