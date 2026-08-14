use ordered_float::OrderedFloat;
use crate::ast::data_type::DataType;
use crate::ast::query::Query;
use crate::token::Span;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

pub type SpannedExpr<'a> = Spanned<Expr<'a>>;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr<'a> {
    Column {
        #[serde(borrow)]
        table: Option<&'a str>,
        #[serde(borrow)]
        column: &'a str,
    },
    Wildcard {
        #[serde(borrow)]
        table: Option<&'a str>,
    },
    #[serde(borrow)]
    Literal(Literal<'a>),
    BinaryOp {
        #[serde(borrow)]
        left: Box<SpannedExpr<'a>>,
        op: BinaryOp,
        #[serde(borrow)]
        right: Box<SpannedExpr<'a>>,
    },
    UnaryOp {
        op: UnaryOp,
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
    },
    Function {
        #[serde(borrow)]
        name: &'a str,
        #[serde(borrow)]
        args: Vec<SpannedExpr<'a>>,
        distinct: bool,
    },
    WindowFunction {
        #[serde(borrow)]
        name: &'a str,
        #[serde(borrow)]
        args: Vec<SpannedExpr<'a>>,
        distinct: bool,
        #[serde(borrow)]
        partition_by: Vec<SpannedExpr<'a>>,
        #[serde(borrow)]
        order_by: Vec<super::pipe_op::OrderByExpr<'a>>,
        window_frame: Option<super::pipe_op::WindowFrame>,
    },
    Case {
        #[serde(borrow)]
        operands: Vec<SpannedExpr<'a>>,
        #[serde(borrow)]
        results: Vec<SpannedExpr<'a>>,
        #[serde(borrow)]
        else_result: Option<Box<SpannedExpr<'a>>>,
    },
    Cast {
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        #[serde(borrow)]
        data_type: DataType<'a>,
    },
    #[serde(borrow)]
    Subquery(Box<Query<'a>>),
    Alias {
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        #[serde(borrow)]
        alias: &'a str,
    },
    Between {
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        #[serde(borrow)]
        low: Box<SpannedExpr<'a>>,
        #[serde(borrow)]
        high: Box<SpannedExpr<'a>>,
        negated: bool,
    },
    InList {
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        #[serde(borrow)]
        list: Vec<SpannedExpr<'a>>,
        negated: bool,
    },
    Like {
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        #[serde(borrow)]
        pattern: Box<SpannedExpr<'a>>,
        negated: bool,
    },
    Exists {
        #[serde(borrow)]
        subquery: Box<Query<'a>>,
        negated: bool,
    },
    #[serde(borrow)]
    ScalarSubquery(Box<Query<'a>>),
    TypedString {
        #[serde(borrow)]
        data_type: DataType<'a>,
        #[serde(borrow)]
        value: &'a str,
    },
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
pub enum Literal<'a> {
    Null,
    Boolean(bool),
    Int(i64),
    Float(OrderedFloat<f64>),
    #[serde(borrow)]
    String(&'a str),
}

impl<'a> PartialEq for Literal<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Null, Literal::Null) => true,
            (Literal::Boolean(a), Literal::Boolean(b)) => a == b,
            (Literal::Int(a), Literal::Int(b)) => a == b,
            (Literal::Float(a), Literal::Float(b)) => a.0.is_nan() && b.0.is_nan() || a.0 == b.0,
            (Literal::String(a), Literal::String(b)) => a == b,
            _ => false,
        }
    }
}
