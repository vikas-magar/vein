use crate::ast::data_type::DataType;
use crate::ast::query::Query;
use crate::token::Span;
mod literal;
pub use literal::Literal;


/// An expression node together with the source span it covers.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Spanned<T> {
    /// The expression node.
    pub node: T,
    /// The source span covering the expression.
    pub span: Span,
}

/// A spanned expression.
pub type SpannedExpr<'a> = Spanned<Expr<'a>>;

/// An expression in the query AST.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr<'a> {
    /// A column reference, optionally qualified by a table name.
    Column {
        /// The qualifying table name.
        #[serde(borrow)]
        table: Option<&'a str>,
        /// The column name.
        #[serde(borrow)]
        column: &'a str,
    },
    /// A `*` or `table.*` wildcard.
    Wildcard {
        /// The optional qualifying table name.
        #[serde(borrow)]
        table: Option<&'a str>,
    },
    /// A literal value.
    Literal(Literal),
    /// A binary operation.
    BinaryOp {
        /// The left operand.
        #[serde(borrow)]
        left: Box<SpannedExpr<'a>>,
        /// The operator.
        op: BinaryOp,
        /// The right operand.
        #[serde(borrow)]
        right: Box<SpannedExpr<'a>>,
    },
    /// A unary operation.
    UnaryOp {
        /// The operator.
        op: UnaryOp,
        /// The operand.
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
    },
    /// A function call.
    Function {
        /// The function name.
        #[serde(borrow)]
        name: &'a str,
        /// The arguments.
        #[serde(borrow)]
        args: Vec<SpannedExpr<'a>>,
        /// Whether `DISTINCT` was specified.
        distinct: bool,
    },
    /// A window function call with `OVER (...)`.
    WindowFunction {
        /// The function name.
        #[serde(borrow)]
        name: &'a str,
        /// The arguments.
        #[serde(borrow)]
        args: Vec<SpannedExpr<'a>>,
        /// Whether `DISTINCT` was specified.
        distinct: bool,
        /// The `PARTITION BY` expressions.
        #[serde(borrow)]
        partition_by: Vec<SpannedExpr<'a>>,
        /// The `ORDER BY` keys.
        #[serde(borrow)]
        order_by: Vec<super::pipe_op::OrderByExpr<'a>>,
        /// The optional `ROWS`/`RANGE` window frame.
        window_frame: Option<super::pipe_op::WindowFrame>,
    },
    /// A `CASE WHEN ... THEN ... ELSE ... END` expression.
    Case {
        /// The `WHEN` conditions.
        #[serde(borrow)]
        operands: Vec<SpannedExpr<'a>>,
        /// The `THEN` results.
        #[serde(borrow)]
        results: Vec<SpannedExpr<'a>>,
        /// The optional `ELSE` result.
        #[serde(borrow)]
        else_result: Option<Box<SpannedExpr<'a>>>,
    },
    /// A `CAST(expr AS type)` expression.
    Cast {
        /// The expression to cast.
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        /// The target data type.
        #[serde(borrow)]
        data_type: DataType<'a>,
    },
    /// A scalar subquery.
    #[serde(borrow)]
    Subquery(Box<Query<'a>>),
    /// An aliased expression (`expr AS name`).
    Alias {
        /// The aliased expression.
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        /// The alias name.
        #[serde(borrow)]
        alias: &'a str,
    },
    /// A `BETWEEN` comparison.
    Between {
        /// The tested expression.
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        /// The lower bound.
        #[serde(borrow)]
        low: Box<SpannedExpr<'a>>,
        /// The upper bound.
        #[serde(borrow)]
        high: Box<SpannedExpr<'a>>,
        /// Whether the comparison is negated (`NOT BETWEEN`).
        negated: bool,
    },
    /// An `IN (list)` comparison.
    InList {
        /// The tested expression.
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        /// The list of candidates.
        #[serde(borrow)]
        list: Vec<SpannedExpr<'a>>,
        /// Whether the comparison is negated (`NOT IN`).
        negated: bool,
    },
    /// A `LIKE` pattern match.
    Like {
        /// The tested expression.
        #[serde(borrow)]
        expr: Box<SpannedExpr<'a>>,
        /// The pattern expression.
        #[serde(borrow)]
        pattern: Box<SpannedExpr<'a>>,
        /// Whether the match is negated (`NOT LIKE`).
        negated: bool,
    },
    /// An `EXISTS` predicate.
    Exists {
        /// The tested subquery.
        #[serde(borrow)]
        subquery: Box<Query<'a>>,
        /// Whether the predicate is negated (`NOT EXISTS`).
        negated: bool,
    },
    /// A scalar subquery used as an operand.
    #[serde(borrow)]
    ScalarSubquery(Box<Query<'a>>),
    /// A typed string literal (`DATE '2024-01-01'`).
    TypedString {
        /// The literal's type.
        #[serde(borrow)]
        data_type: DataType<'a>,
        /// The literal value.
        #[serde(borrow)]
        value: &'a str,
    },
}

mod operator;
pub use operator::{BinaryOp, UnaryOp};
