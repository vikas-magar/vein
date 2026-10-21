//! Typed expression nodes independent of parser and logical-plan crates.

use serde::{Deserialize, Serialize};
use types::{LogicalLiteral, VeinDataType};

/// Scalar binary operation supported by the physical expression executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryExprOp {
    /// Add numeric operands.
    Add,
    /// Subtract the right operand from the left operand.
    Subtract,
    /// Multiply numeric operands.
    Multiply,
    /// Divide the left operand by the right operand.
    Divide,
    /// Compute the remainder of numeric division.
    Modulo,
    /// Concatenate string operands.
    Concat,
    /// Test equality.
    Equal,
    /// Test inequality.
    NotEqual,
    /// Test whether the left operand is less than the right operand.
    Less,
    /// Test whether the left operand is less than or equal to the right operand.
    LessEqual,
    /// Test whether the left operand is greater than the right operand.
    Greater,
    /// Test whether the left operand is greater than or equal to the right operand.
    GreaterEqual,
    /// Apply SQL three-valued logical AND.
    And,
    /// Apply SQL three-valued logical OR.
    Or,
}

/// Scalar unary operation supported by the physical expression executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryExprOp {
    /// Apply SQL three-valued logical NOT.
    Not,
    /// Negate a numeric operand.
    Negate,
    /// Test whether an operand is NULL.
    IsNull,
    /// Test whether an operand is not NULL.
    IsNotNull,
}

/// A fully typed scalar expression with input columns addressed by batch ordinal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhysicalExpr {
    /// Result type chosen by binding and type coercion.
    pub return_type: VeinDataType,
    /// The scalar operation represented by this node.
    pub kind: ExprKind,
}

/// The operation and child expressions of a physical scalar expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExprKind {
    /// Read an existing column from the current input view.
    Column {
        /// Zero-based column ordinal in the current input view.
        index: u32,
    },
    /// Produce a constant value for each active input row.
    Literal(LogicalLiteral),
    /// Apply a binary scalar operation.
    Binary {
        /// Binary operation to apply.
        op: BinaryExprOp,
        /// Left operand.
        left: Box<PhysicalExpr>,
        /// Right operand.
        right: Box<PhysicalExpr>,
    },
    /// Cast a child expression to a bound target type.
    Cast {
        /// Expression to convert.
        expr: Box<PhysicalExpr>,
        /// Bound target type.
        target_type: VeinDataType,
    },
    /// Apply a unary scalar operation.
    Unary {
        /// Unary operation to apply.
        op: UnaryExprOp,
        /// Operand to evaluate.
        expr: Box<PhysicalExpr>,
    },
    /// Test membership in an expression list.
    InList {
        /// Value tested for membership.
        expr: Box<PhysicalExpr>,
        /// Candidate values.
        candidates: Vec<PhysicalExpr>,
        /// Whether to invert the membership result.
        negated: bool,
    },
    /// Match a string expression against a SQL LIKE pattern.
    Like {
        /// String expression tested by the pattern.
        expr: Box<PhysicalExpr>,
        /// Pattern expression.
        pattern: Box<PhysicalExpr>,
        /// Whether to invert the pattern result.
        negated: bool,
    },
    /// Searched CASE arms, evaluated in source order.
    Case {
        /// Ordered WHEN and THEN expression pairs.
        when_then: Vec<(PhysicalExpr, PhysicalExpr)>,
        /// Optional ELSE expression.
        else_expr: Option<Box<PhysicalExpr>>,
    },
    /// Invoke a registered scalar function.
    Call {
        /// Registered function name.
        name: String,
        /// Function arguments in call order.
        args: Vec<PhysicalExpr>,
    },
}
