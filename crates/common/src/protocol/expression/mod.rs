//! Typed scalar expressions carried by canonical physical plans.

mod node;
mod program;
mod safety;

pub use node::{BinaryExprOp, ExprKind, PhysicalExpr, UnaryExprOp};
pub use program::{ExpressionProgram, ExpressionStep};

#[cfg(test)]
mod tests;
