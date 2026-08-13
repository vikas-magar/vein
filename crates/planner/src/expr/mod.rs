//! Expression module containing the logical expression tree definitions and inference logic.

/// Module containing the core LogicalExpr enum and traversal methods.
pub mod logical;

/// Module containing literal definitions.
pub mod literal;

/// Module containing volatility definitions for expressions and functions.
pub mod volatility;

/// Module containing the type inference implementation for LogicalExpr.
pub mod inference;

pub use logical::LogicalExpr;
pub use literal::LogicalLiteral;
pub use volatility::Volatility;
