//! Conservative expression safety checks shared by logical and physical planning.

use super::logical::LogicalExpr;
use parser::ast::expr::BinaryOp;

const MAX_REORDER_SAFETY_DEPTH: usize = 100;

impl LogicalExpr {
    /// Returns whether this expression can safely move across another filter.
    ///
    /// Only bounded built-in predicates over columns and literals are accepted.
    /// Arithmetic, casts, functions, UDFs, and subqueries stay in their original
    /// evaluation order because they can fail or carry non-local behavior.
    pub fn is_reorder_safe(&self) -> bool {
        is_reorder_safe_at_depth(self, 0)
    }
}

fn is_reorder_safe_at_depth(expression: &LogicalExpr, depth: usize) -> bool {
    use LogicalExpr as Expr;

    if depth >= MAX_REORDER_SAFETY_DEPTH {
        return false;
    }

    match expression {
        Expr::Column(_) | Expr::Literal(_) => true,
        Expr::Not { expr } => is_reorder_safe_at_depth(expr, depth + 1),
        Expr::IsNull { expr } | Expr::IsNotNull { expr } => {
            matches!(expr.as_ref(), Expr::Column(_) | Expr::Literal(_))
        }
        Expr::InList { expr, list, .. } => {
            matches!(expr.as_ref(), Expr::Column(_))
                && list.iter().all(|value| matches!(value, Expr::Literal(_)))
        }
        Expr::Like { expr, pattern, .. } => {
            matches!(expr.as_ref(), Expr::Column(_))
                && matches!(pattern.as_ref(), Expr::Literal(_))
        }
        Expr::BinaryOp { left, op, right, .. } => match op {
            BinaryOp::And | BinaryOp::Or => {
                is_reorder_safe_at_depth(left, depth + 1)
                    && is_reorder_safe_at_depth(right, depth + 1)
            }
            BinaryOp::Eq
            | BinaryOp::Neq
            | BinaryOp::Lt
            | BinaryOp::Leq
            | BinaryOp::Gt
            | BinaryOp::Geq => is_scalar(left) && is_scalar(right),
            _ => false,
        },
        _ => false,
    }
}

fn is_scalar(expression: &LogicalExpr) -> bool {
    matches!(expression, LogicalExpr::Column(_) | LogicalExpr::Literal(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::LogicalLiteral;
    use crate::schema::VeinDataType;

    fn int(value: i64) -> LogicalExpr {
        LogicalExpr::Literal(LogicalLiteral::Int64(value))
    }

    fn comparison(op: BinaryOp) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(int(1)),
            op,
            right: Box::new(int(2)),
            return_type: VeinDataType::Boolean,
        }
    }

    #[test]
    fn allows_bounded_comparisons_and_boolean_composition() {
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(comparison(BinaryOp::Geq)),
            op: BinaryOp::And,
            right: Box::new(comparison(BinaryOp::Leq)),
            return_type: VeinDataType::Boolean,
        };

        assert!(predicate.is_reorder_safe());
    }

    #[test]
    fn rejects_arithmetic_and_excessive_nesting() {
        let arithmetic = LogicalExpr::BinaryOp {
            left: Box::new(int(1)),
            op: BinaryOp::Plus,
            right: Box::new(int(2)),
            return_type: VeinDataType::Int64,
        };
        assert!(!arithmetic.is_reorder_safe());

        let nested = (0..MAX_REORDER_SAFETY_DEPTH)
            .fold(int(1), |expr, _| LogicalExpr::Not { expr: Box::new(expr) });
        assert!(!nested.is_reorder_safe());
    }
}
