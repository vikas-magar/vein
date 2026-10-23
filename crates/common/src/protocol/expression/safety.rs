//! Conservative safety proofs used by physical predicate movement.
use super::{BinaryExprOp, ExprKind, PhysicalExpr, UnaryExprOp};

impl PhysicalExpr {
    /// Whether changing this expression's evaluation domain is known to be safe.
    /// Opaque calls, casts and arithmetic retain their original evaluation order.
    pub fn is_reorder_safe(&self) -> bool {
        let mut pending = vec![self];
        while let Some(expr) = pending.pop() {
            match &expr.kind {
                ExprKind::Column { .. } | ExprKind::Literal(_) => {}
                ExprKind::Binary { op, left, right } if matches!(op,
                    BinaryExprOp::Equal | BinaryExprOp::NotEqual | BinaryExprOp::Less
                    | BinaryExprOp::LessEqual | BinaryExprOp::Greater | BinaryExprOp::GreaterEqual
                    | BinaryExprOp::And | BinaryExprOp::Or) => pending.extend([left.as_ref(), right.as_ref()]),
                ExprKind::Unary { op, expr } if matches!(op,
                    UnaryExprOp::Not | UnaryExprOp::IsNull | UnaryExprOp::IsNotNull) => pending.push(expr),
                ExprKind::InList { expr, candidates, .. } => {
                    pending.push(expr);
                    pending.extend(candidates);
                }
                _ => return false,
            }
        }
        true
    }
}
