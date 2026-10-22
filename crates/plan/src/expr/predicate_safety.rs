//! Conservative expression safety checks shared by logical and physical planning.

use super::logical::LogicalExpr;
use crate::schema::VeinDataType;
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
        Expr::Cast { expr, target_type } => is_scalar_cast(expr, target_type, depth + 1),
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
            | BinaryOp::Geq => is_scalar(left, depth + 1) && is_scalar(right, depth + 1),
            _ => false,
        },
        _ => false,
    }
}

fn is_scalar(expression: &LogicalExpr, depth: usize) -> bool {
    if depth >= MAX_REORDER_SAFETY_DEPTH {
        return false;
    }

    match expression {
        LogicalExpr::Column(_) | LogicalExpr::Literal(_) => true,
        LogicalExpr::Cast { expr, target_type } => {
            is_scalar_cast(expr, target_type, depth + 1)
        }
        _ => false,
    }
}

fn is_scalar_cast(expression: &LogicalExpr, target: &VeinDataType, depth: usize) -> bool {
    is_scalar(expression, depth) && is_lossless_widening(&expression.data_type(), target)
}

/// Whether coercion preserves every source value and cannot raise a cast error.
fn is_lossless_widening(source: &VeinDataType, target: &VeinDataType) -> bool {
    use VeinDataType as Type;

    match (source, target) {
        (source, target) if source == target => true,
        (Type::Int8, Type::Int16 | Type::Int32 | Type::Int64)
        | (Type::Int16, Type::Int32 | Type::Int64)
        | (Type::Int32, Type::Int64)
        | (Type::UInt8, Type::UInt16 | Type::UInt32 | Type::UInt64)
        | (Type::UInt16, Type::UInt32 | Type::UInt64)
        | (Type::UInt32, Type::UInt64)
        | (Type::UInt8, Type::Int16 | Type::Int32 | Type::Int64)
        | (Type::UInt16, Type::Int32 | Type::Int64)
        | (Type::UInt32, Type::Int64)
        | (Type::Date32, Type::Date64) => true,
        (Type::Decimal128(source_precision, source_scale), Type::Decimal128(target_precision, target_scale))
        | (Type::Decimal128(source_precision, source_scale), Type::Decimal256(target_precision, target_scale))
        | (Type::Decimal256(source_precision, source_scale), Type::Decimal256(target_precision, target_scale)) => {
            decimal_capacity(*target_precision, *target_scale)
                >= decimal_capacity(*source_precision, *source_scale)
                && target_scale >= source_scale
        }
        (integer, Type::Decimal128(precision, scale) | Type::Decimal256(precision, scale)) => {
            let Some(integer_digits) = integer_decimal_digits(integer) else {
                return false;
            };
            *scale >= 0 && i16::from(*precision) - i16::from(*scale) >= integer_digits
        }
        _ => false,
    }
}

/// Decimal integer-digit capacity, used to compare lossless decimal widenings.
fn decimal_capacity(precision: u8, scale: i8) -> i16 {
    i16::from(precision) - i16::from(scale)
}

fn integer_decimal_digits(data_type: &VeinDataType) -> Option<i16> {
    use VeinDataType as Type;

    Some(match data_type {
        Type::Int8 | Type::UInt8 => 3,
        Type::Int16 | Type::UInt16 => 5,
        Type::Int32 | Type::UInt32 => 10,
        Type::Int64 => 19,
        Type::UInt64 => 20,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::LogicalLiteral;
    use types::{ColumnId, ResolvedColumn};

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

    #[test]
    fn permits_lossless_widening_casts_in_predicates() {
        let cases = [
            (VeinDataType::Int32, VeinDataType::Int64),
            (VeinDataType::Int64, VeinDataType::Decimal128(21, 2)),
            (VeinDataType::Decimal128(15, 2), VeinDataType::Decimal256(30, 2)),
            (VeinDataType::Date32, VeinDataType::Date64),
        ];

        for (source, target) in cases {
            let cast = LogicalExpr::Cast {
                expr: Box::new(LogicalExpr::Column(std::sync::Arc::new(
                    ResolvedColumn::new(ColumnId(1), "value", None, source.clone()),
                ))),
                target_type: target.clone(),
            };
            assert!(
                cast.is_reorder_safe(),
                "expected widening cast from {source:?} to {:?}",
                target
            );
        }
    }

    #[test]
    fn keeps_narrowing_and_lossy_casts_as_reordering_barriers() {
        for (source, target) in [
            (VeinDataType::Int64, VeinDataType::Int32),
            (VeinDataType::Int32, VeinDataType::UInt32),
            (VeinDataType::Decimal128(15, 2), VeinDataType::Decimal128(15, 1)),
            (VeinDataType::Utf8, VeinDataType::Int64),
        ] {
            let cast = LogicalExpr::Cast {
                expr: Box::new(LogicalExpr::Column(std::sync::Arc::new(
                    ResolvedColumn::new(ColumnId(1), "value", None, source.clone()),
                ))),
                target_type: target.clone(),
            };
            assert!(!cast.is_reorder_safe(), "{source:?} -> {target:?}");
        }
    }
}
