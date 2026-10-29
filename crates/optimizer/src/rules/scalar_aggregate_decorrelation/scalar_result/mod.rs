//! Scalar expressions must observe null extension after a correlated join.
use super::{Decorrelation, ScalarJoinKind, nullable_column};
use error::Result;
use plan::{ColumnId, LogicalExpr, LogicalLiteral, ResolvedColumnRef, VeinDataType};

pub(super) fn expression(value: &Decorrelation) -> LogicalExpr {
    value.replacement.clone().unwrap_or_else(|| {
        let column = match value.join_kind {
            ScalarJoinKind::Cross => ResolvedColumnRef::clone(&value.value),
            ScalarJoinKind::Left | ScalarJoinKind::Single => nullable_column(&value.value),
        };
        LogicalExpr::Column(column)
    })
}

/// A grouped COUNT has no row for an empty correlation population.
pub(super) fn count(column: &ResolvedColumnRef) -> LogicalExpr {
    let value = LogicalExpr::Column(nullable_column(column));
    LogicalExpr::Case {
        when: vec![LogicalExpr::IsNull { expr: Box::new(value.clone()) }],
        then: vec![LogicalExpr::Literal(LogicalLiteral::Int64(0))],
        else_: Some(Box::new(value)),
        return_type: VeinDataType::Int64,
    }
}

/// Substitute the aggregate result through all deterministic scalar operators.
pub(super) fn substitute(
    expression: LogicalExpr, column: ColumnId, replacement: &LogicalExpr,
) -> Result<LogicalExpr> {
    if matches!(&expression, LogicalExpr::Column(value) if value.id == column) {
        return Ok(replacement.clone());
    }
    expression.map_children(&mut |child| substitute(child, column, replacement))
}
