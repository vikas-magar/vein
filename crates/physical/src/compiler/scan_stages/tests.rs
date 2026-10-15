use super::*;
use std::sync::Arc;
use plan::{LogicalLiteral, ResolvedColumn, VeinDataType};
use parser::ast::expr::BinaryOp;

fn column(id: usize) -> LogicalExpr {
    LogicalExpr::Column(Arc::new(ResolvedColumn::new(
        ColumnId(id), format!("column_{id}"), None, VeinDataType::Int64,
    )))
}

fn compare(id: usize, op: BinaryOp, value: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(column(id)),
        op,
        right: Box::new(value),
        return_type: VeinDataType::Boolean,
    }
}

fn bound(id: usize) -> LogicalExpr {
    compare(id, BinaryOp::Gt, LogicalExpr::Literal(LogicalLiteral::Int64(0)))
}

#[test]
fn separates_filter_columns_and_keeps_same_column_bounds_together() {
    let lower = bound(2);
    let upper = compare(2, BinaryOp::Lt, LogicalExpr::Literal(LogicalLiteral::Int64(10)));
    let predicates = vec![bound(1), lower.clone(), upper.clone(), bound(3)];
    let stages = group_predicates(&predicates, |_| None);
    assert_eq!(stages, vec![vec![bound(1)], vec![lower, upper], vec![bound(3)]]);
    assert_eq!(stages.iter().map(Vec::len).sum::<usize>(), predicates.len());
}

#[test]
fn orders_by_estimated_rejection_cost_and_defers_correlations() {
    let correlated = compare(1, BinaryOp::Lt, column(2));
    let stages = group_predicates(&[correlated.clone(), bound(1), bound(2)], |expr| {
        let LogicalExpr::BinaryOp { left, .. } = expr else { return None; };
        let LogicalExpr::Column(column) = left.as_ref() else { return None; };
        Some(if column.id == ColumnId(2) { 0.1 } else { 0.9 })
    });
    assert_eq!(stages, vec![vec![bound(2)], vec![bound(1), correlated]]);
}

#[test]
fn unknown_expression_preserves_original_fused_order() {
    let unknown = LogicalExpr::Like {
        expr: Box::new(column(3)),
        pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String("%x".into()))),
        negated: false,
    };
    let predicates = vec![bound(2), unknown, bound(1)];
    assert_eq!(group_predicates(&predicates, |_| panic!("unsafe stages must not be ranked")),
               vec![predicates]);
}

#[test]
fn invalid_estimates_and_unknown_statistics_keep_stable_order() {
    let predicates = vec![bound(3), bound(2), bound(1)];
    for estimate in [None, Some(f64::NAN), Some(-1.0), Some(2.0), Some(1.0)] {
        assert_eq!(group_predicates(&predicates, |_| estimate),
                   predicates.iter().cloned().map(|p| vec![p]).collect::<Vec<_>>());
    }
}

#[test]
fn disjunction_remains_one_predicate_and_null_tests_are_staged() {
    let either = LogicalExpr::BinaryOp {
        left: Box::new(bound(1)),
        op: BinaryOp::Or,
        right: Box::new(bound(2)),
        return_type: VeinDataType::Boolean,
    };
    let null = LogicalExpr::IsNull { expr: Box::new(column(3)) };
    let stages = group_predicates(&[either.clone(), null.clone()], |_| None);
    assert_eq!(stages, vec![vec![either], vec![null]]);
}

#[test]
fn ready_correlations_reject_rows_before_decoding_unrelated_columns() {
    let correlation = compare(1, BinaryOp::Lt, column(2));
    let stages = group_predicates(
        &[bound(1), bound(2), bound(3), correlation.clone()], |_| None);
    assert_eq!(stages, vec![vec![bound(1)], vec![bound(2), correlation], vec![bound(3)]]);
}
