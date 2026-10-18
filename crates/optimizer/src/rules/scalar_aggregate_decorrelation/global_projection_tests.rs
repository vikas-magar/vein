use super::super::ScalarAggregateDecorrelationRule;
use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr, LogicalLiteral, LogicalPlan, ResolvedColumn, ResolvedColumnRef, VeinDataType, VeinSchema};
use std::sync::Arc;
use super::super::tests::scan;

fn decimal_field(id: usize, name: &str, nullable: bool) -> ResolvedColumnRef {
    Arc::new(
        ResolvedColumn::new(
            ColumnId(id),
            name,
            None,
            VeinDataType::Decimal128(15, 2),
        )
            .with_nullable(nullable),
    )
}

fn normalized_scalar_filter(empty_inner: bool) -> (LogicalPlan, ResolvedColumnRef, ResolvedColumnRef) {
    let outer = decimal_field(1, "outer_value", false);
    let measure = decimal_field(2, "measure", false);
    let aggregate_value = decimal_field(3, "aggregate_value", true);
    let scalar_value = Arc::new(
        ResolvedColumn::new(
            ColumnId(4),
            "scalar_value",
            None,
            VeinDataType::Decimal128(15, 6),
        )
            .with_nullable(true),
    );
    let mut inner_input = scan("inner_fixture", vec![Arc::clone(&measure)]);
    if empty_inner {
        inner_input = LogicalPlan::Filter {
            input: Box::new(inner_input),
            predicate: LogicalExpr::Literal(LogicalLiteral::Boolean(false)),
        };
    }
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(inner_input),
        grouping_sets: vec![Vec::new()],
        aggs: vec![LogicalExpr::FunctionCall {
            name: "sum".to_string(),
            args: vec![LogicalExpr::Column(Arc::clone(&measure))],
            is_aggregate: true,
            distinct: false,
            return_type: aggregate_value.data_type.clone(),
        }],
        schema: Box::new(VeinSchema::new(vec![Arc::clone(&aggregate_value)])),
    };
    let scalar_projection = LogicalPlan::Project {
        input: Box::new(aggregate),
        exprs: vec![LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(aggregate_value)),
            op: BinaryOp::Multiply,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Decimal(1, 1, 4))),
            return_type: scalar_value.data_type.clone(),
        }],
        schema: Box::new(VeinSchema::new(vec![Arc::clone(&scalar_value)])),
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(scan("outer_fixture", vec![Arc::clone(&outer)])),
        predicate: LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(Arc::clone(&outer))),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::ScalarSubquery(Box::new(scalar_projection))),
            return_type: VeinDataType::Boolean,
        },
    };
    (filter, outer, scalar_value)
}

fn assert_cross_join_preserves_global_projection(
    optimized: LogicalPlan,
    outer: &ResolvedColumnRef,
    scalar: &ResolvedColumnRef,
    expect_empty_input: bool,
) {
    let LogicalPlan::Project { input: filtered, schema, .. } = optimized else {
        panic!("optimizer must restore the outer filter schema");
    };
    assert_eq!(schema.fields.as_slice(), [Arc::clone(outer)]);
    let LogicalPlan::Filter { input: joined, predicate } = filtered.as_ref() else {
        panic!("the scalar comparison must remain a filter");
    };
    assert!(matches!(predicate, LogicalExpr::BinaryOp { right, .. }
        if matches!(right.as_ref(), LogicalExpr::Column(column) if column.id == scalar.id)));
    let LogicalPlan::Join { join_type, right, .. } = joined.as_ref() else {
        panic!("uncorrelated scalar aggregate must be attached relationally");
    };
    assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Cross);
    assert!(matches!(right.as_ref(), LogicalPlan::Project { input, schema, .. }
        if schema.fields.len() == 1 && schema.fields[0].id == scalar.id && schema.fields[0].nullable
            && matches!(input.as_ref(), LogicalPlan::Aggregate { grouping_sets, input, .. }
                if grouping_sets == &vec![Vec::new()]
                    && (!expect_empty_input || matches!(input.as_ref(), LogicalPlan::Filter { .. })))));
}

#[test]
fn lowers_binder_normalized_decimal_aggregate_projection_in_filter() {
    let (filter, outer, scalar) = normalized_scalar_filter(false);
    let optimized = ScalarAggregateDecorrelationRule
        .apply(&filter, &OptimizerContext::default())
        .expect("scalar rewrite must not fail")
        .expect("normalized global aggregate projection must be lowered");
    assert_cross_join_preserves_global_projection(optimized, &outer, &scalar, false);
}

#[test]
fn preserves_empty_input_global_aggregate_row_for_scalar_semantics() {
    let (filter, outer, scalar) = normalized_scalar_filter(true);
    let optimized = ScalarAggregateDecorrelationRule
        .apply(&filter, &OptimizerContext::default())
        .expect("empty-input scalar rewrite must not fail")
        .expect("global aggregate still produces its scalar row");
    assert_cross_join_preserves_global_projection(optimized, &outer, &scalar, true);
}
