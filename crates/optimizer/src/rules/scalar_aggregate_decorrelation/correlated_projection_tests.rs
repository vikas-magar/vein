use super::super::super::ScalarAggregateDecorrelationRule;
use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use parser::ast::expr::BinaryOp;
use plan::{
    ColumnId, LogicalExpr, LogicalLiteral, LogicalPlan, ResolvedColumn, ResolvedColumnRef,
    TableMetadata, VeinDataType, VeinSchema,
};
use std::sync::Arc;

fn field(id: usize, name: &str, data_type: VeinDataType) -> ResolvedColumnRef {
    Arc::new(ResolvedColumn::new(ColumnId(id), name, None, data_type))
}

fn scan(name: &str, fields: Vec<ResolvedColumnRef>) -> LogicalPlan {
    let schema = VeinSchema::new(fields);
    LogicalPlan::Scan {
        table: Arc::new(TableMetadata::new(name, Arc::new(schema.clone()))),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(schema),
    }
}

fn correlated_average(project_expression: LogicalExpr) -> LogicalPlan {
    let outer_key = field(1, "outer_key", VeinDataType::Int64);
    let outer_measure = field(2, "outer_measure", VeinDataType::Float64);
    let inner_key = field(3, "inner_key", VeinDataType::Int64);
    let inner_measure = field(4, "inner_measure", VeinDataType::Float64);
    let average = field(5, "average", VeinDataType::Float64);
    let scalar = field(6, "threshold", VeinDataType::Float64);
    let correlation = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(Arc::clone(&inner_key))),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer_key))),
        return_type: VeinDataType::Boolean,
    };
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(scan("inner", vec![inner_key, inner_measure.clone()])),
            predicate: correlation,
        }),
        grouping_sets: vec![Vec::new()],
        aggs: vec![LogicalExpr::FunctionCall {
            name: "avg".to_string(),
            args: vec![LogicalExpr::Column(inner_measure)],
            is_aggregate: true,
            distinct: false,
            return_type: VeinDataType::Float64,
        }],
        schema: Box::new(VeinSchema::new(vec![average.clone()])),
    };
    let projection = LogicalPlan::Project {
        input: Box::new(aggregate),
        exprs: vec![project_expression],
        schema: Box::new(VeinSchema::new(vec![scalar])),
    };
    LogicalPlan::Filter {
        input: Box::new(scan("outer", vec![outer_key, outer_measure.clone()])),
        predicate: LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(outer_measure)),
            op: BinaryOp::Lt,
            right: Box::new(LogicalExpr::ScalarSubquery(Box::new(projection))),
            return_type: VeinDataType::Boolean,
        },
    }
}

fn threshold_expression() -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(field(
            5,
            "average",
            VeinDataType::Float64,
        ))),
        op: BinaryOp::Multiply,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Float64(0.2))),
        return_type: VeinDataType::Float64,
    }
}

#[test]
fn decorrelates_projected_average_and_preserves_nullable_left_join_value() {
    let plan = correlated_average(threshold_expression());
    let optimized = ScalarAggregateDecorrelationRule
        .apply(&plan, &OptimizerContext::default())
        .expect("rewrite should not fail")
        .expect("correlated projection over aggregate should be decorrelated");
    let LogicalPlan::Project { input: filtered, .. } = optimized else {
        panic!("outer filter schema should be restored");
    };
    let LogicalPlan::Filter { input: joined, predicate } = filtered.as_ref() else {
        panic!("comparison should remain above the scalar join");
    };
    assert!(matches!(predicate, LogicalExpr::BinaryOp { right, .. }
        if matches!(right.as_ref(), LogicalExpr::Column(column) if column.id == ColumnId(6) && column.nullable)));
    let LogicalPlan::Join { join_type, right, .. } = joined.as_ref() else {
        panic!("correlated aggregate should become a join");
    };
    assert_eq!(*join_type, parser::ast::pipe_op::JoinType::Left);
    let LogicalPlan::Project { input: grouped, exprs, schema } = right.as_ref() else {
        panic!("scalar expression must be projected over the grouped aggregate");
    };
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].id, ColumnId(3));
    assert_eq!(schema.fields[1].id, ColumnId(6));
    assert!(schema.fields[1].nullable, "unmatched outer keys produce NULL");
    assert!(matches!(exprs.as_slice(), [LogicalExpr::Column(key), LogicalExpr::BinaryOp { .. }]
        if key.id == ColumnId(3)));
    assert!(matches!(grouped.as_ref(), LogicalPlan::Aggregate { grouping_sets, .. }
        if grouping_sets == &vec![vec![LogicalExpr::Column(field(3, "inner_key", VeinDataType::Int64))]]));
}

#[test]
fn does_not_decorrelate_volatile_projection_over_aggregate() {
    let volatile = LogicalExpr::FunctionCall {
        name: "random".to_string(),
        args: Vec::new(),
        is_aggregate: false,
        distinct: false,
        return_type: VeinDataType::Float64,
    };
    let plan = correlated_average(volatile);
    assert!(ScalarAggregateDecorrelationRule
        .apply(&plan, &OptimizerContext::default())
        .expect("unsafe projection should be left for validation")
        .is_none());
}
