use std::sync::Arc;

use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{
    ColumnId, LogicalExpr, LogicalLiteral, LogicalPlan, ResolvedColumn, VeinDataType, VeinSchema,
};

use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use crate::rules::ScalarAggregateDecorrelationRule;
use super::conjunction;

fn field(id: usize, name: &str) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn::new(
        ColumnId(id),
        name,
        None,
        VeinDataType::Int64,
    ))
}

fn relation(source_id: u128, fields: Vec<Arc<ResolvedColumn>>) -> LogicalPlan {
    LogicalPlan::Intermediate {
        source_id,
        row_count: 4,
        schema: Box::new(VeinSchema::new(fields)),
    }
}

fn equal(left: Arc<ResolvedColumn>, right: Arc<ResolvedColumn>, outer: bool) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(left)),
        op: BinaryOp::Eq,
        right: Box::new(if outer {
            LogicalExpr::OuterColumn(right)
        } else {
            LogicalExpr::Column(right)
        }),
        return_type: VeinDataType::Boolean,
    }
}

fn correlated_projected_sum(duplicate_first_key: bool) -> LogicalPlan {
    let outer_part = field(1, "ps_partkey");
    let outer_supplier = field(2, "ps_suppkey");
    let avail_qty = field(6, "ps_availqty");
    let inner_part = field(3, "l_partkey");
    let inner_supplier = field(4, "l_suppkey");
    let quantity = field(5, "l_quantity");
    let sum = field(7, "sum_quantity");
    let scaled_sum = Arc::new(ResolvedColumn::new(
        ColumnId(8),
        "half_sum",
        None,
        VeinDataType::Float64,
    ));

    let mut correlations = vec![
        equal(inner_part.clone(), outer_part.clone(), true),
        equal(inner_supplier.clone(), outer_supplier.clone(), true),
    ];
    if duplicate_first_key {
        correlations.push(equal(inner_part.clone(), outer_part.clone(), true));
    }
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(relation(
                2,
                vec![inner_part, inner_supplier, quantity.clone()],
            )),
            predicate: conjunction(correlations).expect("correlation predicates"),
        }),
        grouping_sets: vec![Vec::new()],
        aggs: vec![LogicalExpr::FunctionCall {
            name: "sum".to_string(),
            args: vec![LogicalExpr::Column(quantity)],
            is_aggregate: true,
            distinct: false,
            return_type: VeinDataType::Int64,
        }],
        schema: Box::new(VeinSchema::new(vec![sum.clone()])),
    };
    let scalar_subquery = LogicalPlan::Project {
        input: Box::new(aggregate),
        exprs: vec![LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(sum)),
            op: BinaryOp::Multiply,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Float64(0.5))),
            return_type: VeinDataType::Float64,
        }],
        schema: Box::new(VeinSchema::new(vec![scaled_sum])),
    };

    LogicalPlan::Filter {
        input: Box::new(relation(
            1,
            vec![outer_part, outer_supplier, avail_qty.clone()],
        )),
        predicate: LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(avail_qty)),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::ScalarSubquery(Box::new(scalar_subquery))),
            return_type: VeinDataType::Boolean,
        },
    }
}

fn join_equality_count(expression: &LogicalExpr) -> usize {
    match expression {
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => join_equality_count(left) + join_equality_count(right),
        LogicalExpr::BinaryOp {
            op: BinaryOp::Eq, ..
        } => 1,
        _ => 0,
    }
}

fn assert_composite_left_join(plan: LogicalPlan) {
    let LogicalPlan::Project { input: filtered, .. } = plan else {
        panic!("decorrelation should restore the outer schema");
    };
    let LogicalPlan::Filter { input: joined, .. } = filtered.as_ref() else {
        panic!("the outer scalar comparison should remain a filter");
    };
    let LogicalPlan::Join {
        join_type,
        on: Some(on),
        right,
        ..
    } = joined.as_ref()
    else {
        panic!("correlated aggregate must join on its correlation keys");
    };
    assert_eq!(*join_type, JoinType::Left);
    assert_eq!(join_equality_count(on), 2, "both key equalities must be retained");

    let LogicalPlan::Project { input: grouped, schema, .. } = right.as_ref() else {
        panic!("the scalar expression should be projected over the grouped aggregate");
    };
    assert_eq!(schema.fields.len(), 3, "two keys plus the scalar result");
    let LogicalPlan::Aggregate { grouping_sets, schema, .. } = grouped.as_ref() else {
        panic!("correlated aggregate should group before joining");
    };
    let [keys] = grouping_sets.as_slice() else {
        panic!("one composite grouping set is expected");
    };
    let key_ids: Vec<_> = keys
        .iter()
        .filter_map(|expr| match expr {
            LogicalExpr::Column(column) => Some(column.id),
            _ => None,
        })
        .collect();
    assert_eq!(key_ids, [ColumnId(3), ColumnId(4)]);
    assert_eq!(schema.fields[0].id, ColumnId(3));
    assert_eq!(schema.fields[1].id, ColumnId(4));
}

#[test]
fn decorrelates_projected_aggregate_by_all_correlation_keys() {
    let plan = correlated_projected_sum(false);
    let rewritten = ScalarAggregateDecorrelationRule
        .apply(&plan, &OptimizerContext::default())
        .expect("two-key correlated scalar aggregate is supported")
        .expect("scalar aggregate should be replaced by a join");
    assert_composite_left_join(rewritten);
}

#[test]
fn duplicate_correlation_pairs_do_not_duplicate_group_or_join_keys() {
    let plan = correlated_projected_sum(true);
    let rewritten = ScalarAggregateDecorrelationRule
        .apply(&plan, &OptimizerContext::default())
        .expect("duplicate key predicate is safe")
        .expect("scalar aggregate should be replaced by a join");
    assert_composite_left_join(rewritten);
}
