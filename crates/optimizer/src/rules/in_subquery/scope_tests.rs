use std::sync::Arc;

use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{
    ColumnId, LogicalExpr, LogicalPlan, ResolvedColumn, VeinDataType, VeinSchema,
};

use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use crate::rules::InSubqueryJoinRule;

fn field(id: usize, name: &str) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn::new(
        ColumnId(id),
        name,
        None,
        VeinDataType::Int64,
    ))
}

fn relation(source_id: u128, field: Arc<ResolvedColumn>) -> LogicalPlan {
    LogicalPlan::Intermediate {
        source_id,
        row_count: 4,
        schema: Box::new(VeinSchema::new(vec![field])),
    }
}

fn nested_scalar_membership_plan(nested_outer: Arc<ResolvedColumn>) -> LogicalPlan {
    let probe = field(1, "supplier_key");
    let local_key = field(2, "partsupp_key");
    let inner_key = field(3, "lineitem_key");
    let aggregate_value = field(5, "quantity_sum");
    let nested_aggregate = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(relation(3, inner_key.clone())),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(inner_key.clone())),
                op: BinaryOp::Eq,
                right: Box::new(LogicalExpr::OuterColumn(nested_outer)),
                return_type: VeinDataType::Boolean,
            },
        }),
        grouping_sets: vec![Vec::new()],
        aggs: vec![LogicalExpr::FunctionCall {
            name: "sum".to_string(),
            args: vec![LogicalExpr::Column(inner_key)],
            is_aggregate: true,
            distinct: false,
            return_type: VeinDataType::Int64,
        }],
        schema: Box::new(VeinSchema::new(vec![aggregate_value.clone()])),
    };
    let membership_rhs = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(relation(2, local_key.clone())),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(local_key.clone())),
                op: BinaryOp::Gt,
                right: Box::new(LogicalExpr::ScalarSubquery(Box::new(nested_aggregate))),
                return_type: VeinDataType::Boolean,
            },
        }),
        exprs: vec![LogicalExpr::Column(local_key.clone())],
        schema: Box::new(VeinSchema::new(vec![local_key])),
    };
    LogicalPlan::Filter {
        input: Box::new(relation(1, probe.clone())),
        predicate: LogicalExpr::InList {
            expr: Box::new(LogicalExpr::Column(probe)),
            list: vec![LogicalExpr::ScalarSubquery(Box::new(membership_rhs))],
            negated: false,
        },
    }
}

#[test]
fn nested_scalar_correlation_to_membership_rhs_scope_is_allowed() {
    let plan = nested_scalar_membership_plan(field(2, "partsupp_key"));
    let rewritten = InSubqueryJoinRule
        .apply(&plan, &OptimizerContext::default())
        .expect("local nested correlation should be supported")
        .expect("membership should lower to a join");
    assert!(matches!(rewritten, LogicalPlan::Join { join_type: JoinType::Semi, .. }));
}

#[test]
fn nested_scalar_reference_to_membership_outer_input_is_rejected() {
    let plan = nested_scalar_membership_plan(field(1, "supplier_key"));
    let error = InSubqueryJoinRule
        .apply(&plan, &OptimizerContext::default())
        .expect_err("nested reference to the supplier scope remains unsupported");
    assert!(error.to_string().contains("correlated IN subqueries"));
}

#[test]
fn direct_correlation_to_membership_outer_input_is_rejected() {
    let probe = field(1, "supplier_key");
    let build = field(2, "candidate_key");
    let rhs = LogicalPlan::Filter {
        input: Box::new(relation(2, build.clone())),
        predicate: LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(build)),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::OuterColumn(probe.clone())),
            return_type: VeinDataType::Boolean,
        },
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(relation(1, probe.clone())),
        predicate: LogicalExpr::InList {
            expr: Box::new(LogicalExpr::Column(probe)),
            list: vec![LogicalExpr::ScalarSubquery(Box::new(rhs))],
            negated: false,
        },
    };
    let error = InSubqueryJoinRule
        .apply(&plan, &OptimizerContext::default())
        .expect_err("directly correlated membership is not decorrelated here");
    assert!(error.to_string().contains("correlated IN subqueries"));
}
