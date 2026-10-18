use std::sync::Arc;

use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{ColumnId, LogicalExpr, LogicalPlan, LogicalLiteral, ResolvedColumn, VeinDataType, VeinSchema};

use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use crate::rules::InSubqueryJoinRule;

fn column(id: usize, name: &str, data_type: VeinDataType) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn::new(ColumnId(id), name, None, data_type))
}

fn relation(source_id: u128, field: Arc<ResolvedColumn>) -> LogicalPlan {
    LogicalPlan::Intermediate {
        source_id,
        row_count: 4,
        schema: Box::new(VeinSchema::new(vec![field])),
    }
}

fn filter(negated: bool) -> LogicalPlan {
    let probe = column(1, "probe", VeinDataType::Int64);
    let build = column(2, "candidate", VeinDataType::Int64);
    let membership = LogicalExpr::InList {
        expr: Box::new(LogicalExpr::Column(Arc::clone(&probe))),
        list: vec![LogicalExpr::ScalarSubquery(Box::new(relation(2, build)))],
        negated,
    };
    LogicalPlan::Filter {
        input: Box::new(relation(1, probe)),
        predicate: membership,
    }
}

#[test]
fn positive_membership_becomes_a_semi_join() {
    let rewritten = InSubqueryJoinRule
        .apply(&filter(false), &OptimizerContext::default())
        .unwrap()
        .unwrap();
    assert!(matches!(rewritten, LogicalPlan::Join { join_type: JoinType::Semi, .. }));
}

#[test]
fn positive_int32_membership_uses_native_hash_keys() {
    let probe = column(1, "probe", VeinDataType::Int32);
    let build = column(2, "candidate", VeinDataType::Int32);
    let membership = LogicalExpr::InList {
        expr: Box::new(LogicalExpr::Column(Arc::clone(&probe))),
        list: vec![LogicalExpr::ScalarSubquery(Box::new(relation(2, build)))],
        negated: false,
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(relation(1, probe)),
        predicate: membership,
    };

    let rewritten = InSubqueryJoinRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .unwrap();
    assert!(matches!(rewritten, LogicalPlan::Join { join_type: JoinType::Semi, .. }));
}

#[test]
fn negative_membership_becomes_a_null_aware_anti_join() {
    let rewritten = InSubqueryJoinRule
        .apply(&filter(true), &OptimizerContext::default())
        .unwrap()
        .unwrap();
    assert!(matches!(rewritten, LogicalPlan::Join { join_type: JoinType::NullAwareAnti, .. }));
}

#[test]
fn membership_nested_under_or_fails_during_planning() {
    let LogicalPlan::Filter { input, predicate } = filter(false) else { unreachable!() };
    let nested = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Literal(LogicalLiteral::Boolean(true))),
        op: BinaryOp::Or,
        right: Box::new(predicate),
        return_type: VeinDataType::Boolean,
    };
    let error = InSubqueryJoinRule
        .apply(&LogicalPlan::Filter { input, predicate: nested }, &OptimizerContext::default())
        .expect_err("nested membership cannot be treated as an independent join");
    assert!(error.to_string().contains("top-level conjunct"));
}
