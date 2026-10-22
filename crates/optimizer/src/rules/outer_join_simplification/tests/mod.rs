use std::sync::Arc;

use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr as E, LogicalLiteral as L, ResolvedColumn, TableMetadata, VeinDataType as T, VeinSchema};

use super::*;
mod outer;
mod schema;

fn column(id: usize) -> E {
    E::Column(Arc::new(ResolvedColumn::new(ColumnId(id), "key", None, T::Int64)))
}

fn scan(id: usize) -> LogicalPlan {
    let E::Column(column) = column(id) else { unreachable!() };
    let schema = VeinSchema::new(vec![column]);
    LogicalPlan::Scan {
        table: Arc::new(TableMetadata::new("t", Arc::new(schema.clone()))),
        projection: None,
        filters: vec![],
        schema: Box::new(schema),
    }
}

fn binary(left: E, op: BinaryOp, right: E) -> E {
    E::BinaryOp { left: Box::new(left), op, right: Box::new(right), return_type: T::Boolean }
}

fn filtered(predicate: E) -> LogicalPlan {
    let left = scan(1);
    let right = scan(2);
    let schema = VeinSchema::new(left.schema().fields.iter().chain(&right.schema().fields).cloned().collect());
    LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Join {
            join_type: JoinType::Left,
            left: Box::new(left),
            right: Box::new(right),
            on: Some(binary(column(1), BinaryOp::Eq, column(2))),
            using: vec![],
            schema: Box::new(schema),
        }),
        predicate,
    }
}

fn rule() -> OuterJoinSimplificationRule { OuterJoinSimplificationRule::default() }

#[test]
fn reduces_null_rejecting_left_join_and_preserves_filter_and_output_schema() {
    let plan = filtered(binary(column(2), BinaryOp::Gt, E::Literal(L::Int64(10))));
    let rewritten = rule().apply(&plan, &OptimizerContext::default()).unwrap().unwrap();
    assert_eq!(plan.schema(), rewritten.schema());
    let LogicalPlan::Filter { input, predicate } = rewritten else { panic!("filter must remain") };
    assert!(matches!(input.as_ref(), LogicalPlan::Join { join_type: JoinType::Inner, .. }));
    let LogicalPlan::Filter { predicate: original, .. } = plan else { unreachable!() };
    assert_eq!(predicate, original);
}

#[test]
fn preserves_left_join_when_unmatched_rows_can_satisfy_where() {
    for predicate in [
        E::IsNull { expr: Box::new(column(2)) },
        binary(column(1), BinaryOp::Gt, E::Literal(L::Int64(10))),
        binary(E::IsNotNull { expr: Box::new(column(2)) }, BinaryOp::Or,
               binary(column(1), BinaryOp::Gt, E::Literal(L::Int64(10)))),
    ] {
        assert!(rule().apply(&filtered(predicate), &OptimizerContext::default()).unwrap().is_none());
    }
}

#[test]
fn unsafe_where_cannot_change_the_join_evaluation_domain() {
    let unsafe_expr = binary(column(2), BinaryOp::Divide, E::Literal(L::Int64(0)));
    let predicate = binary(unsafe_expr, BinaryOp::Gt, E::Literal(L::Int64(10)));
    assert!(rule().apply(&filtered(predicate), &OptimizerContext::default()).unwrap().is_none());
}

#[test]
fn custom_semantics_can_decline_a_rewrite() {
    struct NoProof;
    impl PredicateSemantics for NoProof {
        fn reorder_safe(&self, _: &E) -> bool { true }
        fn rejects_nulls(&self, _: &E, _: &HashSet<plan::ColumnId>) -> bool { false }
    }
    let plan = filtered(E::IsNotNull { expr: Box::new(column(2)) });
    assert!(OuterJoinSimplificationRule::with_semantics(NoProof)
        .apply(&plan, &OptimizerContext::default()).unwrap().is_none());
}
