use std::sync::Arc;

use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{ColumnId, LogicalExpr as E, LogicalLiteral as L, LogicalPlan as P, ResolvedColumn, TableMetadata, VeinDataType as T, VeinSchema};
use crate::{Optimizer, OptimizerPipeline};
use plan::PlanTraversal;

fn column(id: usize) -> E {
    E::Column(Arc::new(ResolvedColumn::new(ColumnId(id), "key", None, T::Int64)))
}

fn scan(id: usize) -> P {
    let E::Column(column) = column(id) else { unreachable!() };
    let schema = VeinSchema::new(vec![column]);
    P::Scan {
        table: Arc::new(TableMetadata::new("t", Arc::new(schema.clone()))),
        projection: None,
        filters: vec![],
        schema: Box::new(schema),
    }
}

fn binary(left: E, op: BinaryOp, right: E) -> E {
    E::BinaryOp { left: Box::new(left), op, right: Box::new(right), return_type: T::Boolean }
}

#[test]
fn on_predicates_respect_preserved_inputs_for_every_join_kind() {
    let left_filter = binary(column(1), BinaryOp::Gt, E::Literal(L::Int64(0)));
    let right_filter = binary(column(2), BinaryOp::Gt, E::Literal(L::Int64(0)));
    let key = binary(column(1), BinaryOp::Eq, column(2));
    for (kind, push_left, push_right) in [
        (JoinType::Inner, true, true), (JoinType::Left, false, true),
        (JoinType::Right, true, false), (JoinType::Semi, true, true),
        (JoinType::Anti, false, true), (JoinType::Full, false, false),
        (JoinType::NullAwareAnti, false, false), (JoinType::Single, false, false),
    ] {
        let left = scan(1);
        let right = scan(2);
        let schema = left.schema().clone();
        let plan = P::Join {
            join_type: kind,
            left: Box::new(left),
            right: Box::new(right),
            on: Some(binary(binary(key.clone(), BinaryOp::And, left_filter.clone()), BinaryOp::And, right_filter.clone())),
            using: vec![],
            schema: Box::new(schema),
        };
        let optimized = OptimizerPipeline::new(vec![
            Arc::new(super::super::JoinOnPredicatePushdownRule),
            Arc::new(crate::rules::ScanFilterPushdownRule),
        ]).optimize(plan).unwrap();
        let P::Join { left, right, on, join_type, .. } = optimized else { panic!("expected join") };
        assert_eq!(join_type, kind);
        for (input, filter, pushed) in [(&left, &left_filter, push_left), (&right, &right_filter, push_right)] {
            let P::Scan { filters, .. } = input.as_ref() else { panic!("expected scan") };
            assert_eq!(filters.contains(filter), pushed, "{kind:?}");
            assert_eq!(crate::rules::predicate_analysis::conjuncts(on.as_ref().unwrap()).contains(&filter), !pushed, "{kind:?}");
        }
    }
}
