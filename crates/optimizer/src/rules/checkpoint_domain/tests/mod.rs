use super::CheckpointAggregateDomainRule;
use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use crate::{Optimizer, OptimizerPipeline};
use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{ColumnId, LogicalExpr as E, LogicalPlan as P, PlanTraversal, ResolvedColumn, VeinDataType as T, VeinSchema};
use std::sync::Arc;

mod placement;
mod boundaries;
mod lineage;
mod planning;

fn field(id: usize) -> plan::ResolvedColumnRef {
    Arc::new(ResolvedColumn::new(ColumnId(id), format!("key_{id}"), None, T::Int64).with_nullable(true))
}

fn source(id: u128, columns: &[usize]) -> P {
    P::Intermediate {
        source_id: id,
        row_count: 4,
        schema: Box::new(VeinSchema::new(columns.iter().map(|id| field(*id)).collect())),
    }
}

fn eq(a: usize, b: usize) -> E {
    E::BinaryOp {
        left: Box::new(E::Column(field(a))),
        op: BinaryOp::Eq,
        right: Box::new(E::Column(field(b))),
        return_type: T::Boolean,
    }
}

fn aggregate(input: P, groups: &[usize]) -> P {
    let mut fields: Vec<_> = groups.iter().map(|id| field(*id)).collect();
    fields.push(field(99));
    P::Aggregate {
        input: Box::new(input),
        grouping_sets: vec![groups.iter().map(|id| E::Column(field(*id))).collect()],
        aggs: vec![E::FunctionCall {
            name: "sum".into(),
            args: vec![E::Column(field(20))],
            is_aggregate: true,
            distinct: false,
            return_type: T::Int64
        }],
        schema: Box::new(VeinSchema::new(fields)),
    }
}

fn join(left: P, right: P, kind: JoinType, on: E) -> P {
    let fields = left.schema().fields.iter().chain(&right.schema().fields).cloned().collect();
    P::Join {
        join_type: kind,
        left: Box::new(left),
        right: Box::new(right),
        on: Some(on),
        using: vec![],
        schema: Box::new(VeinSchema::new(fields)),
    }
}

fn candidate() -> P {
    join(source(1, &[1]), aggregate(source(2, &[10, 20]), &[10]), JoinType::Left, eq(1, 10))
}

fn rewrite(plan: P) -> (P, bool) {
    CheckpointAggregateDomainRule.apply_owned(plan, &OptimizerContext::default()).unwrap()
}

fn grouped_input(plan: &P) -> &P {
    let P::Join { right, .. } = plan else { panic!("outer join"); };
    let mut plan = right.as_ref();
    while let P::Project { input, .. } | P::Filter { input, .. } = plan { plan = input; }
    let P::Aggregate { input, .. } = plan else { panic!("aggregate"); };
    input
}

fn semi_keys(plan: &P) -> Vec<(ColumnId, ColumnId)> {
    let P::Join { join_type: JoinType::Semi, left, right, on: Some(on), .. } = grouped_input(plan)
    else { panic!("domain semi join"); };
    crate::rules::join_transitivity::equi_join_keys(on,
                                                    &crate::rules::join_transitivity::schema_ids(left.schema()),
                                                    &crate::rules::join_transitivity::schema_ids(right.schema()))
}
