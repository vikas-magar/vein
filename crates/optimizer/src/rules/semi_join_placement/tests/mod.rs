use std::sync::Arc;

use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{ColumnId, LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumn, VeinDataType, VeinSchema};

use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use crate::{Optimizer, OptimizerPipeline};

use super::SemiJoinPlacementRule;

mod boundaries;
mod placement;
mod semantics;
mod model;
mod cost;

fn column(id: usize) -> LogicalExpr {
    LogicalExpr::Column(Arc::new(ResolvedColumn::new(
        ColumnId(id), format!("key_{id}"), None, VeinDataType::Int64,
    )))
}

fn relation(id: usize) -> LogicalPlan {
    let LogicalExpr::Column(column) = column(id) else { unreachable!() };
    LogicalPlan::Intermediate {
        source_id: id as u128,
        row_count: 4,
        schema: Box::new(VeinSchema::new(vec![column])),
    }
}

fn binary(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

fn equality(left: usize, right: usize) -> LogicalExpr {
    binary(column(left), BinaryOp::Eq, column(right))
}

fn join(left: LogicalPlan, right: LogicalPlan, kind: JoinType, on: LogicalExpr) -> LogicalPlan {
    let mut fields = left.schema().fields.clone();
    if kind == JoinType::Inner {
        fields.extend(right.schema().fields.iter().cloned());
    }
    LogicalPlan::Join {
        join_type: kind,
        left: Box::new(left),
        right: Box::new(right),
        on: Some(on),
        using: vec![],
        schema: Box::new(VeinSchema::new(fields)),
    }
}

fn candidate(probe_key: usize) -> LogicalPlan {
    join(
        join(relation(1), relation(2), JoinType::Inner, equality(1, 2)),
        relation(3), JoinType::Semi, equality(probe_key, 3),
    )
}

fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    SemiJoinPlacementRule::default()
        .apply_owned_plan(plan, &OptimizerContext::default()).unwrap().0
}

fn dense_key_context() -> OptimizerContext {
    let statistics = Arc::new(common::statistics::QueryStatisticsContext::new([]));
    for id in 1..=3 {
        statistics.record_intermediate(id as u128, Arc::new(plan::TableStatistics {
            num_rows: Some(4),
            column_statistics: std::collections::HashMap::from([
                (common::statistics::intermediate_column_key(ColumnId(id)), plan::ColumnStatistics {
                    distinct_count: Some(1),
                    null_count: Some(0),
                    ..Default::default()
                }),
            ]),
            ..Default::default()
        })).unwrap();
    }
    OptimizerContext::with_statistics_provider(Default::default(),
                                               Arc::new(crate::context::QueryStatisticsProvider::new(statistics)))
}
