use std::sync::Arc;
use common::identifiers::RequestId;
use common::protocol::{JoinOutput, PhysicalPlanNode, PhysicalPlanOperator};
use common::statistics::QueryStatisticsContext;
use parser::ast::{expr::BinaryOp, pipe_op::JoinType};
use plan::{ColumnId, LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumn, VeinDataType, VeinSchema};
use crate::{DefaultPhysicalPlanner, PhysicalPlanner};
mod existence;

fn input(source: u128, rows: u64, id: usize) -> LogicalPlan {
    LogicalPlan::Intermediate {
        source_id: source,
        row_count: rows,
        schema: Box::new(VeinSchema::new(vec![
            Arc::new(ResolvedColumn::new(ColumnId(id), "key", None, VeinDataType::Int64)),
            Arc::new(ResolvedColumn::new(ColumnId(id + 1), "value", None, VeinDataType::Int64)),
        ])),
    }
}

fn binary(left: &LogicalPlan, li: usize, op: BinaryOp, right: &LogicalPlan, ri: usize) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(left.schema().fields[li].clone())),
        op,
        right: Box::new(LogicalExpr::Column(right.schema().fields[ri].clone())),
        return_type: VeinDataType::Boolean,
    }
}

fn root(job: &crate::job::Job) -> &PhysicalPlanNode {
    let PhysicalPlanOperator::Sink { input, .. } = &job.stage().tasks()[0].plan.root.operator else { panic!("sink root"); };
    input
}

#[test]
fn query_statistics_swap_only_symmetric_join_inputs_and_remap_residual_and_output_columns() {
    for kind in [JoinType::Inner, JoinType::Left] {
        let left = input(1, 2, 10);
        let right = input(2, 100, 20);
        let equality = binary(&left, 0, BinaryOp::Eq, &right, 0);
        let residual = binary(&left, 1, BinaryOp::Gt, &right, 1);
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(equality),
            op: BinaryOp::And,
            right: Box::new(residual),
            return_type: VeinDataType::Boolean,
        };
        let schema = VeinSchema::new(left.schema().fields.iter().chain(&right.schema().fields).cloned().map(|column| {
            if kind == JoinType::Left && column.id.0 >= 20 { Arc::new(column.as_ref().clone().with_nullable(true)) } else { column }
        }).collect());
        let plan = LogicalPlan::Join {
            join_type: kind,
            left: Box::new(left),
            right: Box::new(right),
            on: Some(predicate),
            using: vec![],
            schema: Box::new(schema.clone()),
        };
        let job = DefaultPhysicalPlanner::new().create_physical_plan_with_statistics(RequestId::new(), plan,
                                                                                     Arc::new(QueryStatisticsContext::new([]))).unwrap();
        let PhysicalPlanOperator::Join { build, plan, .. } = &root(&job).operator else { panic!("join root"); };
        assert_eq!(root(&job).output_schema.as_ref(), &schema);
        if kind == JoinType::Inner {
            assert_eq!(build.output_schema.fields[0].id, ColumnId(10));
            assert_eq!(plan.outputs, vec![JoinOutput::Build(0), JoinOutput::Build(1), JoinOutput::Probe(0), JoinOutput::Probe(1)]);
            assert_eq!(plan.residual.as_ref().unwrap().input_columns, vec![JoinOutput::Build(1), JoinOutput::Probe(1)]);
        } else {
            assert_eq!(build.output_schema.fields[0].id, ColumnId(20), "outer join direction must be preserved");
        }
    }
}

#[test]
fn physical_aggregate_sizing_reads_completed_checkpoint_ndv() {
    let context = Arc::new(QueryStatisticsContext::new([]));
    context.record_intermediate(17, Arc::new(plan::TableStatistics {
        num_rows: Some(1000),
        column_statistics: std::collections::HashMap::from([
            ("10".into(), plan::ColumnStatistics { distinct_count: Some(4), null_count: Some(0), ..Default::default() }),
        ]),
        ..Default::default()
    })).unwrap();
    let input = input(17, 1000, 10);
    let key = input.schema().fields[0].clone();
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(input),
        grouping_sets: vec![vec![LogicalExpr::Column(key.clone())]],
        aggs: vec![],
        schema: Box::new(VeinSchema::new(vec![key])),
    };
    let job = DefaultPhysicalPlanner::new().create_physical_plan_with_statistics(RequestId::new(), aggregate, context).unwrap();
    let PhysicalPlanOperator::Aggregate { plan, .. } = &root(&job).operator else { panic!("aggregate root"); };
    assert_eq!(plan.estimated_groups, Some(4));
}
