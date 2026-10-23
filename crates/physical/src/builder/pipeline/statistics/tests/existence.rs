use super::*;
use common::protocol::JoinKind;

#[test]
fn existence_joins_build_the_smaller_input_and_keep_outer_output_and_residual_mapping() {
    for (kind, inverse) in [(JoinType::Semi, JoinKind::RightSemi), (JoinType::Anti, JoinKind::RightAnti)] {
        for (left_rows, right_rows) in [(2, 100), (100, 2)] {
            let left = input(1, left_rows, 10);
            let right = input(2, right_rows, 20);
            let predicate = LogicalExpr::BinaryOp {
                left: Box::new(binary(&left, 0, BinaryOp::Eq, &right, 0)),
                op: BinaryOp::And,
                right: Box::new(binary(&left, 1, BinaryOp::Neq, &right, 1)),
                return_type: VeinDataType::Boolean,
            };
            let schema = left.schema().clone();
            let plan = LogicalPlan::Join {
                join_type: kind,
                left: Box::new(left),
                right: Box::new(right),
                on: Some(predicate),
                using: vec![],
                schema: Box::new(schema.clone()),
            };
            let job = DefaultPhysicalPlanner::new().create_physical_plan_with_statistics(
                RequestId::new(), plan, Arc::new(QueryStatisticsContext::new([])),
            ).unwrap();
            let node = root(&job);
            let PhysicalPlanOperator::Join { build, plan, .. } = &node.operator else { panic!("join root") };
            assert_eq!(node.output_schema.as_ref(), &schema);
            if left_rows < right_rows {
                assert_eq!(plan.join_type, inverse);
                assert_eq!(build.output_schema.fields[0].id, ColumnId(10));
                assert_eq!(plan.outputs, vec![JoinOutput::Build(0), JoinOutput::Build(1)]);
                assert_eq!(plan.residual.as_ref().unwrap().input_columns, vec![JoinOutput::Build(1), JoinOutput::Probe(1)]);
            } else {
                assert_eq!(plan.join_type, if kind == JoinType::Semi { JoinKind::Semi } else { JoinKind::Anti });
                assert_eq!(build.output_schema.fields[0].id, ColumnId(20));
            }
        }
    }
}
