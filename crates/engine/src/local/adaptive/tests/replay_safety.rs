use super::super::StatisticsCheckpointPolicy;
use super::support::{join, scan};
use plan::{LogicalPlan, VeinDataType};

#[test]
fn volatile_expressions_are_not_replayed_across_adaptive_boundaries() {
    let mut input = scan("a", 100);
    if let LogicalPlan::Scan { filters, .. } = &mut input {
        filters.push(plan::LogicalExpr::BinaryOp {
            left: Box::new(plan::LogicalExpr::FunctionCall {
                name: "random".into(),
                args: vec![],
                is_aggregate: false,
                distinct: false,
                return_type: VeinDataType::Float64,
            }),
            op: parser::ast::expr::BinaryOp::Gt,
            right: Box::new(plan::LogicalExpr::Literal(plan::LogicalLiteral::Float64(0.5))),
            return_type: VeinDataType::Boolean,
        });
    }
    assert!(StatisticsCheckpointPolicy.choose_for_test(&join(input, scan("b", 10))).is_none());
}
