use super::*;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{ColumnId, LogicalExpr, LogicalLiteral, LogicalPlan, ResolvedColumn, TableMetadata, VeinDataType, VeinSchema};
use std::sync::Arc;

fn field(id: usize, name: &str) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn::new(
        ColumnId(id),
        name,
        Some("t".to_string()),
        VeinDataType::Int64,
    ))
}

fn scan(column: Arc<ResolvedColumn>) -> LogicalPlan {
    let schema = VeinSchema { fields: vec![column] };
    LogicalPlan::Scan {
        table: Arc::new(TableMetadata::new("t", Arc::new(schema.clone()))),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(schema),
    }
}

fn greater_than_zero(column: Arc<ResolvedColumn>) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(column)),
        op: BinaryOp::Gt,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
        return_type: VeinDataType::Boolean,
    }
}

fn existence_join(kind: JoinType) -> (LogicalPlan, Arc<ResolvedColumn>) {
    let left_key = field(1, "left_key");
    let right_key = field(2, "right_key");
    let join = LogicalPlan::Join {
        join_type: kind,
        left: Box::new(scan(Arc::clone(&left_key))),
        right: Box::new(scan(right_key)),
        on: None,
        using: Vec::new(),
        schema: Box::new(VeinSchema { fields: vec![Arc::clone(&left_key)] }),
    };
    (join, left_key)
}

#[test]
fn pushes_left_output_filter_below_semi_and_anti_joins() {
    for kind in [JoinType::Semi, JoinType::Anti] {
        let (join, left_key) = existence_join(kind);
        let filter = LogicalPlan::Filter {
            input: Box::new(join),
            predicate: greater_than_zero(left_key),
        };

        let rewritten = PredicatePushdownRule
            .apply(&filter, &OptimizerContext::default())
            .expect("predicate pushdown should succeed")
            .expect("left-output filter should move before the existence probe");
        let LogicalPlan::Join { join_type, left, .. } = rewritten else {
            panic!("the existence join should remain the root");
        };
        assert_eq!(join_type, kind);
        assert!(matches!(left.as_ref(), LogicalPlan::Filter { .. }));
    }
}

#[test]
fn keeps_volatile_left_filter_above_the_join() {
    let (join, left_key) = existence_join(JoinType::Semi);
    let volatile = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::FunctionCall {
            name: "random".to_string(),
            args: Vec::new(),
            is_aggregate: false,
            distinct: false,
            return_type: VeinDataType::Float64,
        }),
        op: BinaryOp::Gt,
        right: Box::new(LogicalExpr::Column(left_key)),
        return_type: VeinDataType::Boolean,
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(join),
        predicate: volatile,
    };

    assert!(PredicatePushdownRule
        .apply(&filter, &OptimizerContext::default())
        .expect("unsafe side movement should be declined")
        .is_none());
}
