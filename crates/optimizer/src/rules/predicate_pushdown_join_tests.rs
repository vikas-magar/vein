use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use crate::Optimizer;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{LogicalExpr, LogicalLiteral, LogicalPlan, ResolvedColumnRef, VeinDataType, VeinSchema};

use super::{
    tests::{bool_predicate, comparison_predicate, field, scan},
    PredicatePushdownRule,
};

fn join(join_type: JoinType, left_field: &ResolvedColumnRef, right_field: &ResolvedColumnRef) -> LogicalPlan {
    LogicalPlan::Join {
        join_type,
        left: Box::new(scan(vec![left_field.clone()])),
        right: Box::new(scan(vec![right_field.clone()])),
        on: None,
        using: vec![],
        schema: Box::new(VeinSchema {
            fields: vec![left_field.clone(), right_field.clone()],
        }),
    }
}

fn filtered_join(join: LogicalPlan, predicate: LogicalExpr) -> LogicalPlan {
    LogicalPlan::Filter {
        input: Box::new(join),
        predicate,
    }
}

fn comparison_expr(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

fn column(field: &ResolvedColumnRef) -> LogicalExpr {
    LogicalExpr::Column(field.clone())
}

fn int(value: i64) -> LogicalExpr {
    LogicalExpr::Literal(LogicalLiteral::Int64(value))
}

fn and(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
    comparison_expr(left, BinaryOp::And, right)
}

#[test]
fn pushes_preserved_side_filter_below_left_join() {
    let left = field(1, "left_id", VeinDataType::Int64);
    let right = field(2, "right_id", VeinDataType::Int64);
    let plan = filtered_join(join(JoinType::Left, &left, &right), bool_predicate(&left));

    let rewritten = PredicatePushdownRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .expect("preserved-side predicate should push below LEFT JOIN");
    let LogicalPlan::Join {
        join_type: JoinType::Left,
        left,
        ..
    } = rewritten
    else {
        panic!("expected LEFT JOIN");
    };
    assert!(matches!(left.as_ref(), LogicalPlan::Filter { .. }));
}

#[test]
fn keeps_nullable_side_filter_above_left_join() {
    let left = field(1, "left_id", VeinDataType::Int64);
    let right = field(2, "right_id", VeinDataType::Int64);
    let plan = filtered_join(join(JoinType::Left, &left, &right), bool_predicate(&right));

    assert!(PredicatePushdownRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .is_none());
}

#[test]
fn keeps_cross_side_filter_above_left_join() {
    let left = field(1, "left_id", VeinDataType::Int64);
    let right = field(2, "right_id", VeinDataType::Int64);
    let predicate = comparison_predicate(&left, BinaryOp::Eq, &right);
    let plan = filtered_join(join(JoinType::Left, &left, &right), predicate);

    assert!(PredicatePushdownRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .is_none());
}

#[test]
fn pushes_safe_date_bounds_through_cross_side_filter_chain() {
    let supplier = field(1, "supplier_nation", VeinDataType::Int64);
    let shipdate = field(2, "shipdate", VeinDataType::Int64);
    let france_germany = and(
        comparison_expr(column(&supplier), BinaryOp::Eq, int(1)),
        comparison_expr(column(&shipdate), BinaryOp::Eq, int(2)),
    );
    let germany_france = and(
        comparison_expr(column(&supplier), BinaryOp::Eq, int(2)),
        comparison_expr(column(&shipdate), BinaryOp::Eq, int(1)),
    );
    let residual = comparison_expr(france_germany, BinaryOp::Or, germany_france);
    let lower = comparison_expr(column(&shipdate), BinaryOp::Geq, int(10));
    let upper = comparison_expr(column(&shipdate), BinaryOp::Leq, int(20));
    let plan = filtered_join(
        filtered_join(
            filtered_join(join(JoinType::Inner, &supplier, &shipdate), residual.clone()),
            lower.clone(),
        ),
        upper.clone(),
    );

    let rules: Vec<std::sync::Arc<dyn OptimizerRule>> = vec![
        std::sync::Arc::new(PredicatePushdownRule),
        std::sync::Arc::new(crate::rules::ScanFilterPushdownRule),
    ];
    let optimized = crate::OptimizerPipeline::new(rules)
        .optimize(plan)
        .expect("safe date predicates should reach the scan");

    let LogicalPlan::Filter {
        input: filtered_join,
        predicate,
    } = optimized
    else {
        panic!("cross-side OR predicate should remain above the join");
    };
    assert_eq!(predicate, residual);
    let LogicalPlan::Join { right, .. } = filtered_join.as_ref() else {
        panic!("expected join beneath the residual filter");
    };
    let LogicalPlan::Scan { filters, .. } = right.as_ref() else {
        panic!("date bounds should be attached to the lineitem scan");
    };
    assert!(filters.contains(&lower));
    assert!(filters.contains(&upper));
}

#[test]
fn does_not_reorder_outer_filter_across_arithmetic_predicate() {
    let left = field(1, "left_value", VeinDataType::Int64);
    let right = field(2, "right_value", VeinDataType::Int64);
    let arithmetic = LogicalExpr::BinaryOp {
        left: Box::new(column(&left)),
        op: BinaryOp::Plus,
        right: Box::new(int(1)),
        return_type: VeinDataType::Int64,
    };
    let unsafe_predicate = comparison_expr(arithmetic, BinaryOp::Gt, int(0));
    let outer = comparison_expr(column(&right), BinaryOp::Gt, int(10));
    let plan = filtered_join(
        filtered_join(join(JoinType::Inner, &left, &right), unsafe_predicate),
        outer,
    );

    assert!(PredicatePushdownRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .is_none());
}
