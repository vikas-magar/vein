use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{
    LogicalExpr, LogicalLiteral, LogicalPlan, PlanTraversal, ResolvedColumnRef, VeinDataType,
    VeinSchema,
};

use super::super::{OptimizerContext, PredicatePushdownRule, tests};
use crate::rule::OptimizerRule;

fn column(field: &ResolvedColumnRef) -> LogicalExpr {
    LogicalExpr::Column(field.clone())
}

fn compare(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

fn range(
    field: &ResolvedColumnRef,
    op: BinaryOp,
    bound: i64,
    cast_to: VeinDataType,
) -> LogicalExpr {
    let value = LogicalExpr::Cast {
        expr: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(bound))),
        target_type: cast_to.clone(),
    };
    let source = column(field);
    let left = LogicalExpr::Cast {
        expr: Box::new(source),
        target_type: cast_to,
    };
    compare(left, op, value)
}

fn conjunction(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
    compare(left, BinaryOp::And, right)
}

fn inner_join(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    let fields = [left.schema().fields.clone(), right.schema().fields.clone()].concat();
    LogicalPlan::Join {
        join_type: JoinType::Inner,
        left: Box::new(left),
        right: Box::new(right),
        on: None,
        using: vec![],
        schema: Box::new(VeinSchema { fields }),
    }
}

#[test]
fn casted_ranges_and_independent_filters_reach_join_inputs() {
    let part_size = tests::field(1, "p_size", VeinDataType::Int32);
    let part_brand = tests::field(2, "p_brand", VeinDataType::Utf8);
    let line_quantity = tests::field(3, "l_quantity", VeinDataType::Decimal128(15, 2));
    let left = tests::scan(vec![part_size.clone(), part_brand.clone()]);
    let right = tests::scan(vec![line_quantity.clone()]);
    let join = inner_join(left, right);

    let size_type = VeinDataType::Int64;
    let quantity_type = VeinDataType::Decimal128(21, 2);
    let first = conjunction(
        range(&part_size, BinaryOp::Geq, 1, size_type.clone()),
        range(&line_quantity, BinaryOp::Leq, 11, quantity_type.clone()),
    );
    let second = conjunction(
        range(&part_size, BinaryOp::Leq, 15, size_type.clone()),
        range(&line_quantity, BinaryOp::Geq, 10, quantity_type),
    );
    let disjunction = compare(first, BinaryOp::Or, second);
    let independent = compare(
        column(&part_brand),
        BinaryOp::Eq,
        LogicalExpr::Literal(LogicalLiteral::String("Brand#12".into())),
    );
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(join),
            predicate: disjunction,
        }),
        predicate: independent,
    };

    let once = PredicatePushdownRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .expect("the independent side filter should cross the casted filter");
    let twice = PredicatePushdownRule
        .apply(&once, &OptimizerContext::default())
        .unwrap()
        .expect("lossless casted disjunction should derive side prefilters");
    let LogicalPlan::Filter { input, .. } = twice else {
        panic!("the original disjunction must remain as a residual filter");
    };
    let LogicalPlan::Join { left, right, .. } = input.as_ref() else {
        panic!("expected join under residual predicate");
    };
    assert!(matches!(left.as_ref(), LogicalPlan::Filter { .. }));
    assert!(matches!(right.as_ref(), LogicalPlan::Filter { .. }));
}
