use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{
    LogicalExpr, LogicalLiteral, LogicalPlan, PlanTraversal, ResolvedColumnRef, VeinDataType,
    VeinSchema,
};

use super::{tests, PredicatePushdownRule};

fn comparison(column: &ResolvedColumnRef, value: &str) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(column.clone())),
        op: BinaryOp::Eq,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::String(value.to_owned()))),
        return_type: VeinDataType::Boolean,
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

fn country_disjunction(
    left: &ResolvedColumnRef,
    right: &ResolvedColumnRef,
) -> (LogicalExpr, LogicalExpr, LogicalExpr) {
    let left_france = comparison(left, "FRANCE");
    let left_germany = comparison(left, "GERMANY");
    let right_france = comparison(right, "FRANCE");
    let right_germany = comparison(right, "GERMANY");
    let first_branch = binary(left_france.clone(), BinaryOp::And, right_germany.clone());
    let second_branch = binary(left_germany.clone(), BinaryOp::And, right_france.clone());
    let residual = binary(first_branch, BinaryOp::Or, second_branch);
    let left_projection = binary(left_france, BinaryOp::Or, left_germany);
    let right_projection = binary(right_germany, BinaryOp::Or, right_france);
    (residual, left_projection, right_projection)
}

fn join(left: LogicalPlan, right: LogicalPlan, join_type: JoinType) -> LogicalPlan {
    let schema = VeinSchema {
        fields: [left.schema().fields.clone(), right.schema().fields.clone()].concat(),
    };
    LogicalPlan::Join {
        join_type,
        left: Box::new(left),
        right: Box::new(right),
        on: None,
        using: vec![],
        schema: Box::new(schema),
    }
}

fn filtered(input: LogicalPlan, predicate: LogicalExpr) -> LogicalPlan {
    LogicalPlan::Filter {
        input: Box::new(input),
        predicate,
    }
}

#[test]
fn derives_local_or_prefilters_and_retains_cross_side_residual() {
    let left_country = tests::field(1, "supplier_country", VeinDataType::Utf8);
    let right_country = tests::field(2, "customer_country", VeinDataType::Utf8);
    let (residual, expected_left, expected_right) =
        country_disjunction(&left_country, &right_country);
    let plan = filtered(
        join(
            tests::scan(vec![left_country]),
            tests::scan(vec![right_country]),
            JoinType::Inner,
        ),
        residual.clone(),
    );

    let rewritten = PredicatePushdownRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .expect("safe side-local necessary predicates should be derived");
    let LogicalPlan::Filter {
        input,
        predicate: actual_residual,
    } = rewritten
    else {
        panic!("the original predicate must remain as a residual filter");
    };
    assert_eq!(actual_residual, residual);
    let LogicalPlan::Join { left, right, .. } = input.as_ref() else {
        panic!("expected the residual filter to remain above an inner join");
    };
    let LogicalPlan::Filter {
        input: left_input,
        predicate: actual_left,
    } = left.as_ref()
    else {
        panic!("expected a necessary filter on the left input");
    };
    let LogicalPlan::Filter {
        input: right_input,
        predicate: actual_right,
    } = right.as_ref()
    else {
        panic!("expected a necessary filter on the right input");
    };
    assert_eq!(actual_left, &expected_left);
    assert_eq!(actual_right, &expected_right);
    assert_eq!(left_input.schema().fields.len(), 1);
    assert_eq!(right_input.schema().fields.len(), 1);
}

#[test]
fn does_not_derive_disjunctive_prefilters_for_outer_joins() {
    let left_country = tests::field(1, "supplier_country", VeinDataType::Utf8);
    let right_country = tests::field(2, "customer_country", VeinDataType::Utf8);
    let (predicate, _, _) = country_disjunction(&left_country, &right_country);
    let plan = filtered(
        join(
            tests::scan(vec![left_country]),
            tests::scan(vec![right_country]),
            JoinType::Left,
        ),
        predicate,
    );

    assert!(PredicatePushdownRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .is_none());
}

#[test]
fn does_not_duplicate_disjunctive_prefilters_already_in_scan_filters() {
    let left_country = tests::field(1, "supplier_country", VeinDataType::Utf8);
    let right_country = tests::field(2, "customer_country", VeinDataType::Utf8);
    let (residual, left_filter, right_filter) =
        country_disjunction(&left_country, &right_country);
    let mut left = tests::scan(vec![left_country]);
    let mut right = tests::scan(vec![right_country]);
    if let LogicalPlan::Scan { filters, .. } = &mut left {
        filters.push(left_filter);
    }
    if let LogicalPlan::Scan { filters, .. } = &mut right {
        filters.push(right_filter);
    }
    let plan = filtered(join(left, right, JoinType::Inner), residual);

    assert!(PredicatePushdownRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .is_none());
}
