use super::*;
use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalLiteral, ResolvedColumn, ResolvedColumnRef, TableMetadata};

fn field(id: usize, name: &str) -> ResolvedColumnRef {
    std::sync::Arc::new(ResolvedColumn::new(
        ColumnId(id),
        name,
        Some("t".to_string()),
        plan::VeinDataType::Int64,
    ))
}

fn scan(fields: Vec<ResolvedColumnRef>) -> LogicalPlan {
    let schema = std::sync::Arc::new(VeinSchema {
        fields: fields.clone(),
    });
    LogicalPlan::Scan {
        table: std::sync::Arc::new(TableMetadata::new("t", schema)),
        projection: None,
        filters: vec![],
        schema: Box::new(VeinSchema { fields }),
    }
}

fn column(field: &ResolvedColumnRef) -> LogicalExpr {
    LogicalExpr::Column(field.clone())
}

fn binary(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: plan::VeinDataType::Boolean,
    }
}

fn join(
    left: &ResolvedColumnRef,
    right: &ResolvedColumnRef,
    join_type: JoinType,
) -> LogicalPlan {
    let left_plan = scan(vec![left.clone()]);
    let right_plan = scan(vec![right.clone()]);
    LogicalPlan::Join {
        join_type,
        left: Box::new(left_plan),
        right: Box::new(right_plan),
        on: Some(binary(column(left), BinaryOp::Eq, column(right))),
        using: vec![],
        schema: Box::new(VeinSchema {
            fields: vec![left.clone(), right.clone()],
        }),
    }
}

#[test]
fn propagates_left_predicate_to_right() {
    let left = field(1, "a_id");
    let right = field(2, "b_id");
    let plan = LogicalPlan::Filter {
        input: Box::new(join(&left, &right, JoinType::Inner)),
        predicate: binary(
            column(&left),
            BinaryOp::Lt,
            LogicalExpr::Literal(LogicalLiteral::Int64(10)),
        ),
    };

    let rewritten = JoinPredicateTransitivityRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .expect("predicate should propagate");

    let LogicalPlan::Join { right, .. } = rewritten else {
        panic!("expected join");
    };
    let LogicalPlan::Filter { predicate, .. } = right.as_ref() else {
        panic!("expected right filter");
    };
    assert_eq!(
        predicate,
        &binary(
            column(&right_field()),
            BinaryOp::Lt,
            LogicalExpr::Literal(LogicalLiteral::Int64(10))
        )
    );
}

fn right_field() -> ResolvedColumnRef {
    field(2, "b_id")
}

#[test]
fn propagates_right_predicate_to_left() {
    let left = field(1, "a_id");
    let right = field(2, "b_id");
    let plan = LogicalPlan::Filter {
        input: Box::new(join(&left, &right, JoinType::Inner)),
        predicate: binary(
            column(&right),
            BinaryOp::Gt,
            LogicalExpr::Literal(LogicalLiteral::Int64(1)),
        ),
    };

    let rewritten = JoinPredicateTransitivityRule
        .apply(&plan, &OptimizerContext::default())
        .unwrap()
        .expect("predicate should propagate");

    let LogicalPlan::Join { left, .. } = rewritten else {
        panic!("expected join");
    };
    let LogicalPlan::Filter { predicate, .. } = left.as_ref() else {
        panic!("expected left filter");
    };
    assert_eq!(
        predicate,
        &binary(
            column(&field(1, "a_id")),
            BinaryOp::Gt,
            LogicalExpr::Literal(LogicalLiteral::Int64(1))
        )
    );
}

#[test]
fn rejects_cross_side_predicate() {
    let left = field(1, "a_id");
    let right = field(2, "b_id");
    let plan = LogicalPlan::Filter {
        input: Box::new(join(&left, &right, JoinType::Inner)),
        predicate: binary(column(&left), BinaryOp::Eq, column(&right)),
    };
    assert!(
        JoinPredicateTransitivityRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .is_none()
    );
}

#[test]
fn rejects_outer_join() {
    let left = field(1, "a_id");
    let right = field(2, "b_id");
    let plan = LogicalPlan::Filter {
        input: Box::new(join(&left, &right, JoinType::Left)),
        predicate: binary(
            column(&left),
            BinaryOp::Lt,
            LogicalExpr::Literal(LogicalLiteral::Int64(10)),
        ),
    };
    assert!(
        JoinPredicateTransitivityRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .is_none()
    );
}

#[test]
fn rejects_non_equality_join() {
    let left = field(1, "a_id");
    let right = field(2, "b_id");
    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        left: Box::new(scan(vec![left.clone()])),
        right: Box::new(scan(vec![right.clone()])),
        on: Some(binary(column(&left), BinaryOp::Lt, column(&right))),
        using: vec![],
        schema: Box::new(VeinSchema {
            fields: vec![left.clone(), right.clone()],
        }),
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(join),
        predicate: binary(
            column(&left),
            BinaryOp::Lt,
            LogicalExpr::Literal(LogicalLiteral::Int64(10)),
        ),
    };
    assert!(
        JoinPredicateTransitivityRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .is_none()
    );
}
