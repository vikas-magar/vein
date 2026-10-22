use super::super::*;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::VeinDataType;
use plan::{
    ColumnId, LogicalLiteral, ResolvedColumn, ResolvedColumnRef, TableMetadata, VeinSchema,
};
use std::sync::Arc;

fn field(id: usize, name: &str, qualifier: &str, data_type: VeinDataType) -> ResolvedColumnRef {
    Arc::new(ResolvedColumn::new(
        ColumnId(id),
        name,
        Some(qualifier.to_owned()),
        data_type,
    ))
}

fn scan(table_name: &str, fields: Vec<ResolvedColumnRef>) -> LogicalPlan {
    let schema = Arc::new(VeinSchema {
        fields: fields.clone(),
    });
    LogicalPlan::Scan {
        table: Arc::new(TableMetadata::new(table_name, schema)),
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
        return_type: VeinDataType::Boolean,
    }
}

fn join(
    on: LogicalExpr,
    left_fields: Vec<ResolvedColumnRef>,
    right_fields: Vec<ResolvedColumnRef>,
) -> LogicalPlan {
    let mut output_fields = left_fields.clone();
    output_fields.extend(right_fields.iter().cloned());
    LogicalPlan::Join {
        join_type: JoinType::Left,
        left: Box::new(scan("left_table", left_fields)),
        right: Box::new(scan("right_table", right_fields)),
        on: Some(on),
        using: vec![],
        schema: Box::new(VeinSchema {
            fields: output_fields,
        }),
    }
}

fn optimize(plan: &LogicalPlan) -> Option<LogicalPlan> {
    JoinOnPredicatePushdownRule
        .apply(plan, &OptimizerContext::default())
        .expect("predicate rewrite should succeed")
}

#[test]
fn pushes_q13_shaped_right_only_not_like_and_preserves_left_join() {
    let left_key = field(1, "c_custkey", "customer", VeinDataType::Int64);
    let right_key = field(2, "o_custkey", "orders", VeinDataType::Int64);
    let comment = field(3, "o_comment", "orders", VeinDataType::Utf8);
    let equality = binary(column(&left_key), BinaryOp::Eq, column(&right_key));
    let not_like = LogicalExpr::Like {
        expr: Box::new(column(&comment)),
        pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String(
            "%special%requests%".into(),
        ))),
        negated: true,
    };
    let original_on = binary(equality.clone(), BinaryOp::And, not_like.clone());

    let rewritten = optimize(&join(
        original_on,
        vec![left_key.clone()],
        vec![right_key.clone(), comment],
    ))
        .expect("right-only ON condition should move to RHS");
    let LogicalPlan::Join {
        join_type: JoinType::Left,
        left,
        right,
        on,
        ..
    } = rewritten
    else {
        panic!("rewrite must retain LEFT JOIN");
    };
    assert!(matches!(left.as_ref(), LogicalPlan::Scan { .. }));
    assert_eq!(on, Some(equality));
    let LogicalPlan::Filter { input, predicate } = right.as_ref() else {
        panic!("safe ON predicate should filter the right input");
    };
    assert!(matches!(input.as_ref(), LogicalPlan::Scan { .. }));
    assert_eq!(predicate, &not_like);
}

#[test]
fn leaves_left_only_and_cross_side_on_terms_as_join_residuals() {
    let left_key = field(1, "left_key", "l", VeinDataType::Int64);
    let left_limit = field(2, "left_limit", "l", VeinDataType::Int64);
    let right_key = field(3, "right_key", "r", VeinDataType::Int64);
    let right_value = field(4, "right_value", "r", VeinDataType::Int64);
    let left_term = binary(
        column(&left_limit),
        BinaryOp::Gt,
        LogicalExpr::Literal(LogicalLiteral::Int64(0)),
    );
    let cross_term = binary(column(&left_key), BinaryOp::Eq, column(&right_key));
    let right_term = binary(
        column(&right_value),
        BinaryOp::Gt,
        LogicalExpr::Literal(LogicalLiteral::Int64(10)),
    );
    let on = binary(
        binary(left_term.clone(), BinaryOp::And, cross_term.clone()),
        BinaryOp::And,
        right_term.clone(),
    );

    let rewritten = optimize(&join(
        on,
        vec![left_key.clone(), left_limit],
        vec![right_key.clone(), right_value],
    ))
        .unwrap();
    let LogicalPlan::Join { on, right, .. } = rewritten else {
        panic!("expected join");
    };
    assert_eq!(on, Some(binary(left_term, BinaryOp::And, cross_term)));
    let LogicalPlan::Filter { predicate, .. } = right.as_ref() else {
        panic!("right-only term should push");
    };
    assert_eq!(predicate, &right_term);
}

#[test]
fn does_not_move_unsafe_conjunct_or_post_join_where_predicate() {
    let left_key = field(1, "left_key", "l", VeinDataType::Int64);
    let right_key = field(2, "right_key", "r", VeinDataType::Int64);
    let right_value = field(3, "right_value", "r", VeinDataType::Int64);
    let safe = binary(
        column(&right_value),
        BinaryOp::Gt,
        LogicalExpr::Literal(LogicalLiteral::Int64(0)),
    );
    let arithmetic = LogicalExpr::BinaryOp {
        left: Box::new(column(&right_value)),
        op: BinaryOp::Divide,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
        return_type: VeinDataType::Int64,
    };
    let unsafe_term = binary(
        arithmetic,
        BinaryOp::Gt,
        LogicalExpr::Literal(LogicalLiteral::Int64(1)),
    );
    let on = binary(safe, BinaryOp::And, unsafe_term);
    assert!(
        optimize(&join(
            on,
            vec![left_key.clone()],
            vec![right_key.clone(), right_value.clone()],
        ))
            .is_none()
    );

    let post_join_filter = LogicalPlan::Filter {
        input: Box::new(join(
            binary(column(&left_key), BinaryOp::Eq, column(&right_key)),
            vec![left_key.clone()],
            vec![right_key.clone(), right_value.clone()],
        )),
        predicate: binary(
            column(&right_value),
            BinaryOp::Gt,
            LogicalExpr::Literal(LogicalLiteral::Int64(0)),
        ),
    };
    assert!(optimize(&post_join_filter).is_none());
}
