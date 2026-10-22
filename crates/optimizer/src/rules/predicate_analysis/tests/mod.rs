use std::sync::Arc;

use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr as E, LogicalLiteral as L, ResolvedColumn, VeinDataType as T};

use super::{BuiltinPredicateSemantics, PredicateSemantics};

fn column(id: usize) -> E {
    E::Column(Arc::new(ResolvedColumn::new(ColumnId(id), "value", None, T::Int64)))
}

fn binary(left: E, op: BinaryOp, right: E) -> E {
    E::BinaryOp { left: Box::new(left), op, right: Box::new(right), return_type: T::Boolean }
}

fn rejects(expr: &E) -> bool {
    BuiltinPredicateSemantics.rejects_nulls(expr, &[ColumnId(2)].into())
}

#[test]
fn comparison_like_and_membership_reject_null_extended_columns() {
    for op in [BinaryOp::Eq, BinaryOp::Neq, BinaryOp::Lt, BinaryOp::Leq, BinaryOp::Gt, BinaryOp::Geq] {
        assert!(rejects(&binary(column(2), op, E::Literal(L::Int64(1)))));
    }
    assert!(rejects(&E::Like {
        expr: Box::new(column(2)),
        pattern: Box::new(E::Literal(L::String("%x".into()))),
        negated: true,
    }));
    for negated in [false, true] {
        assert!(rejects(&E::InList { expr: Box::new(column(2)), list: vec![E::Literal(L::Int64(1))], negated }));
    }
}

#[test]
fn preserves_three_valued_logic_under_and_or_and_not() {
    let rhs = binary(column(2), BinaryOp::Gt, E::Literal(L::Int64(0)));
    let lhs = binary(column(1), BinaryOp::Gt, E::Literal(L::Int64(0)));
    assert!(rejects(&binary(lhs.clone(), BinaryOp::And, rhs.clone())));
    assert!(!rejects(&binary(lhs, BinaryOp::Or, rhs.clone())));
    assert!(rejects(&E::Not { expr: Box::new(rhs) }));
    assert!(!rejects(&E::IsNull { expr: Box::new(column(2)) }));
    assert!(rejects(&E::IsNotNull { expr: Box::new(column(2)) }));
}

#[test]
fn nullable_not_in_keeps_false_as_a_possible_outcome_before_negation() {
    let not_in = E::InList {
        expr: Box::new(column(1)),
        list: vec![E::Literal(L::Int64(1)), E::Literal(L::Null(T::Int64))],
        negated: true,
    };
    assert!(rejects(&not_in));
    assert!(!rejects(&E::Not { expr: Box::new(not_in) }));
}

#[test]
fn unknown_function_semantics_and_depth_exhaustion_never_supply_a_proof() {
    let function = E::FunctionCall {
        name: "custom".into(),
        args: vec![column(2)],
        is_aggregate: false,
        distinct: false,
        return_type: T::Boolean,
    };
    assert!(!rejects(&function));
    let nested = (0..150).fold(column(2), |expr, _| E::Not { expr: Box::new(expr) });
    assert!(!rejects(&nested));
}

#[test]
fn boolean_composition_matches_the_complete_sql_truth_table() {
    let literal = |value| E::Literal(match value {
        Some(value) => L::Boolean(value),
        None => L::Null(T::Boolean)
    });
    for left in [Some(true), Some(false), None] {
        for right in [Some(true), Some(false), None] {
            let and = match (left, right) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None
            };
            let or = match (left, right) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None
            };
            for (op, expected) in [(BinaryOp::And, and), (BinaryOp::Or, or)] {
                let expr = binary(literal(left), op, literal(right));
                assert_eq!(rejects(&expr), expected != Some(true));
                assert_eq!(rejects(&E::Not { expr: Box::new(expr) }), expected != Some(false));
            }
        }
    }
}
