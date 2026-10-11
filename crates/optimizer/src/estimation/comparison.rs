//! Uniform comparison selectivity with SQL NULL semantics.

use std::cmp::Ordering::{Equal, Greater, Less};
use plan::LogicalLiteral;
use parser::ast::expr::BinaryOp;
use super::{columns::ColumnEstimate, selectivity::PredicateEstimate};
use crate::statistics::{compare, distance};

pub(super) fn estimate(column: ColumnEstimate<'_>, value: &LogicalLiteral, op: BinaryOp) -> PredicateEstimate {
    if matches!(value, LogicalLiteral::Null(_)) { return PredicateEstimate::logical(0.0, 0.0); }
    let non_null = column.null_fraction.map(|n| 1.0 - n);
    if non_null == Some(0.0) { return PredicateEstimate::model(Some(0.0), Some(0.0)); }
    let lower = column.min.and_then(|min| compare(value, min));
    let upper = column.max.and_then(|max| compare(value, max));
    let equality = || {
        if lower == Some(Less) || upper == Some(Greater) { return Some(0.0); }
        if column.min.zip(column.max).is_some_and(|(min, max)| compare(min, max) == Some(Equal))
            && lower == Some(Equal) {
            return non_null;
        }
        column.distinct.filter(|d| *d > 0.0).zip(non_null).map(|(d, n)| n / d)
    };
    let less_than = |inclusive: bool| {
        if lower == Some(Less) || (!inclusive && lower == Some(Equal)) { return Some(0.0); }
        if upper == Some(Greater) || (inclusive && upper == Some(Equal)) { return non_null; }
        let (min, max) = column.min.zip(column.max)?;
        let width = distance(max, min)?;
        if width <= 0.0 { return None; }
        let fraction = (distance(value, min)? / width).clamp(0.0, 1.0);
        // Uniform interpolation is an approximation, never a pruning proof.
        non_null.map(|n| n * fraction)
    };
    let truth = match op {
        BinaryOp::Eq | BinaryOp::Neq => equality(),
        BinaryOp::Lt => less_than(false),
        BinaryOp::Leq => less_than(true),
        BinaryOp::Gt => less_than(true).zip(non_null).map(|(l, n)| n - l),
        BinaryOp::Geq => less_than(false).zip(non_null).map(|(l, n)| n - l),
        _ => return PredicateEstimate::unknown(),
    };
    let falsehood = truth.zip(non_null).map(|(t, n)| n - t);
    if op == BinaryOp::Neq {
        PredicateEstimate::model(falsehood, truth)
    } else {
        PredicateEstimate::model(truth, falsehood)
    }
}
