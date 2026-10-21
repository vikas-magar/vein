use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr, LogicalLiteral, ResolvedColumn, VeinDataType};
use std::sync::Arc;

use super::reorder_safe;

fn column() -> LogicalExpr {
    LogicalExpr::Column(Arc::new(ResolvedColumn::new(
        ColumnId(1), "value", None, VeinDataType::Int64,
    )))
}

fn literal(value: i64) -> LogicalExpr {
    LogicalExpr::Literal(LogicalLiteral::Int64(value))
}

fn comparison(op: BinaryOp, left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

#[test]
fn simple_comparisons_and_conjunctions_are_reorder_safe() {
    let equality = comparison(BinaryOp::Eq, column(), literal(3));
    let less_than = comparison(BinaryOp::Lt, column(), literal(9));
    let conjunction = comparison(BinaryOp::And, equality, less_than);
    assert!(reorder_safe(&conjunction));
}

#[test]
fn arithmetic_casts_and_functions_are_not_reorder_safe() {
    let division = comparison(BinaryOp::Gt, comparison(BinaryOp::Divide, column(), literal(0)), literal(1));
    assert!(!reorder_safe(&division));
    assert!(!reorder_safe(&LogicalExpr::Cast {
        expr: Box::new(column()),
        target_type: VeinDataType::Int32,
    }));
    assert!(!reorder_safe(&LogicalExpr::FunctionCall {
        name: "random".into(),
        args: vec![],
        is_aggregate: false,
        distinct: false,
        return_type: VeinDataType::Int64,
    }));
}
