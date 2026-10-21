use std::sync::Arc;

use common::protocol::{BinaryExprOp, ExprKind, ExpressionStep};
use plan::{LogicalExpr, LogicalLiteral};
use plan::schema::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

use super::ExpressionCompiler;

fn schema() -> VeinSchema {
    VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(ColumnId(4), "a", None, VeinDataType::Int64)),
        Arc::new(ResolvedColumn::new(ColumnId(5), "b", None, VeinDataType::Int64)),
    ])
}

#[test]
fn compiles_columns_and_arithmetic_as_typed_tree() {
    let schema = schema();
    let expression = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(schema.fields[0].clone())),
        op: parser::ast::expr::BinaryOp::Plus,
        right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(3))),
        return_type: VeinDataType::Int64,
    };
    let compiled = ExpressionCompiler::new(&schema).unwrap().compile(&expression).unwrap();

    let ExprKind::Binary { op, left, right } = compiled.kind else { panic!("expected binary") };
    assert_eq!(op, BinaryExprOp::Add);
    assert!(matches!(left.kind, ExprKind::Column { index: 0 }));
    assert!(matches!(right.kind, ExprKind::Literal(LogicalLiteral::Int64(3))));
}

#[test]
fn emits_filter_and_projection_steps_without_registers() {
    let schema = schema();
    let compiler = ExpressionCompiler::new(&schema).unwrap();
    let predicate = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(schema.fields[0].clone())),
        op: parser::ast::expr::BinaryOp::Eq,
        right: Box::new(LogicalExpr::Column(schema.fields[1].clone())),
        return_type: VeinDataType::Boolean,
    };

    assert!(matches!(compiler.filter(&predicate).unwrap(), ExpressionStep::Filter(_)));
    assert!(matches!(compiler.projection(&[predicate]).unwrap(), ExpressionStep::Projection(_)));
}

#[test]
fn rejects_missing_or_ambiguous_column_reference() {
    let schema = VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(ColumnId(1), "value", None, VeinDataType::Int64)),
        Arc::new(ResolvedColumn::new(ColumnId(2), "value", None, VeinDataType::Int64)),
    ]);
    let expression = LogicalExpr::Column(Arc::new(
        ResolvedColumn::new(ColumnId(99), "value", None, VeinDataType::Int64),
    ));
    let error = ExpressionCompiler::new(&schema).unwrap().compile(&expression).unwrap_err();
    assert!(error.to_string().contains("ambiguous"));
}
