use super::*;
use crate::ast::expr::{BinaryOp, Literal};
use error::SourceSpan;

#[test]
fn test_column_collector_recursive() {
    use crate::ast::expr::SpannedExpr;
    let c = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::Column {
            table: None,
            column: "c",
        },
    };
    let b = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::Column {
            table: None,
            column: "b",
        },
    };
    let a = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::Column {
            table: None,
            column: "a",
        },
    };
    let ab = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::BinaryOp {
            left: Box::new(a),
            op: BinaryOp::Plus,
            right: Box::new(b),
        },
    };
    let abc = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::BinaryOp {
            left: Box::new(ab),
            op: BinaryOp::Plus,
            right: Box::new(c),
        },
    };

    let mut collector = ColumnCollector { columns: vec![] };
    abc.node
        .accept(&mut collector)
        .expect("test should succeed");

    assert_eq!(collector.columns.len(), 3);
    assert_eq!(collector.columns[0].1, "a");
    assert_eq!(collector.columns[1].1, "b");
    assert_eq!(collector.columns[2].1, "c");
}

#[test]
fn test_inlist_visits_children() {
    use crate::ast::expr::SpannedExpr;
    let x = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::Column {
            table: None,
            column: "x",
        },
    };
    let y = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::Column {
            table: None,
            column: "y",
        },
    };
    let one = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::Literal(Literal::Int(1)),
    };
    let inlist = SpannedExpr {
        span: SourceSpan::default(),
        node: Expr::InList {
            expr: Box::new(x),
            list: vec![one, y],
            negated: false,
        },
    };

    let mut collector = ColumnCollector { columns: vec![] };
    inlist
        .node
        .accept(&mut collector)
        .expect("test should succeed");

    assert_eq!(collector.columns.len(), 2);
    assert_eq!(collector.columns[0].1, "x");
    assert_eq!(collector.columns[1].1, "y");
}
