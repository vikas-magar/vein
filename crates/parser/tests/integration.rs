#![allow(clippy::unwrap_used)]
#![allow(missing_docs)]

use parser::ast::expr::Expr;
use parser::ast::pipe_op::PipeOperator;
use parser::ast::query::{Query, TableSource};
use parser::parser::{ParseError, Parser};

fn parse_query(input: &str) -> Result<Query, ParseError> {
    Parser::new(input).parse_query()
}

fn parse_expr(input: &str) -> Result<parser::ast::expr::SpannedExpr, ParseError> {
    let wrapped = format!("FROM t |> SELECT {}", input);
    let query = parse_query(&wrapped)?;
    match query.pipes.into_iter().next() {
        Some(PipeOperator::Select { mut items }) => Ok(items.remove(0).expr),
        Some(other) => panic!("expected Select, got {:?}", other),
        None => panic!("expected at least one pipe operator"),
    }
}

#[test]
fn test_object_store_source() {
    let q = parse_query("FROM 's3://bucket/data.csv' WITH (access_key = 'abc', region = 'us-east-1') AS t |> SELECT *").unwrap();
    if let TableSource::ObjectStore { url, inline_options, alias } = &q.source {
        assert_eq!(url, "s3://bucket/data.csv");
        assert_eq!(inline_options.len(), 2);
        assert_eq!(inline_options[0], ("access_key".into(), "abc".into()));
        assert_eq!(inline_options[1], ("region".into(), "us-east-1".into()));
        assert_eq!(alias.as_deref(), Some("t"));
    } else {
        panic!("expected ObjectStore");
    }
}

#[test]
fn test_multipart_table_name() {
    let q = parse_query("FROM aws_prod.customers t |> SELECT *").unwrap();
    if let TableSource::Table { name, alias } = &q.source {
        assert_eq!(name, "aws_prod.customers");
        assert_eq!(alias.as_deref(), Some("t"));
    } else {
        panic!("expected Table");
    }
}

#[test]
fn test_subquery_source() {
    let q = parse_query("FROM (FROM t |> SELECT id) AS sub |> SELECT *").unwrap();
    if let TableSource::Subquery { query, alias } = &q.source {
        assert_eq!(alias.as_deref(), Some("sub"));
        assert_eq!(query.pipes.len(), 1);
    } else {
        panic!("expected Subquery");
    }
}

#[test]
fn test_missing_from_error() {
    assert!(parse_query("SELECT name").is_err());
}

#[test]
fn test_multi_pipe() {
    let q = parse_query(
        "FROM users |> SELECT name, age |> WHERE age > 18 |> ORDER age DESC |> LIMIT 10",
    )
    .unwrap();
    assert_eq!(q.pipes.len(), 4);
    assert!(matches!(q.pipes[0], PipeOperator::Select { .. }));
    assert!(matches!(q.pipes[1], PipeOperator::Where { .. }));
    assert!(matches!(q.pipes[2], PipeOperator::OrderBy { .. }));
    assert!(matches!(q.pipes[3], PipeOperator::Limit { .. }));
}

#[test]
fn test_aggregate() {
    let q = parse_query("FROM t |> AGGREGATE count(*) AS cnt, sum(salary)").unwrap();
    if let PipeOperator::Aggregate { aggregates, .. } = &q.pipes[0] {
        assert_eq!(aggregates.len(), 2);
        assert_eq!(aggregates[0].alias, Some("cnt".into()));
        assert!(aggregates[1].alias.is_none());
    } else {
        panic!("expected Aggregate");
    }
}

#[test]
fn test_window_frame_rows() {
    let expr = parse_expr("sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)").unwrap();
    assert!(matches!(expr.node, Expr::WindowFunction { .. }));
}

#[test]
fn test_window_frame_unbounded() {
    let expr = parse_expr("sum(x) OVER (ROWS UNBOUNDED PRECEDING)").unwrap();
    assert!(matches!(expr.node, Expr::WindowFunction { .. }));
}

#[test]
fn test_window_frame_current_row() {
    let expr = parse_expr("sum(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)").unwrap();
    assert!(matches!(expr.node, Expr::WindowFunction { .. }));
}

#[test]
fn test_complex_pipeline() {
    let q = parse_query(
        "FROM sales \
         |> WHERE amount > 0 \
         |> SELECT region, amount, date \
         |> EXTEND amount * 1.1 AS adjusted \
         |> AGGREGATE sum(adjusted) AS total, count(*) AS cnt \
         |> ORDER total DESC \
         |> LIMIT 5",
    )
    .unwrap();
    assert_eq!(q.pipes.len(), 6);
}

#[test]
fn test_recursion_limit() {
    let mut parser = Parser::new("FROM t |> SELECT 1").with_recursion_limit(0);
    let err = parser.parse_query().unwrap_err();
    assert!(matches!(err, ParseError::RecursionLimitExceeded { .. }));
}

#[test]
fn test_invalid_token() {
    let err = parse_query("FROM t |> SELECT @").unwrap_err();
    assert!(matches!(err, ParseError::Lexical(_)));
}

#[test]
fn test_incomplete_pipe() {
    let err = parse_query("FROM t |>").unwrap_err();
    assert!(matches!(err, ParseError::Expected { .. }));
}

#[test]
fn test_scalar_subquery() {
    let expr = parse_expr("(FROM t |> SELECT count(*))").unwrap();
    assert!(matches!(expr.node, Expr::ScalarSubquery(_)));
}

#[test]
fn test_exists_subquery() {
    let expr = parse_expr("EXISTS (FROM t |> SELECT 1)").unwrap();
    assert!(matches!(expr.node, Expr::Exists { negated: false, .. }));
}

#[test]
fn test_not_exists_subquery() {
    let expr = parse_expr("NOT EXISTS (FROM t |> SELECT 1)").unwrap();
    assert!(matches!(expr.node, Expr::Exists { negated: true, .. }));
}

#[test]
fn test_typed_string() {
    let expr = parse_expr("DATE '2024-01-01'").unwrap();
    assert!(matches!(expr.node, Expr::TypedString { .. }));
}

#[test]
fn test_interval_literal() {
    let expr = parse_expr("INTERVAL '1 day'").unwrap();
    assert!(matches!(expr.node, Expr::TypedString { data_type: parser::ast::data_type::DataType::Interval { .. }, .. }));
}

#[test]
fn test_in_subquery() {
    let expr = parse_expr("x IN (FROM t |> SELECT id)").unwrap();
    assert!(matches!(expr.node, Expr::InList { .. }));
}

#[test]
fn test_not_in_subquery() {
    let expr = parse_expr("x NOT IN (FROM t |> SELECT id)").unwrap();
    assert!(matches!(expr.node, Expr::InList { negated: true, .. }));
}

#[test]
fn test_subquery_in_where() {
    let q = parse_query("FROM t |> WHERE EXISTS (FROM u |> SELECT 1)").unwrap();
    assert_eq!(q.pipes.len(), 1);
}

