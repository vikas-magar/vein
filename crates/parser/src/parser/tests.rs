#[cfg(test)]
mod tests {
    use crate::ast::expr::Expr;
    use crate::ast::pipe_op::PipeOperator;
    use crate::ast::query::{Query, TableSource};
    use crate::parser::{ParseError, Parser};
    use crate::token::{Span, SpannedToken, TokenKind};

    fn parse_query(input: &str) -> Result<Query, ParseError> {
        Parser::new(input).parse_query()
    }

    fn parse_expr(input: &str) -> Result<Expr, ParseError> {
        // Wrap expression in a minimal select to parse it
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
    fn test_minimal_select() {
        let q = parse_query("FROM users |> SELECT name").unwrap();
        assert_eq!(q.source, TableSource::Table { name: "users".into(), alias: None });
        assert_eq!(q.pipes.len(), 1);
    }

    #[test]
    fn test_missing_from_error() {
        assert!(parse_query("SELECT name").is_err());
    }

    #[test]
    fn test_select_multiple_columns() {
        let q = parse_query("FROM employees |> SELECT name, age, salary").unwrap();
        assert_eq!(q.pipes.len(), 1);
        if let PipeOperator::Select { items } = &q.pipes[0] {
            assert_eq!(items.len(), 3);
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_select_with_alias() {
        let q = parse_query("FROM t |> SELECT name AS full_name").unwrap();
        if let PipeOperator::Select { items } = &q.pipes[0] {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].alias, Some("full_name".into()));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_where_clause() {
        let q = parse_query("FROM t |> WHERE age > 18").unwrap();
        assert_eq!(q.pipes.len(), 1);
        assert!(matches!(q.pipes[0], PipeOperator::Where { .. }));
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
    fn test_order_by_desc() {
        let q = parse_query("FROM t |> ORDER age DESC").unwrap();
        if let PipeOperator::OrderBy { order_bys } = &q.pipes[0] {
            assert_eq!(order_bys.len(), 1);
            assert!(!order_bys[0].asc);
        } else {
            panic!("expected OrderBy");
        }
    }

    #[test]
    fn test_order_by_nulls_first() {
        let q = parse_query("FROM t |> ORDER age DESC NULLS FIRST").unwrap();
        if let PipeOperator::OrderBy { order_bys } = &q.pipes[0] {
            assert!(order_bys[0].nulls_first);
        } else {
            panic!("expected OrderBy");
        }
    }

    #[test]
    fn test_limit_only() {
        let q = parse_query("FROM t |> LIMIT 5").unwrap();
        if let PipeOperator::Limit { limit, offset } = q.pipes[0] {
            assert_eq!(limit, 5);
            assert_eq!(offset, 0);
        } else {
            panic!("expected Limit");
        }
    }

    #[test]
    fn test_limit_with_offset() {
        let q = parse_query("FROM t |> LIMIT 10 OFFSET 20").unwrap();
        if let PipeOperator::Limit { limit, offset } = q.pipes[0] {
            assert_eq!(limit, 10);
            assert_eq!(offset, 20);
        } else {
            panic!("expected Limit");
        }
    }

    #[test]
    fn test_distinct() {
        let q = parse_query("FROM t |> DISTINCT").unwrap();
        assert_eq!(q.pipes[0], PipeOperator::Distinct);
    }

    #[test]
    fn test_extend() {
        let q = parse_query("FROM t |> EXTEND salary * 2 AS bonus").unwrap();
        if let PipeOperator::Extend { items } = &q.pipes[0] {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].alias, Some("bonus".into()));
        } else {
            panic!("expected Extend");
        }
    }

    #[test]
    fn test_set() {
        let q = parse_query("FROM t |> SET salary = 0").unwrap();
        if let PipeOperator::Set { assignments } = &q.pipes[0] {
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].0, "salary");
        } else {
            panic!("expected Set");
        }
    }

    #[test]
    fn test_drop() {
        let q = parse_query("FROM t |> DROP temp_col").unwrap();
        if let PipeOperator::Drop { columns } = &q.pipes[0] {
            assert_eq!(columns, &["temp_col"]);
        } else {
            panic!("expected Drop");
        }
    }

    #[test]
    fn test_rename() {
        let q = parse_query("FROM t |> RENAME old_name AS new_name").unwrap();
        if let PipeOperator::Rename { renames } = &q.pipes[0] {
            assert_eq!(renames.len(), 1);
            assert_eq!(renames[0], ("old_name".into(), "new_name".into()));
        } else {
            panic!("expected Rename");
        }
    }

    #[test]
    fn test_as_alias() {
        let q = parse_query("FROM t |> AS my_view").unwrap();
        assert_eq!(q.pipes[0], PipeOperator::As { alias: "my_view".into() });
    }

    #[test]
    fn test_call() {
        let q = parse_query("FROM t |> CALL my_func(1, 'x')").unwrap();
        if let PipeOperator::Call { name, args } = &q.pipes[0] {
            assert_eq!(name, "my_func");
            assert_eq!(args.len(), 2);
        } else {
            panic!("expected Call");
        }
    }

    #[test]
    fn test_assert_simple() {
        let q = parse_query("FROM t |> ASSERT age >= 0").unwrap();
        assert!(matches!(q.pipes[0], PipeOperator::Assert { .. }));
    }

    #[test]
    fn test_write() {
        let q = parse_query("FROM t |> WRITE results_table").unwrap();
        assert!(matches!(q.pipes[0], PipeOperator::Write { .. }));
    }

    #[test]
    fn test_tablesample() {
        let q = parse_query("FROM t |> TABLESAMPLE bernoulli(25.0)").unwrap();
        if let PipeOperator::TableSample { method, percent } = &q.pipes[0] {
            assert_eq!(method, "bernoulli");
            assert_eq!(*percent, 25.0);
        } else {
            panic!("expected TableSample");
        }
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
    fn test_join() {
        let q = parse_query("FROM t |> JOIN other ON t.id = other.id").unwrap();
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }));

        let q = parse_query("FROM t |> LEFT JOIN other AS o ON t.id = o.id").unwrap();
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }));

        let q = parse_query("FROM t |> CROSS JOIN other").unwrap();
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }));

        let q = parse_query("FROM t |> JOIN other USING (id)").unwrap();
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }));
    }

    #[test]
    fn test_pivot_unsupported() {
        let err = parse_query("FROM t |> PIVOT ...").unwrap_err();
        assert!(err.to_string().contains("Unsupported"));
    }

    #[test]
    fn test_with_cte() {
        let q = parse_query("FROM t |> WITH x AS (FROM y |> SELECT 1)").unwrap();
        if let PipeOperator::With { ctes } = &q.pipes[0] {
            assert_eq!(ctes.len(), 1);
            assert_eq!(ctes[0].alias, "x");
        } else {
            panic!("expected With");
        }
    }

    #[test]
    fn test_union() {
        let q = parse_query("FROM t |> UNION ALL (FROM u |> SELECT 1)").unwrap();
        assert!(matches!(q.pipes[0], PipeOperator::SetOp { .. }));
    }

    #[test]
    fn test_intersect() {
        let q = parse_query("FROM t |> INTERSECT DISTINCT (FROM u |> SELECT 1)").unwrap();
        assert!(matches!(q.pipes[0], PipeOperator::SetOp { .. }));
    }

    #[test]
    fn test_window_frame_rows() {
        let expr = parse_expr("sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)").unwrap();
        assert!(matches!(expr, Expr::WindowFunction { .. }));
    }

    #[test]
    fn test_window_frame_unbounded() {
        let expr = parse_expr("sum(x) OVER (ROWS UNBOUNDED PRECEDING)").unwrap();
        assert!(matches!(expr, Expr::WindowFunction { .. }));
    }

    #[test]
    fn test_window_frame_current_row() {
        let expr = parse_expr("sum(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)").unwrap();
        assert!(matches!(expr, Expr::WindowFunction { .. }));
    }

    // Expression parsing tests

    #[test]
    fn test_expr_binary_op() {
        let expr = parse_expr("1 + 2").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: crate::ast::expr::BinaryOp::Plus, .. }));
    }

    #[test]
    fn test_expr_mul_precedence() {
        let expr = parse_expr("1 + 2 * 3").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: crate::ast::expr::BinaryOp::Plus, .. }));
    }

    #[test]
    fn test_expr_parentheses() {
        let expr = parse_expr("(1 + 2) * 3").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: crate::ast::expr::BinaryOp::Multiply, .. }));
    }

    #[test]
    fn test_expr_is_null() {
        let expr = parse_expr("name IS NULL").unwrap();
        assert!(matches!(expr, Expr::UnaryOp { op: crate::ast::expr::UnaryOp::IsNull, .. }));
    }

    #[test]
    fn test_expr_in_list() {
        let expr = parse_expr("x IN (1, 2, 3)").unwrap();
        assert!(matches!(expr, Expr::InList { .. }));
    }

    #[test]
    fn test_expr_between() {
        let expr = parse_expr("x BETWEEN 1 AND 10").unwrap();
        assert!(matches!(expr, Expr::Between { .. }));
    }

    #[test]
    fn test_expr_like() {
        let expr = parse_expr("name LIKE '%foo%'").unwrap();
        assert!(matches!(expr, Expr::Like { negated: false, .. }));

        let expr_not = parse_expr("name NOT LIKE '%foo%'").unwrap();
        assert!(matches!(expr_not, Expr::Like { negated: true, .. }));
    }

    #[test]
    fn test_expr_not() {
        let expr = parse_expr("NOT active").unwrap();
        assert!(matches!(expr, Expr::UnaryOp { op: crate::ast::expr::UnaryOp::Not, .. }));
    }

    #[test]
    fn test_expr_negate() {
        let expr = parse_expr("-42").unwrap();
        assert!(matches!(expr, Expr::UnaryOp { op: crate::ast::expr::UnaryOp::Negate, .. }));
    }

    #[test]
    fn test_expr_function() {
        let expr = parse_expr("count(*)").unwrap();
        assert!(matches!(expr, Expr::Function { .. }));
    }

    #[test]
    fn test_expr_qualified_column() {
        let expr = parse_expr("t.name").unwrap();
        assert!(matches!(expr, Expr::Column { table: Some(_), .. }));
    }

    #[test]
    fn test_expr_wildcard() {
        let expr = parse_expr("*").unwrap();
        assert!(matches!(expr, Expr::Wildcard { .. }));
    }

    #[test]
    fn test_expr_cast() {
        let expr = parse_expr("CAST(age AS bigint)").unwrap();
        assert!(matches!(expr, Expr::Cast { .. }));
    }

    #[test]
    fn test_expr_string_concat() {
        let expr = parse_expr("a || ' ' || b").unwrap();
        assert!(matches!(expr, Expr::BinaryOp { op: crate::ast::expr::BinaryOp::Concat, .. }));
    }

    #[test]
    fn test_expr_case() {
        let expr = parse_expr("CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END").unwrap();
        assert!(matches!(expr, Expr::Case { .. }));
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

    // Subquery / EXISTS / TypedString tests

    #[test]
    fn test_scalar_subquery() {
        let expr = parse_expr("(FROM t |> SELECT count(*))").unwrap();
        assert!(matches!(expr, Expr::ScalarSubquery(_, _)));
    }

    #[test]
    fn test_exists_subquery() {
        let expr = parse_expr("EXISTS (FROM t |> SELECT 1)").unwrap();
        assert!(matches!(expr, Expr::Exists { negated: false, .. }));
    }

    #[test]
    fn test_not_exists_subquery() {
        let expr = parse_expr("NOT EXISTS (FROM t |> SELECT 1)").unwrap();
        assert!(matches!(expr, Expr::Exists { negated: true, .. }));
    }

    #[test]
    fn test_typed_string() {
        let expr = parse_expr("DATE '2024-01-01'").unwrap();
        assert!(matches!(expr, Expr::TypedString { .. }));
    }

    #[test]
    fn test_interval_literal() {
        let expr = parse_expr("INTERVAL '1 day'").unwrap();
        assert!(matches!(expr, Expr::TypedString { data_type: crate::ast::data_type::DataType::Interval { .. }, .. }));
    }

    #[test]
    fn test_in_subquery() {
        let expr = parse_expr("x IN (FROM t |> SELECT id)").unwrap();
        assert!(matches!(expr, Expr::InList { .. }));
    }

    #[test]
    fn test_not_in_subquery() {
        let expr = parse_expr("x NOT IN (FROM t |> SELECT id)").unwrap();
        assert!(matches!(expr, Expr::InList { negated: true, .. }));
    }

    #[test]
    fn test_subquery_in_where() {
        let q = parse_query("FROM t |> WHERE EXISTS (FROM u |> SELECT 1)").unwrap();
        assert_eq!(q.pipes.len(), 1);
    }

    // from_tokens tests

    fn spanned(kind: TokenKind, text: &str) -> SpannedToken {
        SpannedToken { kind, text: text.into(), span: Span::default() }
    }

    #[test]
    fn test_from_tokens_simple_query() {
        let tokens = vec![
            spanned(TokenKind::From, "FROM"),
            spanned(TokenKind::Ident, "users"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Select, "SELECT"),
            spanned(TokenKind::Ident, "name"),
        ];
        let mut parser = Parser::from_tokens(tokens);
        let q = parser.parse_query().unwrap();
        assert_eq!(q.source, TableSource::Table { name: "users".into(), alias: None });
        assert_eq!(q.pipes.len(), 1);
    }

    #[test]
    fn test_from_tokens_not_in() {
        let tokens = vec![
            spanned(TokenKind::From, "FROM"),
            spanned(TokenKind::Ident, "t"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Select, "SELECT"),
            spanned(TokenKind::Ident, "x"),
            spanned(TokenKind::Comma, ","),
            spanned(TokenKind::Ident, "y"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Where, "WHERE"),
            spanned(TokenKind::Ident, "x"),
            spanned(TokenKind::Not, "NOT"),
            spanned(TokenKind::In, "IN"),
            spanned(TokenKind::LParen, "("),
            spanned(TokenKind::Number, "1"),
            spanned(TokenKind::Comma, ","),
            spanned(TokenKind::Number, "2"),
            spanned(TokenKind::RParen, ")"),
        ];
        let mut parser = Parser::from_tokens(tokens);
        let q = parser.parse_query().unwrap();
        assert_eq!(q.pipes.len(), 2);
        assert!(matches!(q.pipes[1], PipeOperator::Where { .. }));
    }

    #[test]
    fn test_from_tokens_not_exists() {
        let tokens = vec![
            spanned(TokenKind::From, "FROM"),
            spanned(TokenKind::Ident, "t"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Where, "WHERE"),
            spanned(TokenKind::Not, "NOT"),
            spanned(TokenKind::Exists, "EXISTS"),
            spanned(TokenKind::LParen, "("),
            spanned(TokenKind::From, "FROM"),
            spanned(TokenKind::Ident, "u"),
            spanned(TokenKind::Pipe, "|>"),
            spanned(TokenKind::Select, "SELECT"),
            spanned(TokenKind::Number, "1"),
            spanned(TokenKind::RParen, ")"),
        ];
        let mut parser = Parser::from_tokens(tokens);
        let q = parser.parse_query().unwrap();
        assert_eq!(q.pipes.len(), 1);
    }
}
