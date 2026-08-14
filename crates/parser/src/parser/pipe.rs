use crate::token::TokenKind;
use crate::parser::Parser;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_operator(&mut self) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Select) => self.parse_pipe_select(),
            Ok(TokenKind::Where) => self.parse_pipe_where(),
            Ok(TokenKind::Aggregate) => self.parse_pipe_aggregate(),
            Ok(TokenKind::Order) => self.parse_pipe_order_by(),
            Ok(TokenKind::Limit) => self.parse_pipe_limit(),
            Ok(TokenKind::Distinct) => self.parse_pipe_distinct(),
            Ok(TokenKind::Extend) => self.parse_pipe_extend(),
            Ok(TokenKind::Set) => self.parse_pipe_set(),
            Ok(TokenKind::Drop) => self.parse_pipe_drop(),
            Ok(TokenKind::Rename) => self.parse_pipe_rename(),
            Ok(TokenKind::As) => self.parse_pipe_as(),
            Ok(TokenKind::Call) => self.parse_pipe_call(),
            Ok(TokenKind::Assert) => self.parse_pipe_assert(),
            Ok(TokenKind::Write) => self.parse_pipe_write(),
            Ok(TokenKind::Join) | Ok(TokenKind::Left) | Ok(TokenKind::Right)
                | Ok(TokenKind::Inner) | Ok(TokenKind::Full) | Ok(TokenKind::Cross) => self.parse_pipe_join(),
            Ok(TokenKind::TableSample) => self.parse_pipe_tablesample(),
            Ok(TokenKind::Pivot) => self.parse_pipe_pivot(),
            Ok(TokenKind::Unpivot) => self.parse_pipe_unpivot(),
            Ok(TokenKind::With) => self.parse_pipe_with(),
            Ok(TokenKind::Fork) => self.parse_pipe_fork(),
            Ok(TokenKind::Tee) => self.parse_pipe_tee(),
            Ok(TokenKind::Union) | Ok(TokenKind::Intersect) | Ok(TokenKind::Except) => self.parse_pipe_set_op(),
            _ => Err(super::ParseError::Expected {
                expected: "pipe operator (SELECT, WHERE, AGGREGATE, etc.)".into(),
                found: format!("{:?}", self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)),
                span: self.peek_span().unwrap_or_default(),
            }),
        }
    }

    pub(crate) fn parse_pipe_fork(&mut self) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Fork)?;
        self.expect(TokenKind::LParen)?;
        let mut pipelines = Vec::new();
        loop {
            let mut pipeline = Vec::new();
            while let Ok(()) = self.expect_pipe() {
                pipeline.push(self.parse_pipe_operator()?);
            }
            pipelines.push(pipeline);
            if let Ok(TokenKind::Comma) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        Ok(crate::ast::pipe_op::PipeOperator::Fork { pipelines })
    }

    pub(crate) fn parse_pipe_tee(&mut self) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Tee)?;
        self.expect(TokenKind::LParen)?;
        let mut pipelines = Vec::new();
        loop {
            let mut pipeline = Vec::new();
            while let Ok(()) = self.expect_pipe() {
                pipeline.push(self.parse_pipe_operator()?);
            }
            pipelines.push(pipeline);
            if let Ok(TokenKind::Comma) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        Ok(crate::ast::pipe_op::PipeOperator::Tee { pipelines })
    }
}


#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

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
                assert_eq!(*method, "bernoulli");
                assert_eq!(*percent, 25.0);
            } else {
                panic!("expected TableSample");
            }
        }

    #[test]
        fn test_pivot_unsupported() {
            let err = parse_query("FROM t |> PIVOT ...").unwrap_err();
            assert!(err.to_string().contains("Unsupported"));
        }

    #[test]
        fn test_unpivot_unsupported() {
            let err = parse_query("FROM t |> UNPIVOT ...").unwrap_err();
            assert!(err.to_string().contains("Unsupported"));
        }

    #[test]
    fn test_parse_fork_positive() {
        let q = parse_query("FROM t |> FORK ( |> WHERE x = 1, |> SELECT y )").unwrap();
        if let PipeOperator::Fork { pipelines } = &q.pipes[0] {
            assert_eq!(pipelines.len(), 2);
            assert!(matches!(pipelines[0][0], PipeOperator::Where { .. }));
            assert!(matches!(pipelines[1][0], PipeOperator::Select { .. }));
        } else {
            panic!("Expected FORK");
        }
    }

    #[test]
    fn test_parse_fork_negative_missing_paren() {
        let err = parse_query("FROM t |> FORK |> WHERE x = 1").unwrap_err();
        assert!(err.to_string().contains("Expected LParen"));
    }

    #[test]
    fn test_parse_tee_positive() {
        let q = parse_query("FROM t |> TEE ( |> DROP x ) |> LIMIT 10").unwrap();
        assert_eq!(q.pipes.len(), 2);
        assert!(matches!(q.pipes[0], PipeOperator::Tee { .. }));
        assert!(matches!(q.pipes[1], PipeOperator::Limit { .. }));
    }


}
