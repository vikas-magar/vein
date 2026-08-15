use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_join(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        let join_type = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Left) => {
                self.next_token()?;
                self.expect(TokenKind::Join)?;
                crate::ast::pipe_op::JoinType::Left
            }
            Ok(TokenKind::Right) => {
                self.next_token()?;
                self.expect(TokenKind::Join)?;
                crate::ast::pipe_op::JoinType::Right
            }
            Ok(TokenKind::Inner) => {
                self.next_token()?;
                self.expect(TokenKind::Join)?;
                crate::ast::pipe_op::JoinType::Inner
            }
            Ok(TokenKind::Full) => {
                self.next_token()?;
                self.expect(TokenKind::Join)?;
                crate::ast::pipe_op::JoinType::Full
            }
            Ok(TokenKind::Cross) => {
                self.next_token()?;
                self.expect(TokenKind::Join)?;
                crate::ast::pipe_op::JoinType::Cross
            }
            Ok(TokenKind::Join) => {
                self.next_token()?;
                crate::ast::pipe_op::JoinType::Inner
            }
            _ => {
                return Err(super::ParseError::Expected {
                    expected: "JOIN or join type (LEFT, RIGHT, INNER, FULL, CROSS)".into(),
                    found: format!(
                        "{:?}",
                        self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)
                    ),
                    span: self.peek_span().unwrap_or_default(),
                });
            }
        };

        let right = self.parse_table_source()?;

        let (predicate, using) = if let Ok(TokenKind::On) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            let predicate = self.parse_expr()?;
            (Some(predicate), vec![])
        } else if let Ok(TokenKind::Using) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            self.expect(TokenKind::LParen)?;
            let using = self.parse_comma_separated(|p| p.expect_ident())?;
            self.expect(TokenKind::RParen)?;
            (None, using)
        } else if join_type == crate::ast::pipe_op::JoinType::Cross {
            (None, vec![])
        } else {
            return Err(super::ParseError::Expected {
                expected: "ON or USING".into(),
                found: format!(
                    "{:?}",
                    self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)
                ),
                span: self.peek_span().unwrap_or_default(),
            });
        };

        Ok(crate::ast::pipe_op::PipeOperator::Join {
            join: crate::ast::pipe_op::JoinClause {
                join_type,
                right: Box::new(crate::ast::query::Query {
                    source: right,
                    pipes: vec![],
                }),
                predicate,
                using,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_join() {
        let q =
            parse_query("FROM t |> JOIN other ON t.id = other.id").expect("test should succeed");
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }));

        let q = parse_query("FROM t |> LEFT JOIN other AS o ON t.id = o.id")
            .expect("test should succeed");
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }));

        let q = parse_query("FROM t |> CROSS JOIN other").expect("test should succeed");
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }));

        let q = parse_query("FROM t |> JOIN other USING (id)").expect("test should succeed");
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }));
    }
}
