use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_set_op(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        let kind = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Union) => {
                self.next_token()?;
                crate::ast::query::SetOpKind::Union
            }
            Ok(TokenKind::Intersect) => {
                self.next_token()?;
                crate::ast::query::SetOpKind::Intersect
            }
            Ok(TokenKind::Except) => {
                self.next_token()?;
                crate::ast::query::SetOpKind::Except
            }
            _ => {
                return Err(super::ParseError::Expected {
                    expected: "UNION, INTERSECT, or EXCEPT".into(),
                    found: format!(
                        "{:?}",
                        self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)
                    ),
                    span: self.peek_span().unwrap_or_default(),
                });
            }
        };
        let all = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::All) => {
                self.next_token()?;
                true
            }
            Ok(TokenKind::Distinct) => {
                self.next_token()?;
                false
            }
            _ => false,
        };
        self.expect(TokenKind::LParen)?;
        let right = self.parse_pipe_query()?;
        self.expect(TokenKind::RParen)?;
        Ok(crate::ast::pipe_op::PipeOperator::SetOp {
            kind,
            right: Box::new(right),
            all,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_union() {
        let q =
            parse_query("FROM t |> UNION ALL (FROM u |> SELECT 1)").expect("test should succeed");
        assert!(matches!(q.pipes[0], PipeOperator::SetOp { .. }));
    }

    #[test]
    fn test_intersect() {
        let q = parse_query("FROM t |> INTERSECT DISTINCT (FROM u |> SELECT 1)")
            .expect("test should succeed");
        assert!(matches!(q.pipes[0], PipeOperator::SetOp { .. }));
    }
}
