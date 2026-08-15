use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_select(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Select)?;
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let alias = if let Ok(TokenKind::As) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
                Some(self.expect_ident()?)
            } else {
                None
            };
            items.push(crate::ast::pipe_op::SelectItem { expr, alias });
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        Ok(crate::ast::pipe_op::PipeOperator::Select { items })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::ast::query::TableSource;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_minimal_select() {
        let q = parse_query("FROM users |> SELECT name").expect("test should succeed");
        assert_eq!(
            q.source,
            TableSource::Table {
                name: "users".into(),
                alias: None
            }
        );
        assert_eq!(q.pipes.len(), 1);
    }

    #[test]
    fn test_select_multiple_columns() {
        let q =
            parse_query("FROM employees |> SELECT name, age, salary").expect("test should succeed");
        assert_eq!(q.pipes.len(), 1);
        if let PipeOperator::Select { items } = &q.pipes[0] {
            assert_eq!(items.len(), 3);
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn test_select_with_alias() {
        let q = parse_query("FROM t |> SELECT name AS full_name").expect("test should succeed");
        if let PipeOperator::Select { items } = &q.pipes[0] {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].alias, Some("full_name".into()));
        } else {
            panic!("expected Select");
        }
    }
}
