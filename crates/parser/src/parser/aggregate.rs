use crate::parser::Parser;
use crate::token::TokenKind;

impl Parser {
    pub(crate) fn parse_pipe_aggregate(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator, super::ParseError> {
        self.expect(TokenKind::Aggregate)?;

        let mut aggregates = Vec::new();
        let mut group_by = Vec::new();

        // 1. Parse the aggregate expressions
        // Stop if we see GROUP BY, ORDER BY, or the next Pipe (|>)
        while let Ok(kind) = self.peek().map(|t| &t.kind) {
            if matches!(
                kind,
                TokenKind::Group | TokenKind::Order | TokenKind::Pipe | TokenKind::EOF
            ) {
                break;
            }

            let expr = self.parse_expr()?;
            let alias = if let Ok(TokenKind::As) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
                Some(self.expect_ident()?)
            } else {
                None
            };

            aggregates.push(crate::ast::pipe_op::AggregateExpr { expr, alias });

            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }

        // 2. Parse the optional GROUP BY clause
        if let Ok(TokenKind::Group) = self.peek().map(|t| &t.kind) {
            self.next_token()?; // consume GROUP
            self.expect(TokenKind::By)?;
            loop {
                let expr = self.parse_expr()?;
                let alias = if let Ok(TokenKind::As) = self.peek().map(|t| &t.kind) {
                    self.next_token()?;
                    Some(self.expect_ident()?)
                } else {
                    None
                };

                let final_expr = match alias {
                    Some(a) => crate::ast::expr::SpannedExpr {
                        span: expr.span,
                        node: crate::ast::expr::Expr::Alias {
                            expr: Box::new(expr),
                            alias: a,
                        },
                    },
                    None => expr,
                };

                let mut asc = true;
                if let Ok(TokenKind::Asc) = self.peek().map(|t| &t.kind) {
                    self.next_token()?;
                } else if let Ok(TokenKind::Desc) = self.peek().map(|t| &t.kind) {
                    asc = false;
                    self.next_token()?;
                }
                
                let mut nulls_first = !asc;
                if let Ok(TokenKind::Nulls) = self.peek().map(|t| &t.kind) {
                    self.next_token()?;
                    if let Ok(TokenKind::First) = self.peek().map(|t| &t.kind) {
                        nulls_first = true;
                        self.next_token()?;
                    } else if let Ok(TokenKind::Last) = self.peek().map(|t| &t.kind) {
                        nulls_first = false;
                        self.next_token()?;
                    } else {
                        return Err(super::ParseError::Unsupported("Expected FIRST or LAST after NULLS".to_string()));
                    }
                }

                group_by.push(crate::ast::pipe_op::OrderByExpr {
                    expr: final_expr,
                    asc,
                    nulls_first,
                });

                match self.peek().map(|t| &t.kind) {
                    Ok(TokenKind::Comma) => {
                        self.next_token()?;
                    }
                    _ => break,
                }
            }
        }

        Ok(crate::ast::pipe_op::PipeOperator::Aggregate {
            group_by,
            aggregates,
        })
    }
}

#[cfg(test)]

mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::Parser;

    fn parse_aggregate(input: &str) -> PipeOperator {
        let mut parser = Parser::new(input);
        parser.parse_pipe_aggregate().unwrap()
    }

    #[test]
    fn test_aggregate_no_group_by() {
        let op = parse_aggregate("AGGREGATE sum(x) AS s, count(*)");
        if let PipeOperator::Aggregate {
            group_by,
            aggregates,
        } = op
        {
            assert!(group_by.is_empty(), "group_by should be empty");
            assert_eq!(aggregates.len(), 2);
            assert_eq!(aggregates[0].alias.as_deref(), Some("s"));
            assert_eq!(aggregates[1].alias.as_deref(), None);
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn test_aggregate_with_group_by() {
        let op = parse_aggregate("AGGREGATE sum(x) AS s GROUP BY y AS year, z");
        if let PipeOperator::Aggregate {
            group_by,
            aggregates,
        } = op
        {
            assert_eq!(aggregates.len(), 1);
            assert_eq!(aggregates[0].alias.as_deref(), Some("s"));

            assert_eq!(group_by.len(), 2, "Should have 2 group_by expressions");
            // y AS year is an alias expression
            assert!(
                matches!(group_by[0].expr.node, crate::ast::expr::Expr::Alias { ref alias, .. } if alias == "year")
            );
        } else {
            panic!("Expected Aggregate");
        }
    }
}
