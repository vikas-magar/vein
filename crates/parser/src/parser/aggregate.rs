use crate::parser::Parser;
use crate::token::TokenKind;

impl<'a> Parser<'a> {
    pub(crate) fn parse_pipe_aggregate(
        &mut self,
    ) -> Result<crate::ast::pipe_op::PipeOperator<'a>, super::ParseError> {
        self.expect(TokenKind::Aggregate)?;

        let mut aggregates = Vec::new();
        let mut grouping_sets = vec![Vec::new()];

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
            if let Ok(TokenKind::Grouping) = self.peek().map(|t| &t.kind) {
                self.next_token()?;
                self.expect(TokenKind::Sets)?;
                self.expect(TokenKind::LParen)?;
                grouping_sets.clear();

                loop {
                    self.expect(TokenKind::LParen)?;
                    let mut grouping_set = Vec::new();
                    if !matches!(self.peek().map(|token| &token.kind), Ok(TokenKind::RParen)) {
                        loop {
                            grouping_set.push(self.parse_grouping_expression()?);
                            if !matches!(self.peek().map(|token| &token.kind), Ok(TokenKind::Comma)) {
                                break;
                            }
                            self.next_token()?;
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                    grouping_sets.push(grouping_set);

                    if !matches!(self.peek().map(|token| &token.kind), Ok(TokenKind::Comma)) {
                        break;
                    }
                    self.next_token()?;
                }
                self.expect(TokenKind::RParen)?;
            } else {
                let mut group_by = Vec::new();
                loop {
                    group_by.push(self.parse_grouping_expression()?);
                    if !matches!(self.peek().map(|token| &token.kind), Ok(TokenKind::Comma)) {
                        break;
                    }
                    self.next_token()?;
                }
                grouping_sets = vec![group_by];
            }
        }

        Ok(crate::ast::pipe_op::PipeOperator::Aggregate {
            grouping_sets,
            aggregates,
        })
    }

    fn parse_grouping_expression(
        &mut self,
    ) -> Result<crate::ast::pipe_op::OrderByExpr<'a>, super::ParseError> {
        let expr = self.parse_expr()?;
        let alias = if let Ok(TokenKind::As) = self.peek().map(|token| &token.kind) {
            self.next_token()?;
            Some(self.expect_ident()?)
        } else {
            None
        };
        let expr = match alias {
            Some(alias) => crate::ast::expr::SpannedExpr {
                span: expr.span,
                node: crate::ast::expr::Expr::Alias {
                    expr: Box::new(expr),
                    alias,
                },
            },
            None => expr,
        };
        self.parse_order_suffix(expr)
    }
}

#[cfg(test)]

mod tests {
    use crate::ast::pipe_op::PipeOperator;
    use crate::parser::Parser;

    fn parse_aggregate(input: &str) -> PipeOperator<'_> {
        let mut parser = Parser::new(input);
        parser.parse_pipe_aggregate().expect("test should succeed")
    }

    #[test]
    fn test_aggregate_no_group_by() {
        let op = parse_aggregate("AGGREGATE sum(x) AS s, count(*)");
        if let PipeOperator::Aggregate {
            grouping_sets,
            aggregates,
        } = op
        {
            assert_eq!(grouping_sets, vec![vec![]], "global aggregate has an empty grouping set");
            assert_eq!(aggregates.len(), 2);
            assert_eq!(aggregates[0].alias, Some("s"));
            assert_eq!(aggregates[1].alias, None);
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn test_aggregate_with_group_by() {
        let op = parse_aggregate("AGGREGATE sum(x) AS s GROUP BY y AS year, z");
        if let PipeOperator::Aggregate {
            grouping_sets,
            aggregates,
        } = op
        {
            assert_eq!(aggregates.len(), 1);
            assert_eq!(aggregates[0].alias, Some("s"));

            assert_eq!(grouping_sets.len(), 1);
            assert_eq!(grouping_sets[0].len(), 2, "Should have 2 grouping expressions");
            // y AS year is an alias expression
            assert!(
                matches!(grouping_sets[0][0].expr.node, crate::ast::expr::Expr::Alias { alias, .. } if alias == "year")
            );
        } else {
            panic!("Expected Aggregate");
        }
    }

    #[test]
    fn test_aggregate_with_multiple_grouping_sets() {
        let op = parse_aggregate(
            "AGGREGATE sum(x) AS s GROUP BY GROUPING SETS ((y AS year, z), (y), ())",
        );
        let PipeOperator::Aggregate { grouping_sets, .. } = op else {
            panic!("Expected Aggregate");
        };

        assert_eq!(grouping_sets.len(), 3);
        assert_eq!(grouping_sets[0].len(), 2);
        assert_eq!(grouping_sets[1].len(), 1);
        assert!(grouping_sets[2].is_empty());
        assert!(matches!(
            grouping_sets[0][0].expr.node,
            crate::ast::expr::Expr::Alias { alias, .. } if alias == "year"
        ));
    }

    #[test]
    fn test_aggregate_rejects_empty_grouping_sets_clause() {
        let mut parser = Parser::new("AGGREGATE count(*) GROUP BY GROUPING SETS ()");
        assert!(parser.parse_pipe_aggregate().is_err());
    }
}
