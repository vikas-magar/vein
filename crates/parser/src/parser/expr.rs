use crate::ast::data_type::DataType;
use crate::ast::expr::{BinaryOp, Expr, Literal, SpannedExpr, UnaryOp};
use crate::ast::pipe_op::OrderByExpr;
use crate::parser::{ParseError, Parser, TokenKind};
use crate::token::Span;

/// Precedence table. Higher = tighter binding.
fn infix_prec(kind: &TokenKind) -> Option<(u32, bool)> {
    match kind {
        TokenKind::Or => Some((0, true)),
        TokenKind::And => Some((1, true)),
        TokenKind::Is | TokenKind::In | TokenKind::Between | TokenKind::Like => Some((3, true)),
        TokenKind::Eq
        | TokenKind::Neq
        | TokenKind::Lt
        | TokenKind::Gt
        | TokenKind::Leq
        | TokenKind::Geq => Some((4, true)),
        TokenKind::Concat => Some((5, true)),
        TokenKind::Plus | TokenKind::Minus => Some((6, true)),
        TokenKind::Star | TokenKind::Slash | TokenKind::Percent => Some((7, true)),
        _ => None,
    }
}

fn prefix_prec(kind: &TokenKind) -> Option<(u32, bool)> {
    match kind {
        TokenKind::Not => Some((2, true)),
        TokenKind::Minus | TokenKind::Plus => Some((8, true)),
        _ => None,
    }
}

type WindowSpec<'a> = (
    Vec<SpannedExpr<'a>>,
    Vec<OrderByExpr<'a>>,
    Option<crate::ast::pipe_op::WindowFrame>,
);

impl<'a> Parser<'a> {
    /// Parses a single (possibly parenthesized or operator-chained) expression.
    pub fn parse_expr(&mut self) -> Result<SpannedExpr<'a>, ParseError> {
        self.enter_recursive()?;
        let result = self.parse_binary(0);
        self.exit_recursive();
        result
    }

    fn parse_binary(&mut self, min_prec: u32) -> Result<SpannedExpr<'a>, ParseError> {
        let mut left = self.parse_unary()?;

        while let Ok(peek_kind) = self.peek().map(|t| t.kind) {
            // Handle NOT as special prefix within infix operators
            let mut postfix_op = None;
            if peek_kind == TokenKind::Not {
                let next = match self.peek_next_token_kind() {
                    Ok(k) => k,
                    Err(_) => break,
                };
                if next == TokenKind::In || next == TokenKind::Between || next == TokenKind::Like {
                    postfix_op = Some((next, true));
                }
            } else if peek_kind == TokenKind::In
                || peek_kind == TokenKind::Between
                || peek_kind == TokenKind::Like
                || peek_kind == TokenKind::Is
            {
                postfix_op = Some((peek_kind, false));
            }

            if let Some((op, negated)) = postfix_op {
                // Postfix operators (IN, BETWEEN, LIKE, IS) are only consumed at
                // this recursion level when their precedence allows it. Otherwise
                // they are left for an enclosing loop, e.g. `x = 1 IN (2)` parses
                // as `(x = 1) IN (2)` (IN binds like `=` in GoogleSQL).
                let prec = infix_prec(&op)
                    .expect("IN, BETWEEN, LIKE, IS always have a precedence")
                    .0;
                if prec < min_prec {
                    break;
                }
                if peek_kind == TokenKind::Not {
                    self.next_token()?; // consume NOT from peeked
                    self.next_token()?; // consume operator from lookahead
                } else {
                    self.next_token()?; // consume operator from peeked
                }
                // Postfix operators are applied and skipped so trailing AND/OR
                // terms are still parsed.
                left = self.parse_postfix_binary(left, op, Some(negated))?;
                continue;
            }

            match infix_prec(&peek_kind) {
                Some((prec, left_assoc)) if prec >= min_prec => {
                    self.next_token()?;
                    let right_prec = if left_assoc { prec + 1 } else { prec };
                    let right = self.parse_binary(right_prec)?;
                    let op = token_to_binary_op(&peek_kind)?;
                    let span = left.span.union(right.span);
                    left = SpannedExpr {
                        span,
                        node: Expr::BinaryOp {
                            left: Box::new(left),
                            op,
                            right: Box::new(right),
                        },
                    };
                }
                Some((prec, _)) if prec < min_prec => break,
                _ => break,
            }
        }

        Ok(left)
    }

    /// Handle IS, IN, BETWEEN, LIKE after left operand (possibly negated by NOT).
    fn parse_postfix_binary(
        &mut self,
        left: SpannedExpr<'a>,
        op: TokenKind,
        negated: Option<bool>,
    ) -> Result<SpannedExpr<'a>, ParseError> {
        let is_negated = negated.unwrap_or(false);
        let span = left.span;

        self.enter_recursive()?;
        let result = match op {
            TokenKind::In => {
                let lparen = self.expect(TokenKind::LParen)?;
                let list = if let Ok(TokenKind::From) = self.peek().map(|t| &t.kind) {
                    let query = self.parse_pipe_query()?;
                    let rparen = self.expect(TokenKind::RParen)?;
                    let sub_span = lparen.span.union(rparen.span);
                    vec![SpannedExpr {
                        span: sub_span,
                        node: Expr::Subquery(Box::new(query)),
                    }]
                } else {
                    let items = self.parse_comma_separated(|p| p.parse_expr())?;
                    self.expect(TokenKind::RParen)?;
                    items
                };
                Ok(SpannedExpr {
                    span,
                    node: Expr::InList {
                        expr: Box::new(left),
                        list,
                        negated: is_negated,
                    },
                })
            }
            TokenKind::Between => {
                let low = self.parse_binary(2)?;
                self.expect(TokenKind::And)?;
                let high = self.parse_binary(2)?;
                Ok(SpannedExpr {
                    span,
                    node: Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: is_negated,
                    },
                })
            }
            TokenKind::Like => {
                let pattern = self.parse_expr()?;
                Ok(SpannedExpr {
                    span,
                    node: Expr::Like {
                        expr: Box::new(left),
                        pattern: Box::new(pattern),
                        negated: is_negated,
                    },
                })
            }
            TokenKind::Is => {
                let negated = if let Ok(TokenKind::Not) = self.peek().map(|t| &t.kind) {
                    self.next_token()?;
                    true
                } else {
                    false
                };
                self.expect(TokenKind::Null)?;
                Ok(SpannedExpr {
                    span,
                    node: Expr::UnaryOp {
                        op: if negated {
                            UnaryOp::IsNotNull
                        } else {
                            UnaryOp::IsNull
                        },
                        expr: Box::new(left),
                    },
                })
            }
            _ => Err(ParseError::Unsupported(format!(
                "unexpected postfix op: {:?}",
                op
            ))),
        };
        self.exit_recursive();
        result
    }

    fn parse_unary(&mut self) -> Result<SpannedExpr<'a>, ParseError> {
        let mut ops: Vec<(TokenKind, Span)> = Vec::new();
        loop {
            let peek_kind = match self.peek() {
                Ok(t) => t.kind,
                Err(_) => break,
            };
            if prefix_prec(&peek_kind).is_some() {
                let op_token = self.next_token()?;
                self.enter_recursive()?;
                ops.push((peek_kind, op_token.span));
            } else {
                break;
            }
        }

        let operand_prec = match ops.last() {
            Some((op, _)) => prefix_prec(op).expect("prefix op always has a precedence").0,
            None => return self.parse_primary(),
        };
        let mut expr = self.parse_binary(operand_prec)?;

        for (op, op_span) in ops.iter().rev() {
            self.exit_recursive();
            let span = op_span.union(expr.span);
            expr = match op {
                TokenKind::Not => match expr {
                    SpannedExpr {
                        node:
                            Expr::Exists {
                                subquery,
                                negated: false,
                            },
                        span: _,
                    } => SpannedExpr {
                        span,
                        node: Expr::Exists {
                            subquery,
                            negated: true,
                        },
                    },
                    _ => SpannedExpr {
                        span,
                        node: Expr::UnaryOp {
                            op: UnaryOp::Not,
                            expr: Box::new(expr),
                        },
                    },
                },
                TokenKind::Minus => SpannedExpr {
                    span,
                    node: Expr::UnaryOp {
                        op: UnaryOp::Negate,
                        expr: Box::new(expr),
                    },
                },
                TokenKind::Plus => SpannedExpr {
                    span,
                    node: Expr::UnaryOp {
                        op: UnaryOp::Plus,
                        expr: Box::new(expr),
                    },
                },
                _ => unreachable!(),
            };
        }

        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<SpannedExpr<'a>, ParseError> {
        let peek_kind = match self.peek() {
            Ok(t) => t.kind,
            Err(e) => return Err(e.clone()),
        };

        match peek_kind {
            TokenKind::LParen => {
                let lparen = self.next_token()?;
                if let Ok(TokenKind::From) = self.peek().map(|t| &t.kind) {
                    self.parse_subquery_in_parens(lparen.span)
                } else {
                    let expr = self.parse_expr()?;
                    self.expect(TokenKind::RParen)?;
                    Ok(expr)
                }
            }
            TokenKind::Exists => {
                let token = self.next_token()?;
                self.parse_exists_subquery(token.span)
            }
            TokenKind::Cast => {
                self.next_token()?;
                self.parse_cast_expr()
            }
            TokenKind::Case => {
                let token = self.next_token()?;
                self.parse_case_expr(token.span)
            }
            TokenKind::Ident => {
                let token = self.next_token()?;
                let name = token.text;
                let span = token.span;

                // Function call: IDENT(
                if let Ok(TokenKind::LParen) = self.peek().map(|t| &t.kind) {
                    self.parse_function_call(name, span)
                } else {
                    if let Some(typed) = self.parse_typed_string(name, span)? {
                        return Ok(typed);
                    }

                    // Simple column reference (possibly table.column)
                    if let Ok(TokenKind::Dot) = self.peek().map(|t| &t.kind) {
                        self.next_token()?;
                        let col_token = self.next_token()?;
                        let column = match col_token.kind {
                            TokenKind::Ident => col_token.text,
                            TokenKind::DoubleQuotedIdent => {
                                &col_token.text[1..col_token.text.len() - 1]
                            }
                            TokenKind::Star => {
                                return Ok(SpannedExpr {
                                    span: span.union(col_token.span),
                                    node: Expr::Wildcard { table: Some(name) },
                                });
                            }
                            _ => {
                                return Err(ParseError::Expected {
                                    expected: "identifier after '.'".into(),
                                    found: format!("{:?}", col_token.kind),
                                    span: col_token.span,
                                });
                            }
                        };
                        Ok(SpannedExpr {
                            span: span.union(col_token.span),
                            node: Expr::Column {
                                table: Some(name),
                                column,
                            },
                        })
                    } else {
                        Ok(SpannedExpr {
                            span,
                            node: Expr::Column {
                                table: None,
                                column: name,
                            },
                        })
                    }
                }
            }
            TokenKind::Number => {
                let token = self.next_token()?;
                let text = &token.text;
                let span = token.span;

                if text.starts_with("0x") || text.starts_with("0X") {
                    let val = i64::from_str_radix(&text[2..], 16).map_err(|_| ParseError::NumberParse {
                        message: format!("invalid hex literal: {}", text),
                        span,
                    })?;
                    Ok(SpannedExpr {
                        span,
                        node: Expr::Literal(Literal::Int(val)),
                    })
                } else if text.starts_with("0b") || text.starts_with("0B") {
                    let val = i64::from_str_radix(&text[2..], 2).map_err(|_| ParseError::NumberParse {
                        message: format!("invalid binary literal: {}", text),
                        span,
                    })?;
                    Ok(SpannedExpr {
                        span,
                        node: Expr::Literal(Literal::Int(val)),
                    })
                } else if text.contains('.') || text.contains('e') || text.contains('E') {
                    let val: f64 = text.parse().map_err(|_| ParseError::NumberParse {
                        message: format!("invalid float literal: {}", text),
                        span,
                    })?;
                    if !val.is_finite() {
                        return Err(ParseError::NumberParse {
                            message: format!("float literal overflows f64: {}", text),
                            span,
                        });
                    }
                    Ok(SpannedExpr {
                        span,
                        node: Expr::Literal(Literal::Float(val.into())),
                    })
                } else {
                    let val: i64 = text.parse().map_err(|_| ParseError::NumberParse {
                        message: format!("invalid integer literal: {}", text),
                        span,
                    })?;
                    Ok(SpannedExpr {
                        span,
                        node: Expr::Literal(Literal::Int(val)),
                    })
                }
            }
            TokenKind::String => {
                let token = self.next_token()?;
                let value = &token.text[1..token.text.len() - 1];
                Ok(SpannedExpr {
                    span: token.span,
                    node: Expr::Literal(Literal::String(value)),
                })
            }
            TokenKind::True => {
                let token = self.next_token()?;
                Ok(SpannedExpr {
                    span: token.span,
                    node: Expr::Literal(Literal::Boolean(true)),
                })
            }
            TokenKind::False => {
                let token = self.next_token()?;
                Ok(SpannedExpr {
                    span: token.span,
                    node: Expr::Literal(Literal::Boolean(false)),
                })
            }
            TokenKind::Null => {
                let token = self.next_token()?;
                Ok(SpannedExpr {
                    span: token.span,
                    node: Expr::Literal(Literal::Null),
                })
            }
            TokenKind::Star => {
                let token = self.next_token()?;
                Ok(SpannedExpr {
                    span: token.span,
                    node: Expr::Wildcard { table: None },
                })
            }
            TokenKind::Minus | TokenKind::Plus => {
                // Handled by parse_unary, but if we reach here it's an error
                let token = self.next_token()?;
                Err(ParseError::Unexpected {
                    token: format!("{:?}", peek_kind),
                    span: token.span,
                })
            }
            _ => {
                let token = self.next_token()?;
                Err(ParseError::Unexpected {
                    token: format!("{:?}", token.kind),
                    span: token.span,
                })
            }
        }
    }

    fn parse_subquery_in_parens(
        &mut self,
        lparen_span: Span,
    ) -> Result<SpannedExpr<'a>, ParseError> {
        let query = self.parse_pipe_query()?;
        let rparen = self.expect(TokenKind::RParen)?;
        let span = lparen_span.union(rparen.span);
        Ok(SpannedExpr {
            span,
            node: Expr::ScalarSubquery(Box::new(query)),
        })
    }

    fn parse_exists_subquery(&mut self, exists_span: Span) -> Result<SpannedExpr<'a>, ParseError> {
        self.parse_exists_body(exists_span, false)
    }

    fn parse_exists_body(
        &mut self,
        start_span: Span,
        negated: bool,
    ) -> Result<SpannedExpr<'a>, ParseError> {
        self.expect(TokenKind::LParen)?;
        let query = self.parse_pipe_query()?;
        let rparen = self.expect(TokenKind::RParen)?;
        let span = start_span.union(rparen.span);
        Ok(SpannedExpr {
            span,
            node: Expr::Exists {
                subquery: Box::new(query),
                negated,
            },
        })
    }

    fn parse_cast_expr(&mut self) -> Result<SpannedExpr<'a>, ParseError> {
        self.expect(TokenKind::LParen)?;
        let expr = self.parse_expr()?;
        self.expect(TokenKind::As)?;
        let token = self.expect_ident()?;
        let data_type: DataType<'_> = match token.parse() {
            Ok(dt) => dt,
            Err(_) => return Err(ParseError::Unsupported(format!("unknown type: {}", token))),
        };
        self.expect(TokenKind::RParen)?;
        let span = expr.span;
        Ok(SpannedExpr {
            span,
            node: Expr::Cast {
                expr: Box::new(expr),
                data_type,
            },
        })
    }

    fn parse_case_expr(
        &mut self,
        case_span: crate::token::Span,
    ) -> Result<SpannedExpr<'a>, ParseError> {
        let mut operands = Vec::new();
        let mut results = Vec::new();
        let mut else_result = None;
        let mut end_span = None;

        // Simple CASE: CASE expr WHEN ... THEN ... ELSE ... END
        // Searched CASE: CASE WHEN ... THEN ... ELSE ... END
        let mut case_operand = None;
        if let Ok(TokenKind::When) = self.peek().map(|t| &t.kind) {
            // searched CASE
        } else {
            case_operand = Some(self.parse_expr()?);
        }

        loop {
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::When) => {
                    self.next_token()?;
                    if let Some(ref operand) = case_operand {
                        let when_expr = self.parse_expr()?;
                        operands.push(SpannedExpr {
                            span: operand.span.union(when_expr.span),
                            node: Expr::BinaryOp {
                                left: Box::new(operand.clone()),
                                op: BinaryOp::Eq,
                                right: Box::new(when_expr),
                            },
                        });
                    } else {
                        operands.push(self.parse_expr()?);
                    }
                    self.expect(TokenKind::Then)?;
                    results.push(self.parse_expr()?);
                }
                Ok(TokenKind::Else) => {
                    self.next_token()?;
                    else_result = Some(Box::new(self.parse_expr()?));
                }
                Ok(TokenKind::End) => {
                    let token = self.next_token()?;
                    end_span = Some(token.span);
                    break;
                }
                _ => {
                    let token = self.peek().map_err(|e| e.clone())?;
                    return Err(ParseError::Expected {
                        expected: "WHEN, ELSE, or END".into(),
                        found: token.text.into(),
                        span: token.span,
                    });
                }
            }
        }

        Ok(SpannedExpr {
            span: case_span.union(end_span.expect("END token must be consumed before leaving the loop")),
            node: Expr::Case {
                operands,
                results,
                else_result,
            },
        })
    }

    fn parse_function_call(
        &mut self,
        name: &'a str,
        span: crate::token::Span,
    ) -> Result<SpannedExpr<'a>, ParseError> {
        self.next_token()?; // consume LParen
        let distinct = if let Ok(TokenKind::Distinct) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            true
        } else {
            false
        };

        let mut args = Vec::new();
        if let Ok(TokenKind::Star) = self.peek().map(|t| &t.kind) {
            // COUNT(*) or similar
            if name.eq_ignore_ascii_case("count") {
                self.next_token()?;
                args.push(SpannedExpr {
                    span: Default::default(),
                    node: Expr::Wildcard { table: None },
                });
            } else {
                return Err(ParseError::Unsupported(format!(
                    "* not allowed in {}",
                    name
                )));
            }
        } else if let Ok(TokenKind::RParen) = self.peek().map(|t| &t.kind) {
            // No args
        } else {
            args = self.parse_comma_separated(|p| p.parse_expr())?;
        }

        self.expect(TokenKind::RParen)?;

        // Check for window spec: OVER (...)
        if let Ok(TokenKind::Over) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            let (partition_by, order_by, window_frame) = self.parse_window_spec()?;
            return Ok(SpannedExpr {
                span,
                node: Expr::WindowFunction {
                    name,
                    args,
                    distinct,
                    partition_by,
                    order_by,
                    window_frame,
                },
            });
        }

        Ok(SpannedExpr {
            span,
            node: Expr::Function {
                name,
                args,
                distinct,
            },
        })
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec<'a>, ParseError> {
        self.expect(TokenKind::LParen)?;

        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();

        if let Ok(TokenKind::Partition) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            self.expect(TokenKind::By)?;
            partition_by = self.parse_comma_separated(|p| p.parse_expr())?;
        }

        if let Ok(TokenKind::Order) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            self.expect(TokenKind::By)?;
            order_by = self.parse_comma_separated(|p| p.parse_order_by_expr())?;
        }

        let window_frame = self.parse_window_frame()?;

        self.expect(TokenKind::RParen)?;
        Ok((partition_by, order_by, window_frame))
    }

    fn parse_window_frame(
        &mut self,
    ) -> Result<Option<crate::ast::pipe_op::WindowFrame>, ParseError> {
        let kind = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Rows) => {
                self.next_token()?;
                crate::ast::pipe_op::WindowFrameKind::Rows
            }
            Ok(TokenKind::Range) => {
                self.next_token()?;
                crate::ast::pipe_op::WindowFrameKind::Range
            }
            Ok(TokenKind::Groups) => {
                self.next_token()?;
                crate::ast::pipe_op::WindowFrameKind::Groups
            }
            _ => return Ok(None),
        };

        let (start, end) = if let Ok(TokenKind::Between) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            let start = self.parse_frame_bound()?;
            self.expect(TokenKind::And)?;
            let end = self.parse_frame_bound()?;
            (start, Some(end))
        } else {
            let start = self.parse_frame_bound()?;
            (start, None)
        };

        Ok(Some(crate::ast::pipe_op::WindowFrame { kind, start, end }))
    }

    fn parse_frame_bound(&mut self) -> Result<crate::ast::pipe_op::WindowFrameBound, ParseError> {
        if let Ok(TokenKind::Unbounded) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Preceding) => {
                    self.next_token()?;
                    return Ok(crate::ast::pipe_op::WindowFrameBound::UnboundedPreceding);
                }
                Ok(TokenKind::Following) => {
                    self.next_token()?;
                    return Ok(crate::ast::pipe_op::WindowFrameBound::UnboundedFollowing);
                }
                _ => {
                    return Err(ParseError::Expected {
                        expected: "PRECEDING or FOLLOWING".into(),
                        found: format!(
                            "{:?}",
                            self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)
                        ),
                        span: self.peek_span().unwrap_or_default(),
                    });
                }
            }
        }
        if let Ok(TokenKind::Current) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            self.expect(TokenKind::Row)?;
            return Ok(crate::ast::pipe_op::WindowFrameBound::CurrentRow);
        }
        // offset PRECEDING or offset FOLLOWING
        let token = self.next_token()?;
        let offset: u64 = match token.text.parse() {
            Ok(n) => n,
            Err(_) => {
                return Err(ParseError::NumberParse {
                    message: format!("expected number for frame bound, got '{}'", token.text),
                    span: token.span,
                });
            }
        };
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Preceding) => {
                self.next_token()?;
                Ok(crate::ast::pipe_op::WindowFrameBound::Preceding(offset))
            }
            Ok(TokenKind::Following) => {
                self.next_token()?;
                Ok(crate::ast::pipe_op::WindowFrameBound::Following(offset))
            }
            _ => Err(ParseError::Expected {
                expected: "PRECEDING or FOLLOWING".into(),
                found: format!(
                    "{:?}",
                    self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)
                ),
                span: self.peek_span().unwrap_or_default(),
            }),
        }
    }

    fn parse_typed_string(
        &mut self,
        type_name: &'a str,
        ident_span: Span,
    ) -> Result<Option<SpannedExpr<'a>>, ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::String) => {
                let parsed: Result<DataType<'_>, _> = type_name.parse();
                if let Ok(dt) = parsed {
                    let str_token = self.next_token()?;
                    let value = &str_token.text[1..str_token.text.len() - 1];
                    return Ok(Some(SpannedExpr {
                        span: ident_span.union(str_token.span),
                        node: Expr::TypedString {
                            data_type: dt,
                            value,
                        },
                    }));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Parses the optional ASC/DESC and NULLS FIRST/LAST suffix shared by
    /// ORDER BY, window ORDER BY, and GROUP BY expressions.
    /// Defaults follow DuckDB: ASC -> NULLS LAST, DESC -> NULLS FIRST.
    pub(crate) fn parse_order_suffix(
        &mut self,
        expr: SpannedExpr<'a>,
    ) -> Result<OrderByExpr<'a>, ParseError> {
        let mut asc = true;
        if let Ok(TokenKind::Asc) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
        } else if let Ok(TokenKind::Desc) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            asc = false;
        }

        let mut nulls_first = !asc;
        if let Ok(TokenKind::Nulls) = self.peek().map(|t| &t.kind) {
            self.next_token()?;
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::First) => {
                    self.next_token()?;
                    nulls_first = true;
                }
                Ok(TokenKind::Last) => {
                    self.next_token()?;
                    nulls_first = false;
                }
                _ => {
                    return Err(ParseError::Unsupported(
                        "Expected FIRST or LAST after NULLS".to_string(),
                    ));
                }
            }
        }

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
        })
    }

    pub(crate) fn parse_order_by_expr(&mut self) -> Result<OrderByExpr<'a>, ParseError> {
        let expr = self.parse_expr()?;
        self.parse_order_suffix(expr)
    }

    pub(crate) fn parse_comma_separated<T>(
        &mut self,
        parse_one: impl Fn(&mut Self) -> Result<T, ParseError>,
    ) -> Result<Vec<T>, ParseError> {
        let mut items = Vec::new();
        loop {
            items.push(parse_one(self)?);
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        Ok(items)
    }

    fn peek_next_token_kind(&mut self) -> Result<TokenKind, &ParseError> {
        let saved = self.peeked.take();
        let result = if self.token_pos < self.tokens.len() {
            let token = self.tokens[self.token_pos];
            self.token_pos += 1;
            let kind = token.kind;
            self.lookahead = Some(Ok(token));
            Ok(kind)
        } else {
            match self.lexer.next() {
                Some(Ok(t)) => {
                    let kind = t.kind;
                    self.lookahead = Some(Ok(t));
                    Ok(kind)
                }
                Some(Err(lex_err)) => {
                    self.lookahead = Some(Err(ParseError::Lexical(lex_err)));
                    Ok(TokenKind::EOF)
                }
                None => Ok(TokenKind::EOF),
            }
        };
        if let Some(saved_val) = saved {
            self.peeked = Some(saved_val);
        }
        result
    }
}

fn token_to_binary_op(kind: &TokenKind) -> Result<BinaryOp, ParseError> {
    match kind {
        TokenKind::Plus => Ok(BinaryOp::Plus),
        TokenKind::Minus => Ok(BinaryOp::Minus),
        TokenKind::Star => Ok(BinaryOp::Multiply),
        TokenKind::Slash => Ok(BinaryOp::Divide),
        TokenKind::Percent => Ok(BinaryOp::Modulo),
        TokenKind::Concat => Ok(BinaryOp::Concat),
        TokenKind::Eq => Ok(BinaryOp::Eq),
        TokenKind::Neq => Ok(BinaryOp::Neq),
        TokenKind::Lt => Ok(BinaryOp::Lt),
        TokenKind::Gt => Ok(BinaryOp::Gt),
        TokenKind::Leq => Ok(BinaryOp::Leq),
        TokenKind::Geq => Ok(BinaryOp::Geq),
        TokenKind::And => Ok(BinaryOp::And),
        TokenKind::Or => Ok(BinaryOp::Or),
        _ => Err(ParseError::Unsupported(format!(
            "unsupported binary operator: {:?}",
            kind
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::test_utils::utils::*;

    #[test]
    fn test_expr_binary_op() {
        let expr = parse_expr("1 + 2").expect("test should succeed");
        assert!(matches!(
            expr.node,
            Expr::BinaryOp {
                op: crate::ast::expr::BinaryOp::Plus,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_mul_precedence() {
        let expr = parse_expr("1 + 2 * 3").expect("test should succeed");
        assert!(matches!(
            expr.node,
            Expr::BinaryOp {
                op: crate::ast::expr::BinaryOp::Plus,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_parentheses() {
        let expr = parse_expr("(1 + 2) * 3").expect("test should succeed");
        assert!(matches!(
            expr.node,
            Expr::BinaryOp {
                op: crate::ast::expr::BinaryOp::Multiply,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_is_null() {
        let expr = parse_expr("name IS NULL").expect("test should succeed");
        assert!(matches!(
            expr.node,
            Expr::UnaryOp {
                op: crate::ast::expr::UnaryOp::IsNull,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_in_list() {
        let expr = parse_expr("x IN (1, 2, 3)").expect("test should succeed");
        assert!(matches!(expr.node, Expr::InList { .. }));
    }

    #[test]
    fn test_in_binds_like_comparison() {
        let expr = parse_expr("x = 1 IN (2)").expect("test should succeed");
        match expr.node {
            Expr::InList { expr, list, negated } => {
                assert!(!negated);
                assert_eq!(list.len(), 1);
                assert!(matches!(
                    expr.node,
                    Expr::BinaryOp {
                        op: crate::ast::expr::BinaryOp::Eq,
                        ..
                    }
                ));
            }
            other => panic!("expected InList, got {:?}", other),
        }
    }

    #[test]
    fn test_hex_literal_with_e_digit_parses_as_hex() {
        let expr = parse_expr("0x1e").expect("test should succeed");
        assert_eq!(expr.node, Expr::Literal(Literal::Int(30)));
    }

    #[test]
    fn test_large_hex_literal_overflow_is_error() {
        let err = parse_expr("0xFFFFFFFFFFFFFFFF").expect_err("test should fail");
        assert!(matches!(err, ParseError::NumberParse { .. }));
    }

    #[test]
    fn test_overflowing_int_literal_is_error() {
        let err = parse_expr("18446744073709551615").expect_err("test should fail");
        assert!(matches!(err, ParseError::NumberParse { .. }));
    }

    #[test]
    fn test_overflowing_float_literal_is_error() {
        let err = parse_expr("1e999").expect_err("test should fail");
        assert!(matches!(err, ParseError::NumberParse { .. }));
    }

    #[test]
    fn test_unary_op_chain_nests_outward() {
        let expr = parse_expr("NOT - + 5").expect("test should succeed");
        assert!(matches!(
            expr.node,
            Expr::UnaryOp {
                op: crate::ast::expr::UnaryOp::Not,
                ..
            }
        ));
    }

    #[test]
    fn test_deep_unary_chain_does_not_overflow() {
        let input = format!("{}1", "- ".repeat(100_000));
        let err = parse_expr(&input).expect_err("test should fail");
        assert!(matches!(err, ParseError::RecursionLimitExceeded { .. }));
    }

    #[test]
    fn test_moderately_deep_unary_chain_succeeds() {
        let input = format!("{}1", "- ".repeat(100));
        let expr = parse_expr(&input).expect("test should succeed");
        assert!(matches!(expr.node, Expr::UnaryOp { op: UnaryOp::Negate, .. }));
    }

    #[test]
    fn test_between_chain_nests() {
        let expr = parse_expr("1 BETWEEN 2 BETWEEN 3 AND 4 AND 5").expect("test should succeed");
        assert!(matches!(expr.node, Expr::Between { .. }));
    }

    #[test]
    fn test_deep_between_chain_hits_recursion_limit() {
        let mut input = String::from("1");
        for i in 2..=600 {
            input.push_str(&format!(" BETWEEN {}", i));
        }
        for i in 2..=600 {
            input.push_str(&format!(" AND {}", i));
        }
        let err = parse_expr(&input).expect_err("test should fail");
        assert!(matches!(err, ParseError::RecursionLimitExceeded { .. }));
    }

    #[test]
    fn test_case_searched_has_real_spans() {
        let expr = parse_expr("CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END").expect("test should succeed");
        match expr.node {
            Expr::Case {
                ref operands,
                ref results,
                ref else_result,
            } => {
                assert!(expr.span.start < expr.span.end);
                assert!(operands.iter().all(|o| o.span.start < o.span.end));
                assert!(results.iter().all(|r| r.span.start < r.span.end));
                assert!(else_result.is_some());
            }
            other => panic!("expected Case, got {:?}", other),
        }
    }

    #[test]
    fn test_case_simple_has_real_spans() {
        let expr = parse_expr("CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
            .expect("test should succeed");
        assert!(expr.span.start < expr.span.end);
        if let Expr::Case { operands, results, .. } = expr.node {
            assert_eq!(operands.len(), 2);
            assert!(operands.iter().all(|o| o.span.start < o.span.end));
            assert!(results.iter().all(|r| r.span.start < r.span.end));
        } else {
            panic!("expected Case");
        }
    }

    #[test]
    fn test_case_error_reports_offending_token() {
        let err = parse_expr("CASE WHEN x THEN y BLAH").expect_err("test should fail");
        match err {
            ParseError::Expected { found, .. } => assert_eq!(found, "BLAH"),
            other => panic!("expected Expected, got {:?}", other),
        }
    }

    #[test]
    fn test_column_ref_quoted_part_is_stripped() {
        let expr = parse_expr("a.\"my col\"").expect("test should succeed");
        assert_eq!(
            expr.node,
            Expr::Column {
                table: Some("a"),
                column: "my col",
            }
        );
    }

    #[test]
    fn test_column_ref_missing_part_is_error() {
        let err = parse_expr("a.").expect_err("test should fail");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn test_column_ref_number_part_is_error() {
        let err = parse_expr("a.5").expect_err("test should fail");
        assert!(matches!(err, ParseError::Expected { .. }));
    }

    #[test]
    fn test_table_wildcard() {
        let expr = parse_expr("t.*").expect("test should succeed");
        assert_eq!(expr.node, Expr::Wildcard { table: Some("t") });
    }

    #[test]
    fn test_expr_between() {
        let expr = parse_expr("x BETWEEN 1 AND 10").expect("test should succeed");
        assert!(matches!(expr.node, Expr::Between { .. }));
    }

    #[test]
    fn test_expr_like() {
        let expr = parse_expr("name LIKE '%foo%'").expect("test should succeed");
        assert!(matches!(expr.node, Expr::Like { negated: false, .. }));

        let expr_not = parse_expr("name NOT LIKE '%foo%'").expect("test should succeed");
        assert!(matches!(expr_not.node, Expr::Like { negated: true, .. }));
    }

    #[test]
    fn test_expr_not() {
        let expr = parse_expr("NOT active").expect("test should succeed");
        assert!(matches!(
            expr.node,
            Expr::UnaryOp {
                op: crate::ast::expr::UnaryOp::Not,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_negate() {
        let expr = parse_expr("-42").expect("test should succeed");
        assert!(matches!(
            expr.node,
            Expr::UnaryOp {
                op: crate::ast::expr::UnaryOp::Negate,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_function() {
        let expr = parse_expr("count(*)").expect("test should succeed");
        assert!(matches!(expr.node, Expr::Function { .. }));
    }

    #[test]
    fn test_expr_qualified_column() {
        let expr = parse_expr("t.name").expect("test should succeed");
        assert!(matches!(expr.node, Expr::Column { table: Some(_), .. }));
    }

    #[test]
    fn test_expr_wildcard() {
        let expr = parse_expr("*").expect("test should succeed");
        assert!(matches!(expr.node, Expr::Wildcard { .. }));
    }

    #[test]
    fn test_expr_cast() {
        let expr = parse_expr("CAST(age AS bigint)").expect("test should succeed");
        assert!(matches!(expr.node, Expr::Cast { .. }));
    }

    #[test]
    fn test_expr_string_concat() {
        let expr = parse_expr("a || ' ' || b").expect("test should succeed");
        assert!(matches!(
            expr.node,
            Expr::BinaryOp {
                op: crate::ast::expr::BinaryOp::Concat,
                ..
            }
        ));
    }

    #[test]
    fn test_expr_case() {
        let expr =
            parse_expr("CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END").expect("test should succeed");
        assert!(matches!(expr.node, Expr::Case { .. }));
    }
}
