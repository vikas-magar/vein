use crate::ast::data_type::DataType;
use crate::ast::expr::{BinaryOp, Expr, Literal, UnaryOp};
use crate::ast::pipe_op::OrderByExpr;
use crate::parser::{ParseError, Parser, TokenKind};
use crate::token::Span;

/// Precedence table. Higher = tighter binding.
fn infix_prec(kind: &TokenKind) -> Option<(u32, bool)> {
    match kind {
        TokenKind::Or => Some((0, true)),
        TokenKind::And => Some((1, true)),
        TokenKind::Is | TokenKind::In | TokenKind::Between | TokenKind::Like => Some((3, true)),
        TokenKind::Eq | TokenKind::Neq | TokenKind::Lt | TokenKind::Gt | TokenKind::Leq | TokenKind::Geq => Some((4, true)),
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

type WindowSpec = (Vec<Expr>, Vec<OrderByExpr>, Option<crate::ast::pipe_op::WindowFrame>);

impl Parser {
    pub fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.enter_recursive()?;
        let result = self.parse_binary(0);
        self.exit_recursive();
        result
    }

    fn parse_binary(&mut self, min_prec: u32) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary()?;

        while let Ok(peek_kind) = self.peek().map(|t| t.kind.clone()) {

            // Handle NOT as special prefix within infix operators
            if peek_kind == TokenKind::Not {
                let next = match self.peek_next_token_kind() {
                    Ok(k) => k,
                    Err(_) => break,
                };
                if next == TokenKind::In || next == TokenKind::Between || next == TokenKind::Like {
                    let negated = true;
                    self.next_token()?; // consume NOT from peeked
                    self.next_token()?; // consume operator from lookahead
                    return self.parse_postfix_binary(left, next, min_prec, Some(negated));
                }
            }

            // IN, BETWEEN, LIKE, IS are special postfix operators, not normal binary
            if peek_kind == TokenKind::In
                || peek_kind == TokenKind::Between
                || peek_kind == TokenKind::Like
                || peek_kind == TokenKind::Is
            {
                self.next_token()?;
                return self.parse_postfix_binary(left, peek_kind, min_prec, None);
            }

            match infix_prec(&peek_kind) {
                Some((prec, left_assoc)) if prec >= min_prec => {
                    self.next_token()?;
                    let right_prec = if left_assoc { prec + 1 } else { prec };
                    let right = self.parse_binary(right_prec)?;
                    let span = left.span().union(right.span());
                    left = Expr::BinaryOp {
                        left: Box::new(left),
                        op: token_to_binary_op(&peek_kind),
                        right: Box::new(right),
                        span,
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
        left: Expr,
        op: TokenKind,
        _min_prec: u32,
        negated: Option<bool>,
    ) -> Result<Expr, ParseError> {
        let is_negated = negated.unwrap_or(false);
        let span = left.span();

        match op {
            TokenKind::In => {
                let lparen = self.expect(TokenKind::LParen)?;
                let list = if let Ok(TokenKind::From) = self.peek().map(|t| &t.kind) {
                    let query = self.parse_pipe_query()?;
                    let rparen = self.expect(TokenKind::RParen)?;
                    let sub_span = lparen.span.union(rparen.span);
                    vec![Expr::Subquery(Box::new(query), sub_span)]
                } else {
                    let items = self.parse_comma_separated(|p| p.parse_expr())?;
                    self.expect(TokenKind::RParen)?;
                    items
                };
                Ok(Expr::InList { expr: Box::new(left), list, negated: is_negated, span })
            }
            TokenKind::Between => {
                let low = self.parse_binary(2)?;
                self.expect(TokenKind::And)?;
                let high = self.parse_binary(2)?;
                Ok(Expr::Between { expr: Box::new(left), low: Box::new(low), high: Box::new(high), negated: is_negated, span })
            }
            TokenKind::Like => {
                let pattern = self.parse_expr()?;
                Ok(Expr::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: is_negated,
                    span,
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
                Ok(Expr::UnaryOp {
                    op: if negated { UnaryOp::IsNotNull } else { UnaryOp::IsNull },
                    expr: Box::new(left),
                    span,
                })
            }
            _ => Err(ParseError::Unsupported(format!("unexpected postfix op: {:?}", op))),
        }
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        let peek_kind = match self.peek() {
            Ok(t) => t.kind.clone(),
            Err(_) => return self.parse_primary(),
        };

        if let Some((prec, _)) = prefix_prec(&peek_kind) {
            let op_token = self.next_token()?;
            let expr = self.parse_binary(prec)?;
            let span = op_token.span.union(expr.span());
            match peek_kind {
                TokenKind::Not => {
                    match expr {
                        Expr::Exists { subquery, negated: false, span: _ } => {
                            Ok(Expr::Exists { subquery, negated: true, span })
                        }
                        _ => Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(expr), span }),
                    }
                }
                TokenKind::Minus => {
                    Ok(Expr::UnaryOp { op: UnaryOp::Negate, expr: Box::new(expr), span })
                }
                TokenKind::Plus => {
                    Ok(Expr::UnaryOp { op: UnaryOp::Plus, expr: Box::new(expr), span })
                }
                _ => unreachable!(),
            }
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        let peek_kind = match self.peek() {
            Ok(t) => t.kind.clone(),
            Err(e) => return Err(e.clone()),
        };

        match peek_kind {
            TokenKind::LParen => {
                let lparen = self.next_token()?;
                if let Ok(TokenKind::From) = self.peek().map(|t| &t.kind) {
                    return self.parse_subquery_in_parens(lparen.span);
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
                self.next_token()?;
                self.parse_case_expr()
            }
            TokenKind::Ident => {
                let token = self.next_token()?;
                let name = token.text;
                let span = token.span;

                // Function call: IDENT(
                if let Ok(TokenKind::LParen) = self.peek().map(|t| &t.kind) {
                    self.parse_function_call(name, span)
                } else {
                    if let Some(typed) = self.parse_typed_string(name.clone(), span)? {
                        return Ok(typed);
                    }

                    // Simple column reference (possibly table.column)
                    if let Ok(TokenKind::Dot) = self.peek().map(|t| &t.kind) {
                        self.next_token()?;
                        let col_token = self.next_token()?;
                        Ok(Expr::Column {
                            table: Some(name),
                            column: col_token.text,
                            span: span.union(col_token.span),
                        })
                    } else {
                        Ok(Expr::Column { table: None, column: name, span })
                    }
                }
            }
            TokenKind::Number => {
                let token = self.next_token()?;
                let text = &token.text;
                let span = token.span;

                // Determine if int or float
                if text.contains('.') || text.contains('e') || text.contains('E') {
                    let val: f64 = text.parse().unwrap_or(0.0);
                    Ok(Expr::Literal(Literal::Float(val.into()), span))
                } else if text.starts_with("0x") || text.starts_with("0X") {
                    let val = i64::from_str_radix(&text[2..], 16).unwrap_or(0);
                    Ok(Expr::Literal(Literal::Int(val), span))
                } else if text.starts_with("0b") || text.starts_with("0B") {
                    let val = i64::from_str_radix(&text[2..], 2).unwrap_or(0);
                    Ok(Expr::Literal(Literal::Int(val), span))
                } else {
                    let val: i64 = text.parse().unwrap_or(0);
                    Ok(Expr::Literal(Literal::Int(val), span))
                }
            }
            TokenKind::String => {
                let token = self.next_token()?;
                Ok(Expr::Literal(Literal::String(token.text), token.span))
            }
            TokenKind::True => {
                let token = self.next_token()?;
                Ok(Expr::Literal(Literal::Boolean(true), token.span))
            }
            TokenKind::False => {
                let token = self.next_token()?;
                Ok(Expr::Literal(Literal::Boolean(false), token.span))
            }
            TokenKind::Null => {
                let token = self.next_token()?;
                Ok(Expr::Literal(Literal::Null, token.span))
            }
            TokenKind::Star => {
                let token = self.next_token()?;
                Ok(Expr::Wildcard { table: None, span: token.span })
            }
            TokenKind::Minus | TokenKind::Plus => {
                // Handled by parse_unary, but if we reach here it's an error
                self.next_token()?;
                Err(ParseError::Unexpected {
                    token: format!("{:?}", peek_kind),
                    span: Default::default(),
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

    fn parse_subquery_in_parens(&mut self, lparen_span: Span) -> Result<Expr, ParseError> {
        let query = self.parse_pipe_query()?;
        let rparen = self.expect(TokenKind::RParen)?;
        let span = lparen_span.union(rparen.span);
        Ok(Expr::ScalarSubquery(Box::new(query), span))
    }

    fn parse_exists_subquery(&mut self, exists_span: Span) -> Result<Expr, ParseError> {
        self.parse_exists_body(exists_span, false)
    }

    fn parse_exists_body(&mut self, start_span: Span, negated: bool) -> Result<Expr, ParseError> {
        self.expect(TokenKind::LParen)?;
        let query = self.parse_pipe_query()?;
        let rparen = self.expect(TokenKind::RParen)?;
        let span = start_span.union(rparen.span);
        Ok(Expr::Exists { subquery: Box::new(query), negated, span })
    }

    fn parse_cast_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(TokenKind::LParen)?;
        let expr = self.parse_expr()?;
        self.expect(TokenKind::As)?;
        let token = self.expect_ident()?;
        let data_type: DataType = match token.parse() {
            Ok(dt) => dt,
            Err(_) => return Err(ParseError::Unsupported(format!("unknown type: {}", token))),
        };
        self.expect(TokenKind::RParen)?;
        let span = expr.span();
        Ok(Expr::Cast { expr: Box::new(expr), data_type, span })
    }

    fn parse_case_expr(&mut self) -> Result<Expr, ParseError> {
        let mut operands = Vec::new();
        let mut results = Vec::new();
        let mut else_result = None;

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
                        operands.push(Expr::BinaryOp {
                            left: Box::new(operand.clone()),
                            op: BinaryOp::Eq,
                            right: Box::new(self.parse_expr()?),
                            span: Default::default(),
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
                    self.next_token()?;
                    break;
                }
                _ => {
                    return Err(ParseError::Expected {
                        expected: "WHEN, ELSE, or END".into(),
                        found: "???".into(),
                        span: Default::default(),
                    });
                }
            }
        }

        Ok(Expr::Case { operands, results, else_result, span: Default::default() })
    }

    fn parse_function_call(&mut self, name: String, span: crate::token::Span) -> Result<Expr, ParseError> {
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
                args.push(Expr::Wildcard { table: None, span: Default::default() });
            } else {
                return Err(ParseError::Unsupported(format!("* not allowed in {}", name)));
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
            return Ok(Expr::WindowFunction {
                name,
                args,
                distinct,
                partition_by,
                order_by,
                window_frame,
                span,
            });
        }

        Ok(Expr::Function { name, args, distinct, span })
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
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

    fn parse_window_frame(&mut self) -> Result<Option<crate::ast::pipe_op::WindowFrame>, ParseError> {
        let kind = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Rows) => { self.next_token()?; crate::ast::pipe_op::WindowFrameKind::Rows }
            Ok(TokenKind::Range) => { self.next_token()?; crate::ast::pipe_op::WindowFrameKind::Range }
            Ok(TokenKind::Groups) => { self.next_token()?; crate::ast::pipe_op::WindowFrameKind::Groups }
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
                Ok(TokenKind::Preceding) => { self.next_token()?; return Ok(crate::ast::pipe_op::WindowFrameBound::UnboundedPreceding); }
                Ok(TokenKind::Following) => { self.next_token()?; return Ok(crate::ast::pipe_op::WindowFrameBound::UnboundedFollowing); }
                _ => return Err(ParseError::Expected {
                    expected: "PRECEDING or FOLLOWING".into(),
                    found: format!("{:?}", self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)),
                    span: self.peek_span().unwrap_or_default(),
                }),
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
            Err(_) => return Err(ParseError::NumberParse {
                message: format!("expected number for frame bound, got '{}'", token.text),
                span: token.span,
            }),
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
                found: format!("{:?}", self.peek().map(|t| &t.kind).unwrap_or(&TokenKind::EOF)),
                span: self.peek_span().unwrap_or_default(),
            }),
        }
    }

    fn parse_typed_string(&mut self, type_name: String, ident_span: Span) -> Result<Option<Expr>, ParseError> {
        match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::String) => {
                if let Ok(dt) = type_name.parse::<DataType>() {
                    let str_token = self.next_token()?;
                    return Ok(Some(Expr::TypedString {
                        data_type: dt,
                        value: str_token.text,
                        span: ident_span.union(str_token.span),
                    }));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParseError> {
        let expr = self.parse_expr()?;
        let asc = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Asc) => { self.next_token()?; true }
            Ok(TokenKind::Desc) => { self.next_token()?; false }
            _ => true,
        };
        let nulls_first = match self.peek().map(|t| &t.kind) {
            Ok(TokenKind::Nulls) => {
                self.next_token()?;
                match self.peek().map(|t| &t.kind) {
                    Ok(TokenKind::First) => { self.next_token()?; true }
                    Ok(TokenKind::Last) => { self.next_token()?; false }
                    _ => asc,
                }
            }
            _ => asc,
        };
        Ok(OrderByExpr { expr, asc, nulls_first })
    }

    pub(crate) fn parse_comma_separated<T>(
        &mut self,
        parse_one: impl Fn(&mut Self) -> Result<T, ParseError>,
    ) -> Result<Vec<T>, ParseError> {
        let mut items = Vec::new();
        loop {
            items.push(parse_one(self)?);
            match self.peek().map(|t| &t.kind) {
                Ok(TokenKind::Comma) => { self.next_token()?; }
                _ => break,
            }
        }
        Ok(items)
    }

    fn peek_next_token_kind(&mut self) -> Result<TokenKind, &ParseError> {
        let saved = self.peeked.take();
        let result = if self.token_pos < self.tokens.len() {
            let token = self.tokens[self.token_pos].clone();
            self.token_pos += 1;
            let kind = token.kind.clone();
            self.lookahead = Some(Ok(token));
            Ok(kind)
        } else {
            match self.lexer.next() {
                Some(Ok(t)) => {
                    let kind = t.kind.clone();
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

fn token_to_binary_op(kind: &TokenKind) -> BinaryOp {
    match kind {
        TokenKind::Plus => BinaryOp::Plus,
        TokenKind::Minus => BinaryOp::Minus,
        TokenKind::Star => BinaryOp::Multiply,
        TokenKind::Slash => BinaryOp::Divide,
        TokenKind::Percent => BinaryOp::Modulo,
        TokenKind::Concat => BinaryOp::Concat,
        TokenKind::Eq => BinaryOp::Eq,
        TokenKind::Neq => BinaryOp::Neq,
        TokenKind::Lt => BinaryOp::Lt,
        TokenKind::Gt => BinaryOp::Gt,
        TokenKind::Leq => BinaryOp::Leq,
        TokenKind::Geq => BinaryOp::Geq,
        TokenKind::And => BinaryOp::And,
        TokenKind::Or => BinaryOp::Or,
        _ => BinaryOp::Eq, // fallback
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::test_utils::utils::*;

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

}
