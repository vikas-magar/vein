use crate::token::scan::keyword;
use crate::token::{LexicalError, Span, SpannedToken, TokenKind};

/// An iterator over the tokens of a source string, produced lazily.
///
/// Skips whitespace and comments and yields each token with its source span.
/// Malformed input is reported as `Err(LexicalError)` and lexing continues.
pub struct Lexer<'a> {
    input: &'a str,
    pos: usize,
    line: usize,
    column: usize,
}

impl<'a> Lexer<'a> {
    /// Creates a lexer over the given source text.
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            line: 1,
            column: 1,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn peek_next(&self) -> Option<char> {
        let mut chars = self.input[self.pos..].chars();
        chars.next();
        chars.next()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += ch.len_utf8();
        if ch == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        Some(ch)
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn skip_line_comment(&mut self) -> Result<(), LexicalError> {
        while let Some(ch) = self.peek() {
            if ch == '\n' {
                self.advance();
                return Ok(());
            }
            self.advance();
        }
        Ok(())
    }

    fn skip_block_comment(&mut self) -> Result<(), LexicalError> {
        let start = self.pos;
        let mut depth: u32 = 1;
        while depth > 0 {
            match (self.peek(), self.peek_next()) {
                (Some('*'), Some('/')) => {
                    self.advance();
                    self.advance();
                    depth -= 1;
                }
                (Some('/'), Some('*')) => {
                    self.advance();
                    self.advance();
                    depth += 1;
                }
                (Some(_), _) => {
                    self.advance();
                }
                (None, _) => {
                    return Err(LexicalError {
                        message: "unterminated block comment".into(),
                        span: Span::new(start, self.pos, self.line, self.column),
                        line: self.line,
                        column: self.column,
                    });
                }
            }
        }
        Ok(())
    }

    fn scan_number(&mut self) -> Result<TokenKind, LexicalError> {
        let start = self.pos;
        if self.peek() == Some('0') && self.peek_next().is_some_and(|c| c == 'x' || c == 'X') {
            self.advance();
            self.advance();
            let mut has_digit = false;
            while let Some(ch) = self.peek() {
                if ch.is_ascii_hexdigit() {
                    has_digit = true;
                    self.advance();
                } else {
                    break;
                }
            }
            if !has_digit {
                return Err(LexicalError {
                    message: "incomplete hex number".into(),
                    span: Span::new(start, self.pos, self.line, self.column),
                    line: self.line,
                    column: self.column,
                });
            }
            return Ok(TokenKind::Number);
        }
        if self.peek() == Some('0') && self.peek_next().is_some_and(|c| c == 'b' || c == 'B') {
            self.advance();
            self.advance();
            let mut has_digit = false;
            while let Some(ch) = self.peek() {
                if ch == '0' || ch == '1' {
                    has_digit = true;
                    self.advance();
                } else {
                    break;
                }
            }
            if !has_digit {
                return Err(LexicalError {
                    message: "incomplete binary number".into(),
                    span: Span::new(start, self.pos, self.line, self.column),
                    line: self.line,
                    column: self.column,
                });
            }
            return Ok(TokenKind::Number);
        }
        let mut is_float = false;
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                self.advance();
            } else if ch == '.' && !is_float {
                if self.peek_next() == Some('.') {
                    break;
                }
                is_float = true;
                self.advance();
            } else if ch == 'e' || ch == 'E' {
                self.advance();
                if self.peek() == Some('+') || self.peek() == Some('-') {
                    self.advance();
                }
                if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    return Err(LexicalError {
                        message: "incomplete scientific notation".into(),
                        span: Span::new(start, self.pos, self.line, self.column),
                        line: self.line,
                        column: self.column,
                    });
                }
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    self.advance();
                }
                break;
            } else {
                break;
            }
        }
        Ok(TokenKind::Number)
    }

    fn scan_string(&mut self) -> Result<TokenKind, LexicalError> {
        let start = self.pos - 1;
        loop {
            match self.advance() {
                Some('\'') => {
                    if self.peek() == Some('\'') {
                        self.advance();
                    } else {
                        return Ok(TokenKind::String);
                    }
                }
                Some('\\') => {
                    self.advance();
                }
                Some('\n') | None => {
                    return Err(LexicalError {
                        message: "unterminated string literal".into(),
                        span: Span::new(start, self.pos, self.line, self.column),
                        line: self.line,
                        column: self.column,
                    });
                }
                Some(_) => {}
            }
        }
    }

    fn scan_double_quoted_ident(&mut self) -> Result<TokenKind, LexicalError> {
        let start = self.pos - 1;
        loop {
            match self.advance() {
                Some('"') => {
                    if self.peek() == Some('"') {
                        self.advance();
                    } else {
                        return Ok(TokenKind::DoubleQuotedIdent);
                    }
                }
                Some('\n') | None => {
                    return Err(LexicalError {
                        message: "unterminated double-quoted identifier".into(),
                        span: Span::new(start, self.pos, self.line, self.column),
                        line: self.line,
                        column: self.column,
                    });
                }
                Some(_) => {}
            }
        }
    }

    fn scan_ident(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn current_text(&self, start: usize) -> &'a str {
        &self.input[start..self.pos]
    }

    fn make_token(&self, start: usize, kind: TokenKind) -> SpannedToken<'a> {
        let text = self.current_text(start);
        let span = Span::new(start, self.pos, self.line, self.column);
        SpannedToken { kind, text, span }
    }

    fn make_error(&self, start: usize, message: String) -> LexicalError {
        LexicalError {
            message,
            span: Span::new(start, self.pos, self.line, self.column),
            line: self.line,
            column: self.column,
        }
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<SpannedToken<'a>, LexicalError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pos >= self.input.len() {
                return None;
            }
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                return None;
            }

            let ch = self.input[self.pos..].chars().next()?;
            let start = self.pos;

            return Some(match ch {
                '/' if self.peek_next() == Some('/') => {
                    self.advance();
                    self.advance();
                    match self.skip_line_comment() {
                        Ok(()) => continue,
                        Err(e) => Err(e),
                    }
                }
                '/' if self.peek_next() == Some('*') => {
                    self.advance();
                    self.advance();
                    match self.skip_block_comment() {
                        Ok(()) => continue,
                        Err(e) => Err(e),
                    }
                }
                '/' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Slash))
                }
                '\'' => {
                    self.advance();
                    match self.scan_string() {
                        Ok(kind) => Ok(self.make_token(start, kind)),
                        Err(e) => Err(e),
                    }
                }
                '"' => {
                    self.advance();
                    match self.scan_double_quoted_ident() {
                        Ok(kind) => Ok(self.make_token(start, kind)),
                        Err(e) => Err(e),
                    }
                }
                ch if ch.is_ascii_digit()
                    || (ch == '.' && self.peek_next().is_some_and(|c| c.is_ascii_digit())) =>
                {
                    match self.scan_number() {
                        Ok(kind) => Ok(self.make_token(start, kind)),
                        Err(e) => Err(e),
                    }
                }
                ch if ch.is_ascii_alphabetic() || ch == '_' => {
                    self.scan_ident();
                    let text = self.current_text(start);
                    let kind = keyword(text).unwrap_or(TokenKind::Ident);
                    Ok(self.make_token(start, kind))
                }
                '-' if self.peek_next() == Some('-') => {
                    self.advance();
                    self.advance();
                    match self.skip_line_comment() {
                        Ok(()) => continue,
                        Err(e) => Err(e),
                    }
                }
                '|' if self.peek_next() == Some('|') => {
                    self.advance();
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Concat))
                }
                '|' => {
                    self.advance();
                    if self.peek() == Some('>') {
                        self.advance();
                        Ok(self.make_token(start, TokenKind::Pipe))
                    } else {
                        Err(self.make_error(start, "unexpected '|'".into()))
                    }
                }
                '-' => {
                    self.advance();
                    let kind = if self.peek() == Some('>') {
                        self.advance();
                        TokenKind::Arrow
                    } else {
                        TokenKind::Minus
                    };
                    Ok(self.make_token(start, kind))
                }
                '=' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Eq))
                }
                '+' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Plus))
                }
                '*' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Star))
                }
                '%' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Percent))
                }
                ',' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Comma))
                }
                '.' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Dot))
                }
                ';' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Semicolon))
                }
                ':' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::Colon))
                }
                '(' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::LParen))
                }
                ')' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::RParen))
                }
                '[' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::LBracket))
                }
                ']' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::RBracket))
                }
                '{' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::LBrace))
                }
                '}' => {
                    self.advance();
                    Ok(self.make_token(start, TokenKind::RBrace))
                }
                '<' => {
                    self.advance();
                    let kind = if self.peek() == Some('=') {
                        self.advance();
                        TokenKind::Leq
                    } else if self.peek() == Some('>') {
                        self.advance();
                        TokenKind::Neq
                    } else {
                        TokenKind::Lt
                    };
                    Ok(self.make_token(start, kind))
                }
                '>' => {
                    self.advance();
                    let kind = if self.peek() == Some('=') {
                        self.advance();
                        TokenKind::Geq
                    } else {
                        TokenKind::Gt
                    };
                    Ok(self.make_token(start, kind))
                }
                _ => {
                    self.advance();
                    Err(self.make_error(start, format!("unexpected character '{}'", ch)))
                }
            });
        }
    }
}
