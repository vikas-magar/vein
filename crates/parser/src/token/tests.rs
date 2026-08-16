#[cfg(test)]
mod tests {
    use crate::token::{Lexer, TokenKind};

    fn tokenize(input: &str) -> Vec<(TokenKind, String)> {
        let lexer = Lexer::new(input);
        lexer
            .filter_map(|r| r.ok())
            .map(|t| (t.kind, t.text.to_string()))
            .collect()
    }

    #[test]
    fn test_empty_input() {
        let tokens = tokenize("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_whitespace_only() {
        let tokens = tokenize("   \t\n  ");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_pipe_operator() {
        let tokens = tokenize("|>");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Pipe);
        assert_eq!(tokens[0].1, "|>");
    }

    #[test]
    fn test_arrow_operator() {
        let tokens = tokenize("->");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Arrow);
    }

    #[test]
    fn test_minus() {
        let tokens = tokenize("-");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Minus);
    }

    #[test]
    fn test_comparison_operators() {
        assert_eq!(tokenize("=")[0].0, TokenKind::Eq);
        assert_eq!(tokenize("<")[0].0, TokenKind::Lt);
        assert_eq!(tokenize(">")[0].0, TokenKind::Gt);
        assert_eq!(tokenize("<=")[0].0, TokenKind::Leq);
        assert_eq!(tokenize(">=")[0].0, TokenKind::Geq);
        assert_eq!(tokenize("<>")[0].0, TokenKind::Neq);
    }

    #[test]
    fn test_simple_arithmetic() {
        let tokens = tokenize("+ - * / %");
        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens[0].0, TokenKind::Plus);
        assert_eq!(tokens[1].0, TokenKind::Minus);
        assert_eq!(tokens[2].0, TokenKind::Star);
        assert_eq!(tokens[3].0, TokenKind::Slash);
        assert_eq!(tokens[4].0, TokenKind::Percent);
    }

    #[test]
    fn test_keywords() {
        let tokens = tokenize("SELECT FROM WHERE AND OR NOT IN AS ON JOIN");
        assert_eq!(tokens.len(), 10);
        assert_eq!(tokens[0].0, TokenKind::Select);
        assert_eq!(tokens[1].0, TokenKind::From);
        assert_eq!(tokens[2].0, TokenKind::Where);
        assert_eq!(tokens[3].0, TokenKind::And);
        assert_eq!(tokens[4].0, TokenKind::Or);
        assert_eq!(tokens[5].0, TokenKind::Not);
        assert_eq!(tokens[6].0, TokenKind::In);
        assert_eq!(tokens[7].0, TokenKind::As);
        assert_eq!(tokens[8].0, TokenKind::On);
        assert_eq!(tokens[9].0, TokenKind::Join);
    }

    #[test]
    fn test_keywords_case_insensitive() {
        let tokens = tokenize("select From WHERE");
        assert_eq!(tokens[0].0, TokenKind::Select);
        assert_eq!(tokens[1].0, TokenKind::From);
        assert_eq!(tokens[2].0, TokenKind::Where);
    }

    #[test]
    fn test_identifier() {
        let tokens = tokenize("my_column");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Ident);
        assert_eq!(tokens[0].1, "my_column");
    }

    #[test]
    fn test_integer_literal() {
        let tokens = tokenize("42");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Number);
        assert_eq!(tokens[0].1, "42");
    }

    #[test]
    fn test_float_literal() {
        let tokens = tokenize("3.14");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Number);
        assert_eq!(tokens[0].1, "3.14");
    }

    #[test]
    fn test_hex_number() {
        let tokens = tokenize("0xFF");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Number);
        assert_eq!(tokens[0].1, "0xFF");
    }

    #[test]
    fn test_binary_number() {
        let tokens = tokenize("0b1010");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Number);
        assert_eq!(tokens[0].1, "0b1010");
    }

    #[test]
    fn test_scientific_notation() {
        let tokens = tokenize("1.5e3");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Number);
        assert_eq!(tokens[0].1, "1.5e3");
    }

    #[test]
    fn test_string_literal() {
        let tokens = tokenize("'hello world'");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::String);
        assert_eq!(tokens[0].1, "'hello world'");
    }

    #[test]
    fn test_punctuation() {
        let tokens = tokenize("( ) , . ; :");
        assert_eq!(tokens.len(), 6);
        assert_eq!(tokens[0].0, TokenKind::LParen);
        assert_eq!(tokens[1].0, TokenKind::RParen);
        assert_eq!(tokens[2].0, TokenKind::Comma);
        assert_eq!(tokens[3].0, TokenKind::Dot);
        assert_eq!(tokens[4].0, TokenKind::Semicolon);
        assert_eq!(tokens[5].0, TokenKind::Colon);
    }

    #[test]
    fn test_line_comment() {
        let tokens = tokenize("-- this is a comment\nSELECT");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Select);
    }

    #[test]
    fn test_block_comment() {
        let tokens = tokenize("/* comment */ FROM");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::From);
    }

    #[test]
    fn test_nested_block_comment() {
        let tokens = tokenize("/* outer /* inner */ still outer */ WHERE");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Where);
    }

    #[test]
    fn test_colon() {
        let tokens = tokenize(":");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Colon);
    }

    #[test]
    fn test_simple_pipe_query() {
        let input = "FROM users |> SELECT name, age |> WHERE age > 18";
        let tokens = tokenize(input);
        let kinds: Vec<_> = tokens.iter().map(|(k, _)| *k).collect();
        assert_eq!(
            kinds,
            vec![
                TokenKind::From, TokenKind::Ident,  // FROM users
                TokenKind::Pipe,                     // |>
                TokenKind::Select, TokenKind::Ident, TokenKind::Comma, TokenKind::Ident,  // SELECT name, age
                TokenKind::Pipe,                     // |>
                TokenKind::Where, TokenKind::Ident, TokenKind::Gt, TokenKind::Number,  // WHERE age > 18
            ]
        );
    }

    #[test]
    fn test_true_false_null() {
        let tokens = tokenize("TRUE FALSE NULL");
        assert_eq!(tokens[0].0, TokenKind::True);
        assert_eq!(tokens[1].0, TokenKind::False);
        assert_eq!(tokens[2].0, TokenKind::Null);
    }

    #[test]
    fn test_prefixed_keyword_as_identifier() {
        let tokens = tokenize("myselect from_x");
        assert_eq!(tokens[0].0, TokenKind::Ident);
        assert_eq!(tokens[0].1, "myselect");
        assert_eq!(tokens[1].0, TokenKind::Ident);
        assert_eq!(tokens[1].1, "from_x");
    }

    #[test]
    fn test_concat_operator() {
        let tokens = tokenize("a || b");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[1].0, TokenKind::Concat);
    }

    #[test]
    fn test_standalone_pipe_error() {
        let lexer = Lexer::new("| SELECT");
        let results: Vec<_> = lexer.collect();
        assert!(results[0].is_err());
    }

    #[test]
    fn test_unterminated_double_quoted_ident_error() {
        let lexer = Lexer::new("\"");
        let results: Vec<_> = lexer.collect();
        assert!(matches!(results[0], Err(ref e) if e.message.contains("unterminated")));
    }

    #[test]
    fn test_unterminated_double_quoted_ident_error_with_text() {
        let lexer = Lexer::new("\"abc");
        let results: Vec<_> = lexer.collect();
        assert!(matches!(results[0], Err(ref e) if e.message.contains("unterminated")));
    }

    #[test]
    fn test_escaped_quote_in_double_quoted_ident() {
        let tokens = tokenize("\"a\"\"b\"");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::DoubleQuotedIdent);
        assert_eq!(tokens[0].1, "\"a\"\"b\"");
    }

    #[test]
    fn test_span_reports_token_start_position() {
        // `x` sits on line 2, column 4; the token's span line/column must point
        // at that start, not at the end of the token.
        let lexer = Lexer::new("-- c\n  xyz\n");
        let tok = lexer.filter_map(|r| r.ok()).next().expect("one token");
        assert_eq!(tok.text, "xyz");
        assert_eq!(tok.span.line, 2);
        assert_eq!(tok.span.column, 3, "column of first char of `xyz`");
        assert_eq!(tok.span.start, "-- c\n  ".len());
    }

    #[test]
    fn test_error_span_reports_token_start_position() {
        // Unterminated string starts on line 2 at column 3.
        let lexer = Lexer::new("a\n  'unterminated");
        let err = lexer.into_iter().find_map(|r| r.err()).expect("error");
        assert_eq!(err.span.line, 2);
        assert_eq!(err.span.column, 3, "column of the opening quote");
    }
}
