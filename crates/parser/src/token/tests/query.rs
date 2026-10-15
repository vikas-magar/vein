use super::*;

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
