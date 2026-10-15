#[cfg(test)]
mod tests {
    use crate::token::{Lexer, TokenKind};

    pub(super) fn tokenize(input: &str) -> Vec<(TokenKind, String)> {
        let lexer = Lexer::new(input);
        lexer
            .filter_map(|r| r.ok())
            .map(|t| (t.kind, t.text.to_string()))
            .collect()
    }

    mod lexical;
    mod query;
}
