use crate::{ParseError, SourceSpan, VeinError};

impl VeinError {
    /// Returns the original source location, including through contextual wrappers.
    pub fn source_span(&self) -> Option<SourceSpan> {
        match self {
            Self::Semantic { span, .. } | Self::Unsupported { span, .. }
            | Self::Planning { span, .. } => *span,
            Self::Context { source, .. } => source.source_span(),
            Self::Parse(error) => match error {
                ParseError::Expected { span, .. } | ParseError::Unexpected { span, .. }
                | ParseError::NumberParse { span, .. } => Some(*span),
                ParseError::Lexical(error) => Some(error.span),
                _ => None,
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_real_spans_and_does_not_invent_locations() {
        let span = SourceSpan::new(8, 12, 2, 3);
        let error = VeinError::context("inspection", VeinError::from(ParseError::Expected {
            expected: "expression".into(), found: "EOF".into(), span,
        }));
        assert_eq!(error.source_span(), Some(span));
        assert_eq!(VeinError::semantic("missing column", None).source_span(), None);
    }
}
