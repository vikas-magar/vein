use super::*;
use error::{SourceSpan, VeinError};

#[test]
fn browser_spans_use_utf16_without_splitting_unicode_or_guessing_missing_locations() {
    let query = "'🦀é' + bad";
    let start = query.find("bad").unwrap();
    let error = VeinError::semantic("unknown", Some(SourceSpan::new(start, query.len(), 1, 1)));
    let diagnostic = Diagnostic::from_error(query, &error);
    assert_eq!(diagnostic.from, Some(8));
    assert_eq!(diagnostic.to, Some(11));
    let unknown = Diagnostic::from_error(query, &VeinError::semantic("unknown", None));
    assert_eq!(unknown.from, None);
    let invalid = VeinError::semantic("unknown", Some(SourceSpan::new(2, 4, 1, 1)));
    assert_eq!(Diagnostic::from_error(query, &invalid).from, None);
}
