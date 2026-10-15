//! Source locations shared by parsers, binders, planners, and diagnostics.

/// A half-open byte range `[start, end)` and its 1-based line and column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SourceSpan {
    /// Byte offset of the first character, relative to the source start.
    pub start: usize,
    /// Byte offset one past the last character.
    pub end: usize,
    /// 1-based line number of `start`.
    pub line: usize,
    /// 1-based column number of `start`.
    pub column: usize,
}

impl SourceSpan {
    /// Creates a span covering `[start, end)` at the given position.
    #[must_use]
    pub const fn new(start: usize, end: usize, line: usize, column: usize) -> Self {
        Self {
            start,
            end,
            line,
            column,
        }
    }

    /// Returns the smallest span containing both `self` and `other`.
    #[must_use]
    pub fn union(self, other: Self) -> Self {
        let (line, column) = if self.start <= other.start {
            (self.line, self.column)
        } else {
            (other.line, other.column)
        };
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
            line,
            column,
        }
    }
}

impl Default for SourceSpan {
    fn default() -> Self {
        Self {
            start: 0,
            end: 0,
            line: 1,
            column: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SourceSpan;

    #[test]
    fn union_tracks_earliest_location_and_full_byte_range() {
        let earlier = SourceSpan::new(2, 5, 1, 3);
        let later = SourceSpan::new(8, 11, 2, 4);

        assert_eq!(
            earlier.union(later),
            SourceSpan::new(2, 11, 1, 3)
        );
        assert_eq!(later.union(earlier), earlier.union(later));
    }

    #[test]
    fn source_span_round_trips_through_serde() {
        let span = SourceSpan::new(7, 12, 3, 5);
        let encoded = serde_json::to_string(&span).expect("span should serialize");
        let decoded: SourceSpan =
            serde_json::from_str(&encoded).expect("span should deserialize");

        assert_eq!(decoded, span);
    }
}
