//! Format-neutral predicates and conservative statistics proofs for data-unit pruning.

use std::cmp::Ordering;
use std::collections::HashSet;

use crate::{ColumnStatistics, DataUnitStatistics};
use types::LogicalLiteral;

/// Comparison operators supported by metadata-only pruning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StatisticsComparison {
    /// Equality comparison.
    Equal,
    /// Inequality comparison.
    NotEqual,
    /// Strict less-than comparison.
    Less,
    /// Less-than-or-equal comparison.
    LessOrEqual,
    /// Strict greater-than comparison.
    Greater,
    /// Greater-than-or-equal comparison.
    GreaterOrEqual,
}

/// A format-neutral necessary condition evaluated against scoped column statistics.
///
/// The predicate is deliberately smaller than the logical expression tree. A
/// planner lowers only the forms whose meaning can be preserved by statistics;
/// unsupported expressions become [`Self::Unknown`] and therefore never cause
/// a data unit to be skipped. Each predicate is used as a SQL `WHERE` condition,
/// so only a possible `TRUE` result keeps a unit.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum StatisticsPredicate {
    /// Every child must be capable of matching for the conjunction to match.
    And(Vec<Self>),
    /// At least one child must be capable of matching for the disjunction to match.
    Or(Vec<Self>),
    /// Comparison between one column and a scalar literal.
    Comparison {
        /// Name of the column in the source schema.
        column: String,
        /// Comparison to apply to the column's exact min/max interval.
        comparison: StatisticsComparison,
        /// Scalar constant from the bound predicate.
        literal: LogicalLiteral,
    },
    /// A null test on one source column.
    IsNull {
        /// Name of the column in the source schema.
        column: String,
    },
    /// A non-null test on one source column.
    IsNotNull {
        /// Name of the column in the source schema.
        column: String,
    },
    /// The expression is unsupported, volatile, or could not be lowered safely.
    /// This always means that the data unit may match.
    Unknown,
}

impl StatisticsPredicate {
    /// Adds every referenced source column to `columns`.
    pub fn collect_columns<'a>(&'a self, columns: &mut HashSet<&'a str>) {
        match self {
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.collect_columns(columns);
                }
            }
            Self::Comparison { column, .. }
            | Self::IsNull { column }
            | Self::IsNotNull { column } => {
                columns.insert(column.as_str());
            }
            Self::Unknown => {}
        }
    }
}

/// Returns whether a data unit could contain a row for which `predicate` is true.
///
/// Missing, incomplete, inconsistent, or type-incompatible statistics are
/// treated as unknown and retain the unit. The proof uses exact min/max bounds
/// only; estimates and histograms are never correctness filters.
pub fn may_match_data_unit(
    predicate: &StatisticsPredicate,
    unit: &DataUnitStatistics,
) -> bool {
    if unit.num_rows == Some(0) {
        return false;
    }

    match predicate {
        StatisticsPredicate::And(children) => {
            children.iter().all(|child| may_match_data_unit(child, unit))
        }
        // An empty or malformed OR is unknown so serialized bad metadata cannot
        // accidentally suppress rows.
        StatisticsPredicate::Or(children) => {
            children.is_empty() || children.iter().any(|child| may_match_data_unit(child, unit))
        }
        StatisticsPredicate::Comparison {
            column,
            comparison,
            literal,
        } => unit
            .column_statistics
            .get(column)
            .is_none_or(|statistics| column_may_match(*comparison, literal, statistics)),
        StatisticsPredicate::IsNull { column } => unit
            .column_statistics
            .get(column)
            .and_then(|statistics| statistics.null_count)
            != Some(0),
        StatisticsPredicate::IsNotNull { column } => {
            let Some(rows) = unit.num_rows else {
                return true;
            };
            !unit
                .column_statistics
                .get(column)
                .and_then(|statistics| statistics.null_count)
                .is_some_and(|nulls| nulls == rows)
        }
        StatisticsPredicate::Unknown => true,
    }
}

/// Returns whether an exact interval can contain a value satisfying a comparison.
///
/// Callers must establish that `min` and `max` are complete for the data unit.
/// Incompatible values and invalid intervals return `true` to preserve rows.
pub fn comparison_may_match_bounds(
    comparison: StatisticsComparison,
    literal: &LogicalLiteral,
    min: &LogicalLiteral,
    max: &LogicalLiteral,
) -> bool {
    let (Some(literal_to_min), Some(literal_to_max), Some(min_to_max)) =
        (literal.try_cmp(min), literal.try_cmp(max), min.try_cmp(max))
    else {
        return true;
    };
    if min_to_max == Ordering::Greater {
        return true;
    }

    match comparison {
        StatisticsComparison::Equal => {
            literal_to_min != Ordering::Less && literal_to_max != Ordering::Greater
        }
        StatisticsComparison::NotEqual => {
            min_to_max != Ordering::Equal || literal_to_min != Ordering::Equal
        }
        StatisticsComparison::Less => min.try_cmp(literal) == Some(Ordering::Less),
        StatisticsComparison::LessOrEqual => {
            min.try_cmp(literal).is_some_and(|order| order != Ordering::Greater)
        }
        StatisticsComparison::Greater => max.try_cmp(literal) == Some(Ordering::Greater),
        StatisticsComparison::GreaterOrEqual => {
            max.try_cmp(literal).is_some_and(|order| order != Ordering::Less)
        }
    }
}

fn column_may_match(
    comparison: StatisticsComparison,
    literal: &LogicalLiteral,
    statistics: &ColumnStatistics,
) -> bool {
    if statistics.min_max_complete != Some(true) {
        return true;
    }
    let (Some(min), Some(max)) = (&statistics.min_value, &statistics.max_value) else {
        return true;
    };
    comparison_may_match_bounds(comparison, literal, min, max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StatisticsUnitKind;

    fn row_group(min: LogicalLiteral, max: LogicalLiteral) -> DataUnitStatistics {
        let mut unit = DataUnitStatistics::new(StatisticsUnitKind::RowGroup, 0);
        unit.num_rows = Some(4);
        unit.column_statistics.insert(
            "value".into(),
            ColumnStatistics {
                null_count: Some(0),
                min_value: Some(min),
                max_value: Some(max),
                min_max_complete: Some(true),
                ..ColumnStatistics::default()
            },
        );
        unit
    }

    fn comparison(comparison: StatisticsComparison, value: i64) -> StatisticsPredicate {
        StatisticsPredicate::Comparison {
            column: "value".into(),
            comparison,
            literal: LogicalLiteral::Int64(value),
        }
    }

    #[test]
    fn exact_bounds_prune_only_comparisons_that_cannot_be_true() {
        let unit = row_group(LogicalLiteral::Int64(10), LogicalLiteral::Int64(20));
        assert!(!may_match_data_unit(
            &comparison(StatisticsComparison::Equal, 9),
            &unit
        ));
        assert!(may_match_data_unit(
            &comparison(StatisticsComparison::Equal, 10),
            &unit
        ));
        assert!(!may_match_data_unit(
            &comparison(StatisticsComparison::Less, 10),
            &unit
        ));
        assert!(!may_match_data_unit(
            &comparison(StatisticsComparison::LessOrEqual, 9),
            &unit
        ));
        assert!(!may_match_data_unit(
            &comparison(StatisticsComparison::Greater, 20),
            &unit
        ));
        assert!(!may_match_data_unit(
            &comparison(StatisticsComparison::GreaterOrEqual, 21),
            &unit
        ));
    }

    #[test]
    fn not_equal_prunes_only_an_exact_single_value_interval() {
        let constant = row_group(LogicalLiteral::Int64(10), LogicalLiteral::Int64(10));
        let range = row_group(LogicalLiteral::Int64(10), LogicalLiteral::Int64(20));
        assert!(!may_match_data_unit(
            &comparison(StatisticsComparison::NotEqual, 10),
            &constant
        ));
        assert!(may_match_data_unit(
            &comparison(StatisticsComparison::NotEqual, 10),
            &range
        ));
    }

    #[test]
    fn incomplete_missing_and_incompatible_statistics_fail_open() {
        let mut unit = row_group(LogicalLiteral::Int64(10), LogicalLiteral::Int64(20));
        unit.column_statistics.get_mut("value").unwrap().min_max_complete = Some(false);
        assert!(may_match_data_unit(
            &comparison(StatisticsComparison::Equal, 100),
            &unit
        ));

        unit.column_statistics.remove("value");
        assert!(may_match_data_unit(
            &comparison(StatisticsComparison::Equal, 100),
            &unit
        ));

        let string_unit = row_group(LogicalLiteral::String("a".into()), LogicalLiteral::String("z".into()));
        assert!(may_match_data_unit(
            &comparison(StatisticsComparison::Equal, 100),
            &string_unit
        ));
    }

    #[test]
    fn boolean_composition_is_conservative_and_uses_where_true_semantics() {
        let unit = row_group(LogicalLiteral::Int64(10), LogicalLiteral::Int64(20));
        let impossible = comparison(StatisticsComparison::Greater, 20);
        let possible = comparison(StatisticsComparison::GreaterOrEqual, 20);
        assert!(!may_match_data_unit(
            &StatisticsPredicate::And(vec![possible.clone(), impossible.clone()]),
            &unit
        ));
        assert!(may_match_data_unit(
            &StatisticsPredicate::Or(vec![possible, impossible]),
            &unit
        ));
        assert!(may_match_data_unit(&StatisticsPredicate::Unknown, &unit));
        assert!(may_match_data_unit(&StatisticsPredicate::Or(Vec::new()), &unit));
    }

    #[test]
    fn null_tests_use_exact_counts_and_unknown_counts_never_prune() {
        let mut unit = row_group(LogicalLiteral::Int64(10), LogicalLiteral::Int64(20));
        assert!(!may_match_data_unit(
            &StatisticsPredicate::IsNull {
                column: "value".into(),
            },
            &unit
        ));
        unit.column_statistics.get_mut("value").unwrap().null_count = None;
        assert!(may_match_data_unit(
            &StatisticsPredicate::IsNull {
                column: "value".into(),
            },
            &unit
        ));

        let mut all_null = row_group(LogicalLiteral::Int64(10), LogicalLiteral::Int64(20));
        all_null.column_statistics.get_mut("value").unwrap().null_count = Some(4);
        assert!(!may_match_data_unit(
            &StatisticsPredicate::IsNotNull {
                column: "value".into(),
            },
            &all_null
        ));
    }

    #[test]
    fn comparison_bounds_obey_inclusive_endpoints() {
        let min = LogicalLiteral::Date(10);
        let max = LogicalLiteral::Date(20);
        assert!(comparison_may_match_bounds(
            StatisticsComparison::GreaterOrEqual,
            &LogicalLiteral::Date(20),
            &min,
            &max
        ));
        assert!(!comparison_may_match_bounds(
            StatisticsComparison::Greater,
            &LogicalLiteral::Date(20),
            &min,
            &max
        ));
    }
}
