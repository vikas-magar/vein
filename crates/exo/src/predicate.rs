//! Typed address predicates shared by Exo readers and execution planners.

use crate::addr::{
    AddressGranuleMark, MARK_FLAG_HAS_NON_NULL, MARK_FLAG_HAS_NULL,
};

/// A predicate that can be evaluated directly against canonical address values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddressFilter {
    pub column: String,
    pub predicate: AddressPredicate,
}

impl AddressFilter {
    pub fn new(column: impl Into<String>, predicate: AddressPredicate) -> Self {
        Self {
            column: column.into(),
            predicate: predicate.normalize(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddressPredicate {
    Eq(u64),
    NotEq(u64),
    Lt(u64),
    LtEq(u64),
    Gt(u64),
    GtEq(u64),
    Between { lower: u64, upper: u64 },
    In(Vec<u64>),
    IsNull,
    IsNotNull,
}

/// Evaluation contract for address-native predicates.
///
/// Implementations must be conservative for granule pruning: returning true
/// from may_match is allowed to produce false positives, but returning false
/// must never discard a matching row.
pub trait AddressPredicateEvaluator {
    fn may_match(&self, mark: &AddressGranuleMark) -> bool;
    fn matches(&self, value: u64, valid: bool) -> bool;
}

impl AddressPredicateEvaluator for AddressPredicate {
    fn may_match(&self, mark: &AddressGranuleMark) -> bool {
        let has_non_null = mark.flags & MARK_FLAG_HAS_NON_NULL != 0;
        let has_null = mark.flags & MARK_FLAG_HAS_NULL != 0;
        match self {
            Self::IsNull => has_null,
            Self::IsNotNull => has_non_null,
            Self::Eq(value) => has_non_null && *value >= mark.min_vaddr && *value <= mark.max_vaddr,
            Self::NotEq(_) => has_non_null,
            Self::Lt(value) => has_non_null && mark.min_vaddr < *value,
            Self::LtEq(value) => has_non_null && mark.min_vaddr <= *value,
            Self::Gt(value) => has_non_null && mark.max_vaddr > *value,
            Self::GtEq(value) => has_non_null && mark.max_vaddr >= *value,
            Self::Between { lower, upper } => {
                has_non_null && lower <= upper && mark.max_vaddr >= *lower && mark.min_vaddr <= *upper
            }
            Self::In(values) => {
                has_non_null
                    && values
                        .iter()
                        .any(|value| *value >= mark.min_vaddr && *value <= mark.max_vaddr)
            }
        }
    }

    fn matches(&self, value: u64, valid: bool) -> bool {
        match self {
            Self::IsNull => !valid,
            Self::IsNotNull => valid,
            Self::Eq(expected) => valid && value == *expected,
            Self::NotEq(expected) => valid && value != *expected,
            Self::Lt(expected) => valid && value < *expected,
            Self::LtEq(expected) => valid && value <= *expected,
            Self::Gt(expected) => valid && value > *expected,
            Self::GtEq(expected) => valid && value >= *expected,
            Self::Between { lower, upper } => valid && *lower <= value && value <= *upper,
            Self::In(values) => valid && values.binary_search(&value).is_ok(),
        }
    }
}

impl AddressPredicate {
    /// Sorts and deduplicates an IN list for logarithmic row checks.
    pub fn normalize(self) -> Self {
        match self {
            Self::In(mut values) => {
                values.sort_unstable();
                values.dedup();
                Self::In(values)
            }
            predicate => predicate,
        }
    }
}
