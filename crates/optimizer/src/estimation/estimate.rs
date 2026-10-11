//! Finite planning estimates; these values are never semantic pruning proofs.

/// Origin of an estimate, or the reason a value is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateSource {
    /// Operator semantics determine the value.
    Logical,
    /// Catalog or injected table statistics.
    Statistics,
    /// Predicate selectivity.
    Filter,
    /// Join key distributions.
    Join,
    /// Grouping key distributions.
    Aggregate,
    /// A row-preserving or bounded operator.
    Derived,
    /// No table statistics were supplied.
    MissingStatistics,
    /// The provider explicitly marked statistics stale.
    StaleStatistics,
    /// Supplied statistics contradict their population or bounds.
    InvalidStatistics,
    /// Required column statistics are unavailable.
    MissingColumnStatistics,
    /// The expression has no supported estimation model.
    UnsupportedExpression,
    /// The operator has no supported estimation model.
    UnsupportedOperator,
    /// Arithmetic produced a non-finite value.
    ArithmeticOverflow,
}

/// Strength of the evidence behind a planning value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EstimateConfidence {
    /// A value cannot be estimated.
    Unknown,
    /// Distribution, independence, or sampling assumptions are required.
    Model,
    /// A supplied statistic, which may itself be approximate.
    Statistics,
    /// A value follows from operator semantics alone.
    Logical,
}

/// A non-negative, finite value or an explicit unknown.
///
/// Confidence describes evidence, not a statistical confidence interval.
/// Even a statistical zero must never be used as a correctness proof.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Estimate {
    value: Option<f64>,
    /// Origin or fallback reason.
    pub source: EstimateSource,
    /// Strength of evidence.
    pub confidence: EstimateConfidence,
}

impl Estimate {
    /// Creates an unknown value with a diagnostic reason.
    pub const fn unknown(source: EstimateSource) -> Self {
        Self { value: None, source, confidence: EstimateConfidence::Unknown }
    }

    /// Creates a checked planning value; invalid arithmetic stays unknown.
    pub fn known(value: f64, source: EstimateSource, confidence: EstimateConfidence) -> Self {
        if !value.is_finite() || value < 0.0 || confidence == EstimateConfidence::Unknown {
            return Self::unknown(EstimateSource::ArithmeticOverflow);
        }
        Self { value: Some(value), source, confidence }
    }

    /// Returns the estimate without inventing a fallback cardinality.
    pub const fn value(self) -> Option<f64> {
        self.value
    }

    /// Combines values while retaining the weakest evidence and unknown reason.
    pub(crate) fn combine(
        self, other: Self, source: EstimateSource, f: impl FnOnce(f64, f64) -> f64,
    ) -> Self {
        match (self.value, other.value) {
            (Some(a), Some(b)) => Self::known(f(a, b), source,
                                              self.confidence.min(other.confidence)),
            (None, _) => self,
            (_, None) => other,
        }
    }

    pub(crate) fn map(self, source: EstimateSource, f: impl FnOnce(f64) -> f64) -> Self {
        match self.value {
            Some(value) => Self::known(f(value), source, self.confidence),
            None => self,
        }
    }

    pub(crate) fn model(value: f64, source: EstimateSource) -> Self {
        Self::known(value, source, EstimateConfidence::Model)
    }

    pub(crate) fn logical(value: f64) -> Self {
        Self::known(value, EstimateSource::Logical, EstimateConfidence::Logical)
    }
}
