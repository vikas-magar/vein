//! Physical operator semantics shared by planning and execution.

use serde::{Deserialize, Serialize};

/// State retained by an operator that must consume input before producing output.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StateMode {
    /// Hash table used by a hash join.
    HashJoinTable,
    /// Map-side partial aggregation partitioned for exchange.
    TwoPhaseAggregatePartial {
        /// Number of partitions used by the partial aggregate.
        num_partitions: u16,
    },
    /// Reduce-side final merge aggregation.
    TwoPhaseAggregateMerge,
    /// Buffered window partition.
    WindowBuffer,
    /// Buffered local sort input.
    SortBuffer,
    /// Bounded top-N heap.
    TopNHeap {
        /// Maximum number of rows retained per group.
        n: u64,
    },
}

/// Join semantics represented in the canonical physical plan.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum JoinKind {
    /// Inner join.
    Inner,
    /// Left outer join.
    Left,
    /// Right outer join.
    Right,
    /// Full outer join.
    Full,
    /// Left semi join.
    Semi,
    /// Left anti join.
    Anti,
    /// Each matching build row once, with no probe columns.
    RightSemi,
    /// Each unmatched build row once, with no probe columns.
    RightAnti,
    /// Anti join with SQL `NOT IN` NULL semantics.
    NullAwareAnti,
    /// Single join that errors when one probe row has multiple matches.
    Single,
    /// Cartesian product.
    Cross,
}

impl JoinKind {
    /// Whether removing probe rows that cannot match preserves query results.
    pub fn allows_probe_filtering(self) -> bool {
        matches!(self, Self::Inner | Self::Semi | Self::RightSemi | Self::RightAnti)
    }

    /// Equivalent join semantics after exchanging physical inputs.
    /// NULL-aware anti and scalar single joins keep their enforced orientation.
    pub fn inverse(self) -> Option<Self> {
        Some(match self {
            Self::Inner | Self::Full | Self::Cross => self,
            Self::Left => Self::Right,
            Self::Right => Self::Left,
            Self::Semi => Self::RightSemi,
            Self::Anti => Self::RightAnti,
            Self::RightSemi => Self::Semi,
            Self::RightAnti => Self::Anti,
            Self::NullAwareAnti | Self::Single => return None,
        })
    }
}

/// Aggregate identity registered by the shared function catalog.
pub use functions::AggregateOperation as AggOpCode;

#[cfg(test)]
mod tests;
