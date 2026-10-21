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
    /// Anti join with SQL `NOT IN` NULL semantics.
    NullAwareAnti,
    /// Single join that errors when one probe row has multiple matches.
    Single,
    /// Cartesian product.
    Cross,
}

/// Aggregate identity registered by the shared function catalog.
pub use functions::AggregateOperation as AggOpCode;
