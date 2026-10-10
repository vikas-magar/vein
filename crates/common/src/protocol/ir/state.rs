//! Pipeline breaker state modes, join flavors, and aggregation opcodes for Micro-IR.

use serde::{Deserialize, Serialize};

/// The mode of state materialization for pipeline breakers.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StateMode {
    /// In-memory Hash Table for Hash Join probe.
    HashJoinTable,
    /// Map-side partial hash aggregation partitioned for shuffle.
    TwoPhaseAggregatePartial {
        /// Number of shuffle partitions.
        num_partitions: u16,
    },
    /// Reduce-side final merge aggregation.
    TwoPhaseAggregateMerge,
    /// Window frame partition buffer.
    WindowBuffer,
    /// Local sort buffer.
    SortBuffer,
    /// Bounded Top-N heap.
    TopNHeap {
        /// Maximum number of rows to retain per group.
        n: u64,
    },
}

/// Join flavors supported across distributed state probe stages.
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
    /// Cartesian product: every probe row paired with every build row.
    Cross,
}

/// Aggregate accumulation opcodes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum AggOpCode {
    /// COUNT(input).
    Count,
    /// SUM(input).
    Sum,
    /// MIN(input).
    Min,
    /// MAX(input).
    Max,
    /// AVG(input).
    Avg,
}
