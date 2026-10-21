//! DISTINCT aggregate collectors built from the aggregate's typed grouping path.

use super::*;

mod finish;
mod input;

/// Deduplicates one distinct argument within one logical grouping set.
pub(in crate::operators::aggregate) struct DistinctCollector {
    pub(in crate::operators::aggregate) aggregate_index: usize,
    pub(in crate::operators::aggregate) grouping_set_id: usize,
    /// Parent key indexes, in the same order as the child's leading key columns.
    pub(in crate::operators::aggregate) parent_columns: Vec<usize>,
    pub(in crate::operators::aggregate) sink: Box<AggregateSink>,
}
