use super::QueryExecutionResources;
use std::sync::Arc;

/// Unique lifetime guard transferred from preparation through result consumption.
///
/// Workers retain resources, never another lifetime guard. Closing the consumer
/// therefore cancels execution even while worker references remain alive.
pub struct QueryExecutionGuard {
    resources: Arc<QueryExecutionResources>,
}

impl QueryExecutionGuard {
    pub(super) fn new(resources: Arc<QueryExecutionResources>) -> Self { Self { resources } }
    /// Borrows the query resource authority without extending execution lifetime.
    pub fn resources(&self) -> &Arc<QueryExecutionResources> { &self.resources }
}

impl Drop for QueryExecutionGuard {
    fn drop(&mut self) { self.resources.cancel(); }
}
