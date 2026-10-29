use super::{QueryProgress, QueryStatus};
use common::identifiers::{QueryId, RequestId};
use common::VeinError;
use exec::operators::sink::WriteReceipt;
use exec::runtime::QueryExecutionResources;
use std::sync::Arc;

/// Cloneable observation/cancellation access; never owns or advances the cursor.
///
/// Dropping a control does not cancel execution. The owning handle can outlive
/// its engine/session because its execution and resource owners are retained.
#[derive(Clone)]
pub struct QueryControl {
    pub(super) resources: Arc<QueryExecutionResources>,
}
impl QueryControl {
    /// Stable query identity.
    pub fn query_id(&self) -> QueryId { self.resources.query_id() }
    /// Stable request identity.
    pub fn request_id(&self) -> RequestId { self.resources.request_id() }
    /// Current lifecycle, preserving completed/failed outcomes after cleanup.
    pub fn status(&self) -> QueryStatus { self.resources.query_status() }
    /// Bounded observed counters; no fabricated completion percentage.
    pub fn progress(&self) -> QueryProgress { self.resources.query_progress() }
    /// Requests cooperative cancellation and wakes a pending fetch.
    pub fn cancel(&self) { self.resources.cancel(); }
    /// Retains the original categorized error and its typed source chain.
    pub fn failure(&self) -> Option<Arc<VeinError>> { self.resources.query_failure() }
    /// A confirmed commit receipt, never a prediction of write success.
    pub fn write_receipt(&self) -> Option<Arc<WriteReceipt>> { self.resources.write_receipt() }
}
