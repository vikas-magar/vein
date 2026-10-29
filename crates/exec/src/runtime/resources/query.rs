use super::{QueryExecutionResources, lifecycle::QueryStatus};
use crate::operators::sink::WriteReceipt;
use common::VeinError;
use std::sync::Arc;
use std::task::Waker;

impl QueryExecutionResources {
    /// Claims the sole fetching cursor; controls and observers do not claim it.
    pub fn claim_result_cursor(&self) -> common::Result<()> { self.lifecycle.claim_cursor() }
    /// Starts elapsed-time measurement on the first execution request.
    pub fn begin_query(&self) { self.lifecycle.begin(); }
    /// Records the first failure before waking/cancelling other work.
    pub fn fail_query(&self, error: VeinError) -> Arc<VeinError> {
        let original = self.record_failure(error);
        self.cancel();
        original
    }
    /// Records failure before component teardown publishes cancellation.
    pub(crate) fn record_failure(&self, error: VeinError) -> Arc<VeinError> {
        self.lifecycle.fail(error)
    }
    /// Records drained output or a confirmed write receipt before scope release.
    pub fn finish_query(&self, receipt: Option<WriteReceipt>) { self.lifecycle.finish(receipt); }
    /// Closes result ownership, preserving a pre-existing terminal outcome.
    pub fn close_query(&self) {
        self.lifecycle.terminate(QueryStatus::Closed, None);
        self.cancel();
    }
    /// Current lifecycle state, independent of resource-disposal cancellation.
    pub fn query_status(&self) -> QueryStatus { self.lifecycle.lock().status }
    /// Original terminal error, shared without cloning or flattening its causes.
    pub fn query_failure(&self) -> Option<Arc<VeinError>> { self.lifecycle.lock().failure.clone() }
    /// Confirmed write receipt, if the execution backend published one.
    pub fn write_receipt(&self) -> Option<Arc<WriteReceipt>> { self.lifecycle.lock().receipt.clone() }
    /// Registers the sole cursor's task before checking readiness.
    pub fn register_cursor_waker(&self, waker: &Waker) { self.lifecycle.waker.register(waker); }
    /// Releases the fetching task's wake registration when the cursor closes.
    pub fn clear_cursor_waker(&self) { self.lifecycle.waker.take(); }
    /// Records ownership transfer after a fetch succeeds.
    pub fn record_delivery(&self, rows: usize) -> common::Result<()> { self.lifecycle.delivered(rows) }
}
