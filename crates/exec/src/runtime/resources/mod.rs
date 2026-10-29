//! One query identity, cancellation root, statistics and resource budget.
mod lifetime;
mod pipeline;
mod lifecycle;
mod query;
pub use lifecycle::{QueryStatus, QueryProgress};

use crate::memory::{ExecutionResourceLimits, QueryMemoryTracker};
use crate::spill::SpillStorage;
use common::cancellation::{CancellationRegistration, CancellationToken};
use common::identifiers::{QueryId, RequestId};
use common::protocol::ExecuteTask;
use common::statistics::QueryStatisticsContext;
use common::{Result, VeinError};
pub use lifetime::QueryExecutionGuard;
use std::sync::Arc;

/// Shared authority for all tasks and adaptive stages of one query.
///
/// Returned views retain their own memory leases after the execution guard
/// closes. Cancelling never invalidates already-returned Arrow buffers.
pub struct QueryExecutionResources {
    pub(super) memory: QueryMemoryTracker,
    request_id: RequestId,
    query_id: QueryId,
    statistics: Arc<QueryStatisticsContext>,
    cancel: CancellationToken,
    lifecycle: Arc<lifecycle::Lifecycle>,
    _cancellation: CancellationRegistration,
}

impl QueryExecutionResources {
    /// Creates one scope using the configured storage backend and task context.
    pub fn new(
        task: &ExecuteTask,
        limits: ExecutionResourceLimits,
        cancel: CancellationToken,
        spill: Arc<dyn SpillStorage>,
    ) -> Result<Self> {
        let lifecycle = Arc::new(lifecycle::Lifecycle::new());
        let target = Arc::downgrade(&lifecycle);
        let subscription = cancel.on_cancel(move || {
            if let Some(state) = target.upgrade() {
                state.terminate(QueryStatus::Cancelled, Some(Arc::new(VeinError::cancelled())));
            }
        })?;
        let memory = QueryMemoryTracker::with_spill_storage(limits, cancel.clone(), spill);
        tracing::debug!(target: "vein.audit", event = "query_execution_scope_created",
            request_id = %task.request_id, query_id = %task.query_id,
            memory_limit_bytes = limits.memory_bytes, spill_limit_bytes = limits.spill_disk_bytes,
            statistics_attached = task.statistics_context.is_some(),
            "query identity and resource scope created");
        Ok(Self {
            memory,
            request_id: task.request_id,
            query_id: task.query_id,
            statistics: task
                .statistics_context
                .clone()
                .unwrap_or_else(|| Arc::new(QueryStatisticsContext::new([]))),
            cancel,
            lifecycle,
            _cancellation: subscription,
        })
    }

    /// Identity shared by preparation, tasks and diagnostics.
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }
    /// Identity shared by all physical stages.
    pub fn query_id(&self) -> QueryId {
        self.query_id
    }
    /// The statistics authority retained throughout planning and execution.
    pub fn statistics_context(&self) -> &Arc<QueryStatisticsContext> {
        &self.statistics
    }
    /// The root shared with memory, spill, tasks and readiness subscriptions.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
    /// Stops query work cooperatively and wakes cancellation subscriptions.
    pub fn cancel(&self) {
        if !self.cancel.is_cancelled() {
            tracing::debug!(target: "vein.audit", event = "query_execution_scope_closing",
                request_id = %self.request_id, query_id = %self.query_id,
                memory_used_bytes = self.memory.used(), memory_peak_bytes = self.memory.peak(),
                spill_used_bytes = self.memory.spill_bytes_used(),
                "closing query scope; retained output leases remain valid");
        }
        self.cancel.cancel();
    }
    /// The single reservation tracker, also owning the spill manager.
    pub fn memory_tracker(&self) -> &QueryMemoryTracker {
        &self.memory
    }
    /// Current retained query reservation.
    pub fn memory_used_bytes(&self) -> usize {
        self.memory.used()
    }
    /// Peak reservation across all stages.
    pub fn memory_peak_bytes(&self) -> usize {
        self.memory.peak()
    }
    /// Current accounted spill bytes.
    pub fn spill_used_bytes(&self) -> u64 {
        self.memory.spill_bytes_used()
    }

    /// Rejects cross-query tasks and independently replaced statistics contexts.
    pub fn validate_task(&self, task: &ExecuteTask) -> Result<()> {
        if task.request_id != self.request_id || task.query_id != self.query_id {
            return Err(VeinError::invalid_input(
                "task does not belong to the query resource scope",
            ));
        }
        if task
            .statistics_context
            .as_ref()
            .is_some_and(|stats| !Arc::ptr_eq(stats, &self.statistics))
        {
            return Err(VeinError::invalid_input(
                "task uses a different query statistics context",
            ));
        }
        Ok(())
    }

    /// Owns execution lifetime separately from worker/resource references.
    pub fn guard(self: &Arc<Self>) -> QueryExecutionGuard {
        QueryExecutionGuard::new(Arc::clone(self))
    }

    /// Retains an owned cleanup handle until pruning, close or cancellation.
    ///
    /// Dropping the returned subscription releases the handle normally. Root
    /// cancellation releases it even while the subscription itself stays alive.
    pub fn retain_until_cancel<T: Send + 'static>(&self, resource: T) -> Result<CancellationRegistration> {
        self.cancel.on_cancel(move || drop(resource))
    }
}
