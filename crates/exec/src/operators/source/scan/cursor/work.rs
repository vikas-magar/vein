//! Provider-defined work units shared by independently resumable scan workers.
use super::{ScanCursor, ScanStage};
use crate::operators::source::scan::ScanWorkerState;
use crate::runtime::{ExecutorBuilder, QueryExecutionResources};
use crate::runtime::filter::RuntimeFilterSet;
use common::{Result, VeinError};
use common::protocol::{DataSource, ExecuteTask};
use common::identifiers::TaskId;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Shared morsel ownership; file formats retain authority over unit boundaries.
pub struct ScanWorkQueue {
    builder: Arc<dyn ExecutorBuilder>,
    pending: Mutex<VecDeque<DataSource>>,
    count: usize,
    task_id: TaskId,
    attempt: u32,
    scope: Arc<QueryExecutionResources>,
    filters: Option<Arc<RuntimeFilterSet>>,
}
impl ScanWorkQueue {
    /// Plans work once using the existing provider, metadata and filter contracts.
    pub fn prepare(builder: Arc<dyn ExecutorBuilder>, source: &DataSource, task: &ExecuteTask,
                   scope: Arc<QueryExecutionResources>, filters: Option<Arc<RuntimeFilterSet>>) -> Result<Arc<Self>> {
        scope.validate_task(task)?;
        let units = builder.partition_scan_source_with_runtime_filters(
            source, builder.max_parallelism().max(1), filters.as_deref())?;
        Ok(Arc::new(Self {
            builder,
            count: units.len(),
            pending: Mutex::new(units.into()),
            task_id: task.task_id,
            attempt: task.attempt,
            scope,
            filters,
        }))
    }
    /// Number of planned units, available without opening any source reader.
    pub fn work_units(&self) -> usize { self.count }

    /// Creates independent local cursor state sharing exactly-once work claims.
    pub fn cursor(self: &Arc<Self>) -> ScanCursor<'static> {
        let queue = Arc::clone(self);
        Box::pin(async_stream::try_stream! {
            let worker = Arc::new(ScanWorkerState::default());
            loop {
                if queue.scope.cancellation().is_cancelled() { Err(VeinError::cancelled())?; }
                let unit = queue.claim()?;
                let Some(unit) = unit else { break };
                let source = queue.builder.build_source_with_query_context(
                    &unit, queue.task_id, queue.attempt, queue.filters.as_deref(),
                    Some(Arc::clone(queue.scope.statistics_context())), Arc::clone(&worker))?;
                let mut cursor = source.into_cursor()?;
                while let Some(view) = cursor.next().await { yield view?; }
            }
        })
    }

    /// Creates a scheduler source without consuming work until its first step.
    pub fn stage(self: &Arc<Self>) -> ScanStage {
        ScanStage::new(self.cursor(), self.scope.cancellation().clone())
    }

    fn claim(&self) -> Result<Option<DataSource>> {
        Ok(self.pending.lock()
            .map_err(|_| VeinError::internal("scan cursor work queue poisoned"))?.pop_front())
    }
}
