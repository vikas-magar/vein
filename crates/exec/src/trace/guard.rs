use super::*;

/// Records inclusive wall time even when a runtime future is cancelled by its caller.
pub(crate) struct NodeExecutionGuard {
    pub(super) metrics: Option<Arc<NodeMetrics>>,
    pub(super) started: Instant,
    pub(super) outcome: u8,
}

impl NodeExecutionGuard {
    pub(crate) fn finish(&mut self, result: &common::Result<u64>) {
        self.outcome = match result {
            Ok(_) => 1,
            Err(error::VeinError::Cancelled) => 2,
            Err(_) => 3,
        };
    }
}

impl Drop for NodeExecutionGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.statistics.record_elapsed_ns(
                u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            match self.outcome {
                1 => metrics.execution_completed.add(1),
                2 => metrics.execution_cancelled.add(1),
                3 => metrics.execution_failed.add(1),
                _ => metrics.execution_dropped.add(1),
            }
        }
    }
}

pub(super) fn execution_outcome(metrics: &NodeMetrics) -> &'static str {
    if metrics.execution_completed.value() > 0 {
        "complete"
    } else if metrics.execution_cancelled.value() > 0 {
        "cancelled"
    } else if metrics.execution_failed.value() > 0 {
        "failed"
    } else if metrics.execution_dropped.value() > 0 {
        "cancelled_or_dropped"
    } else {
        "not_started"
    }
}
