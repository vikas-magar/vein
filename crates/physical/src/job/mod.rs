//! A single-stage physical plan for one local query.

use crate::stage::Stage;
use common::identifiers::{QueryId, RequestId};
use common::protocol::ExecutionMode;
use plan::LogicalPlan;
use std::sync::Arc;

/// The physical plan produced for one query by the single-node engine.
#[derive(Debug, Clone)]
pub struct Job {
    /// Request lineage shared by all tasks in this query.
    pub request_id: RequestId,
    /// Identifier for this query.
    pub query_id: QueryId,
    mode: ExecutionMode,
    logical_plan: Arc<LogicalPlan>,
    stage: Stage,
}

impl Job {
    /// Creates a local plan containing one independently executable stage.
    pub fn new(
        request_id: RequestId,
        query_id: QueryId,
        logical_plan: Arc<LogicalPlan>,
        stage: Stage,
        mode: ExecutionMode,
    ) -> Self {
        Self {
            request_id,
            query_id,
            mode,
            logical_plan,
            stage,
        }
    }

    /// Returns the execution mode for this query.
    pub fn mode(&self) -> &ExecutionMode {
        &self.mode
    }

    /// Returns the stage containing this query's executable tasks.
    pub fn stage(&self) -> &Stage {
        &self.stage
    }

    /// Returns the optimized logical plan used to produce this job.
    pub fn logical_plan(&self) -> &Arc<LogicalPlan> {
        &self.logical_plan
    }

    /// Consumes the job, returning its identity and stage so the caller can
    /// move each task into execution without cloning it.
    pub fn into_parts(self) -> (RequestId, QueryId, ExecutionMode, Arc<LogicalPlan>, Stage) {
        (
            self.request_id,
            self.query_id,
            self.mode,
            self.logical_plan,
            self.stage,
        )
    }
}
