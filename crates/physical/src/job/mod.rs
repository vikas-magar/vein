//! A single-stage physical plan for one local query.

use crate::stage::Stage;
use common::identifiers::{QueryId, RequestId};
use common::protocol::ExecutionMode;

/// The physical plan produced for one query by the single-node engine.
#[derive(Debug, Clone)]
pub struct Job {
    /// Request lineage shared by all tasks in this query.
    pub request_id: RequestId,
    /// Identifier for this query.
    pub query_id: QueryId,
    mode: ExecutionMode,
    stage: Stage,
}

impl Job {
    /// Creates a local plan containing one independently executable stage.
    pub fn new(request_id: RequestId, query_id: QueryId, stage: Stage, mode: ExecutionMode) -> Self {
        Self {
            request_id,
            query_id,
            mode,
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
}
