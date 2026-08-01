use crate::planner::dag::StageDag;
use common::protocol::ExecutionMode;
use error::VeinError;
// use vcs::VcsClient;

/// The SqlPlanner is responsible for converting a SQL string into a StageDag.
pub struct SqlPlanner {
    // vcs: VcsClient,
}

impl Default for SqlPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlPlanner {
    pub fn new() -> Self {
        Self { }
    }

    /// Parse SQL and produce an execution StageDag.
    pub async fn plan(&self, _sql: &str, _mode: ExecutionMode) -> Result<StageDag, VeinError> {
        // TODO: Integrate crates/parser and crates/planner
        Err(VeinError::internal("SqlPlanner::plan not yet implemented"))
    }
}
