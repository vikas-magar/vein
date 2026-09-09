//! Directed shuffle dependency edge between execution stages.

use common::identifiers::StageId;

/// A directed edge representing a shuffle dependency between stages.
#[derive(Debug, Clone)]
pub struct StageEdge {
    /// The producing (upstream) stage.
    pub from_stage: StageId,
    /// The consuming (downstream) stage.
    pub to_stage: StageId,
}

impl StageEdge {
    /// Creates a new [`StageEdge`].
    pub fn new(from_stage: StageId, to_stage: StageId) -> Self {
        Self {
            from_stage,
            to_stage,
        }
    }
}
