//! Parallel file tasks belonging to one local query plan.

use crate::task::Task;
use common::identifiers::StageId;

/// A set of independent tasks that together produce one query result.
#[derive(Debug, Clone)]
pub struct Stage {
    /// The unique identifier of this stage.
    pub stage_id: StageId,
    /// The parallel tasks belonging to this stage.
    pub tasks: Vec<Task>,
}

impl Stage {
    /// Create a new, empty stage with the given identifier.
    pub fn new(stage_id: StageId) -> Self {
        Self {
            stage_id,
            tasks: Vec::new(),
        }
    }

    /// Add a task to this stage.
    pub fn with_task(mut self, task: Task) -> Self {
        self.tasks.push(task);
        self
    }

    /// Borrow the tasks in this stage.
    pub fn tasks(&self) -> &[Task] {
        &self.tasks
    }
}
