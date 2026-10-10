//! Parallel file tasks belonging to one local query plan.

use crate::task::Task;
use common::identifiers::StageId;
use crate::pipeline_graph::DerivedPipelineGraph;

/// A set of independent tasks that together produce one query result.
#[derive(Debug, Clone)]
pub struct Stage {
    /// The unique identifier of this stage.
    pub stage_id: StageId,
    /// The parallel tasks belonging to this stage.
    pub tasks: Vec<Task>,
    /// Scheduling graph derived from the canonical input plan before task splitting.
    pipeline_graph: DerivedPipelineGraph,
}

impl Stage {
    /// Create a new, empty stage with the given identifier.
    pub fn new(stage_id: StageId) -> Self {
        Self {
            stage_id,
            tasks: Vec::new(),
            pipeline_graph: DerivedPipelineGraph::default(),
        }
    }

    /// Attach the graph derived from this stage's canonical physical plan.
    pub fn with_pipeline_graph(mut self, pipeline_graph: DerivedPipelineGraph) -> Self {
        self.pipeline_graph = pipeline_graph;
        self
    }

    /// Return pipeline boundaries and dependencies for inspection and scheduling.
    pub fn pipeline_graph(&self) -> &DerivedPipelineGraph {
        &self.pipeline_graph
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
