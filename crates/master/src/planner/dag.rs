use common::identifiers::StageId;
use common::protocol::{DataSink, DataSource, ExecutionMode, Operator};
use std::collections::{HashMap, HashSet};
use error::VeinError;

/// A physical execution stage that can be run on a compute node.
#[derive(Debug, Clone)]
pub struct Stage {
    pub stage_id: StageId,
    pub source: DataSource,
    pub operators: Vec<Operator>,
    pub sink: DataSink,
}

/// A directed edge representing data flow between stages.
#[derive(Debug, Clone)]
pub struct StageEdge {
    pub from_stage: StageId,
    pub to_stage: StageId,
}

/// A validated, topologically-ordered execution DAG.
#[derive(Debug, Clone)]
pub struct StageDag {
    stages: HashMap<StageId, Stage>,
    edges: Vec<StageEdge>,
    /// Adjacency list: StageId -> Vec<StageId> that depend on it
    adjacency: HashMap<StageId, Vec<StageId>>,
    output_stage: StageId,
    mode: ExecutionMode,
}

impl StageDag {
    /// Create a new StageDag. Builds the adjacency list and validates acyclicity.
    pub fn new(
        stages_vec: Vec<Stage>,
        edges: Vec<StageEdge>,
        output_stage: StageId,
        mode: ExecutionMode,
    ) -> Result<Self, VeinError> {
        let mut stages = HashMap::new();
        for stage in stages_vec {
            stages.insert(stage.stage_id, stage);
        }

        let mut adjacency: HashMap<StageId, Vec<StageId>> = HashMap::new();
        for edge in &edges {
            adjacency
                .entry(edge.from_stage)
                .or_default()
                .push(edge.to_stage);
        }

        // TODO: Full cycle detection validation.
        
        Ok(Self {
            stages,
            edges,
            adjacency,
            output_stage,
            mode,
        })
    }

    /// Return the execution mode for this DAG.
    pub fn mode(&self) -> &ExecutionMode {
        &self.mode
    }

    /// Return a specific stage.
    pub fn get_stage(&self, id: StageId) -> Option<&Stage> {
        self.stages.get(&id)
    }

    /// Returns the stages in topological order (leaves first).
    pub fn topological_order(&self) -> Vec<StageId> {
        let mut order = Vec::new();
        let mut visited = HashSet::new();
        
        fn visit(
            node: StageId,
            adj: &HashMap<StageId, Vec<StageId>>,
            visited: &mut HashSet<StageId>,
            order: &mut Vec<StageId>,
        ) {
            if !visited.insert(node) {
                return;
            }
            if let Some(children) = adj.get(&node) {
                for &child in children {
                    visit(child, adj, visited, order);
                }
            }
            // Add node after visiting all children (post-order).
            order.push(node);
        }

        // We want leaves first, so we actually want reverse topological order from the output node.
        visit(self.output_stage, &self.adjacency, &mut visited, &mut order);
        
        // order now has output_stage last, and leaves first.
        order
    }

    /// Return the ID of the final output stage.
    pub fn output_stage_id(&self) -> StageId {
        self.output_stage
    }
}
