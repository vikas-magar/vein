//! The job graph: stages and the shuffle edges between them.

pub mod edge;
pub mod order;
pub mod validate;

pub use edge::StageEdge;

use crate::stage::Stage;
use common::identifiers::{QueryId, StageId};
use common::protocol::ExecutionMode;
use error::VeinError;
use std::collections::HashMap;

/// One query's complete physical plan: a validated, topologically-ordered DAG
/// of [`Stage`]s.
///
/// A [`Job`] is the output of the [`PhysicalPlanner`](crate::PhysicalPlanner)
/// for a single query, and is what the scheduler consumes to drive execution.
#[derive(Debug, Clone)]
pub struct Job {
    /// The query this job answers.
    pub query_id: QueryId,
    /// The execution mode for all stages in this job.
    mode: ExecutionMode,
    /// The stages of the job, keyed by stage id.
    stages: HashMap<StageId, Stage>,
    /// Predecessor adjacency: `StageId -> Vec<StageId>` that feed into it.
    predecessors: HashMap<StageId, Vec<StageId>>,
    /// The stage that produces the job's final output.
    output_stage: StageId,
}

impl Job {
    /// Create a new [`Job`], validating the stage graph.
    ///
    /// Rejects unknown edge endpoints, cycles, a missing output stage, and
    /// stages unreachable from the output stage, so that later infallible
    /// accessors ([`Job::get_stage`], [`Job::topological_order`]) are safe.
    pub fn new(
        query_id: QueryId,
        stages: Vec<Stage>,
        edges: Vec<StageEdge>,
        output_stage: StageId,
        mode: ExecutionMode,
    ) -> Result<Self, VeinError> {
        let mut stages_map = HashMap::new();
        for stage in stages {
            stages_map.insert(stage.stage_id, stage);
        }

        // 1. Validate edge endpoints
        validate::validate_edge_endpoints(&stages_map, &edges)?;

        // 2. Build forward and predecessor adjacency maps
        let mut forward: HashMap<StageId, Vec<StageId>> = HashMap::new();
        let mut predecessors: HashMap<StageId, Vec<StageId>> = HashMap::new();
        for edge in &edges {
            forward
                .entry(edge.from_stage)
                .or_default()
                .push(edge.to_stage);
            predecessors
                .entry(edge.to_stage)
                .or_default()
                .push(edge.from_stage);
        }

        // 3. Detect cycles using Kahn's algorithm
        validate::detect_cycles(&stages_map, &edges, &forward)?;

        // 4. Validate sink existence and reachability
        validate::validate_reachability(&stages_map, output_stage, &predecessors)?;

        Ok(Self {
            query_id,
            mode,
            stages: stages_map,
            predecessors,
            output_stage,
        })
    }

    /// Return the execution mode for this job.
    pub fn mode(&self) -> &ExecutionMode {
        &self.mode
    }

    /// Return a specific stage.
    pub fn get_stage(&self, id: StageId) -> Option<&Stage> {
        self.stages.get(&id)
    }

    /// Returns the stages in topological order (leaves first).
    pub fn topological_order(&self) -> Vec<StageId> {
        order::compute_topological_order(self.output_stage, &self.predecessors)
    }

    /// Return the ID of the final output stage.
    pub fn output_stage_id(&self) -> StageId {
        self.output_stage
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(id: u64) -> Stage {
        Stage::new(StageId::from_raw(id))
    }

    fn edge(from: u64, to: u64) -> StageEdge {
        StageEdge::new(StageId::from_raw(from), StageId::from_raw(to))
    }

    fn positions(order: &[StageId], id: u64) -> usize {
        order
            .iter()
            .position(|&s| s == StageId::from_raw(id))
            .unwrap_or(usize::MAX)
    }

    /// A→B→D, A→C→D: all four stages present, producers before consumers.
    #[test]
    fn test_dogleg_order_is_leaves_first() {
        let job = Job::new(
            QueryId::new(),
            vec![stage(1), stage(2), stage(3), stage(4)],
            vec![edge(1, 2), edge(2, 4), edge(1, 3), edge(3, 4)],
            StageId::from_raw(4),
            ExecutionMode::Batch,
        )
        .expect("valid job");

        let order = job.topological_order();
        assert_eq!(order.len(), 4, "all stages present: {order:?}");
        for e in [edge(1, 2), edge(2, 4), edge(1, 3), edge(3, 4)] {
            let (f, t) = (
                positions(&order, e.from_stage.into_raw()),
                positions(&order, e.to_stage.into_raw()),
            );
            assert!(
                f < t,
                "producer {} must precede consumer {} in {order:?}",
                e.from_stage.into_raw(),
                e.to_stage.into_raw()
            );
        }
        assert_eq!(order.last(), Some(&StageId::from_raw(4)));
    }

    #[test]
    fn test_cycle_rejected() {
        let err = Job::new(
            QueryId::new(),
            vec![stage(1), stage(2)],
            vec![edge(1, 2), edge(2, 1)],
            StageId::from_raw(2),
            ExecutionMode::Batch,
        )
        .expect_err("cycle must be rejected");
        assert!(err.to_string().contains("cycle"), "got {err}");
    }

    #[test]
    fn test_single_stage() {
        let job = Job::new(
            QueryId::new(),
            vec![stage(1)],
            vec![],
            StageId::from_raw(1),
            ExecutionMode::Batch,
        )
        .expect("valid job");
        assert_eq!(job.topological_order(), vec![StageId::from_raw(1)]);
    }

    #[test]
    fn test_dangling_edge_rejected() {
        let err = Job::new(
            QueryId::new(),
            vec![stage(1)],
            vec![edge(1, 99)],
            StageId::from_raw(1),
            ExecutionMode::Batch,
        )
        .expect_err("dangling edge must be rejected");
        assert!(err.to_string().contains("unknown"), "got {err}");
    }

    #[test]
    fn test_missing_output_stage_rejected() {
        let err = Job::new(
            QueryId::new(),
            vec![stage(1)],
            vec![],
            StageId::from_raw(99),
            ExecutionMode::Batch,
        )
        .expect_err("output stage not in stage set must be rejected");
        assert!(
            err.to_string().contains("not in the stage set"),
            "got {err}"
        );
    }

    #[test]
    fn test_unreachable_stage_rejected() {
        let err = Job::new(
            QueryId::new(),
            vec![stage(1), stage(2), stage(3)],
            vec![edge(1, 2)],
            StageId::from_raw(2),
            ExecutionMode::Batch,
        )
        .expect_err("disconnected stage must be rejected");
        assert!(err.to_string().contains("unreachable"), "got {err}");
    }
}
