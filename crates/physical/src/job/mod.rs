//! The job graph: stages and the shuffle edges between them.

use crate::stage::Stage;
use common::identifiers::{QueryId, StageId};
use common::protocol::ExecutionMode;
use error::VeinError;
use std::collections::{HashMap, HashSet};

/// A directed edge representing a shuffle dependency between stages.
#[derive(Debug, Clone)]
pub struct StageEdge {
    /// The producing (upstream) stage.
    pub from_stage: StageId,
    /// The consuming (downstream) stage.
    pub to_stage: StageId,
}

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
    /// `topological_order` walks backwards from the sink over producers so it
    /// emits leaves (sources) first.
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

        // Validate every edge endpoint exists so downstream `get_stage().unwrap()`
        // sites can never panic on a dangling reference.
        for edge in &edges {
            if !stages_map.contains_key(&edge.from_stage) {
                return Err(VeinError::internal(format!(
                    "edge references unknown producer stage {:?}",
                    edge.from_stage
                )));
            }
            if !stages_map.contains_key(&edge.to_stage) {
                return Err(VeinError::internal(format!(
                    "edge references unknown consumer stage {:?}",
                    edge.to_stage
                )));
            }
        }

        // Build both a forward map (for cycle detection) and the predecessor
        // map (for leaves-first topological ordering).
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

        // Kahn's algorithm: reject any cycle up front so `topological_order`
        // (which returns an infallible Vec) is guaranteed a valid ordering.
        let mut in_degree: HashMap<StageId, usize> =
            stages_map.keys().map(|&id| (id, 0)).collect();
        for edge in &edges {
            *in_degree
                .get_mut(&edge.to_stage)
                .expect("endpoint validated") += 1;
        }
        let mut queue: std::collections::VecDeque<StageId> = in_degree
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();
        let mut processed = 0;
        while let Some(id) = queue.pop_front() {
            processed += 1;
            if let Some(consumers) = forward.get(&id) {
                for &consumer in consumers {
                    let deg = in_degree
                        .get_mut(&consumer)
                        .expect("endpoint validated");
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(consumer);
                    }
                }
            }
        }
        if processed != stages_map.len() {
            return Err(VeinError::internal("job contains a cycle"));
        }

        // The sink must itself be a stage: `topological_order` starts from the
        // output stage, so a missing sink would yield an ordering containing a
        // nonexistent StageId and later `get_stage().unwrap()` sites would panic.
        if !stages_map.contains_key(&output_stage) {
            return Err(VeinError::internal(format!(
                "output stage {:?} is not in the stage set",
                output_stage
            )));
        }

        // Every stage must be reachable from the sink via producer edges;
        // otherwise it would be silently dropped by `topological_order` (which
        // walks predecessors from the output stage). Reject disconnected stages
        // up front so the whole graph is always scheduled.
        let mut reachable: HashSet<StageId> = HashSet::new();
        let mut stack = vec![output_stage];
        while let Some(id) = stack.pop() {
            if !reachable.insert(id) {
                continue;
            }
            if let Some(producers) = predecessors.get(&id) {
                for &p in producers {
                    stack.push(p);
                }
            }
        }
        if reachable.len() != stages_map.len() {
            return Err(VeinError::internal(format!(
                "{} stage(s) are unreachable from the output stage",
                stages_map.len() - reachable.len()
            )));
        }

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
    ///
    /// Walks the predecessor edges backwards from the output (sink) stage in
    /// post-order, so every producer appears before its consumers. [`Job::new`]
    /// guarantees the graph is acyclic and every edge endpoint exists, so this
    /// cannot panic and cannot omit a reachable stage.
    pub fn topological_order(&self) -> Vec<StageId> {
        let mut order = Vec::new();
        let mut visited = HashSet::new();

        fn visit(
            node: StageId,
            pred: &HashMap<StageId, Vec<StageId>>,
            visited: &mut HashSet<StageId>,
            order: &mut Vec<StageId>,
        ) {
            if !visited.insert(node) {
                return;
            }
            if let Some(producers) = pred.get(&node) {
                for &producer in producers {
                    visit(producer, pred, visited, order);
                }
            }
            order.push(node);
        }

        visit(
            self.output_stage,
            &self.predecessors,
            &mut visited,
            &mut order,
        );
        order
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
        StageEdge {
            from_stage: StageId::from_raw(from),
            to_stage: StageId::from_raw(to),
        }
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
