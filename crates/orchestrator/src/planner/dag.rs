use common::identifiers::StageId;
use common::protocol::{DataSink, DataSource, ExecutionMode, Operator};
use error::VeinError;
use std::collections::{HashMap, HashSet};

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
    /// Predecessor adjacency: StageId -> Vec<StageId> that feed into it.
    /// `topological_order` walks backwards from the sink over producers so it
    /// emits leaves (sources) first.
    predecessors: HashMap<StageId, Vec<StageId>>,
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

        // Validate every edge endpoint exists so downstream `get_stage().unwrap()`
        // sites can never panic on a dangling reference.
        for edge in &edges {
            if !stages.contains_key(&edge.from_stage) {
                return Err(VeinError::internal(format!(
                    "DAG edge references unknown producer stage {:?}",
                    edge.from_stage
                )));
            }
            if !stages.contains_key(&edge.to_stage) {
                return Err(VeinError::internal(format!(
                    "DAG edge references unknown consumer stage {:?}",
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
        let mut in_degree: HashMap<StageId, usize> = stages.keys().map(|&id| (id, 0)).collect();
        for edge in &edges {
            *in_degree.get_mut(&edge.to_stage).expect("endpoint validated") += 1;
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
        if processed != stages.len() {
            return Err(VeinError::internal("DAG contains a cycle"));
        }

        // The sink must itself be a stage: `topological_order` starts from the
        // output stage, so a missing sink would yield an ordering containing a
        // nonexistent StageId and later `get_stage().unwrap()` sites would panic.
        if !stages.contains_key(&output_stage) {
            return Err(VeinError::internal(format!(
                "DAG output stage {:?} is not in the stage set",
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
        if reachable.len() != stages.len() {
            return Err(VeinError::internal(format!(
                "{} stage(s) are unreachable from the output stage",
                stages.len() - reachable.len()
            )));
        }

        Ok(Self {
            stages,
            predecessors,
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
    ///
    /// Walks the predecessor edges backwards from the output (sink) stage in
    /// post-order, so every producer appears before its consumers. `new()`
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
            // Add node after visiting all producers (post-order).
            order.push(node);
        }

        visit(self.output_stage, &self.predecessors, &mut visited, &mut order);

        // order now has producers first and output_stage last.
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
    use common::protocol::{DataSink, DataSource, Operator};

    fn stage(id: u64) -> Stage {
        Stage {
            stage_id: StageId::from_raw(id),
            source: DataSource::Memory,
            operators: Vec::<Operator>::new(),
            sink: DataSink::Discard,
        }
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
        let dag = StageDag::new(
            vec![stage(1), stage(2), stage(3), stage(4)],
            vec![edge(1, 2), edge(2, 4), edge(1, 3), edge(3, 4)],
            StageId::from_raw(4),
            ExecutionMode::Batch,
        )
        .expect("valid DAG");

        let order = dag.topological_order();
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
        // The sink is emitted last.
        assert_eq!(order.last(), Some(&StageId::from_raw(4)));
    }

    #[test]
    fn test_cycle_rejected() {
        let err = StageDag::new(
            vec![stage(1), stage(2)],
            vec![edge(1, 2), edge(2, 1)],
            StageId::from_raw(2),
            ExecutionMode::Batch,
        )
        .err()
        .expect("cycle must be rejected");
        assert!(err.to_string().contains("cycle"), "got {err}");
    }

    #[test]
    fn test_single_stage() {
        let dag = StageDag::new(
            vec![stage(1)],
            vec![],
            StageId::from_raw(1),
            ExecutionMode::Batch,
        )
        .expect("valid DAG");
        assert_eq!(dag.topological_order(), vec![StageId::from_raw(1)]);
    }

    #[test]
    fn test_dangling_edge_rejected() {
        let err = StageDag::new(
            vec![stage(1)],
            vec![edge(1, 99)],
            StageId::from_raw(1),
            ExecutionMode::Batch,
        )
        .err()
        .expect("dangling edge must be rejected");
        assert!(err.to_string().contains("unknown"), "got {err}");
    }

    #[test]
    fn test_missing_output_stage_rejected() {
        let err = StageDag::new(
            vec![stage(1)],
            vec![],
            StageId::from_raw(99), // sink not present in `stages`
            ExecutionMode::Batch,
        )
        .expect_err("output stage not in stage set must be rejected");
        assert!(err.to_string().contains("not in the stage set"), "got {err}");
    }

    #[test]
    fn test_unreachable_stage_rejected() {
        // Stage 3 is disconnected from the sink (2): it would be silently
        // omitted by `topological_order`, so it must be rejected up front.
        let err = StageDag::new(
            vec![stage(1), stage(2), stage(3)],
            vec![edge(1, 2)],
            StageId::from_raw(2),
            ExecutionMode::Batch,
        )
        .expect_err("disconnected stage must be rejected");
        assert!(err.to_string().contains("unreachable"), "got {err}");
    }
}
