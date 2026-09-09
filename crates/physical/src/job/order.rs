//! Leaves-first topological sort traversal for execution stages.

use std::collections::{HashMap, HashSet};

use common::identifiers::StageId;

/// Computes a leaves-first topological ordering of stages starting from `output_stage`.
///
/// Walks the predecessor edges backwards from the output (sink) stage in
/// post-order, so every producer appears before its consumers.
pub fn compute_topological_order(
    output_stage: StageId,
    predecessors: &HashMap<StageId, Vec<StageId>>,
) -> Vec<StageId> {
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

    visit(output_stage, predecessors, &mut visited, &mut order);
    order
}
