//! DAG validation, cycle detection, and reachability checks for physical jobs.

use std::collections::{HashMap, HashSet, VecDeque};

use common::identifiers::StageId;
use error::VeinError;

use super::edge::StageEdge;
use crate::stage::Stage;

/// Validates that all edge endpoints reference existing stages.
pub fn validate_edge_endpoints(
    stages: &HashMap<StageId, Stage>,
    edges: &[StageEdge],
) -> Result<(), VeinError> {
    for edge in edges {
        if !stages.contains_key(&edge.from_stage) {
            return Err(VeinError::internal(format!(
                "edge references unknown producer stage {:?}",
                edge.from_stage
            )));
        }
        if !stages.contains_key(&edge.to_stage) {
            return Err(VeinError::internal(format!(
                "edge references unknown consumer stage {:?}",
                edge.to_stage
            )));
        }
    }
    Ok(())
}

/// Runs Kahn's algorithm to ensure the stage DAG contains no cycles.
pub fn detect_cycles(
    stages: &HashMap<StageId, Stage>,
    edges: &[StageEdge],
    forward: &HashMap<StageId, Vec<StageId>>,
) -> Result<(), VeinError> {
    let mut in_degree: HashMap<StageId, usize> = stages.keys().map(|&id| (id, 0)).collect();
    for edge in edges {
        *in_degree.get_mut(&edge.to_stage).expect("endpoint validated") += 1;
    }

    let mut queue: VecDeque<StageId> = in_degree
        .iter()
        .filter(|&(_, &deg)| deg == 0)
        .map(|(&id, _)| id)
        .collect();

    let mut processed = 0;
    while let Some(id) = queue.pop_front() {
        processed += 1;
        if let Some(consumers) = forward.get(&id) {
            for &consumer in consumers {
                let deg = in_degree.get_mut(&consumer).expect("endpoint validated");
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(consumer);
                }
            }
        }
    }

    if processed != stages.len() {
        return Err(VeinError::internal("job contains a cycle"));
    }

    Ok(())
}

/// Validates that the output stage exists and that all stages are reachable from it.
pub fn validate_reachability(
    stages: &HashMap<StageId, Stage>,
    output_stage: StageId,
    predecessors: &HashMap<StageId, Vec<StageId>>,
) -> Result<(), VeinError> {
    if !stages.contains_key(&output_stage) {
        return Err(VeinError::internal(format!(
            "output stage {:?} is not in the stage set",
            output_stage
        )));
    }

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

    Ok(())
}
