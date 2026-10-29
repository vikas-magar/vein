use super::super::{OperatorNodeId, OrderingGuarantee};
use super::ResolvedPipelineProperties;
use super::builder::NodeBinding;
use common::protocol::pipeline_graph::PipelinePhase;
use common::{Result, VeinError};
use std::collections::{HashMap, HashSet};

pub(super) fn validate(bindings: &HashMap<OperatorNodeId, NodeBinding>) -> Result<HashMap<OperatorNodeId, ResolvedPipelineProperties>> {
    let mut properties = HashMap::with_capacity(bindings.len());
    let mut visiting = HashSet::new();
    for id in bindings.keys() { infer(*id, bindings, &mut properties, &mut visiting)?; }
    for binding in bindings.values() {
        for (phase, capabilities) in &binding.capabilities {
            let Some(port) = phase.input_port() else { continue };
            if capabilities.inputs.is_empty() { continue; }
            let child = binding.metadata.children.get(port.index())
                .ok_or_else(|| VeinError::planning("executable phase references a missing input port", None))?;
            let required = capabilities.inputs.get(port.index())
                .ok_or_else(|| VeinError::planning("executable phase requirement is missing for its input port", None))?;
            properties.get(&child.node)
                .ok_or_else(|| VeinError::planning("executable phase producer has no resolved properties", None))?
                .satisfies(required)?;
        }
    }
    Ok(properties)
}

fn infer(
    id: OperatorNodeId,
    bindings: &HashMap<OperatorNodeId, NodeBinding>,
    properties: &mut HashMap<OperatorNodeId, ResolvedPipelineProperties>,
    visiting: &mut HashSet<OperatorNodeId>,
) -> Result<ResolvedPipelineProperties> {
    if let Some(resolved) = properties.get(&id) { return Ok(resolved.clone()); }
    if !visiting.insert(id) {
        return Err(VeinError::planning("operator property dependencies contain a cycle", None));
    }
    let binding = bindings.get(&id)
        .ok_or_else(|| VeinError::planning("canonical child is missing an executable binding", None))?;
    let output = binding.capabilities.get(&PipelinePhase::Source).or_else(|| {
        binding.capabilities.iter().find_map(|(phase, capability)|
            matches!(phase, PipelinePhase::Transform(_)).then_some(capability))
    });
    let mut resolved = ResolvedPipelineProperties::unknown();
    if let Some(capability) = output {
        resolved.partitioning = capability.output_partitioning.clone();
        resolved.ordering = match &capability.output_ordering {
            OrderingGuarantee::Unknown => None,
            OrderingGuarantee::Ordered(keys) => Some(keys.clone()),
            OrderingGuarantee::PreservesInput(port) => {
                let child = binding.metadata.children.get(*port)
                    .ok_or_else(|| VeinError::planning("preserved ordering references a missing child", None))?;
                let inherited = infer(child.node, bindings, properties, visiting)?.ordering;
                inherited.filter(|keys| binding.metadata.output_schema.as_ref().is_some_and(|schema|
                    keys.iter().all(|key| (key.column as usize) < schema.fields().len())))
            }
        };
    }
    visiting.remove(&id);
    properties.insert(id, resolved.clone());
    Ok(resolved)
}
