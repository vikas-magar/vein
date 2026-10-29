use super::super::OperatorFactoryRegistry;
use super::super::OperatorNodeId;
use super::{CanonicalPlanIndex, PreparedOperator};
use common::Result;
use common::protocol::{DerivedPipelineGraph, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator};
use std::collections::HashMap;
use std::sync::Arc;

pub(in crate::pipeline::resumable) struct PreparedPlanParts {
    pub(in crate::pipeline::resumable) graph: DerivedPipelineGraph,
    pub(in crate::pipeline::resumable) index: CanonicalPlanIndex,
    pub(in crate::pipeline::resumable) operators: HashMap<OperatorNodeId, Arc<PreparedOperator>>,
}

/// Bound extension state and canonical graph for one query.
pub struct PreparedExtensionPlan {
    parts: PreparedPlanParts,
}

impl PreparedExtensionPlan {
    /// Validates descriptors before allocating extension state.
    pub fn prepare(plan: &PhysicalPlan, registry: &OperatorFactoryRegistry) -> Result<Self> {
        plan.validate()?;
        let graph = DerivedPipelineGraph::derive(plan)?;
        let index = CanonicalPlanIndex::new(plan);
        let mut definitions = Vec::new();
        for node in &index.nodes {
            let PhysicalPlanOperator::Extension { descriptor, .. } = &node.operator else { continue };
            let id = index.id(node)?;
            let factory = Arc::clone(registry.resolve(&descriptor.factory_id)?);
            let definition = factory.decode_definition(descriptor, index.metadata(node)?)?;
            definitions.push((id, factory, definition));
        }
        let mut operators = HashMap::with_capacity(definitions.len());
        for (id, factory, definition) in definitions {
            let global = factory.create_global(definition.as_ref())?;
            operators.insert(id, Arc::new(PreparedOperator { definition, factory, global }));
        }
        Ok(Self { parts: PreparedPlanParts { graph, index, operators } })
    }

    /// Borrows boundaries and dependencies derived from canonical nodes.
    pub fn graph(&self) -> &DerivedPipelineGraph { &self.parts.graph }

    /// Borrows a validated extension implementation for this query node.
    pub fn operator(&self, node: &Arc<PhysicalPlanNode>) -> Option<&PreparedOperator> {
        self.parts.index.id(node).ok().and_then(|id| self.parts.operators.get(&id)).map(Arc::as_ref)
    }

    /// Number of bound extension nodes.
    pub fn len(&self) -> usize { self.parts.operators.len() }
    /// Whether there are no bound extensions.
    pub fn is_empty(&self) -> bool { self.parts.operators.is_empty() }

    pub(in crate::pipeline::resumable) fn into_parts(self) -> PreparedPlanParts { self.parts }
}
