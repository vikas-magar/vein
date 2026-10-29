use super::super::{OperatorChild, OperatorMetadata, OperatorNodeId, OperatorRole};
use common::protocol::{PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator};
use common::{Result, VeinError};
use std::collections::HashMap;
use std::sync::Arc;

pub(in crate::pipeline::resumable) struct CanonicalPlanIndex {
    pub(in crate::pipeline::resumable) nodes: Vec<Arc<PhysicalPlanNode>>,
    ids: HashMap<usize, OperatorNodeId>,
}

impl CanonicalPlanIndex {
    pub(in crate::pipeline::resumable) fn new(plan: &PhysicalPlan) -> Self {
        let mut nodes = Vec::new();
        let mut ids = HashMap::new();
        let mut pending = vec![Arc::clone(&plan.root)];
        while let Some(node) = pending.pop() {
            let pointer = Arc::as_ptr(&node) as usize;
            if ids.contains_key(&pointer) { continue; }
            ids.insert(pointer, OperatorNodeId::new(nodes.len() as u64));
            let children = node.operator.inputs().cloned().collect::<Vec<_>>();
            pending.extend(children.into_iter().rev());
            nodes.push(node);
        }
        Self { nodes, ids }
    }

    pub(in crate::pipeline::resumable) fn id(&self, node: &Arc<PhysicalPlanNode>) -> Result<OperatorNodeId> {
        self.ids.get(&(Arc::as_ptr(node) as usize)).copied()
            .ok_or_else(|| VeinError::planning("pipeline stage is not owned by the canonical plan", None))
    }

    pub(in crate::pipeline::resumable) fn metadata(&self, node: &Arc<PhysicalPlanNode>) -> Result<OperatorMetadata> {
        let role = match &node.operator {
            PhysicalPlanOperator::Pipeline { .. } => OperatorRole::Source,
            PhysicalPlanOperator::Sink { .. } => OperatorRole::Sink,
            PhysicalPlanOperator::Extension { descriptor, .. } => descriptor.role,
            _ => OperatorRole::Transform,
        };
        let children = node.operator.inputs().map(|child| Ok(OperatorChild {
            node: self.id(child)?,
            schema: Arc::new(child.output_schema.to_arrow()),
        })).collect::<Result<Vec<_>>>()?;
        let output = (role != OperatorRole::Sink).then(|| Arc::new(node.output_schema.to_arrow()));
        OperatorMetadata::new(self.id(node)?, role, children, output)
    }
}
