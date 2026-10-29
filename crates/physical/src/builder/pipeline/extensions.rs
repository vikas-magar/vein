use super::*;
use crate::builder::extension::{PhysicalExtensionLowerer, PhysicalExtensionLoweringContext};
use plan::PlanTraversal;

impl DefaultPipelineStageBuilder {
    /// Validates and constructs one stage from the authoritative physical tree.
    pub fn build_canonical(&self, plan: PhysicalPlan) -> Result<Stage> {
        plan.validate()?;
        let graph = DerivedPipelineGraph::derive(&plan)?;
        Ok(Stage::new(StageId::new()).with_pipeline_graph(graph).with_task(
            Task::new(TaskId::new(), PartitionId::from_raw(0), plan)))
    }

    /// Registers a canonical extension lowerer, rejecting ambiguous identities.
    pub fn with_extension_lowerer(mut self, lowerer: Arc<dyn PhysicalExtensionLowerer>) -> Result<Self> {
        let id = lowerer.operator_type_id();
        if self.extensions.iter().any(|registered| registered.operator_type_id() == id) {
            return Err(VeinError::planning(format!("extension lowerer '{id}' is already registered"), None));
        }
        let mut extensions = self.extensions.iter().cloned().collect::<Vec<_>>();
        extensions.push(lowerer);
        self.extensions = extensions.into();
        Ok(self)
    }

    pub(super) fn lower_registered_extension(&self, plan: &LogicalPlan) -> Result<(Arc<PhysicalPlanNode>, VeinSchema)> {
        for lowerer in self.extensions.iter() {
            let Some(node) = lowerer.lower(plan, self)? else { continue };
            let PhysicalPlanOperator::Extension { descriptor, inputs } = &node.operator else {
                return Err(VeinError::planning("extension lowerer must return a canonical extension node", None));
            };
            if descriptor.factory_id != lowerer.operator_type_id() {
                return Err(VeinError::planning("extension lowerer returned a different implementation identity", None));
            }
            descriptor.validate_inputs(inputs)?;
            if node.output_schema.fields != plan.schema().fields {
                return Err(VeinError::planning("extension output schema differs from the logical schema", None));
            }
            return Ok((Arc::clone(&node), (*node.output_schema).clone()));
        }
        Err(VeinError::unsupported(format!(
            "logical operator '{}' is not supported by recursive physical lowering", logical_node_name(plan)), None))
    }
}

impl PhysicalExtensionLoweringContext for DefaultPipelineStageBuilder {
    fn lower_input(&self, logical: &LogicalPlan) -> Result<Arc<PhysicalPlanNode>> {
        self.lower_node(logical).map(|(node, _)| node)
    }
}
