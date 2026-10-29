use super::*;
use crate::protocol::extension::{ExtensionInputMode, OperatorInputPort, OperatorRole};

impl DerivedPipelineGraph {
    pub(super) fn derive_extension(
        &mut self,
        node: &Arc<PhysicalPlanNode>,
        mut operators: Vec<Arc<PhysicalPlanNode>>,
        sink: PipelineBoundary,
    ) -> Result<Vec<PipelineId>> {
        let PhysicalPlanOperator::Extension { descriptor, inputs } = &node.operator else {
            return Err(VeinError::internal("extension graph builder received another operator"));
        };
        descriptor.validate_inputs(inputs)?;
        if descriptor.role == OperatorRole::Source {
            operators.reverse();
            return Ok(vec![self.push_pipeline(PipelineBoundary {
                kind: PipelineBoundaryKind::Source,
                node: Arc::clone(node),
            }, operators, sink)]);
        }
        let streaming = descriptor.inputs.iter().enumerate()
            .filter(|(_, input)| input.mode == ExtensionInputMode::Streaming)
            .map(|(port, _)| port).collect::<Vec<_>>();
        if streaming.len() > 1 {
            return Err(VeinError::planning("extension requires multiple streaming input ports; the linear pipeline graph cannot schedule that capability", None));
        }
        let mut prerequisites = Vec::new();
        for (port, child) in inputs.iter().enumerate() {
            if streaming.first() == Some(&port) { continue; }
            let boundary = PipelineBoundary {
                kind: PipelineBoundaryKind::ExtensionInput(OperatorInputPort::new(port)),
                node: Arc::clone(node),
            };
            prerequisites.extend(self.derive_input(child, Vec::new(), boundary)?);
        }
        let outputs = if let Some(port) = streaming.first() {
            if descriptor.role != OperatorRole::Sink { operators.push(Arc::clone(node)); }
            self.derive_input(&inputs[*port], operators, sink)?
        } else {
            return Ok(self.push_blocking_output(node, operators, sink, prerequisites,
                                                PipelineDependencyKind::BlockingInput));
        };
        for prerequisite in prerequisites {
            for &dependent in &outputs {
                self.dependencies.push(PipelineDependency {
                    prerequisite,
                    dependent,
                    kind: PipelineDependencyKind::BlockingInput,
                });
            }
        }
        Ok(outputs)
    }
}
