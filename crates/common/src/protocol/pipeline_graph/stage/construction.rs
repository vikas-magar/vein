use super::*;
use crate::protocol::pipeline_graph::{DerivedPipeline, PipelineBoundaryKind};

impl DerivedPipeline {
    /// Builds source-to-sink phase bindings without copying operator semantics.
    pub fn stages(&self) -> Result<Vec<PipelineStage>> {
        if self.source.kind != PipelineBoundaryKind::Source {
            return Err(VeinError::planning("pipeline source has an invalid boundary kind", None));
        }
        if Arc::ptr_eq(&self.source.node, &self.sink.node)
            && matches!(&self.source.node.operator, PhysicalPlanOperator::Extension { descriptor, .. }
                if descriptor.role == OperatorRole::Sink)
        {
            if !self.operators.is_empty() || self.sink.kind != PipelineBoundaryKind::ResultSink {
                return Err(VeinError::planning("terminal finalization pipeline contains streaming work", None));
            }
            return Ok(vec![PipelineStage::new(&self.source.node, PipelinePhase::Finalize)?]);
        }
        let source_valid = match &self.source.node.operator {
            PhysicalPlanOperator::Pipeline { .. } | PhysicalPlanOperator::Aggregate { .. }
            | PhysicalPlanOperator::Distinct { .. } | PhysicalPlanOperator::Sort { .. } => true,
            PhysicalPlanOperator::Extension { descriptor, .. } =>
                descriptor.role == OperatorRole::Source
                    || descriptor.inputs.iter().all(|input| input.mode == ExtensionInputMode::Blocking),
            _ => false,
        };
        if !source_valid {
            return Err(VeinError::planning("pipeline source is not a scan or finalized blocker", None));
        }
        let mut stages = Vec::with_capacity(self.operators.len() + 2);
        stages.push(PipelineStage::new(&self.source.node, PipelinePhase::Source)?);
        for operator in &self.operators {
            stages.push(PipelineStage::new(operator, PipelinePhase::Transform(streaming_port(operator)?))?);
        }
        let port = match (&self.sink.kind, &self.sink.node.operator) {
            (PipelineBoundaryKind::JoinBuild, PhysicalPlanOperator::Join { .. }) => OperatorInputPort::new(1),
            (PipelineBoundaryKind::AggregateInput, PhysicalPlanOperator::Aggregate { .. })
            | (PipelineBoundaryKind::DistinctInput, PhysicalPlanOperator::Distinct { .. })
            | (PipelineBoundaryKind::SortInput, PhysicalPlanOperator::Sort { .. }) => OperatorInputPort::new(0),
            (PipelineBoundaryKind::ResultSink, PhysicalPlanOperator::Sink { .. }) => OperatorInputPort::new(0),
            (PipelineBoundaryKind::ResultSink, PhysicalPlanOperator::Extension { descriptor, .. })
            if descriptor.role == OperatorRole::Sink => streaming_port(&self.sink.node)?,
            (PipelineBoundaryKind::ExtensionInput(port), PhysicalPlanOperator::Extension { descriptor, .. })
            if descriptor.inputs.get(port.index()).is_some_and(|input| input.mode == ExtensionInputMode::Blocking) => *port,
            _ => return Err(VeinError::planning("pipeline sink boundary does not match its canonical operator", None)),
        };
        stages.push(PipelineStage::new(&self.sink.node, PipelinePhase::Sink(port))?);
        for pair in stages.windows(2) {
            let port = pair[1].phase().input_port().ok_or_else(||
                VeinError::planning("streaming stage has no input port", None))?;
            let child = pair[1].node().operator.inputs().nth(port.index()).ok_or_else(||
                VeinError::planning("streaming stage child is missing", None))?;
            if !Arc::ptr_eq(pair[0].node(), child) {
                return Err(VeinError::planning("pipeline stages do not follow canonical child edges", None));
            }
            let (Some(output), Some(input)) = (pair[0].output_schema(), pair[1].input_schema()) else {
                return Err(VeinError::planning("pipeline contains an invalid streaming boundary", None));
            };
            if output.fields != input.fields {
                return Err(VeinError::planning("pipeline stage schemas are not contiguous", None));
            }
        }
        Ok(stages)
    }
}
