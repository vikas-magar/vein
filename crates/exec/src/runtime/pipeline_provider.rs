use super::ExecutorBuilder;
use crate::pipeline::resumable::OperatorMetadata;
use crate::pipeline::resumable::executable::{PipelineOperatorProvider, PipelineOperatorState};
use common::Result;
use common::protocol::PhysicalPlanNode;
use common::protocol::pipeline_graph::PipelinePhase;
use std::sync::Arc;

impl<B: ExecutorBuilder + ?Sized> PipelineOperatorProvider for B {
    fn prepare(&self, node: &Arc<PhysicalPlanNode>, metadata: Arc<OperatorMetadata>,
               phases: &[PipelinePhase]) -> Result<Arc<dyn PipelineOperatorState>> {
        self.prepare_pipeline_operator(node, metadata, phases)
    }
}
