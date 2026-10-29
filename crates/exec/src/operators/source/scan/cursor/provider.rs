//! Query-scoped source binding composed with the existing operator provider.
use super::ScanWorkQueue;
use crate::pipeline::resumable::{BatchProcessor, OperatorMetadata, executable::{PipelineOperatorProvider, PipelineOperatorState}};
use crate::runtime::{ExecutorBuilder, QueryExecutionResources};
use common::protocol::{DataSource, ExecuteTask, PhysicalPlanNode, PhysicalPlanOperator,
                       pipeline_graph::PipelinePhase};
use common::{Result, VeinError};
use std::sync::Arc;

/// Resolves canonical file-source nodes using their registered scan providers.
/// Other operator phases remain delegated while their planned migrations proceed.
pub struct ScanPipelineProvider {
    builder: Arc<dyn ExecutorBuilder>,
    task: ExecuteTask,
    scope: Arc<QueryExecutionResources>,
}
impl ScanPipelineProvider {
    /// Binds request identity once, before creating executable pipeline state.
    pub fn new(builder: Arc<dyn ExecutorBuilder>, task: &ExecuteTask,
               scope: Arc<QueryExecutionResources>) -> Result<Self> {
        scope.validate_task(task)?;
        Ok(Self { builder, task: task.clone(), scope })
    }
}
impl PipelineOperatorProvider for ScanPipelineProvider {
    fn prepare(&self, node: &Arc<PhysicalPlanNode>, metadata: Arc<OperatorMetadata>,
               phases: &[PipelinePhase]) -> Result<Arc<dyn PipelineOperatorState>> {
        if let PhysicalPlanOperator::Pipeline { source, program, .. } = &node.operator {
            if matches!(source.as_ref(), DataSource::File(_)) {
                if phases != [PipelinePhase::Source] {
                    return Err(VeinError::invalid_input("scan requested a non-source phase"));
                }
                let processor: Option<Arc<dyn BatchProcessor>> = if program.steps.is_empty() {
                    None
                } else {
                    Some(Arc::from(self.builder.build_batch_processor(program)?))
                };
                let queue = ScanWorkQueue::prepare(Arc::clone(&self.builder), source,
                                                   &self.task, Arc::clone(&self.scope), None)?;
                return queue.pipeline_state_with_processor(metadata, processor);
            }
        }
        self.builder.prepare_pipeline_operator(node, metadata, phases)
    }
}
