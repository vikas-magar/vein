use crate::pipeline::resumable::{OperatorCapabilities, OperatorDefinition, OperatorRole, OperatorTypeId,
                                 OrderingGuarantee, ParallelismCapability, PartitioningGuarantee, TypedOperatorFactory};
use common::Result;
use common::protocol::extension::ExtensionInputMode;
use common::protocol::pipeline_graph::PipelinePhase;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
mod codec;
mod stage;

#[derive(Default)]
pub(in crate::pipeline::resumable::executable::tests) struct Counts {
    pub allocations: AtomicUsize,
    pub inputs: AtomicUsize,
    pub finalizations: AtomicUsize,
}
pub(in crate::pipeline::resumable::executable::tests) struct SinkFactory(pub Arc<Counts>);
pub(in crate::pipeline::resumable::executable::tests) struct Global {
    counts: Arc<Counts>,
    ports: usize,
}

impl TypedOperatorFactory for SinkFactory {
    type Configuration = usize;
    type Codec = codec::Codec;
    type GlobalState = Global;
    type LocalState = PipelinePhase;
    type Operator = stage::Stage;
    fn configuration_codec(&self) -> &Self::Codec { &codec::Codec }
    fn input_modes(&self, definition: &OperatorDefinition<usize>) -> Result<Vec<ExtensionInputMode>> {
        Ok(vec![ExtensionInputMode::Blocking; *definition.configuration()])
    }
    fn validate_definition(&self, definition: &OperatorDefinition<usize>) -> Result<()> {
        if definition.metadata().children.len() != *definition.configuration() {
            return Err(common::VeinError::invalid_input("sink port count does not match configuration"));
        }
        Ok(())
    }
    fn operator_type_id(&self) -> OperatorTypeId { OperatorTypeId::new("tests.blocking_sink").unwrap() }
    fn role(&self) -> OperatorRole { OperatorRole::Sink }
    fn capabilities(&self) -> OperatorCapabilities {
        OperatorCapabilities::new(vec![], OrderingGuarantee::Unknown,
                                  PartitioningGuarantee::Unknown, ParallelismCapability::CombineLocalState)
    }
    fn create_global_state(&self, definition: &OperatorDefinition<usize>) -> Result<Global> {
        self.0.allocations.fetch_add(1, Ordering::Relaxed);
        Ok(Global { counts: Arc::clone(&self.0), ports: *definition.configuration() })
    }
    fn create_local_state(&self, _: &OperatorDefinition<usize>, _: &Global, phase: PipelinePhase) -> Result<PipelinePhase> {
        Ok(phase)
    }
    fn create_operator(&self, _: OperatorDefinition<usize>, global: Arc<Global>,
                       local: PipelinePhase, phase: PipelinePhase) -> Result<stage::Stage> {
        assert_eq!(local, phase);
        Ok(stage::Stage { global, phase: Some(phase), closed: false })
    }
    fn combine_local_state(&self, global: &Global, local: PipelinePhase, phase: PipelinePhase) -> Result<()> {
        assert_eq!(local, phase);
        assert!(matches!(phase, PipelinePhase::Sink(_)));
        global.counts.inputs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
