use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use common::Result;
use common::protocol::pipeline_graph::PipelinePhase;
use error::VeinError;

use crate::pipeline::DataChunkView;
use crate::pipeline::resumable::{
    Handoff, InputEvent, LocalStateOperator, OperatorCapabilities, OperatorDefinition,
    OperatorMetadata, OperatorNodeId, OperatorRole, OperatorStep, OperatorTypeId,
    OrderingGuarantee, ParallelismCapability, PartitioningGuarantee, ResumableOperator,
    TypedOperatorFactory,
};
use crate::pipeline::test_support::dense_view;

pub(super) struct Config {
    pub(super) array: ArrayRef,
    pub(super) combined_value: usize,
}

#[derive(Default)]
pub(super) struct Shared(pub(super) AtomicUsize);

pub(super) struct SourceStage {
    output: Option<DataChunkView>,
    local_value: Option<usize>,
    wait_before_output: bool,
}

impl ResumableOperator for SourceStage {
    type WaitReason = &'static str;

    fn push_input(&mut self, _: crate::pipeline::resumable::OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>> {
        Ok(Handoff::Retry(input))
    }

    fn step(&mut self) -> Result<OperatorStep<Self::WaitReason>> {
        if self.wait_before_output {
            self.wait_before_output = false;
            return Ok(OperatorStep::Blocked("source io"));
        }
        Ok(self.output.take().map_or(OperatorStep::Finished, OperatorStep::Output))
    }
}

impl LocalStateOperator for SourceStage {
    type LocalState = usize;

    fn take_local_state(&mut self) -> Result<usize> {
        self.local_value.take().ok_or_else(|| VeinError::internal("local state already taken"))
    }
}

pub(super) struct SourceFactory(pub(super) OperatorTypeId, pub(super) ParallelismCapability);

mod codec;

impl TypedOperatorFactory for SourceFactory {
    type Codec = codec::SourceCodec;
    fn configuration_codec(&self) -> &Self::Codec { &codec::SourceCodec }
    fn input_modes(&self, _: &OperatorDefinition<Config>) -> Result<Vec<common::protocol::extension::ExtensionInputMode>> { Ok(vec![]) }
    fn validate_definition(&self, definition: &OperatorDefinition<Config>) -> Result<()> {
        let schema = definition.metadata().output_schema.as_ref()
            .ok_or_else(|| VeinError::invalid_input("source output schema missing"))?;
        if schema.fields().len() != 1 || schema.field(0).data_type() != definition.configuration().array.data_type() {
            return Err(VeinError::invalid_input("test source schema must match its array"));
        }
        Ok(())
    }

    type Configuration = Config;
    type GlobalState = Shared;
    type LocalState = usize;
    type Operator = SourceStage;

    fn operator_type_id(&self) -> OperatorTypeId { self.0.clone() }
    fn role(&self) -> OperatorRole { OperatorRole::Source }

    fn capabilities(&self) -> OperatorCapabilities {
        OperatorCapabilities::new(
            vec![],
            OrderingGuarantee::Unknown,
            PartitioningGuarantee::Unknown,
            self.1,
        )
    }

    fn create_global_state(&self, _: &OperatorDefinition<Config>) -> Result<Shared> {
        Ok(Shared::default())
    }

    fn create_local_state(&self, definition: &OperatorDefinition<Config>, _: &Shared, _: PipelinePhase) -> Result<usize> {
        Ok(definition.configuration().combined_value)
    }

    fn create_operator(
        &self,
        definition: OperatorDefinition<Config>,
        _: Arc<Shared>,
        local: usize,
        _: PipelinePhase,
    ) -> Result<SourceStage> {
        Ok(SourceStage {
            output: Some(dense_view(vec![Arc::clone(&definition.configuration().array)])),
            local_value: Some(local),
            wait_before_output: true,
        })
    }

    fn combine_local_state(&self, global: &Shared, local: usize, _: PipelinePhase) -> Result<()> {
        global.0.fetch_add(local, Ordering::Relaxed);
        Ok(())
    }
}

pub(super) fn make_definition(type_id: OperatorTypeId) -> OperatorDefinition<Config> {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let metadata = OperatorMetadata::new(OperatorNodeId::new(9), OperatorRole::Source, vec![], Some(schema))
        .expect("source metadata");
    OperatorDefinition::new(type_id, metadata, Config {
        array: Arc::new(Int64Array::from(vec![4, 8])),
        combined_value: 12,
    })
}
