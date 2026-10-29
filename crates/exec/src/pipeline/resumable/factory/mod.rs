use std::any::Any;
use std::sync::Arc;

use common::protocol::pipeline_graph::PipelinePhase;
use common::{Result, VeinError};
mod phase;

use super::operator::OperatorAdapter;
use super::*;
use common::protocol::extension::{ExtensionInputMode, PhysicalExtension};

mod adapter;
use super::{
    AnyOperatorDefinition, ErasedResumableOperator, LocalStateOperator, OperatorCapabilities,
    OperatorDefinition, OperatorTypeId,
};
pub(super) use adapter::FactoryAdapter;

/// Shared type-erased owner for one factory-created query operator state.
pub type OperatorGlobalState = Arc<dyn Any + Send + Sync>;

/// Typed extension factory. Implementations own state policy and stage creation.
pub trait TypedOperatorFactory: Send + Sync + 'static {
    /// Immutable configuration type accepted by this factory.
    type Configuration: Any + Send + Sync;
    /// Configuration codec resolved once during canonical plan preparation.
    type Codec: super::OperatorConfigurationCodec<Configuration=Self::Configuration>;
    /// Returns the portable configuration codec for this implementation.
    fn configuration_codec(&self) -> &Self::Codec;
    /// Declares the scheduling relationship of each child input port.
    fn input_modes(&self, definition: &OperatorDefinition<Self::Configuration>) -> Result<Vec<ExtensionInputMode>>;
    /// Validates configuration and schema semantics before mutable state is created.
    fn validate_definition(&self, definition: &OperatorDefinition<Self::Configuration>) -> Result<()>;
    /// State shared by all instances created for one query node.
    type GlobalState: Any + Send + Sync;
    /// State owned by one instance and returned after it finishes.
    type LocalState: Any + Send;
    /// Concrete stage implementation, erased only at registry construction.
    type Operator: LocalStateOperator<LocalState=Self::LocalState> + 'static;

    /// Stable registration identifier.
    fn operator_type_id(&self) -> OperatorTypeId;
    /// Canonical role accepted by this implementation.
    fn role(&self) -> OperatorRole;
    /// Declares data-flow and scheduling guarantees.
    fn capabilities(&self) -> OperatorCapabilities;
    /// Constructs query-node shared state once.
    fn create_global_state(
        &self,
        definition: &OperatorDefinition<Self::Configuration>,
    ) -> Result<Self::GlobalState>;
    /// Constructs per-instance state before the stage is created.
    fn create_local_state(
        &self,
        definition: &OperatorDefinition<Self::Configuration>,
        global: &Self::GlobalState,
        phase: PipelinePhase,
    ) -> Result<Self::LocalState>;
    /// Creates one stage; sharing and synchronization remain implementation-owned.
    fn create_operator(
        &self,
        definition: OperatorDefinition<Self::Configuration>,
        global: Arc<Self::GlobalState>,
        local: Self::LocalState,
        phase: PipelinePhase,
    ) -> Result<Self::Operator>;
    /// Combines completed local state when `CombineLocalState` is declared.
    fn combine_local_state(
        &self,
        global: &Self::GlobalState,
        local: Self::LocalState,
        phase: PipelinePhase,
    ) -> Result<()>;
}

pub(super) trait ErasedOperatorFactory: Send + Sync {
    fn operator_type_id(&self) -> &OperatorTypeId;
    fn capabilities(&self) -> &OperatorCapabilities;
    fn phase_capabilities(&self, definition: &dyn AnyOperatorDefinition, phase: PipelinePhase) -> Result<OperatorCapabilities>;
    fn decode_definition(&self, descriptor: &PhysicalExtension, metadata: OperatorMetadata) -> Result<Box<dyn AnyOperatorDefinition>>;
    fn encode_configuration(&self, definition: &dyn AnyOperatorDefinition) -> Result<(u32, Arc<[u8]>)>;
    fn input_modes(&self, definition: &dyn AnyOperatorDefinition) -> Result<Vec<ExtensionInputMode>>;
    fn create_global(&self, definition: &dyn AnyOperatorDefinition) -> Result<OperatorGlobalState>;
    fn create_operator(
        &self,
        definition: &dyn AnyOperatorDefinition,
        global: OperatorGlobalState,
        phase: PipelinePhase,
    ) -> Result<Box<dyn ErasedResumableOperator>>;
    fn combine_local(
        &self,
        global: &OperatorGlobalState,
        local: Box<dyn Any + Send>,
        phase: PipelinePhase,
    ) -> Result<()>;
}
