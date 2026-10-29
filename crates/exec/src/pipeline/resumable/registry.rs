use common::protocol::pipeline_graph::PipelinePhase;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use common::{Result, VeinError};

use super::factory::{ErasedOperatorFactory, FactoryAdapter};
use super::{
    AnyOperatorDefinition, ErasedResumableOperator, OperatorGlobalState, OperatorTypeId,
    TypedOperatorFactory,
};

/// Startup registry for typed operator factories.
///
/// Resolve a definition and create shared state before execution; the future
/// scheduler can then create per-worker stages without per-row registry lookup.
#[derive(Default)]
pub struct OperatorFactoryRegistry {
    factories: HashMap<OperatorTypeId, Arc<dyn ErasedOperatorFactory>>,
}

impl OperatorFactoryRegistry {
    /// Encodes a typed definition into a portable canonical descriptor.
    pub fn describe(
        &self,
        definition: &dyn AnyOperatorDefinition,
        input_schemas: &[types::VeinSchemaRef],
    ) -> Result<common::protocol::extension::PhysicalExtension> {
        use common::protocol::extension::{ExtensionInput, PhysicalExtension};
        let factory = self.resolve(definition.factory_id())?;
        factory.capabilities().validate_for(definition.metadata())?;
        let modes = factory.input_modes(definition)?;
        if input_schemas.len() != definition.metadata().children.len() || modes.len() != input_schemas.len() {
            return Err(VeinError::planning("extension input schemas and dependency ports must cover every child", None));
        }
        for (schema, child) in input_schemas.iter().zip(definition.metadata().children.iter()) {
            if schema.to_arrow() != *child.schema {
                return Err(VeinError::planning("extension canonical and typed child schemas differ", None));
            }
        }
        let (configuration_version, configuration) = factory.encode_configuration(definition)?;
        let inputs = input_schemas.iter().zip(modes).map(|(schema, mode)| ExtensionInput {
            schema: Arc::clone(schema),
            mode,
        }).collect::<Vec<_>>().into();
        Ok(PhysicalExtension {
            factory_id: definition.factory_id().clone(),
            configuration_version,
            configuration,
            role: definition.metadata().role,
            inputs,
        })
    }

    /// Creates an empty registry.
    pub fn new() -> Self { Self::default() }

    /// Registers one typed factory and rejects duplicate identifiers.
    pub fn register<F: TypedOperatorFactory>(&mut self, factory: F) -> Result<()> {
        let adapter = Arc::new(FactoryAdapter::new(factory));
        let type_id = ErasedOperatorFactory::operator_type_id(adapter.as_ref()).clone();
        if self.factories.contains_key(&type_id) {
            return Err(VeinError::invalid_input(format!(
                "operator factory `{type_id}` is already registered"
            )));
        }
        self.factories.insert(type_id, adapter);
        Ok(())
    }

    /// Creates query-node shared state after validating the definition.
    pub fn create_global_state(
        &self,
        definition: &dyn AnyOperatorDefinition,
    ) -> Result<OperatorGlobalState> {
        self.resolve(definition.factory_id())?.create_global(definition)
    }

    /// Creates one erased stage with its own local state and shared query state.
    pub fn create_operator(
        &self,
        definition: &dyn AnyOperatorDefinition,
        global: OperatorGlobalState,
        phase: PipelinePhase,
    ) -> Result<Box<dyn ErasedResumableOperator>> {
        self.resolve(definition.factory_id())?.create_operator(definition, global, phase)
    }

    /// Combines one completed stage's local state into the query-node state.
    pub fn combine_local_state(
        &self,
        type_id: &OperatorTypeId,
        global: &OperatorGlobalState,
        local: Box<dyn Any + Send>,
        phase: PipelinePhase,
    ) -> Result<()> {
        self.resolve(type_id)?.combine_local(global, local, phase)
    }

    /// Returns registered capabilities for validation or pipeline construction.
    pub fn capabilities(&self, type_id: &OperatorTypeId) -> Result<&super::OperatorCapabilities> {
        Ok(self.resolve(type_id)?.capabilities())
    }

    pub(super) fn resolve(&self, type_id: &OperatorTypeId) -> Result<&Arc<dyn ErasedOperatorFactory>> {
        self.factories.get(type_id).ok_or_else(|| {
            VeinError::invalid_input(format!("operator factory `{type_id}` is not registered"))
        })
    }
}
