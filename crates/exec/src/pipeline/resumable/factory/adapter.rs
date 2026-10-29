use super::*;

pub(in crate::pipeline::resumable) struct FactoryAdapter<F: TypedOperatorFactory> {
    factory: F,
    type_id: OperatorTypeId,
    capabilities: OperatorCapabilities,
}

impl<F: TypedOperatorFactory> FactoryAdapter<F> {
    pub(in crate::pipeline::resumable) fn new(factory: F) -> Self {
        let type_id = factory.operator_type_id();
        let capabilities = factory.capabilities();
        Self { factory, type_id, capabilities }
    }

    fn typed_definition<'a>(
        &self,
        definition: &'a dyn AnyOperatorDefinition,
    ) -> Result<&'a OperatorDefinition<F::Configuration>> {
        if definition.factory_id() != &self.type_id {
            return Err(VeinError::invalid_input("operator definition resolved to the wrong factory"));
        }
        if definition.metadata().role != self.factory.role() {
            return Err(VeinError::planning("extension role differs from the registered factory", None));
        }
        definition.as_any().downcast_ref().ok_or_else(|| {
            VeinError::invalid_input("operator definition configuration type does not match factory")
        })
    }
}

impl<F: TypedOperatorFactory> ErasedOperatorFactory for FactoryAdapter<F> {
    fn operator_type_id(&self) -> &OperatorTypeId { &self.type_id }
    fn capabilities(&self) -> &OperatorCapabilities { &self.capabilities }

    fn decode_definition(&self, descriptor: &PhysicalExtension, metadata: OperatorMetadata) -> Result<Box<dyn AnyOperatorDefinition>> {
        if descriptor.role != self.factory.role() {
            return Err(VeinError::planning("extension role differs from the registered factory", None));
        }
        let codec = self.factory.configuration_codec();
        let configuration = codec.decode(descriptor.configuration_version, &descriptor.configuration)?;
        let definition = OperatorDefinition::new(descriptor.factory_id.clone(), metadata, configuration);
        let modes = self.factory.input_modes(&definition)?;
        if modes.iter().copied().ne(descriptor.inputs.iter().map(|input| input.mode)) {
            return Err(VeinError::planning("extension dependency modes differ from the registered factory", None));
        }
        self.capabilities.validate_for(definition.metadata())?;
        self.factory.validate_definition(&definition)?;
        Ok(Box::new(definition))
    }

    fn encode_configuration(&self, definition: &dyn AnyOperatorDefinition) -> Result<(u32, Arc<[u8]>)> {
        let typed = self.typed_definition(definition)?;
        self.factory.validate_definition(typed)?;
        let codec = self.factory.configuration_codec();
        Ok((codec.version(), codec.encode(typed.configuration())?))
    }

    fn input_modes(&self, definition: &dyn AnyOperatorDefinition) -> Result<Vec<ExtensionInputMode>> {
        self.factory.input_modes(self.typed_definition(definition)?)
    }

    fn phase_capabilities(&self, definition: &dyn AnyOperatorDefinition, phase: PipelinePhase) -> Result<OperatorCapabilities> {
        let typed = self.typed_definition(definition)?;
        phase::validate(typed.metadata(), &self.factory.input_modes(typed)?, phase)?;
        let mut capabilities = self.capabilities.clone();
        if matches!(phase, PipelinePhase::Sink(_) | PipelinePhase::Finalize) {
            capabilities.output_ordering = OrderingGuarantee::Unknown;
            capabilities.output_partitioning = PartitioningGuarantee::Unknown;
        }
        Ok(capabilities)
    }

    fn create_global(&self, definition: &dyn AnyOperatorDefinition) -> Result<OperatorGlobalState> {
        let typed = self.typed_definition(definition)?;
        self.capabilities.validate_for(typed.metadata())?;
        self.factory.validate_definition(typed)?;
        Ok(Arc::new(self.factory.create_global_state(typed)?))
    }

    fn create_operator(
        &self,
        definition: &dyn AnyOperatorDefinition,
        global: OperatorGlobalState,
        phase: PipelinePhase,
    ) -> Result<Box<dyn ErasedResumableOperator>> {
        let typed = self.typed_definition(definition)?;
        self.capabilities.validate_for(typed.metadata())?;
        let global = Arc::downcast::<F::GlobalState>(global)
            .map_err(|_| VeinError::invalid_input("global state type does not match operator factory"))?;
        self.phase_capabilities(definition, phase)?;
        let local = self.factory.create_local_state(typed, &global, phase)?;
        let operator = self.factory.create_operator(typed.clone(), global, local, phase)?;
        Ok(Box::new(OperatorAdapter::for_phase(operator, typed.metadata(), phase)?))
    }

    fn combine_local(
        &self,
        global: &OperatorGlobalState,
        local: Box<dyn Any + Send>,
        phase: PipelinePhase,
    ) -> Result<()> {
        if self.capabilities.parallelism != super::ParallelismCapability::CombineLocalState {
            return Err(VeinError::invalid_input("operator factory does not support local-state combination"));
        }
        let global = global.downcast_ref::<F::GlobalState>()
            .ok_or_else(|| VeinError::invalid_input("global state type does not match operator factory"))?;
        let local = local.downcast::<F::LocalState>()
            .map_err(|_| VeinError::invalid_input("local state type does not match operator factory"))?;
        self.factory.combine_local_state(global, *local, phase)
    }
}
