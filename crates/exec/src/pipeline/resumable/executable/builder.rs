use super::super::prepared::CanonicalPlanIndex;
use super::super::{OperatorCapabilities, OperatorFactoryRegistry, OperatorMetadata, OperatorNodeId, ParallelismCapability, PreparedExtensionPlan};
use super::{ExecutablePipeline, ExecutableStage, PipelineOperatorProvider, PipelineOperatorState, ResolvedPipelineProperties};
use common::protocol::pipeline_graph::PipelinePhase;
use common::protocol::{DerivedPipelineGraph, PhysicalPlan, PhysicalPlanNode, PipelineId};
use common::{Result, VeinError};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct NodeBinding {
    pub(super) metadata: Arc<OperatorMetadata>,
    pub(super) state: Arc<dyn PipelineOperatorState>,
    pub(super) capabilities: HashMap<PipelinePhase, OperatorCapabilities>,
}

/// Prepared source/operator/sink sequences sharing the canonical derived graph.
pub struct ExecutablePipelinePlan {
    graph: DerivedPipelineGraph,
    index: CanonicalPlanIndex,
    pipelines: Vec<Arc<ExecutablePipeline>>,
    order: Vec<PipelineId>,
    properties: HashMap<OperatorNodeId, ResolvedPipelineProperties>,
}

impl ExecutablePipelinePlan {
    /// Validates and binds every node once before any local stage executes.
    pub fn prepare<P: PipelineOperatorProvider + ?Sized>(
        plan: &PhysicalPlan,
        registry: &OperatorFactoryRegistry,
        provider: &P,
    ) -> Result<Self> {
        let parts = PreparedExtensionPlan::prepare(plan, registry)?.into_parts();
        let graph = parts.graph;
        let index = parts.index;
        let mut extensions = parts.operators;
        let definitions = graph.pipelines().iter().map(|pipeline| pipeline.stages())
            .collect::<Result<Vec<_>>>()?;
        let mut requests: HashMap<OperatorNodeId, Vec<PipelinePhase>> = HashMap::new();
        for stage in definitions.iter().flatten() {
            let phases = requests.entry(index.id(stage.node())?).or_default();
            if phases.contains(&stage.phase()) {
                return Err(VeinError::planning("a canonical phase reused by multiple pipelines requires replay or multicast support", None));
            }
            phases.push(stage.phase());
        }
        let mut bindings = HashMap::with_capacity(requests.len());
        for node in &index.nodes {
            let id = index.id(node)?;
            let Some(phases) = requests.get(&id) else { continue };
            let metadata = Arc::new(index.metadata(node)?);
            let state: Arc<dyn PipelineOperatorState> = match extensions.remove(&id) {
                Some(state) => state,
                None => provider.prepare(node, Arc::clone(&metadata), phases)?,
            };
            let mut capabilities = HashMap::with_capacity(phases.len());
            for &phase in phases {
                let declared = state.capabilities(phase)?;
                declared.validate_for(&metadata)?;
                capabilities.insert(phase, declared);
            }
            bindings.insert(id, NodeBinding { metadata, state, capabilities });
        }
        let properties = super::validation::validate(&bindings)?;
        let mut pipelines = Vec::with_capacity(definitions.len());
        for (pipeline, definitions) in graph.pipelines().iter().zip(definitions) {
            let mut stages = Vec::with_capacity(definitions.len());
            let mut parallelism = ParallelismCapability::Independent;
            for definition in definitions {
                let binding = &bindings[&index.id(definition.node())?];
                let mut capabilities = binding.capabilities[&definition.phase()].clone();
                if definition.phase() == PipelinePhase::Finalize {
                    capabilities.parallelism = ParallelismCapability::Serial;
                }
                parallelism = combined_parallelism(parallelism, capabilities.parallelism);
                stages.push(ExecutableStage { definition, state: Arc::clone(&binding.state), capabilities });
            }
            pipelines.push(Arc::new(ExecutablePipeline { id: pipeline.id, stages: stages.into(), parallelism }));
        }
        let order = graph.execution_order()
            .ok_or_else(|| VeinError::planning("executable pipeline dependencies contain a cycle", None))?;
        tracing::debug!(target: "vein.audit", event = "executable_pipeline_plan_prepared",
            pipelines = pipelines.len(), operators = bindings.len(), dependencies = graph.dependencies().len(),
            "canonical pipeline stages bound");
        Ok(Self { graph, index, pipelines, order, properties })
    }

    /// Borrows the one derived graph retained from preparation.
    pub fn graph(&self) -> &DerivedPipelineGraph { &self.graph }
    /// Pipelines in deterministic graph construction order.
    pub fn pipelines(&self) -> &[Arc<ExecutablePipeline>] { &self.pipelines }
    /// Dependency-respecting deterministic order used to seed a scheduler.
    pub fn execution_order(&self) -> &[PipelineId] { &self.order }
    /// Proven output properties for one canonical node.
    pub fn properties(&self, node: &Arc<PhysicalPlanNode>) -> Option<&ResolvedPipelineProperties> {
        self.index.id(node).ok().and_then(|id| self.properties.get(&id))
    }
}

fn combined_parallelism(left: ParallelismCapability, right: ParallelismCapability) -> ParallelismCapability {
    use ParallelismCapability::*;
    match (left, right) {
        (Serial, _) | (_, Serial) => Serial,
        (CombineLocalState, _) | (_, CombineLocalState) => CombineLocalState,
        _ => Independent,
    }
}
