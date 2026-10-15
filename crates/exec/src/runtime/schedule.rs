use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use common::protocol::{
    DerivedPipelineGraph, PhysicalPlanNode, PipelineBoundaryKind, PipelineId,
};
use common::Result;
use error::VeinError;

use crate::trace::ExecutionTrace;

/// Runtime completion state for the graph derived from the canonical plan.
///
/// The recursive interpreter still owns operator semantics. This graph state
/// makes pipeline dependencies executable: join probe input cannot flow until
/// every prerequisite build pipeline has completed, and blocking boundaries
/// are marked only after their input pipelines finish.
pub(super) struct PipelineExecutionSchedule {
    graph: DerivedPipelineGraph,
    completed: Vec<Arc<AtomicBool>>,
}

impl PipelineExecutionSchedule {
    pub(super) fn derive(plan: &common::protocol::PhysicalPlan, trace: &ExecutionTrace) -> Result<Self> {
        let graph = DerivedPipelineGraph::derive(plan)?;
        let execution_order = graph.execution_order().ok_or_else(|| {
            VeinError::planning("derived pipeline dependencies contain a cycle", None)
        })?;
        tracing::debug!(
            target: "vein.audit",
            event = "pipeline_schedule_created",
            pipeline_count = execution_order.len(),
            dependency_count = graph.dependencies().len(),
            "pipeline_schedule_created"
        );
        for pipeline in graph.pipelines() {
            let operator_node_ids = pipeline
                .operators
                .iter()
                .map(|node| trace.node_id(node))
                .collect::<Vec<_>>();
            tracing::debug!(
                target: "vein.audit",
                event = "derived_pipeline",
                pipeline_id = pipeline.id.0,
                source_node_id = trace.node_id(&pipeline.source.node),
                source_boundary = ?pipeline.source.kind,
                operator_node_ids = ?operator_node_ids,
                sink_node_id = trace.node_id(&pipeline.sink.node),
                sink_boundary = ?pipeline.sink.kind,
                "derived_pipeline"
            );
        }
        for dependency in graph.dependencies() {
            tracing::debug!(
                target: "vein.audit",
                event = "pipeline_dependency",
                prerequisite_pipeline_id = dependency.prerequisite.0,
                dependent_pipeline_id = dependency.dependent.0,
                dependency_kind = ?dependency.kind,
                "pipeline_dependency"
            );
        }
        Ok(Self {
            completed: (0..graph.pipelines().len())
                .map(|_| Arc::new(AtomicBool::new(false)))
                .collect(),
            graph,
        })
    }

    pub(super) fn dependencies_for_operator(
        &self,
        node: &Arc<PhysicalPlanNode>,
    ) -> Result<Vec<(PipelineId, Arc<AtomicBool>)>> {
        let pipeline_ids = self
            .graph
            .pipelines()
            .iter()
            .filter(|pipeline| {
                pipeline
                    .operators
                    .iter()
                    .any(|operator| Arc::ptr_eq(operator, node))
            })
            .map(|pipeline| pipeline.id)
            .collect::<Vec<_>>();
        if pipeline_ids.is_empty() {
            return Err(VeinError::planning(
                "join operator is missing from the derived pipeline graph",
                None,
            ));
        }

        let mut dependencies = Vec::new();
        for pipeline_id in pipeline_ids {
            for dependency in self
                .graph
                .dependencies()
                .iter()
                .filter(|dependency| dependency.dependent == pipeline_id)
            {
                let state = self
                    .completed
                    .get(dependency.prerequisite.0)
                    .ok_or_else(|| {
                        VeinError::planning(
                            "pipeline dependency references an unknown pipeline",
                            None,
                        )
                    })?;
                if !dependencies
                    .iter()
                    .any(|(id, _): &(PipelineId, Arc<AtomicBool>)| *id == dependency.prerequisite)
                {
                    dependencies.push((dependency.prerequisite, Arc::clone(state)));
                }
            }
        }
        Ok(dependencies)
    }

    pub(super) fn complete_boundary(
        &self,
        node: &Arc<PhysicalPlanNode>,
        kind: PipelineBoundaryKind,
    ) -> Result<()> {
        let mut completed_any = false;
        for pipeline in
            self.graph.pipelines().iter().filter(|pipeline| {
                pipeline.sink.kind == kind && Arc::ptr_eq(&pipeline.sink.node, node)
            })
        {
            for dependency in self
                .graph
                .dependencies()
                .iter()
                .filter(|dependency| dependency.dependent == pipeline.id)
            {
                let state = self
                    .completed
                    .get(dependency.prerequisite.0)
                    .ok_or_else(|| {
                        VeinError::planning(
                            "pipeline dependency references an unknown pipeline",
                            None,
                        )
                    })?;
                if !state.load(Ordering::Acquire) {
                    return Err(VeinError::internal(format!(
                        "pipeline {:?} reached boundary {:?} before prerequisite {:?}",
                        pipeline.id, kind, dependency.prerequisite
                    )));
                }
            }
            self.completed[pipeline.id.0].store(true, Ordering::Release);
            tracing::debug!(
                target: "vein.audit",
                event = "pipeline_boundary_completed",
                pipeline_id = pipeline.id.0,
                boundary = ?kind,
                dependency_count = self
                    .graph
                    .dependencies()
                    .iter()
                    .filter(|dependency| dependency.dependent == pipeline.id)
                    .count(),
                "pipeline_boundary_completed"
            );
            completed_any = true;
        }
        if !completed_any {
            return Err(VeinError::planning(
                "completed operator boundary is missing from the derived pipeline graph",
                None,
            ));
        }
        Ok(())
    }

    pub(super) fn verify_complete(&self) -> Result<()> {
        if let Some((index, _)) = self
            .completed
            .iter()
            .enumerate()
            .find(|(_, completed)| !completed.load(Ordering::Acquire))
        {
            return Err(VeinError::internal(format!(
                "pipeline {:?} did not complete during task execution",
                PipelineId(index)
            )));
        }
        Ok(())
    }
}
