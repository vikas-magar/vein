//! Pipeline boundaries and dependencies derived from the canonical operator tree.
//!
//! A derived graph contains references to canonical plan nodes. It carries no
//! operator semantics of its own; the immutable `PhysicalPlan` remains the
//! authoritative plan, while the graph describes schedulable streaming segments.

use crate::protocol::{PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator};
use crate::Result;
use error::VeinError;
use std::collections::VecDeque;
use std::sync::Arc;

/// Stable index of one derived pipeline within its graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipelineId(pub usize);

/// The role a physical node plays at a pipeline boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineBoundaryKind {
    /// A data source, including an aggregate or sort after it has finalized.
    Source,
    /// A client, storage, or discard sink at the root of the plan.
    ResultSink,
    /// Input consumed to build a join's hash table.
    JoinBuild,
    /// Input consumed by an aggregate operator.
    AggregateInput,
    /// Input consumed by duplicate elimination.
    DistinctInput,
    /// Input consumed by a global ordering operator.
    SortInput,
}

/// A boundary points directly at the canonical physical operator node.
#[derive(Debug, Clone)]
pub struct PipelineBoundary {
    /// Boundary role used by the local scheduler.
    pub kind: PipelineBoundaryKind,
    /// Immutable plan node that owns this boundary.
    pub node: Arc<PhysicalPlanNode>,
}

/// A schedulable linear segment derived from physical operators.
#[derive(Debug, Clone)]
pub struct DerivedPipeline {
    /// Pipeline identity within its graph.
    pub id: PipelineId,
    /// Source plan node, either a base pipeline or a finalized blocker.
    pub source: PipelineBoundary,
    /// Streaming operators in source-to-sink order, as references into the plan.
    pub operators: Vec<Arc<PhysicalPlanNode>>,
    /// The consumer boundary for this segment.
    pub sink: PipelineBoundary,
}

/// Why a downstream pipeline waits for an upstream pipeline to finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineDependencyKind {
    /// The upstream pipeline must finish materializing a blocking operator.
    BlockingInput,
    /// The join build side must finish before any probe work begins.
    JoinBuildBeforeProbe,
}

/// A required ordering edge between derived pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineDependency {
    /// Pipeline that must finish first.
    pub prerequisite: PipelineId,
    /// Pipeline that waits for the prerequisite.
    pub dependent: PipelineId,
    /// Reason the dependency exists.
    pub kind: PipelineDependencyKind,
}

/// Pipeline graph derived from one canonical physical plan.
#[derive(Debug, Clone, Default)]
pub struct DerivedPipelineGraph {
    pipelines: Vec<DerivedPipeline>,
    dependencies: Vec<PipelineDependency>,
}

impl DerivedPipelineGraph {
    /// Derive source-to-sink segments and blocking dependencies from a plan.
    pub fn derive(plan: &PhysicalPlan) -> Result<Self> {
        let PhysicalPlanOperator::Sink { input, .. } = &plan.root.operator else {
            return Err(VeinError::planning(
                "canonical physical plan root must be a sink",
                None,
            ));
        };
        let mut graph = Self::default();
        let sink = PipelineBoundary {
            kind: PipelineBoundaryKind::ResultSink,
            node: Arc::clone(&plan.root),
        };
        graph.derive_input(input, Vec::new(), sink)?;
        Ok(graph)
    }

    /// Pipelines in deterministic construction order.
    pub fn pipelines(&self) -> &[DerivedPipeline] {
        &self.pipelines
    }

    /// Required pipeline completion dependencies.
    pub fn dependencies(&self) -> &[PipelineDependency] {
        &self.dependencies
    }

    /// Return a dependency-respecting execution order, or `None` for a cycle.
    pub fn execution_order(&self) -> Option<Vec<PipelineId>> {
        let mut indegree = vec![0usize; self.pipelines.len()];
        let mut dependents = vec![Vec::new(); self.pipelines.len()];
        for dependency in &self.dependencies {
            if dependency.prerequisite.0 >= self.pipelines.len()
                || dependency.dependent.0 >= self.pipelines.len()
            {
                return None;
            }
            indegree[dependency.dependent.0] += 1;
            dependents[dependency.prerequisite.0].push(dependency.dependent.0);
        }

        let mut ready = indegree
            .iter()
            .enumerate()
            .filter_map(|(index, &degree)| (degree == 0).then_some(index))
            .collect::<VecDeque<_>>();
        let mut order = Vec::with_capacity(self.pipelines.len());
        while let Some(index) = ready.pop_front() {
            order.push(PipelineId(index));
            for &dependent in &dependents[index] {
                indegree[dependent] -= 1;
                if indegree[dependent] == 0 {
                    ready.push_back(dependent);
                }
            }
        }
        (order.len() == self.pipelines.len()).then_some(order)
    }

    fn derive_input(
        &mut self,
        node: &Arc<PhysicalPlanNode>,
        mut operators_from_sink: Vec<Arc<PhysicalPlanNode>>,
        sink: PipelineBoundary,
    ) -> Result<Vec<PipelineId>> {
        match &node.operator {
            PhysicalPlanOperator::Pipeline { .. } => {
                operators_from_sink.reverse();
                Ok(vec![self.push_pipeline(
                    PipelineBoundary {
                        kind: PipelineBoundaryKind::Source,
                        node: Arc::clone(node),
                    },
                    operators_from_sink,
                    sink,
                )])
            }
            PhysicalPlanOperator::Transform { input, .. }
            | PhysicalPlanOperator::Limit { input, .. } => {
                operators_from_sink.push(Arc::clone(node));
                self.derive_input(input, operators_from_sink, sink)
            }
            PhysicalPlanOperator::Aggregate { input, .. } => {
                let input_boundary = PipelineBoundary {
                    kind: PipelineBoundaryKind::AggregateInput,
                    node: Arc::clone(node),
                };
                let inputs = self.derive_input(input, Vec::new(), input_boundary)?;
                Ok(self.push_blocking_output(
                    node,
                    operators_from_sink,
                    sink,
                    inputs,
                    PipelineDependencyKind::BlockingInput,
                ))
            }
            PhysicalPlanOperator::Distinct { input, .. } => {
                let input_boundary = PipelineBoundary {
                    kind: PipelineBoundaryKind::DistinctInput,
                    node: Arc::clone(node),
                };
                let inputs = self.derive_input(input, Vec::new(), input_boundary)?;
                Ok(self.push_blocking_output(
                    node,
                    operators_from_sink,
                    sink,
                    inputs,
                    PipelineDependencyKind::BlockingInput,
                ))
            }
            PhysicalPlanOperator::Sort { input, .. } => {
                let input_boundary = PipelineBoundary {
                    kind: PipelineBoundaryKind::SortInput,
                    node: Arc::clone(node),
                };
                let inputs = self.derive_input(input, Vec::new(), input_boundary)?;
                Ok(self.push_blocking_output(
                    node,
                    operators_from_sink,
                    sink,
                    inputs,
                    PipelineDependencyKind::BlockingInput,
                ))
            }
            PhysicalPlanOperator::Join { probe, build, .. } => {
                let build_sink = PipelineBoundary {
                    kind: PipelineBoundaryKind::JoinBuild,
                    node: Arc::clone(node),
                };
                let build_pipelines = self.derive_input(build, Vec::new(), build_sink)?;

                operators_from_sink.push(Arc::clone(node));
                let probe_pipelines = self.derive_input(probe, operators_from_sink, sink)?;
                for &prerequisite in &build_pipelines {
                    for &dependent in &probe_pipelines {
                        self.dependencies.push(PipelineDependency {
                            prerequisite,
                            dependent,
                            kind: PipelineDependencyKind::JoinBuildBeforeProbe,
                        });
                    }
                }
                Ok(probe_pipelines)
            }
            PhysicalPlanOperator::Sink { .. } => Err(VeinError::planning(
                "sink operator may only appear at the physical plan root",
                None,
            )),
        }
    }

    fn push_blocking_output(
        &mut self,
        node: &Arc<PhysicalPlanNode>,
        mut operators_from_sink: Vec<Arc<PhysicalPlanNode>>,
        sink: PipelineBoundary,
        prerequisites: Vec<PipelineId>,
        dependency_kind: PipelineDependencyKind,
    ) -> Vec<PipelineId> {
        operators_from_sink.reverse();
        let output = self.push_pipeline(
            PipelineBoundary {
                kind: PipelineBoundaryKind::Source,
                node: Arc::clone(node),
            },
            operators_from_sink,
            sink,
        );
        self.dependencies
            .extend(prerequisites.into_iter().map(|prerequisite| PipelineDependency {
                prerequisite,
                dependent: output,
                kind: dependency_kind,
            }));
        vec![output]
    }

    fn push_pipeline(
        &mut self,
        source: PipelineBoundary,
        operators: Vec<Arc<PhysicalPlanNode>>,
        sink: PipelineBoundary,
    ) -> PipelineId {
        let id = PipelineId(self.pipelines.len());
        self.pipelines.push(DerivedPipeline {
            id,
            source,
            operators,
            sink,
        });
        id
    }
}

#[cfg(test)]
mod tests;
