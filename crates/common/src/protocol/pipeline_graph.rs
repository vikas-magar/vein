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
mod tests {
    use super::*;
    use crate::protocol::{
        DataSink, DataSource, JoinKind, JoinPlan, MicroProgram, PhysicalPlanNode, SortPlan,
        AggregatePlan,
    };
    use std::sync::Arc;
    use types::VeinSchema;

    fn node(operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
        Arc::new(PhysicalPlanNode {
            output_schema: Arc::new(VeinSchema::empty()),
            operator,
        })
    }

    fn source() -> Arc<PhysicalPlanNode> {
        node(PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::new(VeinSchema::empty()),
            program: Arc::new(MicroProgram::empty()),
        })
    }

    fn plan(input: Arc<PhysicalPlanNode>) -> PhysicalPlan {
        PhysicalPlan {
            root: node(PhysicalPlanOperator::Sink {
                input,
                sink: Arc::new(DataSink::Discard),
            }),
        }
    }

    #[test]
    fn aggregate_and_sort_create_blocking_boundaries_and_ordered_dependencies() {
        let aggregate = node(PhysicalPlanOperator::Aggregate {
            input: source(),
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: Some(vec![vec![]]),
                emit_empty_group_on_empty_input: true,
                aggs: Vec::new(),
                aggregate_input_types: Vec::new(),
                types: Vec::new(),
            },
        });
        let sorted = node(PhysicalPlanOperator::Sort {
            input: aggregate,
            plan: SortPlan { keys: Vec::new() },
            key_program: None,
            output_columns: 0,
        });
        let graph = DerivedPipelineGraph::derive(&plan(sorted)).expect("valid operator tree");

        assert_eq!(graph.pipelines().len(), 3);
        assert_eq!(graph.dependencies().len(), 2);
        assert_eq!(graph.pipelines()[0].sink.kind, PipelineBoundaryKind::AggregateInput);
        assert_eq!(graph.pipelines()[1].source.kind, PipelineBoundaryKind::Source);
        assert!(matches!(
            &graph.pipelines()[1].source.node.operator,
            PhysicalPlanOperator::Aggregate { .. }
        ));
        assert_eq!(graph.pipelines()[1].sink.kind, PipelineBoundaryKind::SortInput);
        assert_eq!(graph.pipelines()[2].source.kind, PipelineBoundaryKind::Source);
        assert_eq!(graph.pipelines()[2].sink.kind, PipelineBoundaryKind::ResultSink);
        let order = graph.execution_order().expect("blocking dependencies are acyclic");
        assert_eq!(order, vec![PipelineId(0), PipelineId(1), PipelineId(2)]);
    }

    #[test]
    fn chained_join_builds_precede_probe_pipeline_without_copying_plan_nodes() {
        let inner_probe = source();
        let inner_build = source();
        let outer_build = source();
        let inner_join = node(PhysicalPlanOperator::Join {
            probe: Arc::clone(&inner_probe),
            build: Arc::clone(&inner_build),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: Vec::new(),
                build_keys: Vec::new(),
                residual: None,
                outputs: Vec::new(),
                probe_types: Vec::new(),
                build_types: Vec::new(),
            },
        });
        let outer_join = node(PhysicalPlanOperator::Join {
            probe: Arc::clone(&inner_join),
            build: Arc::clone(&outer_build),
            plan: JoinPlan {
                join_type: JoinKind::Inner,
                probe_keys: Vec::new(),
                build_keys: Vec::new(),
                residual: None,
                outputs: Vec::new(),
                probe_types: Vec::new(),
                build_types: Vec::new(),
            },
        });
        let graph = DerivedPipelineGraph::derive(&plan(Arc::clone(&outer_join)))
            .expect("valid chained join tree");

        assert_eq!(graph.pipelines().len(), 3);
        assert_eq!(graph.dependencies().len(), 2);
        assert!(graph
            .dependencies()
            .iter()
            .all(|dependency| dependency.kind == PipelineDependencyKind::JoinBuildBeforeProbe));
        assert!(graph
            .pipelines()
            .iter()
            .take(2)
            .all(|pipeline| pipeline.sink.kind == PipelineBoundaryKind::JoinBuild));
        assert!(graph.pipelines()[2]
            .operators
            .iter()
            .any(|operator| Arc::ptr_eq(operator, &inner_join)));
        assert!(graph.pipelines()[2]
            .operators
            .iter()
            .any(|operator| Arc::ptr_eq(operator, &outer_join)));
        assert!(Arc::ptr_eq(&graph.pipelines()[0].source.node, &outer_build));
        assert!(Arc::ptr_eq(&graph.pipelines()[1].source.node, &inner_build));
        assert!(Arc::ptr_eq(&graph.pipelines()[2].source.node, &inner_probe));
        assert_eq!(
            graph.execution_order(),
            Some(vec![PipelineId(0), PipelineId(1), PipelineId(2)])
        );
    }

    #[test]
    fn rejects_missing_root_sink_and_nested_sink_nodes() {
        assert!(DerivedPipelineGraph::derive(&PhysicalPlan { root: source() }).is_err());

        let nested_sink = node(PhysicalPlanOperator::Sink {
            input: source(),
            sink: Arc::new(DataSink::Discard),
        });
        assert!(DerivedPipelineGraph::derive(&plan(nested_sink)).is_err());
    }
}
