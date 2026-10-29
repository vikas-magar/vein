use super::*;

impl DerivedPipelineGraph {
    pub(super) fn derive_input(
        &mut self,
        node: &Arc<PhysicalPlanNode>,
        mut operators_from_sink: Vec<Arc<PhysicalPlanNode>>,
        sink: PipelineBoundary,
    ) -> Result<Vec<PipelineId>> {
        match &node.operator {
            PhysicalPlanOperator::Extension { .. } => self.derive_extension(node, operators_from_sink, sink),
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

    pub(super) fn push_blocking_output(
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

    pub(super) fn push_pipeline(
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
