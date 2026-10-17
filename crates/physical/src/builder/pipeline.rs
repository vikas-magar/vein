//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::compiler::PipelineCompiler;
use crate::stage::Stage;
use crate::task::Task;
use crate::pipeline_graph::DerivedPipelineGraph;
use common::Result;
use common::identifiers::{PartitionId, StageId, TaskId};
use common::protocol::{
    DataSink, DataSource, MicroProgram, PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator,
};
use error::VeinError;
use plan::schema::VeinSchema;
use plan::LogicalPlan;
use std::sync::Arc;

mod join_keys;
mod join_layout;
mod join_lowering;
mod node_lowering;
mod statistics;

use join_keys::{extract_equi_join_keys, flatten_conjunction, join_kind};
use join_layout::{available_join_outputs, position};
#[cfg(test)]
use join_keys::equality_keys;

/// Trait for lowering supported logical plans into physical [`Stage`]s.
pub trait PipelineStageBuilder: Send + Sync {
    /// Lowers a logical plan into a stage with an explicit physical sink.
    fn build(&self, plan: &LogicalPlan) -> Result<Stage>;
    /// Scope physical estimates to the current query and its completed stages.
    fn build_with_statistics(&self, plan: &LogicalPlan,
                             _statistics: Arc<common::statistics::QueryStatisticsContext>) -> Result<Stage> {
        self.build(plan)
    }

    /// Compiles a fused source segment and returns its source and schemas.
    fn compile_pipeline(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema, VeinSchema)>;
}

/// Default recursive stage builder with fused scan pipelines.
#[derive(Default, Clone)]
pub struct DefaultPipelineStageBuilder {
    statistics: Option<Arc<dyn optimizer::StatisticsProvider>>,
}

impl std::fmt::Debug for DefaultPipelineStageBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("DefaultPipelineStageBuilder")
            .field("query_statistics", &self.statistics.is_some()).finish()
    }
}

impl DefaultPipelineStageBuilder {
    /// Creates a new default recursive stage builder.
    pub fn new() -> Self {
        Self::default()
    }
}

pub(super) fn logical_node_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { .. } => "scan",
        LogicalPlan::ExternalScan { .. } => "external scan",
        LogicalPlan::Intermediate { .. } => "intermediate relation",
        LogicalPlan::Filter { .. } => "filter",
        LogicalPlan::Project { .. } => "projection",
        LogicalPlan::Aggregate { .. } => "aggregate",
        LogicalPlan::Window { .. } => "window",
        LogicalPlan::Join { .. } => "join",
        LogicalPlan::Sort { .. } => "sort",
        LogicalPlan::Limit { .. } => "limit",
        LogicalPlan::Distinct { .. } => "distinct",
        LogicalPlan::SubqueryAlias { .. } => "subquery alias",
        LogicalPlan::Explain { .. } => "explain",
        LogicalPlan::SetOp { .. } => "set operation",
        LogicalPlan::Sample { .. } => "sample",
        LogicalPlan::Assert { .. } => "assert",
        LogicalPlan::Pivot { .. } => "pivot",
        LogicalPlan::Unpivot { .. } => "unpivot",
        LogicalPlan::Fork { .. } => "fork",
        LogicalPlan::Tee { .. } => "tee",
        LogicalPlan::SemanticFilter { .. } => "semantic filter",
        LogicalPlan::SemanticJoin { .. } => "semantic join",
        LogicalPlan::SemanticCalibrate { .. } => "semantic calibration",
        LogicalPlan::Write { .. } => "write",
        LogicalPlan::Call { .. } => "call",
    }
}


impl PipelineStageBuilder for DefaultPipelineStageBuilder {
    fn build_with_statistics(&self, plan: &LogicalPlan,
                             statistics: Arc<common::statistics::QueryStatisticsContext>) -> Result<Stage> {
        Self { statistics: Some(Arc::new(optimizer::QueryStatisticsProvider::new(statistics))) }.build(plan)
    }
    fn build(&self, plan: &LogicalPlan) -> Result<Stage> {
        let (input, sink) = match plan {
            LogicalPlan::Write {
                input,
                target,
                options,
                schema,
            } => {
                let (input, input_schema) = self.lower_node(input)?;
                let output_schema = Arc::new((**schema).clone());
                if input_schema.fields != output_schema.fields {
                    return Err(VeinError::planning(
                        "WRITE output schema does not match its input schema",
                        None,
                    ));
                }
                let key_values = options
                    .iter()
                    .map(|(key, value)| common::protocol::KeyValue {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect();
                // Build Arrow IPC only for the storage sink; client reads do
                // not need a second schema representation.
                let arrow_schema = output_schema.to_arrow();
                let mut schema_bytes = Vec::new();
                {
                    let mut writer =
                        arrow::ipc::writer::StreamWriter::try_new(&mut schema_bytes, &arrow_schema)
                            .map_err(|error| {
                                VeinError::internal(format!("encode WRITE schema: {error}"))
                            })?;
                    writer.finish().map_err(|error| {
                        VeinError::internal(format!("finish WRITE schema: {error}"))
                    })?;
                }
                let target =
                    common::protocol::StorageTarget::new(target.clone(), key_values, output_schema)
                        .with_schema_ipc(schema_bytes);
                (input, DataSink::Storage(target))
            }
            _ => {
                let (input, schema) = self.lower_node(plan)?;
                (
                    input,
                    DataSink::Client {
                        schema: Arc::new(schema),
                    },
                )
            }
        };

        let sink = Arc::new(sink);
        let canonical_output_schema = match sink.as_ref() {
            DataSink::Client { schema } => Arc::clone(schema),
            DataSink::Storage(target) => Arc::clone(&target.schema),
            DataSink::Discard => Arc::new(VeinSchema::empty()),
        };
        let canonical_root = Arc::new(PhysicalPlanNode {
            output_schema: canonical_output_schema,
            operator: PhysicalPlanOperator::Sink {
                input: Arc::clone(&input),
                sink: Arc::clone(&sink),
            },
        });
        let pipeline_graph = DerivedPipelineGraph::derive(&PhysicalPlan {
            root: Arc::clone(&canonical_root),
        })?;
        Ok(Stage::new(StageId::new())
            .with_pipeline_graph(pipeline_graph)
            .with_task(Task::new(
                TaskId::new(),
                PartitionId::from_raw(0),
                PhysicalPlan {
                    root: canonical_root,
                },
            )))
    }

    fn compile_pipeline(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema, VeinSchema)> {
        PipelineCompiler::compile(plan)
    }
}

#[cfg(test)]
mod tests;
