//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::compiler::PipelineCompiler;
use crate::stage::Stage;
use crate::task::Task;
use common::identifiers::{PartitionId, QueryId, StageId, TaskId};
use common::protocol::{DataSink, DataSource, MicroProgram};
use common::Result;
use plan::schema::VeinSchema;
use plan::LogicalPlan;

/// Trait for translating linear, non-blocking logical plans into a single physical [`Stage`].
pub trait PipelineStageBuilder: Send + Sync {
    /// Translates a linear logical plan into a single physical stage with `DataSink::Client`.
    fn build(
        &self,
        query_id: QueryId,
        plan: &LogicalPlan,
        stages: &mut Vec<Stage>,
    ) -> Result<StageId>;

    /// Compiles the linear pipeline into a leaf data source, a [`MicroProgram`], and an output schema.
    fn compile_pipeline(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema)>;
}

/// Default linear pipeline stage builder.
#[derive(Debug, Default, Clone)]
pub struct DefaultPipelineStageBuilder;

impl DefaultPipelineStageBuilder {
    /// Creates a new default linear pipeline stage builder.
    pub fn new() -> Self {
        Self
    }
}

impl PipelineStageBuilder for DefaultPipelineStageBuilder {
    fn build(
        &self,
        _query_id: QueryId,
        plan: &LogicalPlan,
        stages: &mut Vec<Stage>,
    ) -> Result<StageId> {
        let (source, program, _out_schema) = self.compile_pipeline(plan)?;
        let stage_id = StageId::new();
        let task = Task::new(
            TaskId::new(),
            PartitionId::new(),
            source,
            program,
            DataSink::Client,
        );
        let stage = Stage::new(stage_id).with_task(task);
        stages.push(stage);
        Ok(stage_id)
    }

    fn compile_pipeline(
        &self,
        plan: &LogicalPlan,
    ) -> Result<(DataSource, MicroProgram, VeinSchema)> {
        PipelineCompiler::compile(plan)
    }
}
