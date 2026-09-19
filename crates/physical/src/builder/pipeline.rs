//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::compiler::PipelineCompiler;
use crate::stage::Stage;
use crate::task::Task;
use common::identifiers::{PartitionId, QueryId, StageId, TaskId};
use common::protocol::{DataSink, DataSource, FieldSchema, MicroProgram};
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
        let (source, program, out_schema) = self.compile_pipeline(plan)?;
        let client_schema = out_schema
            .fields
            .into_iter()
            .map(|f| FieldSchema {
                field_id: f.id.0 as i32,
                name: f.name,
                data_type: format!("{:?}", f.data_type),
            })
            .collect();
        let sink = match plan {
            LogicalPlan::Write { target, options, .. } => {
                let storage_schema = client_schema;
                let key_values = options
                    .iter()
                    .map(|(k, v)| common::protocol::KeyValue {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect();
                let target_obj = common::protocol::StorageTarget::new(target.clone(), key_values, storage_schema);
                DataSink::Storage(target_obj)
            }
            _ => DataSink::Client {
                schema: client_schema,
            },
        };
        let stage_id = StageId::new();
        let task = Task::new(
            TaskId::new(),
            PartitionId::new(),
            source,
            program,
            sink,
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
