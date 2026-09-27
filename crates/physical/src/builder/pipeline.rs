//! Linear pipeline builder abstraction: translates unbroken operator sequences into a single [`Stage`].

use crate::compiler::PipelineCompiler;
use crate::stage::Stage;
use crate::task::Task;
use common::Result;
use common::identifiers::{PartitionId, QueryId, StageId, TaskId};
use common::protocol::{DataSink, DataSource, FieldSchema, MicroProgram};
use error::VeinError;
use plan::LogicalPlan;
use plan::schema::VeinSchema;

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

pub(crate) fn split_file_scan_source(source: DataSource) -> Result<Vec<(PartitionId, DataSource)>> {
    let DataSource::File(scan) = source else {
        return Ok(vec![(PartitionId::from_raw(0), source)]);
    };

    if scan.files.len() <= 1 {
        return Ok(vec![(PartitionId::from_raw(0), DataSource::File(scan))]);
    }

    scan.files
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, file)| {
            let partition = u16::try_from(index)
                .map(PartitionId::from_raw)
                .map_err(|_| VeinError::planning("file scan has more than u16::MAX files", None))?;
            let mut file_scan = scan.clone();
            file_scan.files = vec![file];
            file_scan.task_id = TaskId::new();
            Ok((partition, DataSource::File(file_scan)))
        })
        .collect()
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
            LogicalPlan::Write {
                target, options, ..
            } => {
                let storage_schema = client_schema;
                let key_values: Vec<common::protocol::KeyValue> = options
                    .iter()
                    .map(|(k, v)| common::protocol::KeyValue {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect();
                let target_obj = common::protocol::StorageTarget::new(
                    target.clone(),
                    key_values,
                    storage_schema,
                );
                DataSink::Storage(target_obj)
            }
            _ => DataSink::Client {
                schema: client_schema,
            },
        };
        let stage_id = StageId::new();
        let mut stage = Stage::new(stage_id);
        for (partition, source) in split_file_scan_source(source)? {
            stage.tasks.push(Task::new(
                TaskId::new(),
                partition,
                source,
                program.clone(),
                sink.clone(),
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::split_file_scan_source;
    use common::identifiers::TaskId;
    use common::protocol::{DataSource, FileFormat, FileScanTask};

    #[test]
    fn splits_multi_file_scan_into_stable_single_file_units() {
        let source = DataSource::File(FileScanTask {
            files: vec!["a.parquet".to_string(), "b.parquet".to_string()],
            format: FileFormat::Parquet,
            byte_range: None,
            column_ids: vec![0],
            projected_schema: Vec::new(),
            task_id: TaskId::new(),
            catalog_snapshot: None,
            shuffle_target_path: None,
        });

        let units = split_file_scan_source(source).expect("file scan should split");

        assert_eq!(units.len(), 2);
        assert_eq!(units[0].0.into_raw(), 0);
        assert_eq!(units[1].0.into_raw(), 1);
        for ((_, source), expected_path) in units.iter().zip(["a.parquet", "b.parquet"]) {
            let DataSource::File(scan) = source else {
                panic!("split scan should remain a file source");
            };
            assert_eq!(scan.files, vec![expected_path]);
        }
    }
}
