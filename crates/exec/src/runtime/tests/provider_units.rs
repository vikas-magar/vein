use super::*;

use arrow::array::{Array, Int64Array};
use common::arrow::SelectionVector;
use common::protocol::{DataSource, FileScanTask};
use crate::factory::DefaultExecutorBuilder;
use crate::operators::source::scan::{
    ScanPlanningRequest, ScanProvider, ScanProviderRegistry, ScanRequest, file_scan_work_unit,
};
use crate::pipeline::{DataChunkView, PipelineNode};
use error::VeinError;
use functions::FunctionRegistry;
use std::sync::Mutex;
use storage::opendal::OpenDalReadStorageFactory;

struct UnevenProvider {
    format: catalog::DataFileFormatId,
    executed: Arc<Mutex<Vec<(String, usize, usize)>>>,
    source_arrays: Arc<Mutex<Vec<Arc<Int64Array>>>>,
}

#[async_trait::async_trait]
impl ScanProvider for UnevenProvider {
    fn format_id(&self) -> &catalog::DataFileFormatId {
        &self.format
    }

    fn plan_work_units(&self, request: ScanPlanningRequest<'_>) -> common::Result<Vec<DataSource>> {
        let ranges = [vec![0..1, 1..4], vec![0..2]];
        Ok(ranges
            .into_iter()
            .enumerate()
            .flat_map(move |(file_index, ranges)| {
                let scan = request.scan;
                ranges.into_iter().map(move |range| {
                    file_scan_work_unit(scan, file_index, Some(range))
                })
            })
            .collect())
    }

    async fn execute(&self, request: ScanRequest<'_>, next: &mut dyn PipelineNode) -> common::Result<()> {
        if request.files.len() != 1 {
            return Err(VeinError::internal("scan unit must contain exactly one file"));
        }
        let file = &request.files[0];
        let range = file
            .scan_unit_range
            .clone()
            .ok_or_else(|| VeinError::internal("provider scan unit has no range"))?;
        let base = if file.relative == "a.data" { 0 } else { 100 };
        let values = Arc::new(Int64Array::from(
            range.clone().map(|index| base + index as i64).collect::<Vec<_>>(),
        ));
        self.executed
            .lock()
            .map_err(|_| VeinError::internal("provider execution test mutex poisoned"))?
            .push((file.relative.clone(), range.start, range.end));
        self.source_arrays
            .lock()
            .map_err(|_| VeinError::internal("provider buffer test mutex poisoned"))?
            .push(Arc::clone(&values));
        let view = DataChunkView::from_arrays(
            vec![values],
            range.end - range.start,
            SelectionVector::new_dense(range.end - range.start),
        )?
            .with_schema(Arc::new(request.projected.to_arrow()))?;
        next.push(view)
    }
}

#[tokio::test]
async fn provider_planned_units_execute_once_and_keep_arrow_buffers() {
    let directory = tempfile::tempdir().expect("temporary data directory");
    let files = ["a.data", "b.data"]
        .map(|name| {
            let path = directory.path().join(name);
            std::fs::write(&path, []).expect("create synthetic scan file");
            path.to_string_lossy().into_owned()
        });
    let executed = Arc::new(Mutex::new(Vec::new()));
    let source_arrays = Arc::new(Mutex::new(Vec::new()));
    let format = catalog::DataFileFormatId::new("uneven_test");
    let mut providers = ScanProviderRegistry::empty();
    providers
        .register(UnevenProvider {
            format: format.clone(),
            executed: Arc::clone(&executed),
            source_arrays: Arc::clone(&source_arrays),
        })
        .expect("register unit provider");
    let builder = DefaultExecutorBuilder::new(Arc::new(FunctionRegistry::new()))
        .with_scan_provider_registry(providers)
        .with_read_storage_factory(Arc::new(OpenDalReadStorageFactory));
    let executor = DefaultTaskExecutor::new(Arc::new(builder));
    let schema = int64_schema();
    let scan = FileScanTask {
        files: files.to_vec(),
        storage_metadata: Vec::new(),
        data_file_format: format,
        format_options: Vec::new(),
        byte_range: None,
        scan_unit_range: None,
        projected: Arc::clone(&schema),
        column_encoding_hints: Vec::new(),
        pushdown: None,
        task_id: TaskId::new(),
        catalog_snapshot: None,
        runtime_statistics: None,
    };
    let mut task = task();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::File(scan)),
            input_schema: Arc::clone(&schema),
            program: Arc::new(ExpressionProgram::empty()),
        },
    );
    task.plan = plan_with_input(&task, input);

    let result = executor
        .execute(&task, CancellationToken::new())
        .await
        .expect("provider work units should execute through the generic scheduler");

    assert_eq!(result.row_count, 6);
    let mut values = result
        .output
        .iter()
        .flat_map(|view| view_int64_values(view, 0))
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![0, 1, 2, 3, 100, 101]);
    let mut units = executed.lock().expect("provider executions").clone();
    units.sort();
    assert_eq!(units, vec![("a.data".into(), 0, 1), ("a.data".into(), 1, 4), ("b.data".into(), 0, 2)]);

    let source_arrays = source_arrays.lock().expect("provider output arrays");
    for view in &result.output {
        let (array, _) = view.resolve(0, 0).expect("resolve output view").expect("output value");
        let output = array.as_any().downcast_ref::<Int64Array>().expect("int64 output");
        assert!(source_arrays.iter().any(|source| source.values().as_ptr() == output.values().as_ptr()));
    }
}
