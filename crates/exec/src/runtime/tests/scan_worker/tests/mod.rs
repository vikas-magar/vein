use std::sync::{Arc, Mutex};

use common::protocol::{DataSource, FileScanTask};
use functions::FunctionRegistry;
use storage::opendal::OpenDalReadStorageFactory;

use super::provider::{StatefulProvider, Visits};
use crate::factory::DefaultExecutorBuilder;
use crate::operators::source::scan::ScanProviderRegistry;
use crate::runtime::tests::*;

async fn verify_worker_lifetime(channel_fallback: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.data");
    std::fs::write(&path, []).unwrap();
    let visits: Visits = Arc::new(Mutex::new(Default::default()));
    let units = std::thread::available_parallelism().map_or(1, |n| n.get()) * 2 + 1;
    let format = DataFileFormatId::new("worker_state_test");
    let mut providers = ScanProviderRegistry::empty();
    providers.register(StatefulProvider {
        format: format.clone(),
        units,
        visits: Arc::clone(&visits),
    }).unwrap();
    let executor = DefaultTaskExecutor::new(Arc::new(
        DefaultExecutorBuilder::new(Arc::new(FunctionRegistry::with_builtins()))
            .with_scan_provider_registry(providers)
            .with_read_storage_factory(Arc::new(OpenDalReadStorageFactory)),
    ));
    let schema = int64_schema();
    let mut task = task();
    let scan = FileScanTask {
        files: vec![path.to_string_lossy().into_owned()],
        storage_metadata: Vec::new(),
        data_file_format: format,
        format_options: Vec::new(),
        byte_range: None,
        scan_unit_range: None,
        projected: Arc::clone(&schema),
        column_encoding_hints: Vec::new(),
        pushdown: None,
        task_id: task.task_id,
        catalog_snapshot: None,
        runtime_statistics: None,
    };
    let input = canonical_input(InputPlanFixture::Pipeline {
        source: DataSource::File(scan),
        program: Arc::new(ExpressionProgram::empty()),
    });
    let input = if channel_fallback {
        plan_node(schema, PhysicalPlanOperator::Limit {
            input,
            spec: LimitSpec { offset: 0, count: units as u64 },
        })
    } else {
        // Aggregate input supports worker-local combination; the plain client
        // sink does not, so it would only exercise channel fallback as well.
        plan_node(schema, PhysicalPlanOperator::Aggregate {
            input,
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: None,
                emit_empty_group_on_empty_input: true,
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![VeinDataType::Int64],
                types: vec![VeinDataType::Int64],
            },
        })
    };
    task.plan = plan_with_input(&task, input);
    let result = executor.execute(&task, CancellationToken::new()).await.unwrap();
    let expected_rows = if channel_fallback { units as u64 } else { 1 };
    assert_eq!(result.row_count, expected_rows);
    let first = visits.lock().unwrap().clone();
    assert!(first.values().any(|counts| counts.len() > 1), "work must outlive one source instance");
    for counts in first.values() {
        assert_eq!(*counts, (1..=counts.len()).collect::<Vec<_>>());
    }
    visits.lock().unwrap().clear();
    task.query_id = QueryId::new();
    let result = executor.execute(&task, CancellationToken::new()).await.unwrap();
    assert_eq!(result.row_count, expected_rows);
    let next_query = visits.lock().unwrap();
    assert!(next_query.keys().all(|worker| !first.contains_key(worker)));
    assert!(next_query.values().all(|counts| counts.first() == Some(&1)));
}

#[tokio::test]
async fn combine_workers_retain_provider_state_across_work_units() {
    verify_worker_lifetime(false).await;
}

#[tokio::test]
async fn channel_workers_retain_provider_state_across_work_units() {
    verify_worker_lifetime(true).await;
}
