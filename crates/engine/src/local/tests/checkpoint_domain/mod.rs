//! Execute domain reduction through binding, checkpoint replay, and real operators.
use super::*;
use optimizer::{OptimizerPipeline, context::OptimizerContext, rule::{OptimizerRule, RuleType}};
use plan::LogicalPlan;
mod fixture;
mod semantics;

struct ObserveDomain(Arc<AtomicUsize>);

impl OptimizerRule for ObserveDomain {
    fn name(&self) -> &str { "ObserveDomain" }
    fn rule_type(&self) -> RuleType { RuleType::Heuristic }
    fn apply(&self, plan: &LogicalPlan, _: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::Join { join_type: parser::ast::pipe_op::JoinType::Semi, right, .. } = plan {
            if matches!(right.as_ref(), LogicalPlan::Project { input, .. }
                if matches!(input.as_ref(), LogicalPlan::Intermediate { .. })) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(None)
    }
}

fn execute(definitions: Vec<(&str, fixture::Columns)>, query: &str) -> Vec<Vec<Option<i64>>> {
    let root = tempfile::tempdir().unwrap();
    let tables = fixture::tables(root.path(), definitions);
    let retained_key = Arc::clone(tables.views["outer_rows"].resolve(0, 0).unwrap().unwrap().0);
    let mut scan_providers = ScanProviderRegistry::empty();
    scan_providers.register(fixture::MemoryProvider {
        tables: Arc::clone(&tables),
        format: DataFileFormatId::new("domain_fixture"),
    }).unwrap();
    let observations = Arc::new(AtomicUsize::new(0));
    let components = LocalEngineComponents {
        catalog: tables,
        functions: Arc::new(FunctionRegistry::with_builtins()),
        scan_providers,
        read_storage_factory: Arc::new(storage::opendal::OpenDalReadStorageFactory),
        write_storage: Arc::new(UnusedWriteStorage),
        statistics_provider: Arc::new(UnusedAnalysisProvider),
        runtime_statistics: RuntimeStatisticsStore::default(),
        optimizer: Arc::new(OptimizerPipeline::production()
            .with_rule(Arc::new(ObserveDomain(Arc::clone(&observations))))),
        planner: Arc::new(physical::DefaultPhysicalPlanner::new()),
        adaptive_policy: Arc::new(StatisticsCheckpointPolicy),
    };
    let engine = LocalEngineBuilder::new(components).build();
    let batches = tokio::runtime::Runtime::new().unwrap().block_on(async {
        engine.prepare(query).await.unwrap().into_stream().try_collect::<Vec<_>>().await.unwrap()
    });
    assert!(observations.load(Ordering::Relaxed) > 0, "domain restriction must actually execute");
    let mut output = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            output.push((0..batch.num_columns()).map(|column| {
                batch.resolve(column, row).unwrap().and_then(|(array, index)| {
                    if column == 0 {
                        assert_eq!(array.to_data().buffers()[0].as_ptr(),
                                   retained_key.to_data().buffers()[0].as_ptr(),
                                   "checkpoint replay and joins must retain the outer key buffer");
                    }
                    let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    (!values.is_null(index)).then(|| values.value(index))
                })
            }).collect::<Vec<_>>());
        }
    }
    output.sort();
    output
}
