use super::*;

#[tokio::test]
async fn inspection_uses_real_binding_and_registered_functions_without_executing() {
    let column = Arc::new(ResolvedColumn::new(ColumnId(0), "id", None, VeinDataType::Int64));
    let catalog = Arc::new(MemoryCatalog {
        table: Arc::new(TableMetadata::new("items", Arc::new(VeinSchema::new(vec![column])))),
        metadata: MetadataStore::new(),
    });
    let engine = LocalEngineBuilder::new(LocalEngineComponents {
        catalog,
        functions: Arc::new(FunctionRegistry::with_builtins()),
        // No scan provider exists: inspection must never attempt to execute a scan.
        scan_providers: ScanProviderRegistry::empty(),
        read_storage_factory: Arc::new(storage::opendal::OpenDalReadStorageFactory),
        write_storage: Arc::new(UnusedWriteStorage),
        statistics_provider: Arc::new(UnusedAnalysisProvider),
        runtime_statistics: RuntimeStatisticsStore::default(),
        optimizer: Arc::new(optimizer::OptimizerPipeline::default()),
        planner: Arc::new(physical::DefaultPhysicalPlanner::new()),
        adaptive_policy: Arc::new(StatisticsCheckpointPolicy),
    }).build();
    let schema = engine.inspect_query("FROM items AS i |> EXTEND i.id + 1 AS next_id").await.unwrap();
    assert!(schema.fields.iter().any(|column| column.name == "next_id"));
    assert!(engine.function_signatures().iter().any(|(name, _)| name.eq_ignore_ascii_case("sum")));
    assert!(matches!(engine.inspect_query("FROM items |> WHERE").await, Err(VeinError::Parse(_))));
    let unknown = engine.inspect_query("FROM items |> SELECT unknown_column").await.unwrap_err();
    assert!(unknown.to_string().contains("unknown_column"));
}
