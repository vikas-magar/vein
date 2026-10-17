//! The default [`ExecutorBuilder`] implementation.
//!
//! Maps task descriptions onto concrete sources, sinks, and Vector VM execution.

mod scan_work;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;

use crate::operators::join::JoinBloomFilterFactory;
use crate::operators::sink::{CollectSink, DiscardSink, WriteSink, WriteStorage};
use crate::memory::ExecutionResourceLimits;
use crate::spill::{LocalSpillStorageFactory, SpillStorageFactory};
use crate::operators::source::scan::{PARQUET_FOOTER, ScanFile, ScanSourceConfig};
use crate::operators::source::ScanSource;
use crate::pipeline::{DataChunk, DataChunkView, PipelineNode, Sink, Source, Transform};
use crate::runtime_filter::{
    ConservativeRuntimeFilterPolicy, RuntimeFilterDecisionPolicy, RuntimeFilterFactory,
    RuntimeFilterSet,
};
use crate::runtime::{ExecutorBuilder, MaterializeViewNode, split_scan_by_file};
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use catalog::{MetadataKey, MetadataStore};
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, FileFormat, FileScanTask, MicroProgram};
use common::statistics::{QueryStatisticsContext, RuntimeStatisticsStore};
use error::VeinError;
use opendal::Operator;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::ParquetMetaData;

/// Builds a [`TaskExecutor`](crate::runtime::TaskExecutor) with configurable
/// stages and Vector VM execution engine.
///
/// `new()` wires the default implementation ([`DefaultExecutorBuilder`] with a
/// default [`FunctionRegistry`]); the `with_*` methods override specific parts
/// for customization.
pub struct TaskExecutorBuilder {
    builder: Arc<dyn ExecutorBuilder>,
    registry: Arc<FunctionRegistry>,
    storage: Option<Arc<dyn WriteStorage>>,
    metadata_store: MetadataStore,
    resource_limits: ExecutionResourceLimits,
    spill_storage_factory: Arc<dyn SpillStorageFactory>,
    runtime_filter_factory: Arc<dyn RuntimeFilterFactory>,
    runtime_filter_policy: Arc<dyn RuntimeFilterDecisionPolicy>,
    runtime_statistics: RuntimeStatisticsStore,
}

impl TaskExecutorBuilder {
    /// Build a task executor with the default stages and Vector VM engine.
    pub fn new() -> Self {
        let registry = Arc::new(FunctionRegistry::new());
        Self {
            builder: Arc::new(DefaultExecutorBuilder::new(registry.clone())),
            registry,
            storage: None,
            metadata_store: MetadataStore::new(),
            resource_limits: ExecutionResourceLimits::default(),
            spill_storage_factory: Arc::new(LocalSpillStorageFactory),
            runtime_filter_factory: Arc::new(JoinBloomFilterFactory::default()),
            runtime_filter_policy: Arc::new(ConservativeRuntimeFilterPolicy),
            runtime_statistics: RuntimeStatisticsStore::default(),
        }
    }

    /// Share the catalog's parsed-metadata store with the executor.
    ///
    /// The catalog that resolves tables fills the store; handing it to the
    /// executor lets scans reuse file metadata the catalog already parsed
    /// instead of reading the same files again.
    pub fn with_metadata_store(mut self, metadata_store: MetadataStore) -> Self {
        self.metadata_store = metadata_store;
        self.rebuild_default();
        self
    }

    /// Share the catalog's runtime statistics store with executing scan sources.
    pub fn with_runtime_statistics_store(mut self, store: RuntimeStatisticsStore) -> Self {
        self.runtime_statistics = store;
        self.rebuild_default();
        self
    }

    /// Set shared per-query memory and spill-disk budgets for blocking operators.
    pub fn with_resource_limits(mut self, limits: ExecutionResourceLimits) -> Self {
        self.resource_limits = limits;
        self
    }

    /// Configure the query-local spill backend factory.
    pub fn with_spill_storage_factory(mut self, factory: Arc<dyn SpillStorageFactory>) -> Self {
        self.spill_storage_factory = factory;
        self
    }

    /// Configure the strategy used to create query-local join filters.
    ///
    /// Source adapters still decide whether they can apply the resulting filter
    /// before decoding payload columns.
    pub fn with_runtime_filter_factory(
        mut self,
        factory: Arc<dyn RuntimeFilterFactory>,
    ) -> Self {
        self.runtime_filter_factory = factory;
        self.rebuild_default();
        self
    }

    /// Configure the current-query policy for selecting join runtime filters.
    pub fn with_runtime_filter_decision_policy(
        mut self,
        policy: Arc<dyn RuntimeFilterDecisionPolicy>,
    ) -> Self {
        self.runtime_filter_policy = policy;
        self.rebuild_default();
        self
    }

    /// Override the UDF function registry used by the Vector VM.
    pub fn with_registry(mut self, registry: Arc<FunctionRegistry>) -> Self {
        self.registry = registry;
        self.rebuild_default();
        self
    }

    /// Inject the transactional destination backend used by `WRITE` sinks.
    pub fn with_write_storage(mut self, storage: Arc<dyn WriteStorage>) -> Self {
        self.storage = Some(storage);
        self.rebuild_default();
        self
    }

    fn rebuild_default(&mut self) {
        let mut builder = DefaultExecutorBuilder::new(self.registry.clone());
        if let Some(storage) = &self.storage {
            builder = builder.with_write_storage(storage.clone());
        }
        builder = builder.with_metadata_store(self.metadata_store.clone());
        builder = builder.with_runtime_statistics_store(self.runtime_statistics.clone());
        builder = builder.with_runtime_filter_factory(Arc::clone(&self.runtime_filter_factory));
        builder = builder.with_runtime_filter_decision_policy(Arc::clone(&self.runtime_filter_policy));
        self.builder = Arc::new(builder);
    }

    /// Override the executor builder (which maps the wire protocol to stages) entirely.
    pub fn with_executor_builder(mut self, builder: Arc<dyn ExecutorBuilder>) -> Self {
        self.builder = builder;
        self
    }

    /// Build the task executor.
    pub fn build(self) -> crate::runtime::DefaultTaskExecutor {
        crate::runtime::DefaultTaskExecutor::with_resource_limits(self.builder, self.resource_limits)
            .with_spill_storage_factory(self.spill_storage_factory)
    }
}

impl Default for TaskExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// How many storage handles one executor keeps before it starts over.
///
/// A query resolves one root per stage, so this is far more than a single query
/// needs; the bound only stops a long-lived executor from growing the map
/// without limit.
const OPERATOR_CACHE_CAPACITY: usize = 64;

/// The default executor builder: builds concrete sources, sinks, and the
/// [`VectorVmExecutor`] from a wire task description.
pub struct DefaultExecutorBuilder {
    registry: Arc<FunctionRegistry>,
    storage: Option<Arc<dyn WriteStorage>>,
    metadata_store: MetadataStore,
    runtime_filter_factory: Arc<dyn RuntimeFilterFactory>,
    runtime_filter_policy: Arc<dyn RuntimeFilterDecisionPolicy>,
    runtime_statistics: RuntimeStatisticsStore,
    /// Storage handles, keyed by the root they were opened on.
    ///
    /// Opening a handle resolves the root and wires up the service behind it,
    /// which costs more than the first read it serves, and every task of a
    /// stage scans files under the same root. Keeping the handle here — rather
    /// than in the task, which is a serializable description crossing the wire —
    /// lets a stage's tasks share one.
    operators: Mutex<HashMap<PathBuf, Operator>>,
}

impl DefaultExecutorBuilder {
    /// Create a builder with the given UDF function registry for the Vector VM.
    pub fn new(registry: Arc<FunctionRegistry>) -> Self {
        Self {
            registry,
            storage: None,
            metadata_store: MetadataStore::new(),
            runtime_filter_factory: Arc::new(JoinBloomFilterFactory::default()),
            runtime_filter_policy: Arc::new(ConservativeRuntimeFilterPolicy),
            runtime_statistics: RuntimeStatisticsStore::default(),
            operators: Mutex::new(HashMap::new()),
        }
    }

    /// Attach a transactional table storage implementation.
    pub fn with_write_storage(mut self, storage: Arc<dyn WriteStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Share a parsed-metadata store with scans built by this executor.
    pub fn with_metadata_store(mut self, metadata_store: MetadataStore) -> Self {
        self.metadata_store = metadata_store;
        self
    }

    /// Share runtime scan observations with the local catalog and optimizer.
    pub fn with_runtime_statistics_store(mut self, store: RuntimeStatisticsStore) -> Self {
        self.runtime_statistics = store;
        self
    }

    /// Configure the strategy used to create query-local join filters.
    pub fn with_runtime_filter_factory(
        mut self,
        factory: Arc<dyn RuntimeFilterFactory>,
    ) -> Self {
        self.runtime_filter_factory = factory;
        self
    }

    /// Attach the runtime policy that selects safe join filter candidates.
    pub fn with_runtime_filter_decision_policy(
        mut self,
        policy: Arc<dyn RuntimeFilterDecisionPolicy>,
    ) -> Self {
        self.runtime_filter_policy = policy;
        self
    }

    fn build_source_inner(
        &self,
        source: &DataSource,
        runtime_filters: &RuntimeFilterSet,
        query_statistics: Option<Arc<QueryStatisticsContext>>,
    ) -> Result<Box<dyn Source<DataChunkView>>> {
        match source {
            DataSource::Memory => Err(VeinError::internal(
                "a DataSource::Memory task carries no payload to materialize",
            )),
            DataSource::Intermediate(_) => Err(VeinError::internal(
                "intermediate sources must be resolved by the query task executor",
            )),
            DataSource::File(scan) => {
                if scan.scan_unit_range.is_some() && scan.files.len() != 1 {
                    return Err(VeinError::planning(
                        "a scan-unit range must address exactly one file",
                        None,
                    ));
                }
                let (root, relative) = local_scan_paths(&scan.files)?;
                let files = relative
                    .into_iter()
                    .map(|relative| ScanFile {
                        absolute: root.join(&relative),
                        relative,
                        scan_unit_range: scan.scan_unit_range.clone(),
                    })
                    .collect();
                let op = self.local_operator(&root)?;
                let source = ScanSource::new(ScanSourceConfig {
                    operator: op,
                    files,
                    format: scan.format.clone(),
                    projected: scan.projected.clone(),
                    dictionary_columns: scan.dictionary_columns.clone(),
                    pushdown: scan.pushdown.clone(),
                    runtime_filters: runtime_filters.clone(),
                    metadata_store: self.metadata_store.clone(),
                    registry: self.registry.clone(),
                    runtime_statistics: self.runtime_statistics.clone(),
                    statistics_scan: scan.runtime_statistics.clone(),
                });
                let source = match query_statistics {
                    Some(context) => source.with_query_statistics_context(context),
                    None => source,
                };
                Ok(Box::new(source))
            }
            DataSource::Stream(_) => Err(
                VeinError::internal("this local executor supports only memory and Parquet sources"),
            ),
        }
    }

    /// Returns a storage handle rooted at `root`, opening one only the first time.
    ///
    /// The lock is deliberately held across the build: a stage's tasks ask for
    /// the same root at the same instant, and one build with the rest waiting
    /// costs less than a build per task racing to publish the same handle.
    fn local_operator(&self, root: &Path) -> Result<Operator> {
        let mut operators = self
            .operators
            .lock()
            .map_err(|_| VeinError::internal("operator cache mutex poisoned"))?;
        if let Some(operator) = operators.get(root) {
            return Ok(operator.clone());
        }
        if operators.len() >= OPERATOR_CACHE_CAPACITY {
            operators.clear();
        }
        use opendal::services::Fs;
        let builder = Fs::default().root(&root.to_string_lossy());
        let operator = Operator::new(builder)
            .map_err(|error| VeinError::internal(format!("build fs operator: {error}")))?;
        operators.insert(root.to_path_buf(), operator.clone());
        Ok(operator)
    }
}

#[async_trait::async_trait]
impl ExecutorBuilder for DefaultExecutorBuilder {
    fn partition_scan_source(
        &self,
        source: &DataSource,
        max_parallelism: usize,
    ) -> Result<Vec<DataSource>> {
        partition_source(source, &self.metadata_store, max_parallelism, None)
    }

    fn partition_scan_source_with_runtime_filters(
        &self,
        source: &DataSource,
        max_parallelism: usize,
        runtime_filters: Option<&RuntimeFilterSet>,
    ) -> Result<Vec<DataSource>> {
        partition_source(source, &self.metadata_store, max_parallelism, runtime_filters)
    }

    fn build_source(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
    ) -> Result<Box<dyn Source<DataChunkView>>> {
        self.build_source_inner(source, &RuntimeFilterSet::empty(), None)
    }

    fn build_source_with_runtime_filters(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
        filters: &RuntimeFilterSet,
    ) -> Result<Box<dyn Source<DataChunkView>>> {
        self.build_source_inner(source, filters, None)
    }

    fn build_source_with_query_context(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
        runtime_filters: Option<&RuntimeFilterSet>,
        query_statistics: Option<Arc<QueryStatisticsContext>>,
    ) -> Result<Box<dyn Source<DataChunkView>>> {
        self.build_source_inner(
            source,
            runtime_filters.unwrap_or(&RuntimeFilterSet::empty()),
            query_statistics,
        )
    }

    fn supports_runtime_filter_pushdown(
        &self,
        source: &DataSource,
        columns: &[types::ColumnId],
    ) -> bool {
        match source {
            DataSource::File(scan) if scan.format == FileFormat::Parquet => columns.iter().all(|id| {
                scan.projected.fields.iter().any(|column| column.id == *id)
            }),
            _ => false,
        }
    }

    fn runtime_filter_factory(&self) -> Option<Arc<dyn RuntimeFilterFactory>> {
        Some(Arc::clone(&self.runtime_filter_factory))
    }

    fn runtime_filter_decision_policy(&self) -> Arc<dyn RuntimeFilterDecisionPolicy> {
        Arc::clone(&self.runtime_filter_policy)
    }

    fn build_vm(
        &self,
        program: &Arc<MicroProgram>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Result<Box<dyn Transform<DataChunk>>> {
        Ok(Box::new(VectorVmExecutor::new(
            program.clone(),
            self.registry.clone(),
            next,
        )))
    }

    fn build_view_vm(
        &self,
        program: &Arc<MicroProgram>,
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> Result<Box<dyn Transform<DataChunkView>>> {
        Ok(Box::new(VectorVmExecutor::<DataChunkView>::new_view(
            program.clone(),
            self.registry.clone(),
            next,
        )))
    }

    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunkView>>> {
        match sink {
            DataSink::Discard => Ok(Box::new(DiscardSink::new())),
            DataSink::Client { schema } => {
                Ok(Box::new(CollectSink::new(Arc::new(schema.to_arrow()))))
            }
            DataSink::Storage(target) => {
                let storage = self.storage.as_ref().ok_or_else(|| {
                    VeinError::internal("WRITE requires a configured table storage backend")
                })?;
                Ok(Box::new(MaterializeViewNode::new(
                    Box::new(WriteSink::new(target.clone(), storage.clone())?), "storage",
                )))
            }
        }
    }

    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<RecordBatch>,
    ) -> Result<Box<dyn Sink<DataChunkView>>> {
        match sink {
            DataSink::Client { schema } => {
                Ok(Box::new(CollectSink::with_sender(
                    Arc::new(schema.to_arrow()),
                    output,
                )))
            }
            _ => self.build_sink(sink),
        }
    }
}

/// Splits a Parquet scan into footer-selected, byte-balanced row-group ranges.
/// A missing cached footer falls back to a whole-file source, so partitioning
/// never rereads metadata or requires a format-specific assumption in the shared
/// runtime.
fn partition_source(
    source: &DataSource,
    metadata_store: &MetadataStore,
    max_parallelism: usize,
    runtime_filters: Option<&RuntimeFilterSet>,
) -> Result<Vec<DataSource>> {
    let mut partitions = match source {
        DataSource::File(scan)
        if scan.format == FileFormat::Parquet && scan.scan_unit_range.is_none() =>
            {
                partition_parquet_scan(scan, metadata_store, max_parallelism, runtime_filters)
            }
        DataSource::File(scan) if scan.scan_unit_range.is_some() => {
            if scan.files.len() != 1 {
                return Err(VeinError::planning(
                    "a scan-unit range must address exactly one file",
                    None,
                ));
            }
            Ok(vec![source.clone()])
        }
        _ => Ok(split_scan_by_file(source)),
    }?;
    let partition_count = partitions.len();
    for (partition_index, partition) in partitions.iter_mut().enumerate() {
        let DataSource::File(scan) = partition else {
            continue;
        };
        if let Some(statistics) = &mut scan.runtime_statistics {
            statistics.partition_index = partition_index;
            statistics.partition_count = partition_count;
        }
    }
    Ok(partitions)
}

fn partition_parquet_scan(
    scan: &FileScanTask,
    metadata_store: &MetadataStore,
    max_parallelism: usize,
    runtime_filters: Option<&RuntimeFilterSet>,
) -> Result<Vec<DataSource>> {
    let (root, relative_paths) = local_scan_paths(&scan.files)?;
    let mut predicates = scan
        .pushdown
        .as_ref()
        .map_or_else(Vec::new, |predicate| predicate.statistics.clone());
    let static_predicate_count = predicates.len();
    let runtime_predicates = runtime_filters.map_or_else(Vec::new, |filters| {
        filters.statistics_predicates(scan.projected.as_ref())
    });
    let runtime_predicate_count = runtime_predicates.len();
    predicates.extend(runtime_predicates);
    let mut whole_file_work = Vec::new();
    let mut row_group_files = Vec::new();
    let mut footer_hits = 0usize;
    let mut footer_misses = 0usize;
    let mut candidate_units = 0usize;
    let mut total_row_groups = 0usize;

    for (file, relative) in scan.files.iter().zip(relative_paths) {
        let absolute = root.join(relative);
        let key = MetadataKey::for_canonical_file(PARQUET_FOOTER, &absolute);
        let metadata = key
            .as_ref()
            .and_then(|key| metadata_store.get::<ParquetMetaData>(key));
        let Some(metadata) = metadata else {
            footer_misses += 1;
            whole_file_work.push(file_scan_work_unit(scan, file, None));
            continue;
        };
        footer_hits += 1;
        total_row_groups = total_row_groups.saturating_add(metadata.num_row_groups());

        let arrow_metadata = ArrowReaderMetadata::try_new(
            Arc::clone(&metadata),
            ArrowReaderOptions::new(),
        )
            .map_err(|error| {
                VeinError::internal(format!("Failed to derive Parquet scan schema: {error}"))
            })?;
        let candidates = crate::parquet_statistics::prune_row_groups(
            arrow_metadata.schema().as_ref(),
            &metadata,
            &predicates,
        );
        candidate_units = candidate_units.saturating_add(candidates.selected.len());
        if !candidates.selected.is_empty() {
            let weights = candidates.selected.iter().map(|index| {
                let group = metadata.row_group(*index);
                let bytes = group.columns().iter().filter(|column| {
                    scan.projected.fields.is_empty() || scan.projected.fields.iter()
                        .any(|field| column.column_path().parts().first() == Some(&field.name))
                }).fold(0_u64, |sum, column| sum.saturating_add(column.compressed_size().max(1) as u64));
                bytes.max(1)
            }).collect::<Vec<_>>();
            row_group_files.push((file, candidates.selected, weights));
        }
    }

    // Keep one active and one ready morsel per worker. More work than workers
    // permits stealing while amortizing reader setup over adjacent row groups.
    // Compressed bytes cover only projected columns, including filter inputs.
    let total_weight = row_group_files.iter().flat_map(|(_, _, weights)| weights)
        .fold(0_u64, |sum, weight| sum.saturating_add(*weight));
    let target_bytes = (max_parallelism > 1).then(|| total_weight
        .div_ceil(max_parallelism.saturating_mul(2).max(1) as u64).max(1));
    let mut work = whole_file_work;
    for (file, groups, weights) in row_group_files {
        for range in scan_work::row_group_ranges(groups.into_iter().zip(weights), target_bytes) {
            work.push(file_scan_work_unit(scan, file, Some(range)));
        }
    }

    tracing::debug!(
        target: "vein.audit",
        event = "scan_partitioned",
        format = "parquet",
        file_count = scan.files.len(),
        footer_hits,
        footer_misses,
        static_statistics_predicates = static_predicate_count,
        runtime_statistics_predicates = runtime_predicate_count,
        runtime_filter_names = ?runtime_filters.map_or_else(Vec::new, RuntimeFilterSet::names),
        total_row_groups,
        candidate_row_groups = candidate_units,
        pruned_row_groups = total_row_groups.saturating_sub(candidate_units),
        max_parallelism,
        scheduled_work_units = work.len(),
        "Parquet scan decomposed into byte-balanced row-group ranges"
    );
    Ok(work)
}


fn file_scan_work_unit(
    scan: &FileScanTask,
    file: &str,
    scan_unit_range: Option<std::ops::Range<usize>>,
) -> DataSource {
    let mut work_unit = scan.clone();
    work_unit.files = vec![file.to_owned()];
    work_unit.scan_unit_range = scan_unit_range;
    work_unit.task_id = TaskId::new();
    DataSource::File(work_unit)
}


fn local_scan_paths(files: &[String]) -> Result<(PathBuf, Vec<String>)> {
    let paths = files
        .iter()
        .map(|file| {
            std::fs::canonicalize(file)
                .map_err(|error| VeinError::io(format!("resolve local scan file '{file}'"), error))
        })
        .collect::<Result<Vec<_>>>()?;
    let first = paths
        .first()
        .ok_or_else(|| VeinError::internal("file scan contains no files"))?;
    let root = first
        .ancestors()
        .skip(1)
        .find(|candidate| paths.iter().all(|path| path.starts_with(candidate)))
        .ok_or_else(|| VeinError::internal("local scan files have no common directory"))?
        .to_path_buf();
    let relative = paths
        .iter()
        .map(|path| {
            path.strip_prefix(&root)
                .map(|path| path.to_string_lossy().into_owned())
                .map_err(|error| {
                    VeinError::internal(format!("local scan path is outside its root: {error}"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((root, relative))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use common::identifiers::TaskId;
    use common::protocol::FileFormat;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn write_row_groups(path: &Path, row_group_count: usize) -> ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let file = std::fs::File::create(path).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
            .expect("create Parquet writer");
        for row_group in 0..row_group_count {
            let first = (row_group * 2) as i64;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![first, first + 1]))],
            )
                .expect("valid row-group batch");
            writer.write(&batch).expect("write row group");
            writer.flush().expect("finish row group");
        }
        writer.close().expect("close Parquet writer");

        let reader = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(path).expect("open Parquet footer"),
        )
            .expect("read Parquet footer");
        reader.metadata().as_ref().clone()
    }


    #[test]
    fn parquet_scan_partitions_cached_footer_by_available_parallelism() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("part.parquet");
        let metadata = Arc::new(write_row_groups(&path, 5));
        let key = MetadataKey::for_file(PARQUET_FOOTER, &path).expect("canonical footer key");
        let metadata_store = MetadataStore::new();
        metadata_store.insert(key, metadata);
        let scan = FileScanTask {
            files: vec![path.to_string_lossy().into_owned()],
            format: FileFormat::Parquet,
            byte_range: None,
            scan_unit_range: None,
            projected: Arc::new(types::VeinSchema::empty()),
            dictionary_columns: Vec::new(),
            pushdown: None,
            task_id: TaskId::new(),
            catalog_snapshot: None,
            runtime_statistics: None,
        };

        let work = partition_parquet_scan(&scan, &metadata_store, 3, None)
            .expect("partition cached Parquet footer");
        let ranges = work
            .iter()
            .map(|source| match source {
                DataSource::File(task) => task
                    .scan_unit_range
                    .clone()
                    .expect("footer-backed work unit has a row-group range"),
                _ => panic!("Parquet partition must produce file work units"),
            })
            .collect::<Vec<_>>();

        assert_eq!(ranges.len(), 5);
        assert_eq!(ranges.first().map(|range| range.start), Some(0));
        assert_eq!(ranges.last().map(|range| range.end), Some(5));
        assert!(ranges.windows(2).all(|pair| pair[0].end == pair[1].start));
    }

    #[test]
    fn runtime_join_key_bounds_prune_row_groups_before_scheduling() {
        use crate::memory::QueryMemoryTracker;
        use crate::runtime_filter::RuntimeFilterBuildInput;
        use common::cancellation::CancellationToken;
        use types::{ColumnId, LogicalLiteral, ResolvedColumn, VeinDataType};

        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("runtime_bounds.parquet");
        let metadata = Arc::new(write_row_groups(&path, 5));
        let key = MetadataKey::for_file(PARQUET_FOOTER, &path).expect("canonical footer key");
        let metadata_store = MetadataStore::new();
        metadata_store.insert(key, metadata);
        let scan = FileScanTask {
            files: vec![path.to_string_lossy().into_owned()],
            format: FileFormat::Parquet,
            byte_range: None,
            scan_unit_range: None,
            projected: Arc::new(types::VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
                ColumnId(70),
                "id",
                None,
                VeinDataType::Int64,
            ))])),
            dictionary_columns: Vec::new(),
            pushdown: None,
            task_id: TaskId::new(),
            catalog_snapshot: None,
            runtime_statistics: None,
        };
        let tracker = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
        let build_hashes = [123_u64];
        let filters = JoinBloomFilterFactory::default()
            .build_filters(RuntimeFilterBuildInput {
                probe_columns: &[ColumnId(70)],
                build_hashes: &build_hashes,
                build_key_bounds: vec![Some((
                    LogicalLiteral::Int64(2),
                    LogicalLiteral::Int64(3),
                ))],
                memory: &tracker,
            })
            .expect("build runtime join filters");
        let filters = RuntimeFilterSet::new(filters);

        assert!(filters.names().contains(&"join_key_range"));
        let work = partition_parquet_scan(&scan, &metadata_store, 3, Some(&filters))
            .expect("partition with runtime join bounds");
        let ranges = work
            .iter()
            .map(|source| match source {
                DataSource::File(task) => task
                    .scan_unit_range
                    .clone()
                    .expect("runtime-pruned work has a row-group range"),
                _ => panic!("Parquet partition must produce file work units"),
            })
            .collect::<Vec<_>>();

        assert_eq!(ranges, vec![1..2], "only the row group spanning build bounds remains scheduled");
    }

    #[test]
    fn parallel_parquet_work_covers_every_group_across_files_once() {
        for (file_count, groups_per_file, workers) in [(2, 5, 3), (5, 17, 8), (7, 3, 4)] {
            let dir = tempfile::tempdir().expect("temporary directory");
            let metadata_store = MetadataStore::new();
            let files = (0..file_count).map(|index| {
                let path = dir.path().join(format!("part-{index}.parquet"));
                let metadata = Arc::new(write_row_groups(&path, groups_per_file));
                let key = MetadataKey::for_file(PARQUET_FOOTER, &path).unwrap();
                metadata_store.insert(key, metadata);
                path.to_string_lossy().into_owned()
            }).collect::<Vec<_>>();
            let scan = FileScanTask {
                files: files.clone(),
                format: FileFormat::Parquet,
                byte_range: None,
                scan_unit_range: None,
                projected: Arc::new(types::VeinSchema::empty()),
                dictionary_columns: Vec::new(),
                pushdown: None,
                task_id: TaskId::new(),
                catalog_snapshot: None,
                runtime_statistics: None,
            };
            let work = partition_parquet_scan(&scan, &metadata_store, workers, None).unwrap();
            assert!(work.len() > workers);
            for file in files {
                let mut groups = work.iter().filter_map(|source| {
                    let DataSource::File(task) = source else { panic!("expected file work") };
                    (task.files == [file.clone()]).then(|| task.scan_unit_range.clone().unwrap())
                }).flatten().collect::<Vec<_>>();
                groups.sort_unstable();
                assert_eq!(groups, (0..groups_per_file).collect::<Vec<_>>());
            }
        }
    }
}
