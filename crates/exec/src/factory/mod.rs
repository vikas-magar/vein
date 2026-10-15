//! The default [`ExecutorBuilder`] implementation.
//!
//! Maps task descriptions onto concrete sources, sinks, and Vector VM execution.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;

use crate::operators::join::JoinBloomFilterFactory;
use crate::operators::sink::{CollectSink, DiscardSink, WriteSink, WriteStorage};
use crate::memory::ExecutionResourceLimits;
use crate::spill::{LocalSpillStorageFactory, SpillStorageFactory};
use crate::operators::source::scan::{PARQUET_FOOTER, ScanFile};
use crate::operators::source::ScanSource;
use crate::pipeline::{DataChunk, DataChunkView, PipelineNode, Sink, Source, Transform};
use crate::runtime_filter::{RuntimeFilterFactory, RuntimeFilterSet};
use crate::runtime::{ExecutorBuilder, MaterializeViewNode, split_scan_by_file};
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use catalog::{MetadataKey, MetadataStore};
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, FileFormat, FileScanTask, MicroProgram};
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
        builder = builder.with_runtime_filter_factory(Arc::clone(&self.runtime_filter_factory));
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

    /// Configure the strategy used to create query-local join filters.
    pub fn with_runtime_filter_factory(
        mut self,
        factory: Arc<dyn RuntimeFilterFactory>,
    ) -> Self {
        self.runtime_filter_factory = factory;
        self
    }

    fn build_source_inner(
        &self,
        source: &DataSource,
        runtime_filters: &RuntimeFilterSet,
    ) -> Result<Box<dyn Source<DataChunkView>>> {
        match source {
            DataSource::Memory => Err(VeinError::internal(
                "a DataSource::Memory task carries no payload to materialize",
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
                Ok(Box::new(ScanSource::new_with_runtime_filters(
                    op,
                    files,
                    scan.format.clone(),
                    scan.projected.clone(),
                    scan.dictionary_columns.clone(),
                    scan.pushdown.clone(),
                    self.metadata_store.clone(),
                    self.registry.clone(),
                    runtime_filters.clone(),
                )))
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
        self.build_source_inner(source, &RuntimeFilterSet::empty())
    }

    fn build_source_with_runtime_filters(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
        filters: &RuntimeFilterSet,
    ) -> Result<Box<dyn Source<DataChunkView>>> {
        self.build_source_inner(source, filters)
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
    match source {
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
    }
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
            let weights = candidates
                .selected
                .iter()
                .map(|index| {
                    metadata.row_groups()[*index]
                        .total_byte_size()
                        .max(1) as u64
                })
                .collect::<Vec<_>>();
            row_group_files.push(RowGroupFilePlan {
                file,
                groups: candidates.selected,
                weights,
            });
        }
    }

    // One reader per range avoids reopening a file for every row group. Allocate
    // additional ranges to the files with the most estimated bytes, up to the
    // executor's parallelism and the number of candidate row groups.
    let fixed_units = whole_file_work.len()
        + row_group_files
            .iter()
            .map(|file| contiguous_row_group_run_count(&file.groups))
            .sum::<usize>();
    let candidate_work_units = whole_file_work.len()
        + row_group_files
            .iter()
            .map(|file| file.groups.len())
            .sum::<usize>();
    let target_work_units = max_parallelism
        .max(fixed_units)
        .min(candidate_work_units);
    let mut range_counts = row_group_files
        .iter()
        .map(|file| contiguous_row_group_run_count(&file.groups))
        .collect::<Vec<_>>();
    let mut remaining_ranges = target_work_units.saturating_sub(fixed_units);
    while remaining_ranges > 0 {
        let mut best = None;
        for (index, file) in row_group_files.iter().enumerate() {
            if range_counts[index] >= file.groups.len() {
                continue;
            }
            let Some(best_index) = best else {
                best = Some(index);
                continue;
            };
            let file_weight = file.weights.iter().copied().sum::<u64>() as u128;
            let best_weight = row_group_files[best_index]
                .weights
                .iter()
                .copied()
                .sum::<u64>() as u128;
            if file_weight * range_counts[best_index] as u128
                > best_weight * range_counts[index] as u128
            {
                best = Some(index);
            }
        }
        let Some(index) = best else {
            break;
        };
        range_counts[index] += 1;
        remaining_ranges -= 1;
    }

    let mut work = whole_file_work;
    for (file, range_count) in row_group_files.into_iter().zip(range_counts) {
        for range in weighted_row_group_ranges(
            &file.groups,
            &file.weights,
            range_count,
        ) {
            work.push(file_scan_work_unit(scan, file.file, Some(range)));
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

struct RowGroupFilePlan<'a> {
    file: &'a str,
    groups: Vec<usize>,
    weights: Vec<u64>,
}

fn contiguous_row_group_run_count(groups: &[usize]) -> usize {
    let Some((_, rest)) = groups.split_first() else {
        return 0;
    };
    1 + rest
        .iter()
        .zip(groups.iter())
        .filter(|(current, previous)| **current != previous.saturating_add(1))
        .count()
}

fn weighted_row_group_ranges(
    groups: &[usize],
    weights: &[u64],
    range_count: usize,
) -> Vec<std::ops::Range<usize>> {
    if groups.is_empty() || weights.len() != groups.len() || range_count == 0 {
        return Vec::new();
    }

    // A reader range must never bridge a row group rejected by statistics.
    // Split the candidate ordinals into contiguous runs first, then distribute
    // any additional parallel ranges within those runs by estimated bytes.
    let mut runs = Vec::new();
    let mut run_start = 0usize;
    for index in 1..groups.len() {
        if groups[index] != groups[index - 1].saturating_add(1) {
            runs.push(run_start..index);
            run_start = index;
        }
    }
    runs.push(run_start..groups.len());

    let range_count = range_count.max(runs.len()).min(groups.len());
    let mut run_range_counts = vec![1usize; runs.len()];
    let mut remaining_ranges = range_count - runs.len();
    while remaining_ranges > 0 {
        let mut best_run = None;
        for (index, run) in runs.iter().enumerate() {
            if run_range_counts[index] >= run.len() {
                continue;
            }
            let weight = weights[run.clone()]
                .iter()
                .copied()
                .fold(0u64, u64::saturating_add) as u128;
            let Some(best) = best_run else {
                best_run = Some(index);
                continue;
            };
            let best_weight = weights[runs[best].clone()]
                .iter()
                .copied()
                .fold(0u64, u64::saturating_add) as u128;
            if weight * run_range_counts[best] as u128
                > best_weight * run_range_counts[index] as u128
            {
                best_run = Some(index);
            }
        }
        let Some(index) = best_run else {
            break;
        };
        run_range_counts[index] += 1;
        remaining_ranges -= 1;
    }

    let mut ranges = Vec::with_capacity(range_count);
    for (run, run_range_count) in runs.into_iter().zip(run_range_counts) {
        let mut chunk_start = run.start;
        let mut remaining_weight = weights[run.clone()]
            .iter()
            .copied()
            .fold(0u64, u64::saturating_add);
        for range_index in 0..run_range_count {
            let ranges_left = run_range_count - range_index;
            let mut chunk_end = chunk_start;
            let mut chunk_weight = 0u64;
            if ranges_left == 1 {
                chunk_end = run.end;
                chunk_weight = remaining_weight;
            } else {
                let target_weight = remaining_weight.div_ceil(ranges_left as u64);
                // Keep at least one candidate for every range that follows.
                // A skewed final row group can make the byte target unreachable
                // before this limit; stop there rather than consuming the run.
                let max_chunk_end = run.end - (ranges_left - 1);
                while chunk_end < max_chunk_end {
                    chunk_weight = chunk_weight.saturating_add(weights[chunk_end]);
                    chunk_end += 1;
                    if chunk_weight >= target_weight {
                        break;
                    }
                }
            }
            let first_group = groups[chunk_start];
            let last_group = groups[chunk_end - 1];
            ranges.push(first_group..last_group.saturating_add(1));
            remaining_weight = remaining_weight.saturating_sub(chunk_weight);
            chunk_start = chunk_end;
        }
    }
    ranges
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
    fn weighted_row_group_ranges_cover_file_once_and_keep_each_candidate() {
        let selected = [1, 4, 8];
        let weights = [2, 4, 2];
        let ranges = weighted_row_group_ranges(&selected, &weights, 2);

        assert_eq!(ranges, vec![1..2, 4..5, 8..9]);
        assert!(ranges.iter().all(|range| range.start < range.end));
        for group in selected {
            assert_eq!(
                ranges.iter().filter(|range| range.contains(&group)).count(),
                1,
                "candidate row group {group} must be assigned exactly once"
            );
        }
        for group in [0, 2, 3, 5, 6, 7, 9] {
            assert!(
                ranges.iter().all(|range| !range.contains(&group)),
                "rejected row group {group} must not be included in scheduled ranges"
            );
        }
    }

    #[test]
    fn weighted_row_group_ranges_assign_all_candidates_to_last_range() {
        let groups = (0..17).collect::<Vec<_>>();
        let weights = vec![1; groups.len()];

        let ranges = weighted_row_group_ranges(&groups, &weights, 2);

        assert_eq!(ranges, vec![0..9, 9..17]);
    }

    #[test]
    fn weighted_row_group_ranges_reserve_candidates_when_byte_target_is_unreachable() {
        let groups = [0, 1, 2];
        let weights = [1, 1, 100];

        let ranges = weighted_row_group_ranges(&groups, &weights, 3);

        assert_eq!(ranges, vec![0..1, 1..2, 2..3]);
        for group in groups {
            assert_eq!(
                ranges.iter().filter(|range| range.contains(&group)).count(),
                1,
                "candidate row group {group} must be assigned exactly once"
            );
        }
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

        assert_eq!(ranges.len(), 3);
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
}
