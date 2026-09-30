//! The default [`ExecutorBuilder`] implementation.
//!
//! Maps task descriptions onto concrete sources, sinks, and Vector VM execution.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::operators::sink::{CollectSink, DiscardSink, WriteSink, WriteStorage};
use crate::operators::source::scan::ScanFile;
use crate::operators::source::{MemorySource, ScanSource};
use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use crate::runtime::ExecutorBuilder;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use catalog::MetadataStore;
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, MicroProgram};
use error::VeinError;

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
        self.builder = Arc::new(builder);
    }

    /// Override the executor builder (which maps the wire protocol to stages) entirely.
    pub fn with_executor_builder(mut self, builder: Arc<dyn ExecutorBuilder>) -> Self {
        self.builder = builder;
        self
    }

    /// Build the task executor.
    pub fn build(self) -> crate::runtime::DefaultTaskExecutor {
        crate::runtime::DefaultTaskExecutor::new(self.builder)
    }
}

impl Default for TaskExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The default executor builder: builds concrete sources, sinks, and the
/// [`VectorVmExecutor`] from a wire task description.
pub struct DefaultExecutorBuilder {
    registry: Arc<FunctionRegistry>,
    storage: Option<Arc<dyn WriteStorage>>,
    metadata_store: MetadataStore,
}

impl DefaultExecutorBuilder {
    /// Create a builder with the given UDF function registry for the Vector VM.
    pub fn new(registry: Arc<FunctionRegistry>) -> Self {
        Self {
            registry,
            storage: None,
            metadata_store: MetadataStore::new(),
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
}

#[async_trait::async_trait(?Send)]
impl ExecutorBuilder for DefaultExecutorBuilder {
    fn build_source(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
    ) -> Result<Box<dyn Source<DataChunk>>> {
        match source {
            DataSource::Memory => Ok(Box::new(MemorySource::dummy())),
            DataSource::File(scan) => {
                let (root, relative) = local_scan_paths(&scan.files)?;
                let files = relative
                    .into_iter()
                    .map(|relative| ScanFile {
                        absolute: root.join(&relative),
                        relative,
                    })
                    .collect();
                use opendal::services::Fs;
                let builder = Fs::default().root(&root.to_string_lossy());
                let op = opendal::Operator::new(builder)
                    .map_err(|e| VeinError::internal(format!("build fs operator: {e}")))?;
                Ok(Box::new(ScanSource::new(
                    op,
                    files,
                    scan.format.clone(),
                    scan.projected_schema.clone(),
                    self.metadata_store.clone(),
                )))
            }
            DataSource::Stream(_) => Err(
                VeinError::internal("this local executor supports only memory and Parquet sources"),
            ),
        }
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

    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunk>>> {
        match sink {
            DataSink::Discard => Ok(Box::new(DiscardSink::new())),
            DataSink::Client { schema } => {
                let field_names = schema.iter().map(|f| f.name.clone()).collect();
                Ok(Box::new(CollectSink::with_field_names(field_names)))
            }
            DataSink::Storage(target) => {
                let storage = self.storage.as_ref().ok_or_else(|| {
                    VeinError::internal("WRITE requires a configured table storage backend")
                })?;
                Ok(Box::new(WriteSink::new(target.clone(), storage.clone())?))
            }
        }
    }

    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<RecordBatch>,
    ) -> Result<Box<dyn Sink<DataChunk>>> {
        match sink {
            DataSink::Client { schema } => {
                let field_names = schema.iter().map(|field| field.name.clone()).collect();
                Ok(Box::new(CollectSink::with_field_names_and_sender(
                    field_names,
                    output,
                )))
            }
            _ => self.build_sink(sink),
        }
    }
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
