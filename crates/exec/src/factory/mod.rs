//! The default [`ExecutorBuilder`] implementation.
//!
//! Maps the wire protocol (`common::protocol::{DataSource, MicroProgram, DataSink}`)
//! onto the executor's concrete stages and Vector VM execution engine.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::exchange::SharedExchangeMetadata;
use crate::operators::join::HashJoinSource;
use crate::operators::sink::{CollectSink, DiscardSink, ShuffleSink, StorageSink};
use crate::operators::source::{MemorySource, ScanSource, VfsExchangeSource};
use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use crate::runtime::ExecutorBuilder;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use common::Result;
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
    metadata: Option<SharedExchangeMetadata>,
}

impl TaskExecutorBuilder {
    /// Build a task executor with the default stages and Vector VM engine.
    pub fn new() -> Self {
        let registry = Arc::new(FunctionRegistry::new());
        Self {
            builder: Arc::new(DefaultExecutorBuilder::new(registry.clone())),
            registry,
            metadata: None,
        }
    }

    /// Override the UDF function registry used by the Vector VM.
    pub fn with_registry(mut self, registry: Arc<FunctionRegistry>) -> Self {
        self.registry = registry;
        self.rebuild_default();
        self
    }

    /// Configure VCS-backed exchange metadata for shuffle and join stages.
    pub fn with_exchange_metadata(mut self, metadata: SharedExchangeMetadata) -> Self {
        self.metadata = Some(metadata);
        self.rebuild_default();
        self
    }

    fn rebuild_default(&mut self) {
        self.builder = Arc::new(DefaultExecutorBuilder::with_exchange_metadata(
            self.registry.clone(),
            self.metadata.clone(),
        ));
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
    metadata: Option<SharedExchangeMetadata>,
}

impl DefaultExecutorBuilder {
    /// Create a builder with the given UDF function registry for the Vector VM.
    pub fn new(registry: Arc<FunctionRegistry>) -> Self {
        Self::with_exchange_metadata(registry, None)
    }

    /// Creates a builder with optional VCS-backed exchange metadata.
    pub fn with_exchange_metadata(
        registry: Arc<FunctionRegistry>,
        metadata: Option<SharedExchangeMetadata>,
    ) -> Self {
        Self { registry, metadata }
    }
}

impl ExecutorBuilder for DefaultExecutorBuilder {
    fn build_source(&self, source: &DataSource) -> Result<Box<dyn Source<DataChunk>>> {
        match source {
            DataSource::Memory => Ok(Box::new(MemorySource::dummy())),
            DataSource::File(scan) => {
                // TODO(storage): pick the OpenDAL service from the scan URI scheme
                // (e.g. s3://, fs://) instead of always mounting local filesystem.
                use opendal::services::Fs;
                let mut builder = Fs::default();
                builder.root("./");
                let op = opendal::Operator::new(builder)
                    .map_err(|e| VeinError::internal(format!("build fs operator: {e}")))?
                    .finish();
                let projection = if scan.projected_schema.is_empty() {
                    None
                } else {
                    Some(scan.projected_schema.clone())
                };
                Ok(Box::new(
                    ScanSource::new(op, scan.files.clone(), scan.format.clone())
                        .with_projection(projection)
                        .with_scan_predicates(&scan.scan_predicates),
                ))
            }
            DataSource::Exchange(exchange) => {
                if exchange.partition_count == 0
                    || exchange.partition_id >= exchange.partition_count as u32
                {
                    return Err(VeinError::internal("exchange partition is out of range"));
                }
                let store = vfs::VfsTableStore::shared_or_default()
                    .map_err(|e| VeinError::internal(format!("open VFS exchange store: {e}")))?;
                let path = format!(
                    "{}/partition_{:05}",
                    exchange.path_prefix, exchange.partition_id
                );
                let source = VfsExchangeSource::new(store, path, exchange.schema.clone());
                let source = match &self.metadata {
                    Some(metadata) => source.with_exchange_metadata(
                        exchange.exchange_key,
                        common::identifiers::PartitionId::from_raw(exchange.partition_id as u16),
                        metadata.clone(),
                    ),
                    None => source,
                };
                Ok(Box::new(source))
            }
            DataSource::Join(join) => {
                let store = vfs::VfsTableStore::shared_or_default()
                    .map_err(|e| VeinError::internal(format!("open VFS join store: {e}")))?;
                let source = match &self.metadata {
                    Some(metadata) => HashJoinSource::with_store_and_metadata(
                        join.clone(),
                        store,
                        metadata.clone(),
                    ),
                    None => HashJoinSource::with_store(join.clone(), store),
                };
                Ok(Box::new(source))
            }
            DataSource::Stream(_) => {
                Err(VeinError::internal("Stream source is not yet implemented"))
            }
        }
    }

    fn build_vm(
        &self,
        program: &MicroProgram,
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
            DataSink::Shuffle(target) => Ok(Box::new(ShuffleSink::with_vfs_store_and_metadata(
                target.clone(),
                None,
                self.metadata.clone(),
            ))),
            DataSink::Storage(target) => Ok(Box::new(StorageSink::new(target.clone()))),
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
