//! The default [`ExecutorBuilder`] implementation.
//!
//! Maps the wire protocol (`common::protocol::{DataSource, MicroProgram, DataSink}`)
//! onto the executor's concrete stages and Vector VM execution engine.

use std::sync::Arc;

use crate::operators::sink::{CollectSink, DiscardSink, StorageSink};
use crate::operators::source::{MemorySource, ScanSource};
use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use crate::runtime::ExecutorBuilder;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use common::protocol::{DataSink, DataSource, MicroProgram};
use common::Result;
use error::VeinError;

/// Builds a [`TaskExecutor`](crate::runtime::TaskExecutor) with configurable
/// stages and Vector VM execution engine.
///
/// `new()` wires the default implementation ([`DefaultExecutorBuilder`] with a
/// default [`FunctionRegistry`]); the `with_*` methods override specific parts
/// for customization.
pub struct TaskExecutorBuilder {
    builder: Arc<dyn ExecutorBuilder>,
}

impl TaskExecutorBuilder {
    /// Build a task executor with the default stages and Vector VM engine.
    pub fn new() -> Self {
        let registry = Arc::new(FunctionRegistry::new());
        Self {
            builder: Arc::new(DefaultExecutorBuilder::new(registry)),
        }
    }

    /// Override the UDF function registry used by the Vector VM.
    pub fn with_registry(mut self, registry: Arc<FunctionRegistry>) -> Self {
        self.builder = Arc::new(DefaultExecutorBuilder::new(registry));
        self
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
}

impl DefaultExecutorBuilder {
    /// Create a builder with the given UDF function registry for the Vector VM.
    pub fn new(registry: Arc<FunctionRegistry>) -> Self {
        Self { registry }
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
                        .with_projection(projection),
                ))
            }
            DataSource::Exchange(_) => Err(VeinError::internal(
                "Exchange source is not yet implemented",
            )),
            DataSource::Stream(_) => Err(VeinError::internal(
                "Stream source is not yet implemented",
            )),
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
            DataSink::Shuffle(_) => Err(VeinError::internal(
                "Shuffle sink is not yet implemented",
            )),
            DataSink::Storage(target) => Ok(Box::new(StorageSink::new(target.clone()))),
        }
    }
}
