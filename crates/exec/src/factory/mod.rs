//! The default [`ExecutorBuilder`] implementation.
//!
//! Maps task descriptions onto concrete sources, sinks, and Vector VM execution.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::operators::sink::{CollectSink, DiscardSink};
use crate::operators::source::{MemorySource, ScanSource};
use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use crate::runtime::ExecutorBuilder;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
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
}

impl TaskExecutorBuilder {
    /// Build a task executor with the default stages and Vector VM engine.
    pub fn new() -> Self {
        let registry = Arc::new(FunctionRegistry::new());
        Self {
            builder: Arc::new(DefaultExecutorBuilder::new(registry.clone())),
            registry,
        }
    }

    /// Override the UDF function registry used by the Vector VM.
    pub fn with_registry(mut self, registry: Arc<FunctionRegistry>) -> Self {
        self.registry = registry;
        self.rebuild_default();
        self
    }

    fn rebuild_default(&mut self) {
        self.builder = Arc::new(DefaultExecutorBuilder::new(self.registry.clone()));
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
                let (root, paths) = local_scan_paths(&scan.files)?;
                use opendal::services::Fs;
                let mut builder = Fs::default();
                builder.root(&root.to_string_lossy());
                let op = opendal::Operator::new(builder)
                    .map_err(|e| VeinError::internal(format!("build fs operator: {e}")))?
                    .finish();
                Ok(Box::new(ScanSource::new(op, paths, scan.format.clone())))
            }
            DataSource::Stream(_) => Err(
                VeinError::internal("this local executor supports only memory and Parquet sources"),
            ),
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
            DataSink::Storage(_) => Err(VeinError::internal(
                "this local executor supports only client and discard sinks",
            )),
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
