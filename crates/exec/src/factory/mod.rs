//! The default [`ExecutorBuilder`] implementation.
//!
//! Maps the wire protocol (`common::protocol::{DataSource, Operator, DataSink}`)
//! onto the executor's concrete stages. New sources/operators/sinks are
//! added by extending the matches here.

use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::daemon::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use crate::expr::{ColumnExpr, PhysicalExpr};
use crate::operators::source::memory::MemoryExecutor;
use crate::operators::source::scan::ScanExecutor;
use crate::operators::sink::client::CollectExecutor;
use crate::operators::sink::discard::DiscardExecutor;
use crate::operators::transform::filter::FilterExecutor;
use crate::operators::transform::project::ProjectExecutor;
use crate::runtime::{ExecutorBuilder, PhysicalExprFactory};
use common::protocol::{DataSink, DataSource, Operator};
use common::Result;
use error::VeinError;

/// Filter-expression building (`Parser::parse_expr` -> [`PhysicalExpr`]).
pub mod expr;

pub use expr::DefaultPhysicalExprFactory;

/// Builds a [`TaskExecutor`](crate::runtime::TaskExecutor) with configurable
/// stages and expressions.
///
/// `new()` wires the default implementation ([`DefaultExecutorBuilder`] with a
/// [`DefaultPhysicalExprFactory`]); the `with_*` methods override specific parts
/// for customization.
pub struct TaskExecutorBuilder {
    builder: Arc<dyn ExecutorBuilder>,
}

impl TaskExecutorBuilder {
    /// Build a task executor with the default stages and expression factory.
    pub fn new() -> Self {
        let expr_factory = Arc::new(DefaultPhysicalExprFactory::new());
        Self {
            builder: Arc::new(DefaultExecutorBuilder::new(expr_factory)),
        }
    }

    /// Override the expression factory used for filter predicates.
    pub fn with_expr_factory(mut self, expr_factory: Arc<dyn PhysicalExprFactory>) -> Self {
        self.builder = Arc::new(DefaultExecutorBuilder::new(expr_factory));
        self
    }

    /// Override the executor builder (which maps the wire protocol to stages)
    /// entirely.
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

/// The default executor builder: builds the concrete stages that implement
/// the seam traits from a wire task description.
pub struct DefaultExecutorBuilder {
    expr_factory: Arc<dyn PhysicalExprFactory>,
}

impl DefaultExecutorBuilder {
    /// Create a builder that uses `expr_factory` to turn filter predicates
    /// (`Operator::Filter{expr}`) into [`PhysicalExpr`]s.
    pub fn new(expr_factory: Arc<dyn PhysicalExprFactory>) -> Self {
        Self { expr_factory }
    }
}

impl ExecutorBuilder for DefaultExecutorBuilder {
    fn build_source(&self, source: &DataSource) -> Result<Box<dyn Source<DataChunk>>> {
        match source {
            DataSource::Memory => Ok(Box::new(MemoryExecutor::dummy())),
            DataSource::File(scan) => {
                // TODO(storage): pick the OpenDAL service from the scan URI scheme
                // (e.g. s3://, fs://) instead of always mounting local filesystem.
                use opendal::services::Fs;
                let mut builder = Fs::default();
                builder.root("/");
                let op = opendal::Operator::new(builder)
                    .map_err(|e| VeinError::internal(format!("build fs operator: {e}")))?
                    .finish();
                Ok(Box::new(ScanExecutor::new(op, scan.files.clone(), scan.format.clone())))
            }
            DataSource::Exchange(_) => Err(VeinError::internal(
                "Exchange source is not yet implemented",
            )),
            DataSource::Stream(_) => Err(VeinError::internal(
                "Stream source is not yet implemented",
            )),
        }
    }

    fn build_transform(
        &self,
        op: &Operator,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Result<Box<dyn Transform<DataChunk>>> {
        match op {
            Operator::Filter(pred) => {
                let predicate = self.expr_factory.build(pred)?;
                Ok(Box::new(FilterExecutor::new(predicate, next)))
            }
            Operator::Project { columns } => {
                // TODO(schema): derive the output schema from the source instead
                // of an empty schema (ProjectExecutor does not read it today).
                let schema = Arc::new(Schema::empty());
                let exprs: Vec<Box<dyn PhysicalExpr>> = columns
                    .iter()
                    .map(|&i| {
                        let name = format!("col_{i}");
                        Box::new(ColumnExpr::new(&name, i as usize)) as Box<dyn PhysicalExpr>
                    })
                    .collect();
                Ok(Box::new(ProjectExecutor::new(exprs, schema, next)))
            }
            other => Err(VeinError::internal(format!(
                "operator not yet implemented: {other:?}"
            ))),
        }
    }

    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunk>>> {
        match sink {
            DataSink::Discard => Ok(Box::new(DiscardExecutor::new())),
            DataSink::Client => Ok(Box::new(CollectExecutor::new())),
            DataSink::Shuffle(_) => Err(VeinError::internal(
                "Shuffle sink is not yet implemented",
            )),
            DataSink::Storage(_) => Err(VeinError::internal(
                "Storage sink is not yet implemented",
            )),
        }
    }
}