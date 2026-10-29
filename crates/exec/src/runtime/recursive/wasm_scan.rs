use std::sync::Arc;

use common::Result;
use common::cancellation::CancellationToken;
use common::protocol::{DataSource, ExecuteTask, ExpressionProgram};
use common::statistics::QueryStatisticsContext;
use tracing::Instrument;

use crate::operators::source::scan::ScanWorkerState;
use crate::pipeline::PipelineNode;
use crate::runtime::builder::ExecutorBuilder;
use crate::runtime::filter::RuntimeFilterSet;
use crate::runtime::pipeline_adapters::{SuppressFlush, TransformHandle};

pub(super) async fn execute(
    builder: &dyn ExecutorBuilder,
    sources: Vec<DataSource>,
    program: &Arc<ExpressionProgram>,
    task: &ExecuteTask,
    cancel: &CancellationToken,
    runtime_filters: Option<Arc<RuntimeFilterSet>>,
    statistics: Arc<QueryStatisticsContext>,
    span: tracing::Span,
    next: Box<dyn PipelineNode>,
) -> Result<u64> {
    let worker_state = Arc::new(ScanWorkerState::default());
    let pipeline = if program.is_empty() {
        next
    } else {
        Box::new(TransformHandle(
            builder.build_expression_executor(program, next)?,
        ))
    };
    let mut pipeline = SuppressFlush::new(pipeline);
    let mut bytes_read = 0_u64;

    tracing::debug!(
        target: "vein.audit",
        event = "scan_fan_in_selected",
        mode = "browser_serial",
        work_unit_count = sources.len(),
        worker_count = 1,
        "browser scan processes work units on the event loop"
    );

    for source_spec in sources {
        if cancel.is_cancelled() {
            return Err(common::VeinError::cancelled());
        }
        let mut source = builder.build_source_with_query_context(
            &source_spec,
            task.task_id,
            task.attempt,
            runtime_filters.as_deref(),
            Some(Arc::clone(&statistics)),
            Arc::clone(&worker_state),
        )?;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(common::VeinError::cancelled()),
            result = source.execute(&mut pipeline).instrument(span.clone()) => result?,
        }
        bytes_read = bytes_read.saturating_add(source.bytes_read());
    }

    pipeline.finish().await?;
    Ok(bytes_read)
}
