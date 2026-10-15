use std::time::Instant;

use arrow::record_batch::RecordBatch;
use common::Result;
use common::protocol::{InputMaterialization, OutputRepresentation, PhysicalPlanOperator};
use error::VeinError;

use crate::pipeline::{DataChunk, DataChunkView, MaterializationReport, PipelineNode, Sink};
use crate::trace::MaterializationTrace;
use crate::operators::sink::WriteReceipt;

/// Explicit array boundary for blocking operators and RecordBatch result sinks.
/// Reports conversion work once per flush, inside the owning task's trace span.
pub(crate) struct MaterializeViewNode<
    N: PipelineNode<DataChunk> + ?Sized = dyn PipelineNode<DataChunk>,
> {
    pub(super) next: Box<N>,
    pub(super) boundary: &'static str,
    pub(super) report: MaterializationReport,
    pub(super) batches: u64,
    pub(super) rows: u64,
    pub(super) conversion_us: u64,
    pub(super) trace: Option<MaterializationTrace>,
}

impl<N: PipelineNode<DataChunk> + ?Sized> MaterializeViewNode<N> {
    /// Only whole-batch consumers may install this adapter. Deriving the policy
    /// prevents runtime wiring from silently bypassing an operator's contract.
    pub(super) fn for_operator(next: Box<N>, operator: &PhysicalPlanOperator) -> Result<Self> {
        let contract = operator.data_flow();
        if contract.input_materialization != InputMaterialization::WholeBatch {
            return Err(VeinError::planning(
                format!(
                    "operator '{}' does not require whole-batch materialization",
                    contract.operator
                ),
                None,
            ));
        }
        let boundary = if contract.output == OutputRepresentation::Sink {
            "result"
        } else {
            contract.operator
        };
        Ok(Self::new(next, boundary))
    }

    pub(crate) fn new(next: Box<N>, boundary: &'static str) -> Self {
        Self {
            next,
            boundary,
            report: MaterializationReport::default(),
            batches: 0,
            rows: 0,
            conversion_us: 0,
            trace: None,
        }
    }

    pub(super) fn with_trace(mut self, trace: Option<MaterializationTrace>) -> Self {
        self.trace = trace;
        self
    }
}

impl<N: PipelineNode<DataChunk> + ?Sized> PipelineNode<DataChunkView> for MaterializeViewNode<N> {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        let started = Instant::now();
        let (chunk, report) = view.to_data_chunk_with_report()?;
        let conversion_elapsed = started.elapsed();
        self.conversion_us = self
            .conversion_us
            .saturating_add(u64::try_from(conversion_elapsed.as_micros()).unwrap_or(u64::MAX));
        self.report.accumulate(report);
        self.batches = self.batches.saturating_add(1);
        self.rows = self.rows.saturating_add(view.num_rows() as u64);
        if let Some(trace) = &self.trace {
            trace.record(view.num_rows(), report, conversion_elapsed);
        }
        self.next.push(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()?;
        tracing::info!(
            target: "vein.audit",
            event = "view_materialization",
            query_id = self.trace.as_ref().map(MaterializationTrace::query_id).unwrap_or(""),
            task_id = self.trace.as_ref().map(MaterializationTrace::task_id).unwrap_or(""),
            node_id = self.trace.as_ref().map(MaterializationTrace::node_id).unwrap_or(u32::MAX),
            boundary = self.boundary,
            batches = self.batches,
            rows = self.rows,
            shared_columns = self.report.shared_columns,
            gathered_columns = self.report.gathered_columns,
            empty_columns = self.report.empty_columns,
            gathered_output_buffer_bytes = self.report.gathered_output_buffer_bytes,
            conversion_us = self.conversion_us,
            "view_materialization"
        );
        Ok(())
    }

    fn finished(&self) -> bool {
        self.next.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        let Some(next) = self.next.fork_local()? else {
            return Ok(None);
        };
        let local = MaterializeViewNode::<dyn PipelineNode<DataChunk>>::new(next, self.boundary)
            .with_trace(self.trace.clone());
        Ok(Some(Box::new(local)))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<MaterializeViewNode<dyn PipelineNode<DataChunk>>>()
            .ok_or_else(|| {
                VeinError::internal("materialization local state has a mismatched target")
            })?;
        if self.boundary != target.boundary {
            return Err(VeinError::internal(
                "cannot combine materialization nodes with different boundaries",
            ));
        }
        self.next.combine_local(target.next.as_mut())?;
        target.report.accumulate(self.report);
        target.batches = target.batches.saturating_add(self.batches);
        target.rows = target.rows.saturating_add(self.rows);
        target.conversion_us = target.conversion_us.saturating_add(self.conversion_us);
        Ok(())
    }
}

impl<N: Sink<DataChunk> + ?Sized> Sink<DataChunkView> for MaterializeViewNode<N> {
    fn collect(&mut self) -> Vec<RecordBatch> {
        self.next.collect()
    }

    fn row_count(&self) -> u64 {
        self.next.row_count()
    }

    fn write_receipt(&self) -> Option<&WriteReceipt> {
        self.next.write_receipt()
    }
}
