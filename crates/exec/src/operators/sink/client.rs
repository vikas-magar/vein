//! Client result boundary for Arrow batches and reference-backed views.

use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode, Sink};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::Result;
use error::VeinError;
use std::time::Instant;

/// A result batch that retains its source arrays and row mappings.
///
/// The schema is validated when the batch is created. Consumers may resolve
/// values through the view or explicitly request an Arrow result later.
#[derive(Debug, Clone)]
pub struct ViewResultBatch {
    schema: SchemaRef,
    view: DataChunkView,
}

impl ViewResultBatch {
    /// Validates the schema and retains the view without gathering any columns.
    pub fn new(schema: SchemaRef, view: DataChunkView) -> Result<Self> {
        view.validate_schema(&schema)?;
        Ok(Self { schema, view })
    }

    /// The planned schema, including names, types, nullability, and metadata.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Borrow the reference-backed result.
    pub fn view(&self) -> &DataChunkView {
        &self.view
    }

    /// Number of selected result rows.
    pub fn num_rows(&self) -> usize {
        self.view.num_rows()
    }

    /// Returns a logical row window while retaining the same source arrays.
    ///
    /// The operation only changes selection metadata. It does not gather or
    /// copy column values, which lets presentation layers materialize a bounded
    /// prefix instead of converting an entire result batch.
    pub fn slice_rows(&self, offset: usize, limit: usize) -> Result<Self> {
        let selection = self.view.selection().slice(offset, limit);
        let view = self.view.with_selection(selection)?;
        Ok(Self {
            schema: self.schema.clone(),
            view,
        })
    }

    /// Take the reference-backed result without converting its arrays.
    pub fn into_view(self) -> DataChunkView {
        self.view
    }

    /// Explicitly convert this result to Arrow, reporting actual conversion work.
    pub fn to_record_batch_with_report(&self) -> Result<(RecordBatch, MaterializationReport)> {
        self.view.to_record_batch_with_report(self.schema.clone())
    }
}

/// Selects the representation produced by a client result boundary.
///
/// A format owns the conversion policy; collection, streaming, accounting, and
/// pipeline execution are shared. Implementations must validate the supplied
/// schema and report only conversions actually performed.
pub trait ClientOutput: Send + Sync + 'static + Sized {
    /// Stable format label for client boundary diagnostics.
    const FORMAT: &'static str;

    /// Build one result from an owned view and the immutable planned schema.
    fn from_view(view: DataChunkView, schema: SchemaRef) -> Result<(Self, MaterializationReport)>;

    /// Number of selected rows represented by this result.
    fn num_rows(&self) -> usize;
}

impl ClientOutput for RecordBatch {
    const FORMAT: &'static str = "record_batch";

    fn from_view(view: DataChunkView, schema: SchemaRef) -> Result<(Self, MaterializationReport)> {
        view.to_record_batch_with_report(schema)
    }

    fn num_rows(&self) -> usize {
        RecordBatch::num_rows(self)
    }
}

impl ClientOutput for ViewResultBatch {
    const FORMAT: &'static str = "view";

    fn from_view(view: DataChunkView, schema: SchemaRef) -> Result<(Self, MaterializationReport)> {
        Ok((Self::new(schema, view)?, MaterializationReport::default()))
    }

    fn num_rows(&self) -> usize {
        ViewResultBatch::num_rows(self)
    }
}

/// Captures or streams client results in the requested representation.
///
/// Views remain reference-backed up to this node. Arrow results use one explicit
/// conversion; view results retain the input without any payload conversion.
pub struct CollectSink<O = RecordBatch> {
    output: Vec<O>,
    output_sender: Option<flume::Sender<O>>,
    rows_emitted: u64,
    schema: SchemaRef,
    chunks_in: u64,
    chunks_skipped: u64,
    batches_out: u64,
    materialize_us: u64,
    report: MaterializationReport,
}

impl<O: ClientOutput> CollectSink<O> {
    /// Creates a result collector with the immutable planned Arrow schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            output: Vec::new(),
            output_sender: None,
            rows_emitted: 0,
            schema,
            chunks_in: 0,
            chunks_skipped: 0,
            batches_out: 0,
            materialize_us: 0,
            report: MaterializationReport::default(),
        }
    }

    /// Streams results through the supplied bounded channel without retaining them.
    pub fn with_sender(schema: SchemaRef, output_sender: flume::Sender<O>) -> Self {
        Self {
            output_sender: Some(output_sender),
            ..Self::new(schema)
        }
    }

    /// Accumulated view conversions at this result boundary.
    pub fn materialization_report(&self) -> MaterializationReport {
        self.report
    }
}

impl<O: ClientOutput> PipelineNode<DataChunkView> for CollectSink<O> {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.chunks_in = self.chunks_in.saturating_add(1);
        if view.is_empty() {
            // Empty input is still subject to the fixed schema contract.
            view.validate_schema(&self.schema)?;
            self.chunks_skipped = self.chunks_skipped.saturating_add(1);
            return Ok(());
        }

        let started = Instant::now();
        let (batch, report) = O::from_view(view, self.schema.clone())?;
        self.materialize_us = self
            .materialize_us
            .saturating_add(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        self.report.accumulate(report);
        let rows = batch.num_rows() as u64;
        let rows_emitted = self
            .rows_emitted
            .checked_add(rows)
            .ok_or_else(|| VeinError::internal("client row count overflow"))?;
        if let Some(sender) = &self.output_sender {
            sender
                .send(batch)
                .map_err(|e| VeinError::internal(format!("client result channel closed: {e}")))?;
        } else {
            self.output.push(batch);
        }
        self.rows_emitted = rows_emitted;
        self.batches_out = self.batches_out.saturating_add(1);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        tracing::info!(
            target: "vein.audit",
            event = "client_sink_finished",
            result_format = O::FORMAT,
            chunks_in = self.chunks_in,
            chunks_skipped = self.chunks_skipped,
            batches_out = self.batches_out,
            rows = self.rows_emitted,
            materialize_us = self.materialize_us,
            shared_columns = self.report.shared_columns,
            gathered_columns = self.report.gathered_columns,
            empty_columns = self.report.empty_columns,
            gathered_output_buffer_bytes = self.report.gathered_output_buffer_bytes,
            "client_sink_finished"
        );
        Ok(())
    }
}

impl<O: ClientOutput> Sink<DataChunkView, O> for CollectSink<O> {
    fn collect(&mut self) -> Vec<O> {
        std::mem::take(&mut self.output)
    }

    fn row_count(&self) -> u64 {
        self.rows_emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{DataChunk, ViewColumnRef, ViewInputRows};
    use arrow::array::{Array, ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use common::arrow::SelectionVector;
    use std::sync::Arc;

    fn schema(nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            nullable,
        )]))
    }

    fn view(array: &ArrayRef, rows: Vec<u32>) -> DataChunkView {
        DataChunkView::from_chunk(DataChunk::new(
            vec![Arc::clone(array)],
            SelectionVector::new_sparse(rows),
        ))
            .unwrap()
    }

    #[test]
    fn client_formats_share_contiguous_payload_buffers_and_planned_schema() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let schema = schema(false);
        let input = view(&source, vec![1, 2]);
        let mut arrow = CollectSink::<RecordBatch>::new(Arc::clone(&schema));
        let mut references = CollectSink::<ViewResultBatch>::new(Arc::clone(&schema));
        arrow.push(input.clone()).unwrap();
        references.push(input).unwrap();
        arrow.flush().unwrap();
        references.flush().unwrap();
        let batches = arrow.collect();
        let views = references.collect();
        let source_values = source.as_any().downcast_ref::<Int64Array>().unwrap();
        let output_values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            output_values.values().as_ptr(),
            source_values.values().as_ptr().wrapping_add(1)
        );
        assert_eq!(output_values, &Int64Array::from(vec![20, 30]));
        assert!(Arc::ptr_eq(&batches[0].schema(), &schema));
        assert!(Arc::ptr_eq(views[0].schema(), &schema));
        assert!(!batches[0].schema().field(0).is_nullable());
        assert!(Arc::ptr_eq(
            views[0].view().resolve(0, 0).unwrap().unwrap().0,
            &source
        ));
        assert_eq!(arrow.materialization_report().shared_columns, 1);
        assert_eq!(arrow.materialization_report().gathered_columns, 0);
        assert_eq!(
            references.materialization_report(),
            MaterializationReport::default()
        );
        assert_eq!(arrow.row_count(), 2);
        assert_eq!(references.row_count(), 2);
        assert!(arrow.collect().is_empty());
        assert!(references.collect().is_empty());
    }

    #[test]
    fn sparse_repeated_and_null_extended_results_convert_only_at_arrow_boundary() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
        let input =
            DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&source)])).unwrap();
        let rows = vec![Some(2), None, Some(0), Some(2), Some(1)];
        let input = DataChunkView::combine(
            &[ViewInputRows {
                view: &input,
                rows: &rows,
            }],
            &[ViewColumnRef {
                input: 0,
                column: 0,
            }],
        )
            .unwrap();
        let mut arrow = CollectSink::<RecordBatch>::new(schema(true));
        let mut references = CollectSink::<ViewResultBatch>::new(schema(true));
        references.push(input.clone()).unwrap();
        let result = references.collect().pop().unwrap();
        assert_eq!(
            references.materialization_report(),
            MaterializationReport::default()
        );
        assert!(Arc::ptr_eq(
            result.view().resolve(0, 0).unwrap().unwrap().0,
            &source
        ));
        arrow.push(input).unwrap();
        let batch = arrow.collect().pop().unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![Some(30), None, Some(10), Some(30), None])
        );
        let report = arrow.materialization_report();
        assert_eq!(report.gathered_columns, 1);
        assert_eq!(report.shared_columns, 0);
        assert_eq!(
            report.gathered_output_buffer_bytes,
            batch.column(0).get_buffer_memory_size()
        );
        let (converted, explicit_report) = result.to_record_batch_with_report().unwrap();
        assert_eq!(converted, batch);
        assert_eq!(explicit_report, report);
    }

    #[test]
    fn result_formats_preserve_zero_column_row_counts_and_skip_empty_batches() {
        let schema = Arc::new(Schema::empty());
        let mut arrow = CollectSink::<RecordBatch>::new(Arc::clone(&schema));
        let mut references = CollectSink::<ViewResultBatch>::new(schema);
        for rows in [0, 3] {
            let input =
                DataChunkView::from_chunk(DataChunk::new(vec![], SelectionVector::new_dense(rows)))
                    .unwrap();
            arrow.push(input.clone()).unwrap();
            references.push(input).unwrap();
        }
        let batches = arrow.collect();
        let views = references.collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(views.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(views[0].num_rows(), 3);
        assert_eq!(views[0].view().num_columns(), 0);
        assert_eq!(
            views[0].to_record_batch_with_report().unwrap().0.num_rows(),
            3
        );
        assert_eq!(arrow.row_count(), 3);
        assert_eq!(references.row_count(), 3);
        assert_eq!(
            arrow.materialization_report(),
            MaterializationReport::default()
        );
    }

    fn assert_schema_errors<O: ClientOutput>() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(20)]));
        let mut sink = CollectSink::<O>::new(schema(false));
        // The NULL exists only in an excluded source row.
        sink.push(view(&source, vec![1])).unwrap();
        assert!(sink.push(view(&source, vec![0])).is_err());
        assert_eq!(sink.row_count(), 1);
        assert_eq!(sink.collect().len(), 1);
        let wrong_width =
            DataChunkView::from_chunk(DataChunk::new(vec![], SelectionVector::new_dense(2)))
                .unwrap();
        assert!(
            sink.push(wrong_width)
                .unwrap_err()
                .to_string()
                .contains("columns")
        );
        let mut wrong_type = CollectSink::<O>::new(Arc::new(Schema::new(vec![Field::new(
            "text",
            DataType::Utf8,
            true,
        )])));
        assert!(
            wrong_type
                .push(view(&source, vec![1]))
                .unwrap_err()
                .to_string()
                .contains("type")
        );
        assert!(wrong_type.push(view(&source, vec![])).is_err());
        assert_eq!(wrong_type.row_count(), 0);
    }

    #[test]
    fn both_formats_reject_schema_drift_and_selected_nonnullable_nulls() {
        assert_schema_errors::<RecordBatch>();
        assert_schema_errors::<ViewResultBatch>();
    }

    fn assert_streaming<O: ClientOutput>() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let (sender, receiver) = flume::bounded(1);
        let mut sink = CollectSink::<O>::with_sender(schema(false), sender);
        sink.push(view(&source, vec![2, 0])).unwrap();
        assert_eq!(receiver.recv().unwrap().num_rows(), 2);
        assert!(sink.collect().is_empty());
        assert_eq!(sink.row_count(), 2);
        drop(receiver);
        assert!(
            sink.push(view(&source, vec![1]))
                .unwrap_err()
                .to_string()
                .contains("client result channel closed")
        );
        assert_eq!(
            sink.row_count(),
            2,
            "failed sends are not counted as emitted rows"
        );
    }

    #[test]
    fn result_row_slice_retains_source_arrays_and_composes_logical_offsets() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
        let result = ViewResultBatch::new(schema(false), view(&source, vec![3, 1, 2, 0]))
            .expect("reference-backed result");

        let slice = result.slice_rows(1, 2).expect("slice result rows");
        assert_eq!(slice.num_rows(), 2);
        for (logical_row, source_row) in [(0, 1), (1, 2)] {
            let (array, row) = slice
                .view()
                .resolve(0, logical_row)
                .expect("resolve row")
                .expect("non-null source row");
            assert!(Arc::ptr_eq(array, &source));
            assert_eq!(row, source_row);
        }
        assert_eq!(result.num_rows(), 4, "slicing leaves the source result intact");
        assert_eq!(result.slice_rows(9, 2).unwrap().num_rows(), 0);
    }

    #[test]
    fn both_formats_stream_without_retention_and_propagate_channel_failure() {
        assert_streaming::<RecordBatch>();
        assert_streaming::<ViewResultBatch>();
    }
}
