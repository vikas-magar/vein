//! Arrow `RecordBatch` adapters for external readers and writers.
//!
//! Pipeline contracts use [`DataChunkView`]. This module is the explicit
//! boundary for Arrow APIs that require a materialized `RecordBatch`.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;

use super::view::{DataChunkView, MaterializationReport};

/// Move Arrow array handles from a reader batch into a reference-backed view.
/// The column buffers are shared; only view and schema metadata are created.
pub fn from_record_batch(batch: RecordBatch) -> Result<DataChunkView> {
    let schema = batch.schema();
    let (_, arrays, row_count) = batch.into_parts();
    DataChunkView::from_arrays(arrays, row_count, SelectionVector::new_dense(row_count))?
        .with_schema(schema)
}

/// Wrap a borrowed reader batch when an Arrow callback retains ownership.
///
/// This clones only `ArrayRef` handles; the Arrow buffers remain shared.
pub fn from_record_batch_ref(batch: &RecordBatch) -> Result<DataChunkView> {
    let schema = batch.schema();
    let arrays = batch.columns().to_vec();
    let row_count = batch.num_rows();
    DataChunkView::from_arrays(arrays, row_count, SelectionVector::new_dense(row_count))?
        .with_schema(schema)
}

/// Convert a view only when an Arrow writer requires a `RecordBatch`.
///
/// Contiguous columns retain their buffers; sparse row mappings are gathered
/// once here and reported to the caller.
pub fn to_record_batch_with_report(
    view: &DataChunkView,
    schema: SchemaRef,
) -> Result<(RecordBatch, MaterializationReport)> {
    view.validate_schema(&schema)?;
    let (columns, report) = view.materialize_columns_with_report()?;
    let options = RecordBatchOptions::new().with_row_count(Some(view.num_rows()));
    let batch = RecordBatch::try_new_with_options(schema, columns, &options)
        .map_err(|error| VeinError::internal(format!("view result batch failed: {error}")))?;
    Ok((batch, report))
}
