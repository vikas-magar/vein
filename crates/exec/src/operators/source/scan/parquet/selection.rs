//! Selection-preserving scan handoff and shallow Arrow projection.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use common::Result;
use error::VeinError;

use crate::pipeline::DataChunkView;

/// Projects only schema/array handles; Arrow payload buffers remain shared.
pub(super) fn project_batch(batch: &RecordBatch, columns: &[usize]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let fields = columns
        .iter()
        .map(|&index| schema.fields().get(index).cloned().ok_or_else(|| {
            VeinError::internal(format!("scan output column {index} is outside the read batch"))
        }))
        .collect::<Result<Vec<_>>>()?;
    let arrays = columns
        .iter()
        .map(|&index| batch.columns().get(index).cloned().ok_or_else(|| {
            VeinError::internal(format!("scan output column {index} is outside the read batch"))
        }))
        .collect::<Result<Vec<_>>>()?;
    let projected_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(projected_schema, arrays, &options)
        .map_err(|error| VeinError::internal(format!("scan output projection failed: {error}")))
}

pub(super) fn handoff_view(
    batch: RecordBatch,
    output_columns: &[usize],
) -> Result<DataChunkView> {
    Ok(DataChunkView::from_record_batch(batch)?.project(output_columns)?)
}

#[cfg(test)]
#[path = "selection_tests.rs"]
mod tests;
