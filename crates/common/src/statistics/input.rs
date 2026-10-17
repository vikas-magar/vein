//! Borrowed input contract shared by materialized batches and mapped views.

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::Result;

/// Reads logical values without requiring contiguous or newly gathered arrays.
/// Implementations resolve selections and NULL extensions before returning a
/// borrowed array and its physical row. No column payload is retained here.
pub trait StatisticsInput {
    /// Number of active logical rows.
    fn num_rows(&self) -> usize;
    /// Number of logical columns.
    fn num_columns(&self) -> usize;
    /// Physical Arrow type of one column, including empty columns.
    fn column_type(&self, column: usize) -> Result<&DataType>;
    /// Source value for a logical row; `None` represents a NULL extension.
    fn value(&self, column: usize, row: usize) -> Result<Option<(&dyn Array, usize)>>;
}

impl StatisticsInput for RecordBatch {
    fn num_rows(&self) -> usize { RecordBatch::num_rows(self) }
    fn num_columns(&self) -> usize { RecordBatch::num_columns(self) }
    fn column_type(&self, column: usize) -> Result<&DataType> {
        self.columns().get(column).map(|array| array.data_type())
            .ok_or_else(|| crate::VeinError::internal("statistics input column is out of bounds"))
    }
    fn value(&self, column: usize, row: usize) -> Result<Option<(&dyn Array, usize)>> {
        if row >= self.num_rows() {
            return Err(crate::VeinError::internal("statistics input row is out of bounds"));
        }
        self.columns().get(column).map(|array| Some((array.as_ref(), row)))
            .ok_or_else(|| crate::VeinError::internal("statistics input column is out of bounds"))
    }
}
