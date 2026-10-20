//! Format-neutral contract for reading values from a columnar execution view.

use arrow::array::Array;
use arrow::datatypes::DataType;

use crate::Result;

/// Reads active logical rows without gathering or retaining column payloads.
///
/// Execution representations implement this contract so statistics algorithms
/// remain independent of storage formats and batch container types.
pub trait StatisticsInput {
    /// Number of active logical rows.
    fn num_rows(&self) -> usize;

    /// Number of logical columns.
    fn num_columns(&self) -> usize;

    /// Physical Arrow type of one logical column.
    fn column_type(&self, column: usize) -> Result<&DataType>;

    /// Source value for a logical row. `None` represents a NULL extension.
    fn value(&self, column: usize, row: usize) -> Result<Option<(&dyn Array, usize)>>;
}

#[cfg(test)]
mod tests_support {
    use super::*;
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;
    use crate::arrow::SelectionVector;

    /// Arrow-backed fixture used only to test the format-neutral statistics API.
    pub struct OwnedSelectedRecordBatch {
        batch: RecordBatch,
        selection: Option<SelectionVector>,
    }

    impl OwnedSelectedRecordBatch {
        pub fn new(batch: RecordBatch, selection: Option<SelectionVector>) -> Self {
            Self { batch, selection }
        }

        pub fn schema(&self) -> SchemaRef {
            self.batch.schema()
        }

        pub fn retained_bytes(&self) -> usize {
            self.batch.get_array_memory_size()
        }
    }

    impl StatisticsInput for OwnedSelectedRecordBatch {
        fn num_rows(&self) -> usize {
            self.selection
                .as_ref()
                .map_or_else(|| self.batch.num_rows(), SelectionVector::size)
        }

        fn num_columns(&self) -> usize {
            self.batch.num_columns()
        }

        fn column_type(&self, column: usize) -> Result<&DataType> {
            self.batch
                .columns()
                .get(column)
                .map(|array| array.data_type())
                .ok_or_else(|| crate::VeinError::internal("statistics input column is out of bounds"))
        }

        fn value(&self, column: usize, row: usize) -> Result<Option<(&dyn Array, usize)>> {
            if row >= self.num_rows() {
                return Err(crate::VeinError::internal("statistics input row is out of bounds"));
            }
            let array = self.batch.columns().get(column).ok_or_else(|| {
                crate::VeinError::internal("statistics input column is out of bounds")
            })?;
            let physical_row = self
                .selection
                .as_ref()
                .map_or(row, |selection| selection.get_index(row));
            if physical_row >= array.len() {
                return Err(crate::VeinError::internal(
                    "statistics selection is outside the source batch",
                ));
            }
            Ok(Some((array.as_ref(), physical_row)))
        }
    }

    impl StatisticsInput for RecordBatch {
        fn num_rows(&self) -> usize {
            RecordBatch::num_rows(self)
        }

        fn num_columns(&self) -> usize {
            RecordBatch::num_columns(self)
        }

        fn column_type(&self, column: usize) -> Result<&DataType> {
            self.columns()
                .get(column)
                .map(|array| array.data_type())
                .ok_or_else(|| crate::VeinError::internal("statistics input column is out of bounds"))
        }

        fn value(&self, column: usize, row: usize) -> Result<Option<(&dyn Array, usize)>> {
            if row >= self.num_rows() {
                return Err(crate::VeinError::internal("statistics input row is out of bounds"));
            }
            self.columns()
                .get(column)
                .map(|array| Some((array.as_ref(), row)))
                .ok_or_else(|| crate::VeinError::internal("statistics input column is out of bounds"))
        }
    }
}

#[cfg(test)]
pub(crate) use tests_support::OwnedSelectedRecordBatch;
