use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use error::{Result, VeinError};
use std::any::Any;

use super::PhysicalExpr;

/// A physical expression that references a column by its index in the RecordBatch.
#[derive(Debug, Clone)]
pub struct ColumnExpr {
    /// The name of the column (primarily for debugging).
    #[allow(dead_code)]
    name: String,
    /// The physical index in the RecordBatch.
    index: usize,
}

impl ColumnExpr {
    /// Creates a new ColumnExpr.
    pub fn new(name: impl Into<String>, index: usize) -> Self {
        Self {
            name: name.into(),
            index,
        }
    }
}

impl PhysicalExpr for ColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        if self.index >= input_schema.fields().len() {
            return Err(VeinError::internal(format!(
                "Column index {} out of bounds for schema",
                self.index
            )));
        }
        Ok(input_schema.field(self.index).data_type().clone())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        if self.index >= batch.num_columns() {
            return Err(VeinError::internal(format!(
                "Column index {} out of bounds for batch with {} columns",
                self.index,
                batch.num_columns()
            )));
        }
        // Zero-copy clone of the Arc pointer
        Ok(batch.column(self.index).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_column_expr() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let array1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let array2 = Arc::new(StringArray::from(vec!["x", "y", "z"]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![array1 as ArrayRef, array2 as ArrayRef])
                .unwrap();

        let expr = ColumnExpr::new("b", 1);

        // Test data_type
        assert_eq!(expr.data_type(&schema).unwrap(), DataType::Utf8);

        // Test evaluate
        let result = expr.evaluate(&batch).unwrap();
        assert_eq!(result.data_type(), &DataType::Utf8);
        assert_eq!(result.len(), 3);

        let result_str = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_str.value(0), "x");
        assert_eq!(result_str.value(2), "z");
    }
}
