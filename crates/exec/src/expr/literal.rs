use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use error::Result;

use super::PhysicalExpr;

/// Represents a scalar literal value.
#[derive(Debug, Clone)]
pub enum LiteralValue {
    /// A boolean literal.
    Boolean(bool),
    /// A 64-bit integer literal.
    Int64(i64),
    /// A 64-bit float literal.
    Float64(f64),
    /// A string literal.
    String(String),
}

impl LiteralValue {
    /// Get the Arrow DataType for this literal.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Boolean(_) => DataType::Boolean,
            Self::Int64(_) => DataType::Int64,
            Self::Float64(_) => DataType::Float64,
            Self::String(_) => DataType::Utf8,
        }
    }

    /// Convert the scalar into an Arrow array of length `len`.
    pub fn to_array(&self, len: usize) -> ArrayRef {
        match self {
            Self::Boolean(b) => Arc::new(BooleanArray::from(vec![*b; len])),
            Self::Int64(i) => Arc::new(Int64Array::from(vec![*i; len])),
            Self::Float64(f) => Arc::new(Float64Array::from(vec![*f; len])),
            Self::String(s) => Arc::new(StringArray::from(vec![s.as_str(); len])),
        }
    }
}

/// A physical expression representing a constant literal value.
#[derive(Debug, Clone)]
pub struct LiteralExpr {
    value: LiteralValue,
}

impl LiteralExpr {
    /// Creates a new LiteralExpr.
    pub fn new(value: LiteralValue) -> Self {
        Self { value }
    }
}

impl PhysicalExpr for LiteralExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.value.data_type())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        Ok(self.value.to_array(batch.num_rows()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_literal_expr() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let array1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array1 as ArrayRef]).unwrap();

        let int_expr = LiteralExpr::new(LiteralValue::Int64(42));
        assert_eq!(int_expr.data_type(&schema).unwrap(), DataType::Int64);
        
        let result = int_expr.evaluate(&batch).unwrap();
        let result_int = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_int.len(), 3);
        assert_eq!(result_int.value(0), 42);
        assert_eq!(result_int.value(1), 42);
        assert_eq!(result_int.value(2), 42);
    }
}

