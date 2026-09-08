use std::any::Any;

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};
use error::{Result, VeinError};

use super::PhysicalExpr;

/// A physical expression that casts its inner expression to a new type.
#[derive(Debug)]
pub struct CastExpr {
    expr: Box<dyn PhysicalExpr>,
    cast_type: DataType,
}

impl CastExpr {
    /// Creates a new CastExpr.
    pub fn new(expr: Box<dyn PhysicalExpr>, cast_type: DataType) -> Self {
        Self { expr, cast_type }
    }
}

impl PhysicalExpr for CastExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn is_deterministic(&self) -> bool {
        self.expr.is_deterministic()
    }

    fn evaluate(&self, chunk: &crate::daemon::pipeline::DataChunk) -> Result<ArrayRef> {
        let array = self.expr.evaluate(chunk)?;
        let casted = cast(&array, &self.cast_type)
            .map_err(|e| VeinError::internal(format!("Cast failed: {}", e)))?;
        Ok(casted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::pipeline::DataChunk;
    use crate::expr::column::ColumnExpr;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_cast_expr() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let col_expr = Box::new(ColumnExpr::new("a", 0));
        let cast_expr = CastExpr::new(col_expr, DataType::Float64);

        assert_eq!(cast_expr.data_type(&schema).unwrap(), DataType::Float64);

        let array1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let chunk = DataChunk::from_vectors(vec![array1 as ArrayRef]);
        let result = cast_expr.evaluate(&chunk).unwrap();
        assert_eq!(result.data_type(), &DataType::Float64);

        let result_float = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result_float.value(0), 1.0);
        assert_eq!(result_float.value(1), 2.0);
        assert_eq!(result_float.value(2), 3.0);
    }
}
