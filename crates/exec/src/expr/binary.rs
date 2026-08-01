use std::any::Any;

use arrow::array::ArrayRef;
use arrow::compute::kernels::{boolean, cmp, numeric};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use error::{Result, VeinError};

use super::PhysicalExpr;

/// Mathematical and logical binary operators for physical execution.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    /// Addition (`+`)
    Plus,
    /// Subtraction (`-`)
    Minus,
    /// Multiplication (`*`)
    Multiply,
    /// Division (`/`)
    Divide,
    /// Equality (`=`)
    Eq,
    /// Inequality (`!=`)
    Neq,
    /// Less than (`<`)
    Lt,
    /// Greater than (`>`)
    Gt,
    /// Less than or equal to (`<=`)
    Leq,
    /// Greater than or equal to (`>=`)
    Geq,
    /// Logical AND (`AND`)
    And,
    /// Logical OR (`OR`)
    Or,
}

/// A physical expression that applies a binary operator to two child expressions.
#[derive(Debug)]
pub struct BinaryExpr {
    left: Box<dyn PhysicalExpr>,
    op: BinaryOperator,
    right: Box<dyn PhysicalExpr>,
}

impl BinaryExpr {
    /// Creates a new BinaryExpr.
    pub fn new(
        left: Box<dyn PhysicalExpr>,
        op: BinaryOperator,
        right: Box<dyn PhysicalExpr>,
    ) -> Self {
        Self { left, op, right }
    }
}

impl PhysicalExpr for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let left_type = self.left.data_type(input_schema)?;
        let right_type = self.right.data_type(input_schema)?;

        // In a real system, the physical planner ensures types are coerced to be equal
        // before creating a BinaryExpr. We enforce this here.
        if left_type != right_type {
            return Err(VeinError::internal(format!(
                "Binary expression left and right types must match. Got {} and {}",
                left_type, right_type
            )));
        }

        match self.op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => Ok(left_type),
            BinaryOperator::Eq
            | BinaryOperator::Neq
            | BinaryOperator::Lt
            | BinaryOperator::Gt
            | BinaryOperator::Leq
            | BinaryOperator::Geq
            | BinaryOperator::And
            | BinaryOperator::Or => Ok(DataType::Boolean),
        }
    }

    fn is_deterministic(&self) -> bool {
        self.left.is_deterministic() && self.right.is_deterministic()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let left_array = self.left.evaluate(batch)?;
        let right_array = self.right.evaluate(batch)?;

        let result =
            match self.op {
                BinaryOperator::Plus => numeric::add(&left_array, &right_array),
                BinaryOperator::Minus => numeric::sub(&left_array, &right_array),
                BinaryOperator::Multiply => numeric::mul(&left_array, &right_array),
                BinaryOperator::Divide => numeric::div(&left_array, &right_array),
                BinaryOperator::Eq => {
                    cmp::eq(&left_array, &right_array).map(|a| std::sync::Arc::new(a) as ArrayRef)
                }
                BinaryOperator::Neq => {
                    cmp::neq(&left_array, &right_array).map(|a| std::sync::Arc::new(a) as ArrayRef)
                }
                BinaryOperator::Lt => {
                    cmp::lt(&left_array, &right_array).map(|a| std::sync::Arc::new(a) as ArrayRef)
                }
                BinaryOperator::Gt => {
                    cmp::gt(&left_array, &right_array).map(|a| std::sync::Arc::new(a) as ArrayRef)
                }
                BinaryOperator::Leq => cmp::lt_eq(&left_array, &right_array)
                    .map(|a| std::sync::Arc::new(a) as ArrayRef),
                BinaryOperator::Geq => cmp::gt_eq(&left_array, &right_array)
                    .map(|a| std::sync::Arc::new(a) as ArrayRef),
                BinaryOperator::And => {
                    let left_bool = left_array
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .ok_or_else(|| VeinError::internal("AND requires boolean arrays"))?;
                    let right_bool = right_array
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .ok_or_else(|| VeinError::internal("AND requires boolean arrays"))?;
                    boolean::and(left_bool, right_bool).map(|a| std::sync::Arc::new(a) as ArrayRef)
                }
                BinaryOperator::Or => {
                    let left_bool = left_array
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .ok_or_else(|| VeinError::internal("OR requires boolean arrays"))?;
                    let right_bool = right_array
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .ok_or_else(|| VeinError::internal("OR requires boolean arrays"))?;
                    boolean::or(left_bool, right_bool).map(|a| std::sync::Arc::new(a) as ArrayRef)
                }
            };

        result
            .map_err(|e| VeinError::internal(format!("Binary expression evaluation failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::column::ColumnExpr;
    use crate::expr::literal::{LiteralExpr, LiteralValue};
    use arrow::array::{BooleanArray, Int64Array};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn test_binary_expr_plus() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let array1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array1 as ArrayRef]).unwrap();

        let left = Box::new(ColumnExpr::new("a", 0));
        let right = Box::new(LiteralExpr::new(LiteralValue::Int64(10)));
        let expr = BinaryExpr::new(left, BinaryOperator::Plus, right);

        let result = expr.evaluate(&batch).unwrap();
        let result_int = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_int.value(0), 11);
        assert_eq!(result_int.value(1), 12);
        assert_eq!(result_int.value(2), 13);
    }

    #[test]
    fn test_binary_expr_gt() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let array1 = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array1 as ArrayRef]).unwrap();

        let left = Box::new(ColumnExpr::new("a", 0));
        let right = Box::new(LiteralExpr::new(LiteralValue::Int64(2)));
        let expr = BinaryExpr::new(left, BinaryOperator::Gt, right);

        let result = expr.evaluate(&batch).unwrap();
        let result_bool = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_bool.value(0), false);
        assert_eq!(result_bool.value(1), false);
        assert_eq!(result_bool.value(2), true);
    }
}
