use arrow::array::new_empty_array;
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::super::{ScalarFunction, FunctionInput, UdfOutput};

#[derive(Debug)]
pub(super) struct VectorDistanceFunction;

impl ScalarFunction for VectorDistanceFunction {
    fn name(&self) -> &str {
        "vector_distance"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 || !arg_types.iter().all(is_float32_vector) {
            return Err(VeinError::planning(
                "VECTOR_DISTANCE expects two Float32 vector arguments",
                None,
            ));
        }
        Ok(DataType::Float64)
    }

    fn invoke(
        &self,
        args: &[FunctionInput<'_>],
        rows: usize,
        return_type: &DataType,
    ) -> Result<UdfOutput> {
        let [lhs, rhs] = args else {
            return Err(VeinError::internal("VECTOR_DISTANCE expects two arguments"));
        };
        if rows == 0 {
            return Ok(UdfOutput::from_array(new_empty_array(return_type)));
        }
        let scalar = lhs.is_scalar() && rhs.is_scalar();
        let evaluation_rows = if scalar { 1 } else { rows };
        let output = crate::expression::kernels::eval_cosine_distance_values(
            lhs.expression_value(),
            rhs.expression_value(),
            evaluation_rows,
        )?;
        if scalar {
            UdfOutput::from_scalar(output)
        } else {
            Ok(UdfOutput::from_array(output))
        }
    }
}

fn is_float32_vector(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::FixedSizeList(field, _) => {
            field.data_type() == &DataType::Float32
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;

    use super::*;
    use crate::expression::value::ExpressionValue;
    use crate::udf::test_support::invoke_values;

    fn list_vectors(rows: Vec<Vec<f32>>) -> ArrayRef {
        let mut values = Vec::new();
        let mut lengths = Vec::with_capacity(rows.len());
        for row in rows {
            lengths.push(row.len());
            values.extend(row);
        }
        let values = Arc::new(Float32Array::from(values));
        let field = Arc::new(Field::new("item", DataType::Float32, false));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths(lengths),
            values,
            None,
        ))
    }

    #[test]
    fn invokes_shared_cosine_kernel_with_float64_result() {
        let lhs = list_vectors(vec![vec![1.0, 0.0], vec![1.0, 0.0]]);
        let rhs = list_vectors(vec![vec![0.0, 1.0], vec![1.0, 0.0]]);
        let result = crate::udf::test_support::invoke_dense(
            &VectorDistanceFunction,
            vec![lhs, rhs],
        )
            .unwrap();
        let distances = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(distances.value(0), 1.0);
        assert_eq!(distances.value(1), 0.0);
    }

    #[test]
    fn broadcasts_scalar_vectors_without_expanding_the_scalar_result() {
        let lhs = ExpressionValue::Scalar(list_vectors(vec![vec![1.0, 0.0]]));
        let rhs = ExpressionValue::Array(list_vectors(vec![vec![0.0, 1.0], vec![1.0, 0.0]]));
        let result = invoke_values(&VectorDistanceFunction, &[lhs, rhs], 2)
            .expect("scalar vector should broadcast over rows");
        let distances = result
            .array()
            .expect("column result")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 result");

        assert_eq!(distances.value(0), 1.0);
        assert_eq!(distances.value(1), 0.0);
    }

    #[test]
    fn scalar_vector_inputs_return_one_broadcast_result() {
        let lhs = ExpressionValue::Scalar(list_vectors(vec![vec![1.0, 0.0]]));
        let rhs = ExpressionValue::Scalar(list_vectors(vec![vec![0.0, 1.0]]));
        let result = invoke_values(&VectorDistanceFunction, &[lhs, rhs], 4)
            .expect("scalar vector inputs should produce one scalar result");

        assert!(result.is_scalar());
        assert_eq!(result.physical_len(), 1);
    }

    #[test]
    fn validates_two_float32_vector_arguments() {
        let vector_type = DataType::List(Arc::new(Field::new("item", DataType::Float32, false)));
        assert_eq!(
            VectorDistanceFunction
                .return_type(&[vector_type.clone(), vector_type])
                .unwrap(),
            DataType::Float64
        );
        let fixed_size_vector_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3);
        assert!(
            VectorDistanceFunction
                .return_type(&[fixed_size_vector_type.clone(), fixed_size_vector_type])
                .is_ok()
        );
        assert!(VectorDistanceFunction.return_type(&[]).is_err());
        assert!(
            VectorDistanceFunction
                .return_type(&[DataType::Float32, DataType::Float32])
                .is_err()
        );
        assert!(crate::udf::test_support::invoke_dense(&VectorDistanceFunction, vec![]).is_err());
    }
}
