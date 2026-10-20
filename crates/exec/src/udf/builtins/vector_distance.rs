use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use error::{Result, VeinError};

use super::super::ScalarUDFImpl;

#[derive(Debug)]
pub(super) struct VectorDistanceFunction;

impl ScalarUDFImpl for VectorDistanceFunction {
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

    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [lhs, rhs] = args else {
            return Err(VeinError::internal(
                "VECTOR_DISTANCE expects two arguments",
            ));
        };
        crate::expression::kernels::eval_cosine_distance(lhs, rhs)
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

    use arrow::array::{Array, Float32Array, Float64Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;

    use super::*;

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
        let result = VectorDistanceFunction.invoke(&[lhs, rhs]).unwrap();
        let distances = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(distances.value(0), 1.0);
        assert_eq!(distances.value(1), 0.0);
    }

    #[test]
    fn validates_two_float32_vector_arguments() {
        let vector_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float32,
            false,
        )));
        assert_eq!(
            VectorDistanceFunction
                .return_type(&[vector_type.clone(), vector_type])
                .unwrap(),
            DataType::Float64
        );
        let fixed_size_vector_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            3,
        );
        assert!(VectorDistanceFunction
            .return_type(&[fixed_size_vector_type.clone(), fixed_size_vector_type])
            .is_ok());
        assert!(VectorDistanceFunction.return_type(&[]).is_err());
        assert!(VectorDistanceFunction.return_type(&[DataType::Float32, DataType::Float32]).is_err());
        assert!(VectorDistanceFunction.invoke(&[]).is_err());
    }
}
