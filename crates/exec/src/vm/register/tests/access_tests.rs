use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, new_null_array};
use arrow::datatypes::DataType;

use crate::vm::register::RegisterFile;

#[test]
fn dense_projection_broadcasts_scalar_values_and_typed_nulls() {
    let mut registers = RegisterFile::new(2);
    let value: ArrayRef = Arc::new(Int64Array::from(vec![17]));
    let null = new_null_array(&DataType::Boolean, 1);
    registers.set_scalar(0, Arc::clone(&value)).unwrap();
    registers.set_scalar(1, null).unwrap();

    let projected = registers.project_dense(&[0, 1], 3).unwrap();
    let values = projected[0].as_any().downcast_ref::<Int64Array>().unwrap();
    let nulls = projected[1].as_any().downcast_ref::<BooleanArray>().unwrap();

    assert_eq!(values.values(), &[17, 17, 17]);
    assert_eq!(nulls.len(), 3);
    assert_eq!(nulls.null_count(), 3);
    assert_eq!(projected[1].data_type(), &DataType::Boolean);
}

#[test]
fn dense_projection_keeps_one_row_scalars_shared_and_handles_empty_input() {
    let mut registers = RegisterFile::new(1);
    let value: ArrayRef = Arc::new(Int64Array::from(vec![29]));
    registers.set_scalar(0, Arc::clone(&value)).unwrap();

    let one_row = registers.project_dense(&[0], 1).unwrap();
    assert!(Arc::ptr_eq(&one_row[0], &value));

    let empty = registers.project_dense(&[0], 0).unwrap();
    assert_eq!(empty[0].len(), 0);
    assert_eq!(empty[0].data_type(), &DataType::Int64);
}
