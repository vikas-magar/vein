use super::*;
use arrow::array::{Array, Float32Array, Float64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};

fn make_list_array(rows: Vec<Vec<f32>>) -> ArrayRef {
    let mut flat = Vec::new();
    let mut lengths = Vec::new();
    for r in rows {
        lengths.push(r.len());
        flat.extend(r);
    }
    let values = Arc::new(Float32Array::from(flat));
    let offsets = OffsetBuffer::from_lengths(lengths);
    let field = Arc::new(Field::new("item", DataType::Float32, false));
    Arc::new(ListArray::new(field, offsets, values, None))
}

#[test]
fn test_vector_distances() {
    // [1.0, 0.0] and [0.0, 1.0] -> Orthogonal vectors
    let vec1 = make_list_array(vec![vec![1.0, 0.0], vec![3.0, 4.0]]);
    let vec2 = make_list_array(vec![vec![0.0, 1.0], vec![3.0, 4.0]]);

    // Cosine distance of orthogonal is 1.0; identical is 0.0
    let cos = eval_cosine_distance(&vec1, &vec2).unwrap();
    let cos_arr = cos.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((cos_arr.value(0) - 1.0).abs() < 1e-6);
    assert!((cos_arr.value(1) - 0.0).abs() < 1e-6);

}

#[test]
fn cosine_distance_propagates_nulls_and_reads_sliced_vectors() {
    let lhs = make_list_array(vec![vec![8.0, 9.0], vec![1.0, 0.0], vec![0.0, 0.0]]);
    let rhs = make_list_array(vec![vec![8.0, 9.0], vec![0.0, 1.0], vec![1.0, 0.0]]);
    let sliced_lhs = lhs.slice(1, 2);
    let sliced_rhs = rhs.slice(1, 2);
    let result = eval_cosine_distance(&sliced_lhs, &sliced_rhs).unwrap();
    let distances = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(distances.value(0), 1.0);
    assert_eq!(distances.value(1), 0.0);

    let nullable_lhs = make_nullable_list_array(vec![Some(vec![1.0, 0.0]), None]);
    let rhs = make_list_array(vec![vec![1.0, 0.0], vec![1.0, 0.0]]);
    let result = eval_cosine_distance(&nullable_lhs, &rhs).unwrap();
    let distances = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(distances.value(0), 0.0);
    assert!(distances.is_null(1));

    let nullable_elements = Arc::new(Float32Array::from(vec![Some(1.0), None]));
    let nullable_field = Arc::new(Field::new("item", DataType::Float32, true));
    let nullable_vector = Arc::new(ListArray::new(
        nullable_field,
        OffsetBuffer::from_lengths(vec![2]),
        nullable_elements,
        None,
    )) as ArrayRef;
    let valid_vector = make_list_array(vec![vec![1.0, 0.0]]);
    let result = eval_cosine_distance(&nullable_vector, &valid_vector).unwrap();
    let distances = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(distances.is_null(0));

    let different_dimensions = make_list_array(vec![vec![1.0]]);
    assert!(eval_cosine_distance(&different_dimensions, &valid_vector).is_err());
    assert!(eval_cosine_distance(&valid_vector, &make_list_array(vec![])).is_err());
}

fn make_nullable_list_array(rows: Vec<Option<Vec<f32>>>) -> ArrayRef {
    let mut flat = Vec::new();
    let mut lengths = Vec::new();
    let mut nulls = Vec::new();
    for row in rows {
        match row {
            Some(values) => {
                lengths.push(values.len());
                flat.extend(values);
                nulls.push(true);
            }
            None => {
                lengths.push(0);
                nulls.push(false);
            }
        }
    }
    let values = Arc::new(Float32Array::from(flat));
    let offsets = OffsetBuffer::from_lengths(lengths);
    let field = Arc::new(Field::new("item", DataType::Float32, false));
    Arc::new(ListArray::new(field, offsets, values, Some(nulls.into())))
}
