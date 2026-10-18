use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};

use super::{DataChunk, HashTable, JoinKeys};

fn chunk(values: &[Option<i32>]) -> DataChunk {
    DataChunk::from_vectors(vec![Arc::new(Int32Array::from(values.to_vec())) as ArrayRef])
}

#[test]
fn int32_join_keys_match_without_widening_the_arrow_column() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(chunk(&[Some(2), Some(7), None])).expect("build int32 keys");

    let mut matches = Vec::new();
    table
        .probe(&chunk(&[Some(7), Some(9), None]), &mut matches)
        .expect("probe int32 keys");

    assert_eq!(matches, [(0, (0, 1))]);
    let retained = table.build_chunk(0).resolve(0, 0).expect("resolve retained key").unwrap().0;
    assert_eq!(retained.data_type(), &arrow::datatypes::DataType::Int32);
}
