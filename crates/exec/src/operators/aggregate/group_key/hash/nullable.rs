use std::hash::{Hash, Hasher};

use arrow::buffer::NullBuffer;
use common::arrow::SelectionVector;

pub(super) fn hash_nullable_column<T: Hash, H: Hasher>(
    column: u32,
    selection: &SelectionVector,
    grouping_set_ids: &[usize],
    grouping_set_count: usize,
    validity: Option<&NullBuffer>,
    value_at: impl Fn(usize) -> T,
    hashers: &mut [H],
) {
    for logical_row in 0..selection.size() {
        let row = selection.get_index(logical_row);
        for &set_id in grouping_set_ids {
            let hasher = &mut hashers[logical_row * grouping_set_count + set_id];
            column.hash(hasher);
            if validity.is_some_and(|nulls| !nulls.is_valid(row)) {
                Option::<T>::None.hash(hasher);
            } else {
                Some(value_at(row)).hash(hasher);
            }
        }
    }
}
