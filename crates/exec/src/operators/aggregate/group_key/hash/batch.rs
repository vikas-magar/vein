use std::hash::{Hash, Hasher};

use arrow::array::Array;
use common::arrow::SelectionVector;

use super::super::GroupKeyColumn;
use super::nullable::hash_nullable_column;

impl GroupKeyColumn {
    pub(in crate::operators::aggregate) fn hash_dictionary_ids<H: Hasher>(
        &self,
        column: u32,
        dictionary_ids: &[usize],
        hashers: &mut [H],
    ) {
        let Self::DictionaryUtf8(_, values) = self else {
            unreachable!("dictionary IDs are hashed only for dictionary key columns");
        };
        for (&dictionary_id, hasher) in dictionary_ids.iter().zip(hashers) {
            column.hash(hasher);
            let value = (dictionary_id < values.len() && !values.is_null(dictionary_id))
                .then(|| values.value(dictionary_id));
            value.hash(hasher);
        }
    }

    pub(in crate::operators::aggregate) fn hash_batch<H: Hasher>(
        &self,
        column: u32,
        selection: &SelectionVector,
        grouping_set_ids: &[usize],
        grouping_set_count: usize,
        hashers: &mut [H],
    ) {
        match self {
            Self::Date32(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| values.value(row), hashers,
            ),
            Self::Int8(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| i64::from(values.value(row)), hashers,
            ),
            Self::Int16(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| i64::from(values.value(row)), hashers,
            ),
            Self::Int32(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| i64::from(values.value(row)), hashers,
            ),
            Self::Int64(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| values.value(row), hashers,
            ),
            Self::UInt8(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| u64::from(values.value(row)), hashers,
            ),
            Self::UInt16(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| u64::from(values.value(row)), hashers,
            ),
            Self::UInt32(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| u64::from(values.value(row)), hashers,
            ),
            Self::UInt64(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| values.value(row), hashers,
            ),
            Self::Decimal128(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| values.value(row), hashers,
            ),
            Self::Decimal256(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| values.value(row), hashers,
            ),
            Self::Utf8(values) => hash_nullable_column(
                column, selection, grouping_set_ids, grouping_set_count,
                values.nulls(), |row| values.value(row), hashers,
            ),
            Self::DictionaryUtf8(keys, values) => {
                for logical_row in 0..selection.size() {
                    let row = selection.get_index(logical_row);
                    for &set_id in grouping_set_ids {
                        let hasher = &mut hashers[logical_row * grouping_set_count + set_id];
                        column.hash(hasher);
                        let value = if keys.keys().is_null(row) {
                            None
                        } else {
                            let value_index = keys.keys().value(row) as usize;
                            (!values.is_null(value_index)).then(|| values.value(value_index))
                        };
                        value.hash(hasher);
                    }
                }
            }
            Self::View(_) => {
                for logical_row in 0..selection.size() {
                    let row = selection.get_index(logical_row);
                    for &set_id in grouping_set_ids {
                        let hasher = &mut hashers[logical_row * grouping_set_count + set_id];
                        column.hash(hasher);
                        super::view::hash_value(self, row, hasher);
                    }
                }
            }
        }
    }
}
