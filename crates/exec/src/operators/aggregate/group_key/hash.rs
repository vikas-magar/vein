//! Hash selected typed keys without materializing row values.

use super::*;

impl GroupKeyColumn {
    pub(in crate::operators::aggregate) fn hash_dictionary_ids<H: Hasher>(
        &self,
        column: u32,
        ids: &[usize],
        hashers: &mut [H],
    ) {
        for (&dictionary_id, hasher) in ids.iter().zip(hashers) {
            column.hash(hasher);
            self.dictionary_value(dictionary_id).hash(hasher);
        }
    }

    /// Hash one retained key using the same typed and NULL-aware contract as
    /// `hash_batch`. Spill partitioning uses this after decoding keys from IPC,
    /// where dictionary UTF-8 values have already been normalized to strings.
    #[cfg(test)]
    pub(in crate::operators::aggregate) fn hash_row<H: Hasher>(&self, column: u32, row: usize, hasher: &mut H) {
        column.hash(hasher);
        match self {
            Self::Int64(values) => {
                if values.is_null(row) {
                    Option::<i64>::None.hash(hasher);
                } else {
                    Some(values.value(row)).hash(hasher);
                }
            }
            Self::UInt8(values) => {
                hash_optional_unsigned(values.is_null(row), u64::from(values.value(row)), hasher)
            }
            Self::UInt16(values) => {
                hash_optional_unsigned(values.is_null(row), u64::from(values.value(row)), hasher)
            }
            Self::UInt32(values) => {
                hash_optional_unsigned(values.is_null(row), u64::from(values.value(row)), hasher)
            }
            Self::UInt64(values) => {
                hash_optional_unsigned(values.is_null(row), values.value(row), hasher)
            }
            Self::Utf8(values) => {
                if values.is_null(row) {
                    Option::<&str>::None.hash(hasher);
                } else {
                    Some(values.value(row)).hash(hasher);
                }
            }
            Self::DictionaryUtf8(keys, values) => {
                if keys.keys().is_null(row) {
                    Option::<&str>::None.hash(hasher);
                } else {
                    let value_index = keys.keys().value(row) as usize;
                    if values.is_null(value_index) {
                        Option::<&str>::None.hash(hasher);
                    } else {
                        Some(values.value(value_index)).hash(hasher);
                    }
                }
            }
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
            Self::Int64(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| values.value(row),
                hashers,
            ),
            Self::UInt8(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| u64::from(values.value(row)),
                hashers,
            ),
            Self::UInt16(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| u64::from(values.value(row)),
                hashers,
            ),
            Self::UInt32(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| u64::from(values.value(row)),
                hashers,
            ),
            Self::UInt64(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| values.value(row),
                hashers,
            ),
            Self::Utf8(values) => hash_nullable_column(
                column,
                selection,
                grouping_set_ids,
                grouping_set_count,
                values.nulls(),
                |row| values.value(row),
                hashers,
            ),
            Self::DictionaryUtf8(keys, values) => {
                for logical_row in 0..selection.size() {
                    let row = selection.get_index(logical_row);
                    for &set_id in grouping_set_ids {
                        let hasher = &mut hashers[logical_row * grouping_set_count + set_id];
                        column.hash(hasher);
                        let value = if keys.is_null(row) {
                            None
                        } else {
                            let value_index = keys.keys().value(row) as usize;
                            (!values.is_null(value_index)).then(|| values.value(value_index))
                        };
                        value.hash(hasher);
                    }
                }
            }
        }
    }
}
fn hash_nullable_column<T: Hash, H: Hasher>(
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

#[cfg(test)]
fn hash_optional_unsigned<H: Hasher>(is_null: bool, value: u64, hasher: &mut H) {
    if is_null {
        Option::<u64>::None.hash(hasher);
    } else {
        Some(value).hash(hasher);
    }
}
