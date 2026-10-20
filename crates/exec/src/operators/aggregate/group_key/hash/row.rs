use std::hash::{Hash, Hasher};

use arrow::array::Array;

use super::super::GroupKeyColumn;

impl GroupKeyColumn {
    /// Hash one retained key with the same typed, NULL-aware contract as a batch.
    /// Spill partitioning uses this for keys decoded from intermediate runs.
    pub(in crate::operators::aggregate) fn hash_row<H: Hasher>(
        &self,
        column: u32,
        row: usize,
        hasher: &mut H,
    ) {
        column.hash(hasher);
        match self {
            Self::Date32(values) => hash_optional(values.is_null(row), || values.value(row), hasher),
            Self::Int8(values) => hash_optional(values.is_null(row), || i64::from(values.value(row)), hasher),
            Self::Int16(values) => hash_optional(values.is_null(row), || i64::from(values.value(row)), hasher),
            Self::Int32(values) => hash_optional(values.is_null(row), || i64::from(values.value(row)), hasher),
            Self::Int64(values) => hash_optional(values.is_null(row), || values.value(row), hasher),
            Self::UInt8(values) => hash_optional(values.is_null(row), || u64::from(values.value(row)), hasher),
            Self::UInt16(values) => hash_optional(values.is_null(row), || u64::from(values.value(row)), hasher),
            Self::UInt32(values) => hash_optional(values.is_null(row), || u64::from(values.value(row)), hasher),
            Self::UInt64(values) => hash_optional(values.is_null(row), || values.value(row), hasher),
            Self::Decimal128(values) => hash_optional(values.is_null(row), || values.value(row), hasher),
            Self::Decimal256(values) => hash_optional(values.is_null(row), || values.value(row), hasher),
            Self::Utf8(values) => hash_optional(values.is_null(row), || values.value(row), hasher),
            Self::DictionaryUtf8(keys, values) => {
                let value_index = (!keys.keys().is_null(row)).then(|| keys.keys().value(row) as usize);
                let value = value_index.filter(|index| !values.is_null(*index));
                value.map(|index| values.value(index)).hash(hasher);
            }
            Self::View(_) => super::view::hash_value(self, row, hasher),
        }
    }
}

fn hash_optional<T: Hash, H: Hasher>(is_null: bool, value: impl FnOnce() -> T, hasher: &mut H) {
    if is_null {
        Option::<T>::None.hash(hasher);
    } else {
        Some(value()).hash(hasher);
    }
}
