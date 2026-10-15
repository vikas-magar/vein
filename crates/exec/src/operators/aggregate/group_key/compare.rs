//! Null-aware equality and borrowed key-value access.

use super::*;

impl GroupKeyColumn {
    pub(in crate::operators::aggregate) fn dictionary_ids_equal(&self, left: usize, right: usize) -> bool {
        self.dictionary_value(left) == self.dictionary_value(right)
    }

    pub(in crate::operators::aggregate) fn value(&self, row: usize) -> KeyValueRef<'_> {
        match self {
            Self::Int64(values) if values.is_null(row) => KeyValueRef::Null,
            Self::Int64(values) => KeyValueRef::Int64(values.value(row)),
            Self::UInt8(values) if values.is_null(row) => KeyValueRef::Null,
            Self::UInt8(values) => KeyValueRef::UInt64(values.value(row) as u64),
            Self::UInt16(values) if values.is_null(row) => KeyValueRef::Null,
            Self::UInt16(values) => KeyValueRef::UInt64(values.value(row) as u64),
            Self::UInt32(values) if values.is_null(row) => KeyValueRef::Null,
            Self::UInt32(values) => KeyValueRef::UInt64(values.value(row) as u64),
            Self::UInt64(values) if values.is_null(row) => KeyValueRef::Null,
            Self::UInt64(values) => KeyValueRef::UInt64(values.value(row)),
            Self::Utf8(values) if values.is_null(row) => KeyValueRef::Null,
            Self::Utf8(values) => KeyValueRef::Utf8(values.value(row)),
            Self::DictionaryUtf8(keys, _) if keys.keys().is_null(row) => KeyValueRef::Null,
            Self::DictionaryUtf8(keys, values) => {
                let value = keys.keys().value(row) as usize;
                if values.is_null(value) {
                    KeyValueRef::Null
                } else {
                    KeyValueRef::Utf8(values.value(value))
                }
            }
        }
    }

    pub(in crate::operators::aggregate) fn rows_equal(&self, left: usize, other: &Self, right: usize) -> bool {
        self.value(left) == other.value(right)
    }
}
