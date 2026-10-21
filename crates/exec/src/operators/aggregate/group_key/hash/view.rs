//! Hash mapped keys with the same scalar hash contract as typed Arrow keys.

use std::hash::{Hash, Hasher};

use super::super::super::KeyValueRef;
use super::super::GroupKeyColumn;

pub(super) fn hash_value<H: Hasher>(column: &GroupKeyColumn, row: usize, hasher: &mut H) {
    match column.value(row) {
        KeyValueRef::Null => Option::<i64>::None.hash(hasher),
        KeyValueRef::Date32(value) => Some(value).hash(hasher),
        KeyValueRef::Int64(value) => Some(value).hash(hasher),
        KeyValueRef::UInt64(value) => Some(value).hash(hasher),
        KeyValueRef::Decimal128(value) => Some(value).hash(hasher),
        KeyValueRef::Decimal256(value) => Some(value).hash(hasher),
        KeyValueRef::Utf8(value) => Some(value).hash(hasher),
    }
}
