use std::cmp::Ordering;
use std::mem::size_of;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use error::VeinError;
use types::{HashJoinKeyType, LogicalLiteral, VeinDataType};

use common::Result;

use crate::operators::join::hash_table::fnv1a;

#[derive(Debug, Clone)]
pub(super) enum KeyArray {
    Int32(Vec<Int32Array>),
    Int64(Vec<Int64Array>),
    Utf8(Vec<StringArray>),
}

impl KeyArray {
    pub(super) fn resolve(column: usize, arrays: &[ArrayRef]) -> Result<Self> {
        let first = arrays.first().ok_or_else(|| {
            VeinError::internal(format!("join key column {column} has no source arrays"))
        })?;
        let data_type = VeinDataType::try_from(first.data_type()).map_err(|error| {
            VeinError::internal(format!(
                "join key column {column} has unsupported Arrow type {:?}: {error}",
                first.data_type()
            ))
        })?;
        match data_type.hash_join_key_type() {
            Some(HashJoinKeyType::Int32) => {
                Ok(Self::Int32(downcast_sources(column, "Int32", arrays)?))
            }
            Some(HashJoinKeyType::Int64) => {
                Ok(Self::Int64(downcast_sources(column, "Int64", arrays)?))
            }
            Some(HashJoinKeyType::Utf8) => {
                Ok(Self::Utf8(downcast_sources(column, "Utf8", arrays)?))
            }
            None => Err(VeinError::internal(format!(
                "join key column {column} has type {:?}, which is not supported by hash join",
                first.data_type()
            ))),
        }
    }

    pub(super) fn is_single_i64(&self) -> bool {
        matches!(self, Self::Int64(_))
    }

    pub(super) fn is_null(&self, source: usize, row: usize) -> Result<bool> {
        Ok(match self {
            Self::Int32(arrays) => get(arrays, source)?.is_null(row),
            Self::Int64(arrays) => get(arrays, source)?.is_null(row),
            Self::Utf8(arrays) => get(arrays, source)?.is_null(row),
        })
    }

    pub(super) fn hash_into(&self, source: usize, row: usize, hash: u64) -> Result<Option<u64>> {
        if self.is_null(source, row)? {
            return Ok(None);
        }
        Ok(Some(match self {
            Self::Int32(arrays) => fnv1a(&get(arrays, source)?.value(row).to_ne_bytes(), hash),
            Self::Int64(arrays) => fnv1a(&get(arrays, source)?.value(row).to_ne_bytes(), hash),
            Self::Utf8(arrays) => fnv1a(get(arrays, source)?.value(row).as_bytes(), hash),
        }))
    }

    pub(super) fn int64_value(&self, source: usize, row: usize) -> Result<Option<i64>> {
        match self {
            Self::Int64(arrays) => {
                let array = get(arrays, source)?;
                Ok((!array.is_null(row)).then(|| array.value(row)))
            }
            _ => Err(VeinError::internal("requested Int64 value from a non-Int64 join key")),
        }
    }

    pub(super) fn value_eq(
        &self,
        source: usize,
        row: usize,
        other: &Self,
        other_source: usize,
        other_row: usize,
    ) -> Result<bool> {
        if self.is_null(source, row)? || other.is_null(other_source, other_row)? {
            return Ok(false);
        }
        Ok(match (self, other) {
            (Self::Int32(left), Self::Int32(right)) => {
                get(left, source)?.value(row) == get(right, other_source)?.value(other_row)
            }
            (Self::Int64(left), Self::Int64(right)) => {
                get(left, source)?.value(row) == get(right, other_source)?.value(other_row)
            }
            (Self::Utf8(left), Self::Utf8(right)) => {
                get(left, source)?.value(row) == get(right, other_source)?.value(other_row)
            }
            _ => false,
        })
    }

    pub(super) fn compare_values(
        &self,
        source: usize,
        row: usize,
        other: &Self,
        other_source: usize,
        other_row: usize,
    ) -> Result<Option<Ordering>> {
        if self.is_null(source, row)? || other.is_null(other_source, other_row)? {
            return Ok(None);
        }
        Ok(match (self, other) {
            (Self::Int32(left), Self::Int32(right)) => Some(
                get(left, source)?.value(row).cmp(&get(right, other_source)?.value(other_row)),
            ),
            (Self::Int64(left), Self::Int64(right)) => Some(
                get(left, source)?.value(row).cmp(&get(right, other_source)?.value(other_row)),
            ),
            (Self::Utf8(left), Self::Utf8(right)) => Some(
                get(left, source)?.value(row).cmp(get(right, other_source)?.value(other_row)),
            ),
            _ => None,
        })
    }

    pub(super) fn logical_literal(&self, source: usize, row: usize) -> Result<LogicalLiteral> {
        match self {
            Self::Int32(arrays) => Ok(LogicalLiteral::Int64(i64::from(get(arrays, source)?.value(row)))),
            Self::Int64(arrays) => Ok(LogicalLiteral::Int64(get(arrays, source)?.value(row))),
            Self::Utf8(arrays) => Ok(LogicalLiteral::String(get(arrays, source)?.value(row).to_owned())),
        }
    }

    pub(super) fn same_type(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Int32(_), Self::Int32(_))
                | (Self::Int64(_), Self::Int64(_))
                | (Self::Utf8(_), Self::Utf8(_))
        )
    }

    pub(super) fn metadata_bytes(&self) -> usize {
        match self {
            Self::Int32(arrays) => arrays.capacity().saturating_mul(size_of::<Int32Array>()),
            Self::Int64(arrays) => arrays.capacity().saturating_mul(size_of::<Int64Array>()),
            Self::Utf8(arrays) => arrays.capacity().saturating_mul(size_of::<StringArray>()),
        }
    }
}

fn downcast_sources<T: Array + Clone + 'static>(
    column: usize,
    expected: &str,
    arrays: &[ArrayRef],
) -> Result<Vec<T>> {
    arrays
        .iter()
        .enumerate()
        .map(|(source, array)| {
            array
                .as_any()
                .downcast_ref::<T>()
                .cloned()
                .ok_or_else(|| {
                    VeinError::internal(format!(
                        "join key column {column} source {source} does not match its declared {expected} type"
                    ))
                })
        })
        .collect()
}

fn get<T>(arrays: &[T], source: usize) -> Result<&T> {
    arrays.get(source).ok_or_else(|| {
        VeinError::internal(format!("join key source array {source} is out of bounds"))
    })
}
