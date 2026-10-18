//! Build output key arrays from retained group-key references.

use super::*;

/// One key column, built from every group.
pub(in crate::operators::aggregate) fn key_column(keys: &[KeyValueRef<'_>], data_type: &VeinDataType) -> Result<ArrayRef> {
    Ok(match data_type {
        VeinDataType::Date32 => {
            let values: Vec<Option<i32>> = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Date32(value) => Some(*value),
                    KeyValueRef::Null => None,
                    KeyValueRef::Int64(_)
                    | KeyValueRef::UInt64(_)
                    | KeyValueRef::Utf8(_)
                    | KeyValueRef::Decimal128(_)
                    | KeyValueRef::Decimal256(_) => {
                        unreachable!("a date key column holds date keys")
                    }
                })
                .collect();
            Arc::new(Date32Array::from(values))
        }
        VeinDataType::Int8 => Arc::new(Int8Array::from(signed_key_values(keys, "Int8", i8::try_from)?)),
        VeinDataType::Int16 => Arc::new(Int16Array::from(signed_key_values(keys, "Int16", i16::try_from)?)),
        VeinDataType::Int32 => Arc::new(Int32Array::from(signed_key_values(keys, "Int32", i32::try_from)?)),
        VeinDataType::Int64 => {
            let values: Vec<Option<i64>> = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Int64(value) => Some(*value),
                    KeyValueRef::Null => None,
                    KeyValueRef::Date32(_)
                    | KeyValueRef::UInt64(_)
                    | KeyValueRef::Utf8(_)
                    | KeyValueRef::Decimal128(_)
                    | KeyValueRef::Decimal256(_) => {
                        unreachable!("an int64 key column holds int64 keys")
                    }
                })
                .collect();
            Arc::new(Int64Array::from(values))
        }
        VeinDataType::UInt8 => Arc::new(UInt8Array::from(unsigned_key_values(keys, |value| {
            value as u8
        }))),
        VeinDataType::UInt16 => Arc::new(UInt16Array::from(unsigned_key_values(keys, |value| {
            value as u16
        }))),
        VeinDataType::UInt32 => Arc::new(UInt32Array::from(unsigned_key_values(keys, |value| {
            value as u32
        }))),
        VeinDataType::UInt64 => {
            Arc::new(UInt64Array::from(unsigned_key_values(keys, |value| value)))
        }
        VeinDataType::Decimal128(precision, scale) => {
            let values = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Decimal128(value) => Some(*value),
                    KeyValueRef::Null => None,
                    _ => unreachable!("a Decimal128 key column holds Decimal128 keys"),
                })
                .collect::<Vec<_>>();
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| VeinError::internal(format!("invalid Decimal128 group key type: {error}")))?,
            )
        }
        VeinDataType::Decimal256(precision, scale) => {
            let values = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Decimal256(value) => Some(*value),
                    KeyValueRef::Null => None,
                    _ => unreachable!("a Decimal256 key column holds Decimal256 keys"),
                })
                .collect::<Vec<_>>();
            Arc::new(
                Decimal256Array::from(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| VeinError::internal(format!("invalid Decimal256 group key type: {error}")))?,
            )
        }
        VeinDataType::Utf8 => {
            let values: Vec<Option<&str>> = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Utf8(value) => Some(*value),
                    KeyValueRef::Null => None,
                    KeyValueRef::Date32(_) | KeyValueRef::Int64(_) | KeyValueRef::UInt64(_)
                    | KeyValueRef::Decimal128(_) | KeyValueRef::Decimal256(_) => {
                        unreachable!("a string key column holds string keys")
                    }
                })
                .collect();
            Arc::new(StringArray::from(values))
        }
        other => {
            return Err(VeinError::internal(format!(
                "grouping by {other:?} is not supported yet"
            )));
        }
    })
}

fn signed_key_values<T, E>(
    keys: &[KeyValueRef<'_>],
    type_name: &str,
    convert: impl Fn(i64) -> std::result::Result<T, E>,
) -> Result<Vec<Option<T>>>
where
    E: std::fmt::Display,
{
    keys.iter()
        .map(|key| match key {
            KeyValueRef::Int64(value) => convert(*value)
                .map(Some)
                .map_err(|error| VeinError::internal(format!("{type_name} group key is out of range: {error}"))),
            KeyValueRef::Null => Ok(None),
            _ => unreachable!("a signed integer key column holds signed integer keys"),
        })
        .collect()
}

fn unsigned_key_values<T>(keys: &[KeyValueRef<'_>], convert: impl Fn(u64) -> T) -> Vec<Option<T>> {
    keys.iter()
        .map(|key| match key {
            KeyValueRef::UInt64(value) => Some(convert(*value)),
            KeyValueRef::Null => None,
            KeyValueRef::Date32(_)
            | KeyValueRef::Int64(_)
            | KeyValueRef::Utf8(_)
            | KeyValueRef::Decimal128(_)
            | KeyValueRef::Decimal256(_) => {
                unreachable!("an unsigned key column holds unsigned keys")
            }
        })
        .collect()
}
