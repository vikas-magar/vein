//! Build output key arrays from retained group-key references.

use super::*;

/// One key column, built from every group.
pub(in crate::operators::aggregate) fn key_column(keys: &[KeyValueRef<'_>], data_type: &VeinDataType) -> Result<ArrayRef> {
    Ok(match data_type {
        VeinDataType::Int64 => {
            let values: Vec<Option<i64>> = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Int64(value) => Some(*value),
                    KeyValueRef::Null => None,
                    KeyValueRef::UInt64(_) | KeyValueRef::Utf8(_) => {
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
        VeinDataType::Utf8 => {
            let values: Vec<Option<&str>> = keys
                .iter()
                .map(|key| match key {
                    KeyValueRef::Utf8(value) => Some(*value),
                    KeyValueRef::Null => None,
                    KeyValueRef::Int64(_) | KeyValueRef::UInt64(_) => {
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

fn unsigned_key_values<T>(keys: &[KeyValueRef<'_>], convert: impl Fn(u64) -> T) -> Vec<Option<T>> {
    keys.iter()
        .map(|key| match key {
            KeyValueRef::UInt64(value) => Some(convert(*value)),
            KeyValueRef::Null => None,
            KeyValueRef::Int64(_) | KeyValueRef::Utf8(_) => {
                unreachable!("an unsigned key column holds unsigned keys")
            }
        })
        .collect()
}
