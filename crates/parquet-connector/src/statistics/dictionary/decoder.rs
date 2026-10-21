//! Zero-copy decoding of supported PLAIN dictionary values.

use bytes::Bytes;
use types::LogicalLiteral;

use super::DictionaryPredicate;

pub(super) trait PlainDictionaryValue {
    type Value;

    fn decode(bytes: &Bytes, index: usize, type_length: i32, offset: &mut usize)
              -> Option<Self::Value>;
}

pub(super) struct BoolType;
pub(super) struct Int32Type;
pub(super) struct Int64Type;
pub(super) struct FloatType;
pub(super) struct DoubleType;
pub(super) struct ByteArrayType;
pub(super) struct FixedLenByteArrayType;

impl PlainDictionaryValue for BoolType {
    type Value = bool;

    fn decode(bytes: &Bytes, index: usize, _type_length: i32, _offset: &mut usize) -> Option<Self::Value> {
        let byte = *bytes.get(index / 8)?;
        Some(byte & (1 << (index % 8)) != 0)
    }
}

macro_rules! impl_fixed_dictionary_value {
    ($marker:ty, $value:ty, $width:literal, $decode:path) => {
        impl PlainDictionaryValue for $marker {
            type Value = $value;

            fn decode(bytes: &Bytes, index: usize, _type_length: i32, _offset: &mut usize) -> Option<Self::Value> {
                let start = index.checked_mul($width)?;
                let end = start.checked_add($width)?;
                let encoded: [u8; $width] = bytes.get(start..end)?.try_into().ok()?;
                Some($decode(encoded))
            }
        }
    };
}

impl_fixed_dictionary_value!(Int32Type, i32, 4, i32::from_le_bytes);
impl_fixed_dictionary_value!(Int64Type, i64, 8, i64::from_le_bytes);
impl_fixed_dictionary_value!(FloatType, f32, 4, f32::from_le_bytes);
impl_fixed_dictionary_value!(DoubleType, f64, 8, f64::from_le_bytes);

impl PlainDictionaryValue for ByteArrayType {
    type Value = Bytes;

    fn decode(bytes: &Bytes, _index: usize, _type_length: i32, offset: &mut usize) -> Option<Self::Value> {
        let length_end = (*offset).checked_add(4)?;
        let encoded_length: [u8; 4] = bytes.get(*offset..length_end)?.try_into().ok()?;
        let value_length = usize::try_from(u32::from_le_bytes(encoded_length)).ok()?;
        let value_end = length_end.checked_add(value_length)?;
        let value_range = length_end..value_end;
        bytes.get(value_range.clone())?;
        let value = bytes.slice(value_range);
        *offset = value_end;
        Some(value)
    }
}

impl PlainDictionaryValue for FixedLenByteArrayType {
    type Value = Bytes;

    fn decode(bytes: &Bytes, index: usize, type_length: i32, _offset: &mut usize) -> Option<Self::Value> {
        let width = usize::try_from(type_length).ok().filter(|width| *width > 0)?;
        let start = index.checked_mul(width)?;
        let end = start.checked_add(width)?;
        let value_range = start..end;
        bytes.get(value_range.clone())?;
        Some(bytes.slice(value_range))
    }
}

pub(super) fn decode_dictionary<T>(
    bytes: Bytes,
    value_count: usize,
    _type_length: i32,
    predicates: &[DictionaryPredicate],
    mut candidate_matches: impl FnMut(&T::Value, &LogicalLiteral) -> Option<bool>,
) -> Vec<Option<bool>>
where
    T: PlainDictionaryValue,
{
    let mut found = vec![false; predicates.len()];
    let mut supported = vec![true; predicates.len()];
    let mut offset = 0;
    for value_index in 0..value_count {
        if found.iter().zip(&supported).all(|(found, supported)| *found || !*supported) {
            break;
        }
        let Some(value) = T::decode(&bytes, value_index, _type_length, &mut offset) else {
            return vec![None; predicates.len()];
        };
        for (index, predicate) in predicates.iter().enumerate() {
            if found[index] || !supported[index] {
                continue;
            }
            for candidate in &predicate.candidates {
                match candidate_matches(&value, candidate) {
                    Some(true) => {
                        found[index] = true;
                        break;
                    }
                    Some(false) => {}
                    None => supported[index] = false,
                }
                if !supported[index] {
                    break;
                }
            }
        }
    }
    found
        .into_iter()
        .zip(supported)
        .map(|(found, supported)| {
            if found {
                Some(true)
            } else if supported {
                Some(false)
            } else {
                None
            }
        })
        .collect()
}
