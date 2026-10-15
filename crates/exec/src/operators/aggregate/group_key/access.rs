//! Typed array selection and dictionary accessors for group keys.

use super::*;

impl GroupKeyColumn {
    pub(in crate::operators::aggregate) fn new(array: &ArrayRef) -> Result<Self> {
        let invalid_type = || {
            VeinError::internal(format!(
                "grouping by {:?} is not supported yet",
                array.data_type()
            ))
        };
        if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            return Ok(Self::Int64(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
            return Ok(Self::UInt8(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
            return Ok(Self::UInt16(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
            return Ok(Self::UInt32(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
            return Ok(Self::UInt64(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Utf8(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let strings = values
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(invalid_type)?;
            return Ok(Self::DictionaryUtf8(values.clone(), strings.clone()));
        }
        Err(invalid_type())
    }

    pub(in crate::operators::aggregate) fn dictionary_cardinality(&self) -> Option<usize> {
        match self {
            Self::DictionaryUtf8(_, values) => Some(values.len()),
            _ => None,
        }
    }

    /// Clone the typed Arrow wrapper while sharing its underlying buffers.
    pub(in crate::operators::aggregate) fn array_ref(&self) -> ArrayRef {
        match self {
            Self::Int64(values) => Arc::new(values.clone()),
            Self::UInt8(values) => Arc::new(values.clone()),
            Self::UInt16(values) => Arc::new(values.clone()),
            Self::UInt32(values) => Arc::new(values.clone()),
            Self::UInt64(values) => Arc::new(values.clone()),
            Self::Utf8(values) => Arc::new(values.clone()),
            Self::DictionaryUtf8(keys, _) => Arc::new(keys.clone()),
        }
    }

    /// Returns a dictionary value index, using `cardinality` for a null key.
    pub(in crate::operators::aggregate) fn dictionary_id(&self, row: usize) -> Option<usize> {
        match self {
            Self::DictionaryUtf8(keys, values) => Some(if keys.keys().is_null(row) {
                values.len()
            } else {
                keys.keys().value(row) as usize
            }),
            _ => None,
        }
    }

    pub(in crate::operators::aggregate) fn dictionary_value(&self, dictionary_id: usize) -> Option<&str> {
        match self {
            Self::DictionaryUtf8(_, values) if dictionary_id < values.len() => {
                (!values.is_null(dictionary_id)).then(|| values.value(dictionary_id))
            }
            Self::DictionaryUtf8(_, values) if dictionary_id == values.len() => None,
            _ => unreachable!("dictionary values are requested only for dictionary keys"),
        }
    }
}
