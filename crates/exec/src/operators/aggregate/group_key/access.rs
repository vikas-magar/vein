//! Typed array selection and dictionary accessors for group keys.

use super::*;

impl GroupKeyColumn {
    pub(in crate::operators::aggregate) fn len(&self) -> usize {
        match self {
            Self::Date32(values) => values.len(),
            Self::Int8(values) => values.len(),
            Self::Int16(values) => values.len(),
            Self::Int32(values) => values.len(),
            Self::Int64(values) => values.len(),
            Self::UInt8(values) => values.len(),
            Self::UInt16(values) => values.len(),
            Self::UInt32(values) => values.len(),
            Self::UInt64(values) => values.len(),
            Self::Decimal128(values) => values.len(),
            Self::Decimal256(values) => values.len(),
            Self::Utf8(values) => values.len(),
            Self::DictionaryUtf8(keys, _) => keys.len(),
            Self::View(view) => view.num_rows(),
        }
    }

    pub(in crate::operators::aggregate) fn source_arrays(&self) -> Result<Vec<ArrayRef>> {
        match self {
            Self::View(view) => Ok(view.source_arrays_for_column(0)?.to_vec()),
            _ => Ok(vec![self.array_ref()]),
        }
    }

    /// Resolves a logical key row to its retained source array without gathering
    /// the value. Null-extended view rows have no source array.
    pub(in crate::operators::aggregate) fn source_row(
        &self,
        row: usize,
    ) -> Result<Option<(ArrayRef, usize)>> {
        if row >= self.len() {
            return Err(VeinError::internal("aggregate key row is outside its key column"));
        }
        match self {
            Self::View(view) => Ok(view
                .resolve(0, row)?
                .map(|(array, source_row)| (Arc::clone(array), source_row))),
            _ => Ok(Some((self.array_ref(), row))),
        }
    }

    pub(in crate::operators::aggregate) fn from_view(view: DataChunkView) -> Result<Self> {
        if view.num_columns() != 1 {
            return Err(VeinError::internal(format!(
                "view-backed group key expects one column, got {}",
                view.num_columns()
            )));
        }
        let data_type = view.column_data_type(0)?;
        if !super::view::supports(data_type) {
            return Err(VeinError::internal(format!(
                "grouping by {data_type:?} is not supported yet"
            )));
        }
        Ok(Self::View(view))
    }

    pub(in crate::operators::aggregate) fn new(array: &ArrayRef) -> Result<Self> {
        let invalid_type = || {
            VeinError::internal(format!(
                "grouping by {:?} is not supported yet",
                array.data_type()
            ))
        };
        if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
            return Ok(Self::Date32(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
            return Ok(Self::Int8(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
            return Ok(Self::Int16(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
            return Ok(Self::Int32(values.clone()));
        }
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
        if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
            return Ok(Self::Decimal128(values.clone()));
        }
        if let Some(values) = array.as_any().downcast_ref::<Decimal256Array>() {
            return Ok(Self::Decimal256(values.clone()));
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
            Self::Date32(values) => Arc::new(values.clone()),
            Self::Int8(values) => Arc::new(values.clone()),
            Self::Int16(values) => Arc::new(values.clone()),
            Self::Int32(values) => Arc::new(values.clone()),
            Self::Int64(values) => Arc::new(values.clone()),
            Self::UInt8(values) => Arc::new(values.clone()),
            Self::UInt16(values) => Arc::new(values.clone()),
            Self::UInt32(values) => Arc::new(values.clone()),
            Self::UInt64(values) => Arc::new(values.clone()),
            Self::Decimal128(values) => Arc::new(values.clone()),
            Self::Decimal256(values) => Arc::new(values.clone()),
            Self::Utf8(values) => Arc::new(values.clone()),
            Self::DictionaryUtf8(keys, _) => Arc::new(keys.clone()),
            Self::View(_) => unreachable!("view-backed keys retain their source arrays directly"),
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
