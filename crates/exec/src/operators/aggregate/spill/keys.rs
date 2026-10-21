use super::*;

pub(super) trait SpillKeyBuilder {
    fn append(&mut self, key: Option<&AggregateGroupKeyRef>) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<ArrayRef>;
}

struct PrimitiveKeyColumnBuilder<T: ArrowPrimitiveType>(PrimitiveBuilder<T>);

impl<T: ArrowPrimitiveType> SpillKeyBuilder for PrimitiveKeyColumnBuilder<T> {
    fn append(&mut self, key: Option<&AggregateGroupKeyRef>) -> Result<()> {
        let source = resolve_source_key(key)?;
        let Some((array, row)) = source.as_ref() else {
            self.0.append_null();
            return Ok(());
        };
        let values = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| key_type_error(array, &T::DATA_TYPE))?;
        if values.is_null(*row) {
            self.0.append_null();
        } else {
            self.0.append_value(values.value(*row));
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.0.finish()))
    }
}

struct Utf8KeyColumnBuilder {
    values: StringBuilder,
    payload_bytes: usize,
}

impl Utf8KeyColumnBuilder {
    fn append_value(&mut self, value: &str) -> Result<()> {
        self.payload_bytes = self
            .payload_bytes
            .checked_add(value.len())
            .filter(|&size| size <= i32::MAX as usize)
            .ok_or_else(|| {
                VeinError::internal("aggregate spill key exceeds Arrow UTF-8 offset capacity")
            })?;
        self.values.append_value(value);
        Ok(())
    }
}

impl SpillKeyBuilder for Utf8KeyColumnBuilder {
    fn append(&mut self, key: Option<&AggregateGroupKeyRef>) -> Result<()> {
        let source = resolve_source_key(key)?;
        let Some((array, row)) = source.as_ref() else {
            self.values.append_null();
            return Ok(());
        };
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            if values.is_null(*row) {
                self.values.append_null();
            } else {
                self.append_value(values.value(*row))?;
            }
            return Ok(());
        }
        if let Some(dictionary) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let keys = dictionary
                .keys()
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| key_type_error(array, &DataType::Utf8))?;
            if keys.is_null(*row) {
                self.values.append_null();
                return Ok(());
            }
            let value_index = usize::try_from(keys.value(*row))
                .map_err(|_| VeinError::internal("aggregate spill dictionary key is negative"))?;
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| key_type_error(array, &DataType::Utf8))?;
            if value_index >= values.len() {
                return Err(VeinError::internal(
                    "aggregate spill dictionary key is outside its values array",
                ));
            }
            if values.is_null(value_index) {
                self.values.append_null();
            } else {
                self.append_value(values.value(value_index))?;
            }
            return Ok(());
        }
        Err(key_type_error(array, &DataType::Utf8))
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.values.finish()))
    }
}

pub(super) fn key_builder(data_type: &VeinDataType, capacity: usize) -> Result<Box<dyn SpillKeyBuilder>> {
    macro_rules! primitive_builder {
    ($native:ty) => {
        Box::new(PrimitiveKeyColumnBuilder::<$native>(
            PrimitiveBuilder::with_capacity(capacity),
        )) as Box<dyn SpillKeyBuilder>
    };
}

    Ok(match data_type {
        VeinDataType::Date32 => primitive_builder!(arrow::datatypes::Date32Type),
        VeinDataType::Int8 => primitive_builder!(arrow::datatypes::Int8Type),
        VeinDataType::Int16 => primitive_builder!(arrow::datatypes::Int16Type),
        VeinDataType::Int32 => primitive_builder!(arrow::datatypes::Int32Type),
        VeinDataType::Int64 => primitive_builder!(arrow::datatypes::Int64Type),
        VeinDataType::UInt8 => primitive_builder!(arrow::datatypes::UInt8Type),
        VeinDataType::UInt16 => primitive_builder!(arrow::datatypes::UInt16Type),
        VeinDataType::UInt32 => primitive_builder!(arrow::datatypes::UInt32Type),
        VeinDataType::UInt64 => primitive_builder!(arrow::datatypes::UInt64Type),
        VeinDataType::Utf8 => Box::new(Utf8KeyColumnBuilder {
            values: StringBuilder::with_capacity(capacity, 0),
            payload_bytes: 0,
        }),
        VeinDataType::Decimal128(precision, scale) => {
            return decimal_key_builder::decimal128(capacity, *precision, *scale);
        }
        VeinDataType::Decimal256(precision, scale) => {
            return decimal_key_builder::decimal256(capacity, *precision, *scale);
        }
        unsupported => {
            return Err(VeinError::internal(format!(
                "aggregate spill does not support grouping key type {unsupported:?}"
            )));
        }
    })
}
