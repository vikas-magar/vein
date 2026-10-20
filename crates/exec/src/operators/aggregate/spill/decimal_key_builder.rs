//! Arrow IPC spill builders for fixed-precision decimal group keys.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use super::SpillKeyBuilder;

macro_rules! decimal_key_builder {
    ($builder:ident, $function:ident, $array:ty, $native:ty, $data_type:ident) => {
        struct $builder {
            values: Vec<Option<$native>>,
            precision: u8,
            scale: i8,
        }

        impl SpillKeyBuilder for $builder {
            fn append(&mut self, key: Option<&super::super::intermediate::AggregateGroupKeyRef>) -> Result<()> {
                let source = super::resolve_source_key(key)?;
                let Some((array, row)) = source.as_ref() else {
                    self.values.push(None);
                    return Ok(());
                };
                let expected = DataType::$data_type(self.precision, self.scale);
                let values = array
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(|| super::key_type_error(array, &expected))?;
                self.values.push((!values.is_null(*row)).then(|| values.value(*row)));
                Ok(())
            }

            fn finish(self: Box<Self>) -> Result<ArrayRef> {
                let array = <$array>::from(self.values)
                    .with_precision_and_scale(self.precision, self.scale)
                    .map_err(|error| {
                        VeinError::internal(format!("invalid decimal spill key type: {error}"))
                    })?;
                Ok(Arc::new(array))
            }
        }

        pub(super) fn $function(
            capacity: usize,
            precision: u8,
            scale: i8,
        ) -> Result<Box<dyn SpillKeyBuilder>> {
            let mut values = Vec::new();
            values.try_reserve_exact(capacity).map_err(|error| {
                VeinError::internal(format!("cannot reserve decimal spill keys: {error}"))
            })?;
            Ok(Box::new($builder {
                values,
                precision,
                scale,
            }))
        }
    };
}

decimal_key_builder!(
    Decimal128KeyColumnBuilder,
    decimal128,
    Decimal128Array,
    i128,
    Decimal128
);
decimal_key_builder!(
    Decimal256KeyColumnBuilder,
    decimal256,
    Decimal256Array,
    arrow::datatypes::i256,
    Decimal256
);
