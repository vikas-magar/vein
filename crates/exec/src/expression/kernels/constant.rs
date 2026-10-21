//! Arrow array construction from [`LogicalLiteral`] values.
//!
//! Provides [`literal_to_array`] to materialize constant values into Arrow
//! arrays for typed expression results. Logical literals are converted directly
//! from the engine's own protocol representation.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int64Array,
    IntervalMonthDayNanoArray, ListArray, new_null_array, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow::array::types::IntervalMonthDayNano;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use common::Result;
use error::VeinError;
use types::LogicalLiteral;

/// Constructs a length-1 Arrow array holding `literal`, for scalar broadcasting.
///
/// Element-wise kernels broadcast this single value across the chunk, so a
/// literal costs one element per program instead of one element per row per batch.
pub fn literal_to_scalar(literal: &LogicalLiteral) -> Result<ArrayRef> {
    literal_to_array(literal, 1)
}

/// Constructs an Arrow [`ArrayRef`] of length `row_count` holding copies of `literal`.
///
/// Only used where a constant genuinely has to become a full column, such as the
/// input to a UDF or a projected output column.
fn literal_to_array(literal: &LogicalLiteral, row_count: usize) -> Result<ArrayRef> {
    let array: ArrayRef = match literal {
        LogicalLiteral::Null(data_type) => new_null_array(&data_type.to_arrow(), row_count),
        LogicalLiteral::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; row_count])),
        LogicalLiteral::Int64(v) => Arc::new(Int64Array::from(vec![*v; row_count])),
        LogicalLiteral::Float64(v) => Arc::new(Float64Array::from(vec![*v; row_count])),
        LogicalLiteral::String(v) => {
            let iter = std::iter::repeat_n(v.as_str(), row_count);
            Arc::new(StringArray::from_iter_values(iter))
        }
        // `Date32` is a day count and `Time64`/`Timestamp` are microsecond
        // counts, so the logical value is already the Arrow payload.
        LogicalLiteral::Date(v) => Arc::new(Date32Array::from(vec![*v; row_count])),
        LogicalLiteral::Timestamp(v) => Arc::new(TimestampMicrosecondArray::from(vec![*v; row_count])),
        LogicalLiteral::Time(v) => Arc::new(Time64MicrosecondArray::from(vec![*v; row_count])),
        LogicalLiteral::Interval { months, days, nanos } => {
            let value = IntervalMonthDayNano::new(*months, *days, *nanos);
            Arc::new(IntervalMonthDayNanoArray::from_iter_values(
                std::iter::repeat_n(value, row_count),
            ))
        }
        LogicalLiteral::Decimal(v, precision, scale) => {
            let values = std::iter::repeat_n(*v, row_count);
            Arc::new(
                Decimal128Array::from_iter_values(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| {
                        VeinError::internal(format!("invalid decimal literal: {error}"))
                    })?,
            )
        }
        LogicalLiteral::Vector(elements) => {
            let dim = elements.len();
            let mut flat_values = Vec::with_capacity(row_count * dim);
            for _ in 0..row_count {
                flat_values.extend_from_slice(elements);
            }
            let values_array = Arc::new(Float32Array::from(flat_values));
            let offsets = OffsetBuffer::from_lengths(vec![dim; row_count]);
            let field = Arc::new(Field::new("item", DataType::Float32, false));
            Arc::new(ListArray::new(field, offsets, values_array, None))
        }
        other => {
            return Err(VeinError::internal(format!(
                "literal cannot be materialized as an Arrow value yet: {other:?}"
            )));
        }
    };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_to_array() {
        let i64_arr = literal_to_array(&LogicalLiteral::Int64(42), 3).unwrap();
        assert_eq!(i64_arr.len(), 3);
        let values = i64_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.value(0), 42);
        assert_eq!(values.value(2), 42);

        let str_arr = literal_to_array(&LogicalLiteral::String("vein".to_string()), 2).unwrap();
        assert_eq!(str_arr.len(), 2);
        let values = str_arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.value(0), "vein");
        assert_eq!(values.value(1), "vein");

        let vec_arr = literal_to_array(&LogicalLiteral::Vector(vec![1.0, 2.0]), 2).unwrap();
        assert_eq!(vec_arr.len(), 2);
    }

    /// A decimal literal has no narrower representation to fall back on, so it
    /// must reach a `Decimal128` array carrying its own precision and scale.
    #[test]
    fn decimal_literal_keeps_its_precision_and_scale() {
        let array = literal_to_array(&LogicalLiteral::Decimal(1234, 15, 2), 1).unwrap();
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(array.value(0), 1234);
        assert_eq!(array.precision(), 15);
        assert_eq!(array.scale(), 2);
    }

    #[test]
    fn typed_null_literal_preserves_its_arrow_type_and_nulls() {
        let boolean = literal_to_array(&LogicalLiteral::Null(types::VeinDataType::Boolean), 3)
            .expect("typed Boolean NULL should materialize");
        let values = boolean
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Boolean NULL should use a BooleanArray");
        assert_eq!(values, &BooleanArray::from(vec![None, None, None]));

        let decimal = literal_to_array(
            &LogicalLiteral::Null(types::VeinDataType::Decimal128(15, 2)),
            2,
        )
            .expect("typed decimal NULL should materialize");
        assert_eq!(
            decimal.data_type(),
            &DataType::Decimal128(15, 2),
            "typed NULL must retain its logical decimal precision and scale"
        );
        assert_eq!(decimal.null_count(), 2);

        let untyped = literal_to_array(&LogicalLiteral::Null(types::VeinDataType::Null), 1)
            .expect("untyped NULL should materialize");
        assert_eq!(untyped.data_type(), &DataType::Null);
    }

    #[test]
    fn interval_literal_keeps_month_day_nano_components() {
        let array = literal_to_array(
            &LogicalLiteral::Interval {
                months: 2,
                days: 90,
                nanos: 1234,
            },
            2,
        )
            .unwrap();
        let array = array
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();

        assert_eq!(array.len(), 2);
        assert_eq!(array.value(0), IntervalMonthDayNano::new(2, 90, 1234));
        assert_eq!(array.value(1), IntervalMonthDayNano::new(2, 90, 1234));
    }

    #[test]
    fn date_literal_uses_date32_arrow_type() {
        let array = literal_to_array(&LogicalLiteral::Date(10), 1).unwrap();

        assert_eq!(array.data_type(), &DataType::Date32);
        assert_eq!(array.as_any().downcast_ref::<Date32Array>().unwrap().value(0), 10);
    }

    #[test]
    fn timestamp_and_time_literals_keep_their_arrow_types() {
        let timestamp = literal_to_array(&LogicalLiteral::Timestamp(1_000_000), 2).unwrap();
        assert_eq!(
            timestamp.data_type(),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        let timestamp = timestamp
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(timestamp.values(), &[1_000_000, 1_000_000]);

        let time = literal_to_array(&LogicalLiteral::Time(45_000_000_000), 1).unwrap();
        assert_eq!(
            time.data_type(),
            &DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        );
        assert_eq!(
            time.as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap()
                .value(0),
            45_000_000_000
        );
    }
}
