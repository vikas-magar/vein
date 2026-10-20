use arrow::array::{Array, ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray};
use arrow::compute::kernels::temporal::DatePart;
use arrow::datatypes::DataType;
use std::sync::Arc;

use super::ExtractFunction;
use crate::udf::ScalarFunction;
use crate::udf::test_support::invoke_dense;

#[test]
fn extract_handles_date_and_timestamp_units_and_nulls() {
    let date = Arc::new(Date32Array::from(vec![Some(0), Some(19_723), None])) as ArrayRef;
    let years = invoke_dense(
        &ExtractFunction {
            name: "year",
            part: DatePart::Year,
        },
        vec![date],
    )
        .expect("extract year");
    let years = years.as_any().downcast_ref::<Int32Array>().expect("Int32 output");
    assert_eq!(years.value(0), 1970);
    assert_eq!(years.value(1), 2024);
    assert!(years.is_null(2));

    let timestamp = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(3_600_000_000),
        None,
    ])) as ArrayRef;
    let hours = invoke_dense(
        &ExtractFunction {
            name: "hour",
            part: DatePart::Hour,
        },
        vec![timestamp],
    )
        .expect("extract hour");
    let hours = hours.as_any().downcast_ref::<Int32Array>().expect("Int32 output");
    assert_eq!(hours.value(0), 1);
    assert!(hours.is_null(1));
}

#[test]
fn extract_rejects_incompatible_parts_and_argument_types() {
    let date = ExtractFunction {
        name: "year",
        part: DatePart::Year,
    };
    assert!(date.return_type(&[DataType::Int64]).is_err());
    assert!(date.return_type(&[]).is_err());
    let hour = ExtractFunction {
        name: "hour",
        part: DatePart::Hour,
    };
    assert!(hour.return_type(&[DataType::Date32]).is_err());
}
