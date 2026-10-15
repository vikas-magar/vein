//! Borrowed, type-resolved Arrow inputs for one aggregation batch.

use super::*;

/// A borrowed aggregate input, downcast once for the current input batch.
///
/// The array remains owned by the input chunk; this view only caches its
/// physical layout while rows are folded into their groups.
#[derive(Clone, Copy)]
pub(in crate::operators::aggregate) enum AggregateInput<'a> {
    Any(&'a ArrayRef),
    Count(&'a ArrayRef),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float16(&'a Float16Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Decimal128(&'a Decimal128Array),
    Decimal256(&'a Decimal256Array),
}

impl<'a> AggregateInput<'a> {
    pub(in crate::operators::aggregate) fn new(op: AggOpCode, input_type: &VeinDataType, array: &'a ArrayRef) -> Result<Self> {
        if op == AggOpCode::SingleValue {
            return Ok(Self::Any(array));
        }
        if op == AggOpCode::Count {
            return Ok(Self::Count(array));
        }
        // The borrowed array always follows the argument layout. The
        // accumulator may use a wider result layout, such as Float32 SUM.
        let data_type = input_type;
        let mismatch = || {
            VeinError::internal(format!(
                "aggregate input array does not match {data_type:?} kernel"
            ))
        };
        match data_type {
            VeinDataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or_else(mismatch),
            VeinDataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or_else(mismatch),
            VeinDataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(mismatch),
            VeinDataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(mismatch),
            VeinDataType::UInt8 => array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .map(Self::UInt8)
                .ok_or_else(mismatch),
            VeinDataType::UInt16 => array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .map(Self::UInt16)
                .ok_or_else(mismatch),
            VeinDataType::UInt32 => array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .map(Self::UInt32)
                .ok_or_else(mismatch),
            VeinDataType::UInt64 => array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map(Self::UInt64)
                .ok_or_else(mismatch),
            VeinDataType::Float16 => array
                .as_any()
                .downcast_ref::<Float16Array>()
                .map(Self::Float16)
                .ok_or_else(mismatch),
            VeinDataType::Float32 => array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(Self::Float32)
                .ok_or_else(mismatch),
            VeinDataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(Self::Float64)
                .ok_or_else(mismatch),
            VeinDataType::Decimal128(_, _) => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(Self::Decimal128)
                .ok_or_else(mismatch),
            VeinDataType::Decimal256(_, _) => array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .map(Self::Decimal256)
                .ok_or_else(mismatch),
            other => Err(VeinError::internal(format!(
                "aggregate input type {other:?} is not supported yet"
            ))),
        }
    }
}
