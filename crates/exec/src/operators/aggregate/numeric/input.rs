//! Borrowed, type-resolved aggregate inputs for one batch.

use super::*;
use super::column::MappedArray;
use super::super::input_batch::AggregateColumn;

/// Typed aggregate arguments read directly from retained Arrow arrays.
pub(in crate::operators::aggregate) enum AggregateInput<'a> {
    Any(&'a AggregateColumn),
    Count(&'a AggregateColumn),
    Int8(MappedArray<'a, Int8Array>),
    Int16(MappedArray<'a, Int16Array>),
    Int32(MappedArray<'a, Int32Array>),
    Int64(MappedArray<'a, Int64Array>),
    UInt8(MappedArray<'a, UInt8Array>),
    UInt16(MappedArray<'a, UInt16Array>),
    UInt32(MappedArray<'a, UInt32Array>),
    UInt64(MappedArray<'a, UInt64Array>),
    Float16(MappedArray<'a, Float16Array>),
    Float32(MappedArray<'a, Float32Array>),
    Float64(MappedArray<'a, Float64Array>),
    Decimal128(MappedArray<'a, Decimal128Array>),
    Decimal256(MappedArray<'a, Decimal256Array>),
}

impl<'a> AggregateInput<'a> {
    pub(in crate::operators::aggregate) fn new(
        op: AggOpCode,
        input_type: &VeinDataType,
        column: &'a AggregateColumn,
    ) -> Result<Self> {
        if op == AggOpCode::SingleValue {
            return Ok(Self::Any(column));
        }
        if op == AggOpCode::Count {
            return Ok(Self::Count(column));
        }
        match input_type {
            VeinDataType::Int8 => MappedArray::new(column, input_type).map(Self::Int8),
            VeinDataType::Int16 => MappedArray::new(column, input_type).map(Self::Int16),
            VeinDataType::Int32 => MappedArray::new(column, input_type).map(Self::Int32),
            VeinDataType::Int64 => MappedArray::new(column, input_type).map(Self::Int64),
            VeinDataType::UInt8 => MappedArray::new(column, input_type).map(Self::UInt8),
            VeinDataType::UInt16 => MappedArray::new(column, input_type).map(Self::UInt16),
            VeinDataType::UInt32 => MappedArray::new(column, input_type).map(Self::UInt32),
            VeinDataType::UInt64 => MappedArray::new(column, input_type).map(Self::UInt64),
            VeinDataType::Float16 => MappedArray::new(column, input_type).map(Self::Float16),
            VeinDataType::Float32 => MappedArray::new(column, input_type).map(Self::Float32),
            VeinDataType::Float64 => MappedArray::new(column, input_type).map(Self::Float64),
            VeinDataType::Decimal128(_, _) => {
                MappedArray::new(column, input_type).map(Self::Decimal128)
            }
            VeinDataType::Decimal256(_, _) => {
                MappedArray::new(column, input_type).map(Self::Decimal256)
            }
            other => Err(VeinError::internal(format!(
                "aggregate input type {other:?} is not supported yet"
            ))),
        }
    }
}
