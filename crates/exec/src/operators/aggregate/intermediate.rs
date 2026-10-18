//! Typed, lossless partial aggregate state used when groups are externalized.
//!
//! These values describe accumulator state rather than final SQL output. In
//! particular, AVG keeps its exact running sum and count, and SINGLE_VALUE
//! keeps row cardinality independently from the nullable value. Arrow-backed
//! scalar values are retained by reference until a spill writer serializes
//! them.

use arrow::array::ArrayRef;
use arrow::datatypes::i256;
use common::protocol::AggOpCode;
use error::VeinError;
use std::io::{Cursor, Read};

#[derive(Clone)]
pub(super) struct NumericPartial<T> {
    pub(super) op: AggOpCode,
    pub(super) value: Option<T>,
    pub(super) count: i64,
    pub(super) scale: i8,
    pub(super) decimal_precision_limit: bool,
}

#[derive(Clone)]
pub(super) enum AggregateIntermediateState {
    SingleValue {
        rows: u64,
        value: Option<(ArrayRef, usize)>,
    },
    Count(i64),
    Int64(NumericPartial<i64>),
    Int128Avg(NumericPartial<i128>),
    UInt64(NumericPartial<u64>),
    UInt128Avg(NumericPartial<u128>),
    Float32(NumericPartial<f32>),
    Float64(NumericPartial<f64>),
    Decimal128(NumericPartial<i128>),
    Decimal256(NumericPartial<i256>),
}

/// Aggregate state tied to a group identity. Key arrays are shared Arrow views,
/// so a spill writer can consume this snapshot after the sink releases its own
/// references without copying key payloads.
#[derive(Clone)]
pub(super) struct AggregateIntermediateGroup {
    /// Hash produced by the owning aggregate's query-local hash builder.
    /// Runs retain this value so records can be routed back to the same key
    /// partition without reconstructing a hasher or changing hash seeds.
    pub(super) hash: u64,
    pub(super) grouping_set_id: usize,
    pub(super) keys: Vec<Option<(ArrayRef, usize)>>,
    pub(super) aggregate_states: Vec<AggregateIntermediateState>,
}

impl AggregateIntermediateGroup {
    /// Encode the accumulator portion of a grouped spill record.
    ///
    /// Keys are written as typed Arrow columns by the run writer. This compact,
    /// versioned payload stores only fixed-width accumulator state. A grouped
    /// spill cannot contain `SINGLE_VALUE`: that aggregate is restricted to a
    /// global aggregate and does not grow with the number of groups.
    pub(super) fn encode_aggregate_states(&self) -> common::Result<Vec<u8>> {
        let count = u32::try_from(self.aggregate_states.len())
            .map_err(|_| VeinError::internal("too many aggregate states in spill record"))?;
        let capacity = self
            .aggregate_states
            .len()
            .checked_mul(48)
            .and_then(|size| size.checked_add(8))
            .ok_or_else(|| VeinError::internal("aggregate spill record size overflow"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| VeinError::internal("cannot allocate aggregate spill record"))?;
        bytes.extend_from_slice(b"AGG\x01");
        bytes.extend_from_slice(&count.to_le_bytes());
        for state in &self.aggregate_states {
            match state {
                AggregateIntermediateState::SingleValue { .. } => {
                    return Err(VeinError::internal(
                        "SINGLE_VALUE state cannot be written to a grouped aggregate spill",
                    ));
                }
                AggregateIntermediateState::Count(value) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
                AggregateIntermediateState::Int64(value) => encode_numeric(2, value, &mut bytes)?,
                AggregateIntermediateState::Int128Avg(value) => {
                    encode_numeric(3, value, &mut bytes)?
                }
                AggregateIntermediateState::UInt64(value) => encode_numeric(4, value, &mut bytes)?,
                AggregateIntermediateState::UInt128Avg(value) => {
                    encode_numeric(5, value, &mut bytes)?
                }
                AggregateIntermediateState::Float32(value) => encode_numeric(6, value, &mut bytes)?,
                AggregateIntermediateState::Float64(value) => encode_numeric(7, value, &mut bytes)?,
                AggregateIntermediateState::Decimal128(value) => {
                    encode_numeric(8, value, &mut bytes)?
                }
                AggregateIntermediateState::Decimal256(value) => {
                    encode_numeric(9, value, &mut bytes)?
                }
            }
        }
        Ok(bytes)
    }

    /// Decode a state payload produced by [`Self::encode_aggregate_states`].
    pub(super) fn decode_aggregate_states(
        bytes: &[u8],
    ) -> common::Result<Vec<AggregateIntermediateState>> {
        let invalid = |message: &'static str| VeinError::internal(message);
        let mut cursor = Cursor::new(bytes);
        let mut header = [0; 4];
        cursor
            .read_exact(&mut header)
            .map_err(|_| invalid("truncated aggregate spill state header"))?;
        if &header != b"AGG\x01" {
            return Err(invalid("unsupported aggregate spill state format"));
        }
        let count = read_u32(&mut cursor)? as usize;
        // Every state needs at least one tag byte. Bound allocation by the
        // actual record length before reserving attacker or disk controlled size.
        if count > bytes.len().saturating_sub(cursor.position() as usize) {
            return Err(invalid("invalid aggregate spill state count"));
        }
        let mut states = Vec::new();
        states
            .try_reserve_exact(count)
            .map_err(|_| invalid("cannot allocate aggregate spill state"))?;
        for _ in 0..count {
            let tag = read_u8(&mut cursor)?;
            states.push(match tag {
                1 => {
                    let count = read_i64(&mut cursor)?;
                    if count < 0 {
                        return Err(invalid("negative aggregate spill COUNT state"));
                    }
                    AggregateIntermediateState::Count(count)
                }
                2 => AggregateIntermediateState::Int64(decode_numeric(&mut cursor)?),
                3 => AggregateIntermediateState::Int128Avg(decode_numeric(&mut cursor)?),
                4 => AggregateIntermediateState::UInt64(decode_numeric(&mut cursor)?),
                5 => AggregateIntermediateState::UInt128Avg(decode_numeric(&mut cursor)?),
                6 => AggregateIntermediateState::Float32(decode_numeric(&mut cursor)?),
                7 => AggregateIntermediateState::Float64(decode_numeric(&mut cursor)?),
                8 => AggregateIntermediateState::Decimal128(decode_numeric(&mut cursor)?),
                9 => AggregateIntermediateState::Decimal256(decode_numeric(&mut cursor)?),
                _ => return Err(invalid("unknown aggregate spill state tag")),
            });
        }
        if cursor.position() as usize != bytes.len() {
            return Err(invalid("trailing bytes in aggregate spill state"));
        }
        Ok(states)
    }
}

trait SpillValue: Copy + Sized {
    fn write_value(self, bytes: &mut Vec<u8>);
    fn read_value(cursor: &mut Cursor<&[u8]>) -> common::Result<Self>;
}

macro_rules! spill_primitive {
    ($type:ty) => {
        impl SpillValue for $type {
            fn write_value(self, bytes: &mut Vec<u8>) {
                bytes.extend_from_slice(&self.to_le_bytes());
            }

            fn read_value(cursor: &mut Cursor<&[u8]>) -> common::Result<Self> {
                let mut bytes = [0; size_of::<Self>()];
                cursor
                    .read_exact(&mut bytes)
                    .map_err(|_| VeinError::internal("truncated aggregate spill numeric value"))?;
                Ok(Self::from_le_bytes(bytes))
            }
        }
    };
}

spill_primitive!(i64);
spill_primitive!(i128);
spill_primitive!(u64);
spill_primitive!(u128);
spill_primitive!(f32);
spill_primitive!(f64);

impl SpillValue for i256 {
    fn write_value(self, bytes: &mut Vec<u8>) {
        let (low, high) = self.to_parts();
        bytes.extend_from_slice(&low.to_le_bytes());
        bytes.extend_from_slice(&high.to_le_bytes());
    }

    fn read_value(cursor: &mut Cursor<&[u8]>) -> common::Result<Self> {
        let low = read_u128(cursor)?;
        let high = read_i128(cursor)?;
        Ok(Self::from_parts(low, high))
    }
}

fn encode_numeric<T: SpillValue>(
    tag: u8,
    value: &NumericPartial<T>,
    bytes: &mut Vec<u8>,
) -> common::Result<()> {
    bytes.push(tag);
    bytes.push(encode_op(value.op));
    match value.value {
        Some(value) => {
            bytes.push(1);
            value.write_value(bytes);
        }
        None => bytes.push(0),
    }
    bytes.extend_from_slice(&value.count.to_le_bytes());
    bytes.push(value.scale as u8);
    bytes.push(u8::from(value.decimal_precision_limit));
    Ok(())
}

fn decode_numeric<T: SpillValue>(cursor: &mut Cursor<&[u8]>) -> common::Result<NumericPartial<T>> {
    let op = decode_op(read_u8(cursor)?)?;
    let value = match read_u8(cursor)? {
        0 => None,
        1 => Some(T::read_value(cursor)?),
        _ => return Err(VeinError::internal("invalid aggregate spill value marker")),
    };
    let count = read_i64(cursor)?;
    let scale = read_u8(cursor)? as i8;
    let decimal_precision_limit = match read_u8(cursor)? {
        0 => false,
        1 => true,
        _ => return Err(VeinError::internal("invalid aggregate spill boolean")),
    };
    if count < 0 {
        return Err(VeinError::internal(
            "negative aggregate spill accumulator count",
        ));
    }
    Ok(NumericPartial {
        op,
        value,
        count,
        scale,
        decimal_precision_limit,
    })
}

fn encode_op(op: AggOpCode) -> u8 {
    match op {
        AggOpCode::Count => 1,
        AggOpCode::CountDistinct => 7,
        AggOpCode::Sum => 2,
        AggOpCode::Min => 3,
        AggOpCode::Max => 4,
        AggOpCode::Avg => 5,
        AggOpCode::SingleValue => 6,
    }
}

fn decode_op(op: u8) -> common::Result<AggOpCode> {
    match op {
        1 => Ok(AggOpCode::Count),
        7 => Ok(AggOpCode::CountDistinct),
        2 => Ok(AggOpCode::Sum),
        3 => Ok(AggOpCode::Min),
        4 => Ok(AggOpCode::Max),
        5 => Ok(AggOpCode::Avg),
        6 => Ok(AggOpCode::SingleValue),
        _ => Err(VeinError::internal("unknown aggregate spill opcode")),
    }
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> common::Result<u8> {
    let mut bytes = [0; 1];
    cursor
        .read_exact(&mut bytes)
        .map_err(|_| VeinError::internal("truncated aggregate spill state"))?;
    Ok(bytes[0])
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> common::Result<u32> {
    let mut bytes = [0; 4];
    cursor
        .read_exact(&mut bytes)
        .map_err(|_| VeinError::internal("truncated aggregate spill state"))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_i64(cursor: &mut Cursor<&[u8]>) -> common::Result<i64> {
    let mut bytes = [0; 8];
    cursor
        .read_exact(&mut bytes)
        .map_err(|_| VeinError::internal("truncated aggregate spill state"))?;
    Ok(i64::from_le_bytes(bytes))
}

fn read_i128(cursor: &mut Cursor<&[u8]>) -> common::Result<i128> {
    let mut bytes = [0; 16];
    cursor
        .read_exact(&mut bytes)
        .map_err(|_| VeinError::internal("truncated aggregate spill state"))?;
    Ok(i128::from_le_bytes(bytes))
}

fn read_u128(cursor: &mut Cursor<&[u8]>) -> common::Result<u128> {
    let mut bytes = [0; 16];
    cursor
        .read_exact(&mut bytes)
        .map_err(|_| VeinError::internal("truncated aggregate spill state"))?;
    Ok(u128::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests;
