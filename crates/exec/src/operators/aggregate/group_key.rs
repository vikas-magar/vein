//! Typed Arrow views and hashing for aggregate group keys.
//!
//! Cloned Arrow arrays retain shared buffers; key values are not copied when
//! the aggregation sink keeps a typed view.

use super::*;

/// A type-resolved Arrow key column. Cloning an array only clones its shared
/// buffers, so the sink can retain this view without copying key values.
#[derive(Clone)]
pub(super) enum GroupKeyColumn {
    Date32(Date32Array),
    Int8(Int8Array),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    UInt8(UInt8Array),
    UInt16(UInt16Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
    Decimal128(Decimal128Array),
    Decimal256(Decimal256Array),
    Utf8(StringArray),
    DictionaryUtf8(DictionaryArray<Int32Type>, StringArray),
}

mod access;
mod compare;
mod hash;
mod materialize;

pub(super) use materialize::key_column;
