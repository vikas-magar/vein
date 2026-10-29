//! Signed 128-bit integers represented by the existing logical-extension contract.
use crate::VeinDataType;
use arrow::datatypes::DataType;
use std::sync::Arc;

/// Stable Arrow extension identity for native signed 128-bit integer semantics.
pub const INT128_EXTENSION: &str = "vein.int128";
/// Lossless Arrow interchange layout, including the 39-digit signed extrema.
/// Accumulators use native i128; Arrow has no primitive Int128 layout.
pub const INT128_STORAGE: DataType = DataType::Decimal256(39, 0);

/// Validates a physical interchange value before exposing native integer semantics.
pub fn decode(value: arrow::datatypes::i256) -> error::Result<i128> {
    if value < arrow::datatypes::i256::from(i128::MIN)
        || value > arrow::datatypes::i256::from(i128::MAX) {
        return Err(error::VeinError::invalid_input("value is outside the signed Int128 range"));
    }
    Ok(value.to_parts().0 as i128)
}

impl VeinDataType {
    /// Native signed 128-bit integer with an explicit Arrow storage adapter.
    pub fn int128() -> Self {
        Self::Extension(INT128_EXTENSION.to_owned(), Arc::new(Self::Decimal256(39, 0)))
    }

    /// Whether this is Vein's signed 128-bit logical integer.
    pub fn is_int128(&self) -> bool {
        matches!(self, Self::Extension(name, storage)
            if name == INT128_EXTENSION && storage.as_ref() == &Self::Decimal256(39, 0))
    }
}

pub(crate) fn common_type(left: &VeinDataType, right: &VeinDataType) -> Option<VeinDataType> {
    if !left.is_int128() && !right.is_int128() { return None; }
    let other = if left.is_int128() { right } else { left };
    if other.is_int128() || matches!(other, VeinDataType::Int8 | VeinDataType::Int16 |
        VeinDataType::Int32 | VeinDataType::Int64 | VeinDataType::UInt8 |
        VeinDataType::UInt16 | VeinDataType::UInt32 | VeinDataType::UInt64) {
        Some(VeinDataType::int128())
    } else if matches!(other, VeinDataType::Float16 | VeinDataType::Float32 | VeinDataType::Float64) {
        Some(VeinDataType::Float64)
    } else { None }
}

#[cfg(test)]
mod tests;
