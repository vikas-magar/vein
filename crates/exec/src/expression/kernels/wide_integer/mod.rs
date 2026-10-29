//! Range checks and metadata-only result normalization for native Int128.
use arrow::array::{Array, ArrayRef, Decimal256Array};
use common::Result;
use error::VeinError;
use std::sync::Arc;

/// Validates all non-null values and retags Arrow's derived decimal precision.
/// Only array metadata is cloned; the values and validity buffers remain shared.
pub(crate) fn normalize(array: &dyn Array) -> Result<ArrayRef> {
    let values = array.as_any().downcast_ref::<Decimal256Array>()
        .ok_or_else(|| VeinError::internal("Int128 result requires its declared Arrow storage"))?;
    if values.scale() != 0 {
        return Err(VeinError::internal("Int128 result has a fractional scale"));
    }
    for value in values.iter().flatten() { types::wide_integer::decode(value)?; }
    Ok(Arc::new(values.clone().with_precision_and_scale(39, 0)
        .map_err(|error| VeinError::internal(format!("invalid Int128 result layout: {error}")))?))
}

#[cfg(test)]
mod tests;
