use std::sync::Arc;

use super::FunctionRegistry;

mod lower;
mod round;

/// Registers scalar built-ins that have executable implementations.
pub fn register_builtin_scalars(registry: &mut FunctionRegistry) {
    registry.register_scalar(Arc::new(lower::LowerFunction));
    registry.register_scalar(Arc::new(round::RoundFunction));
}
