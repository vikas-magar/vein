use std::sync::Arc;

use arrow::datatypes::DataType;
use common::Result;
use functions::FunctionRegistry;

/// Builds opaque spill bytes through the same registered kernel used at runtime.
pub(in crate::operators::aggregate) fn count_state(count: usize) -> Result<Arc<Vec<u8>>> {
    let registry = FunctionRegistry::with_builtins();
    let mut accumulator = registry.create_aggregate_accumulator(
        "COUNT",
        &[DataType::Int64],
        &DataType::Int64,
    )?;
    accumulator.ensure_groups(1)?;
    let group_ids = vec![0; count];
    accumulator.update_batch(&[], &group_ids)?;
    let mut encoded = Vec::new();
    accumulator.encode_group(0, &mut encoded)?;
    Ok(Arc::new(encoded))
}
