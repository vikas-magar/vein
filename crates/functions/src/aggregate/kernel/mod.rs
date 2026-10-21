//! Runtime extension contracts for vectorized aggregate kernels.

use std::any::Any;
use std::ops::Range;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use error::Result;

use crate::FunctionInput;

/// Creates typed accumulator state for one aggregate call.
pub trait AggregateFunction: Send + Sync {
    /// Infers the result type from the bound input types.
    fn return_type(&self, argument_types: &[DataType]) -> Result<DataType>;

    /// Creates empty state sized by later calls to `ensure_groups`.
    fn create_accumulator(
        &self,
        argument_types: &[DataType],
        result_type: &DataType,
    ) -> Result<Box<dyn AggregateAccumulator>>;
}

/// Vectorized per-group state owned by an aggregate operator.
///
/// Implementations receive one group ID per logical input row. `FunctionInput`
/// exposes borrowed Arrow segments and row references; kernels must not gather
/// input values into temporary columns to evaluate their aggregate.
pub trait AggregateAccumulator: Send + Sync {
    /// Ensures state exists for group slots in `0..group_count`.
    fn ensure_groups(&mut self, group_count: usize) -> Result<()>;

    /// Updates group state from one logical batch.
    fn update_batch(&mut self, arguments: &[FunctionInput<'_>], group_ids: &[usize]) -> Result<()>;

    /// Combines one source group into one destination group.
    fn combine_group(
        &mut self,
        target_group: usize,
        source: &dyn AggregateAccumulator,
        source_group: usize,
    ) -> Result<()>;

    /// Finalizes the requested group slots into one Arrow array.
    fn finalize(&self, groups: Range<usize>) -> Result<ArrayRef>;

    /// Conservative state bytes added for each newly allocated group.
    fn estimated_bytes_per_group(&self) -> usize;

    /// Appends one group's mergeable state to a reusable spill buffer.
    fn encode_group(&self, group: usize, output: &mut Vec<u8>) -> Result<()>;

    /// Merges one encoded group state and advances `input` past the payload.
    fn merge_encoded_group(&mut self, group: usize, input: &mut &[u8]) -> Result<()>;

    /// Exposes the concrete state for checked same-kernel combination.
    fn as_any(&self) -> &dyn Any;
}
