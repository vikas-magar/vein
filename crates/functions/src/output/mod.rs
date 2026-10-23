use arrow::array::{ArrayRef, new_empty_array};
use arrow::datatypes::DataType;
use error::{Result, VeinError};

mod segment;
pub use segment::OutputSegment;

/// Scalar-function result representation.
#[derive(Debug, Clone)]
pub enum FunctionOutput {
    /// Computed Arrow array.
    Array(ArrayRef),
    /// One-value Arrow array broadcast over a logical batch.
    Scalar(ArrayRef),
    /// Ordered result segments, retained without concatenating their buffers.
    Segments(Vec<OutputSegment>),
    /// Reuses an input expression's exact view or Arrow storage.
    ReuseInput(usize),
}

impl FunctionOutput {
    /// Wraps a computed array without copying its buffers.
    pub fn from_array(array: ArrayRef) -> Self { Self::Array(array) }

    /// Wraps a one-value array as a broadcast scalar.
    pub fn from_scalar(array: ArrayRef) -> Result<Self> {
        if array.len() != 1 {
            return Err(VeinError::internal("scalar function output must contain one value"));
        }
        Ok(Self::Scalar(array))
    }

    /// Retains ordered result arrays without concatenating their buffers.
    pub fn from_segments(arrays: Vec<ArrayRef>) -> Result<Self> {
        Self::from_runs(arrays.into_iter().map(OutputSegment::dense).collect())
    }

    /// Retains dense and repeated runs with their logical row counts.
    pub fn from_runs(mut runs: Vec<OutputSegment>) -> Result<Self> {
        match runs.len() {
            0 => Err(VeinError::internal("function returned no output segments")),
            1 if !runs[0].is_scalar() => {
                let run = runs.pop().expect("one segment remains");
                Ok(Self::Array(run.into_array()))
            }
            _ => Ok(Self::Segments(runs)),
        }
    }

    /// Returns the input argument at this ordinal unchanged.
    pub const fn reuse_input(index: usize) -> Self { Self::ReuseInput(index) }

    /// Creates an empty array for an empty logical batch.
    pub fn empty(data_type: &DataType) -> Self { Self::Array(new_empty_array(data_type)) }

    /// Validates result type, shape, and reuse references.
    pub fn validate(
        &self,
        row_count: usize,
        expected_type: &DataType,
        arguments: &[crate::FunctionInput<'_>],
    ) -> Result<()> {
        match self {
            Self::Array(array) => validate_array(array, row_count, expected_type),
            Self::Scalar(array) => {
                validate_array(array, 1, expected_type)
            }
            Self::Segments(arrays) => {
                let total = arrays.iter().try_fold(0usize, |total, run| {
                    validate_array(run.array(), if run.is_scalar() { 1 } else { run.len() }, expected_type)?;
                    total.checked_add(run.len())
                        .ok_or_else(|| VeinError::internal("function output length overflow"))
                })?;
                if total != row_count {
                    return Err(VeinError::internal(format!(
                        "function returned {total} rows for a batch of {row_count}"
                    )));
                }
                Ok(())
            }
            Self::ReuseInput(index) => {
                let input = arguments.get(*index).ok_or_else(|| {
                    VeinError::internal(format!("function reused missing input {index}"))
                })?;
                if input.data_type() != expected_type || input.len() != row_count {
                    return Err(VeinError::internal("reused function input has an incompatible type or shape"));
                }
                Ok(())
            }
        }
    }
}

fn validate_array(array: &ArrayRef, rows: usize, data_type: &DataType) -> Result<()> {
    if array.len() != rows || array.data_type() != data_type {
        return Err(VeinError::internal(format!(
            "function returned {} values of type {:?}, expected {rows} values of type {data_type:?}",
            array.len(), array.data_type()
        )));
    }
    Ok(())
}
