//! Parquet scan provider and format-specific execution path.

mod parquet;
mod provider;

pub use provider::{PARQUET_FORMAT_ID, ParquetScanProvider};
pub(crate) use parquet::analysis_inputs;
pub(crate) use parquet::load_footer_from_storage;


#[cfg(test)]
mod tests;
