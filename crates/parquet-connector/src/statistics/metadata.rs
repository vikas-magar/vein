//! Parquet footer statistics extraction and conservative rollups.

mod columns;
mod summary;
mod values;

pub use columns::row_group_column_statistics;
pub use summary::table_statistics;

pub(super) use columns::{populate_column_statistics, row_group_unit};
pub(super) use summary::{complete_sum, nonnegative};
pub(super) use values::{array_u64_at, date64_as_days, logical_literal_at, to_micros};

#[cfg(test)]
pub(super) use values::exact_scalar_at;
