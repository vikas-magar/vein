//! Parquet format integration for Vein.

#![deny(missing_docs)]

/// Shared Parquet metadata loading and cache access.
pub mod metadata;

/// Parquet catalog manifest types and path validation.
pub mod catalog;

/// Parquet scan provider, reader, and pruning implementation.
pub mod scan;

/// Parquet metadata statistics and conservative pruning decisions.
#[path = "statistics/mod.rs"]
pub mod parquet_statistics;

/// Parquet encoding and managed-table write lifecycle.
pub mod writer;

pub use metadata::{PARQUET_FOOTER, ParquetMetadataService};
