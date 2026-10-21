//! Parquet-specific metadata contracts and services.

mod service;

#[cfg(test)]
mod tests;

pub use service::{PARQUET_FOOTER, ParquetMetadataService};
