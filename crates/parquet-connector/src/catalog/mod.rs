//! Parquet-specific catalog integration.

mod statistics;
mod source_identity;
mod analysis;
mod discovery;
mod metadata;
mod provider;
mod publication;
mod snapshot;

#[cfg(test)]
mod tests;

/// Canonical manifest representation and safe artifact paths.
pub mod manifest;

pub use source_identity::statistics_source_identity;
pub use statistics::apply_analyzed_statistics;
pub use provider::ParquetCatalog;
pub use publication::ParquetStatisticsPublisher;
