//! Shared on-disk manifest schema and path validation.

mod model;
mod path;

pub use model::{ExternalStatisticsSidecar, TableManifest};
pub use path::{artifact_path, external_statistics_path, table_manifest_path};

#[cfg(test)]
mod tests;
