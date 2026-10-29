//! Format-neutral file scans and their registered execution providers.

mod planning;
pub mod cursor;
mod provider;
mod registry;
mod request;
mod source;
mod worker;

pub use planning::ScanPlanningRequest;
pub use planning::{coalesce_scan_units, file_scan_work_unit};
pub(crate) use planning::{split_scan_by_file, validate_file_scan};
pub use provider::ScanProvider;
pub use cursor::ScanCursor;
pub use registry::ScanProviderRegistry;
pub use request::ScanRequest;
pub use worker::ScanWorkerState;
pub use source::{ScanFile, ScanSource, ScanSourceConfig};

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "registry_tests.rs"]
mod registry_tests;
