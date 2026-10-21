//! Parquet connector scan implementation, isolated from the generic executor.

pub(crate) use crate::PARQUET_FOOTER;

mod batch_size;
mod analysis;
mod execute;
mod file;
mod metrics;
mod page_header_reader;
mod page_pruning;
mod planning;
mod predicate;
mod projection;
mod reader;
mod stream;

pub(crate) use analysis::analysis_inputs;
pub(super) use execute::execute_parquet_scan;
pub(crate) use file::load_footer_from_storage;
pub(crate) use planning::plan_scan;

#[cfg(test)]
use common::statistics::runtime::{RUNTIME_STATISTIC_METRICS, runtime_metric_availability};

#[cfg(test)]
#[path = "../reader_metrics_tests.rs"]
mod reader_metrics_tests;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "runtime_statistics_tests.rs"]
mod runtime_statistics_tests;

#[cfg(test)]
#[path = "selection_tests.rs"]
mod selection_tests;
