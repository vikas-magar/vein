//! Format-neutral contracts and helpers for dividing file scans into work units.

mod ranges;
mod request;
mod work_unit;

pub use ranges::coalesce_scan_units;
pub use request::ScanPlanningRequest;
pub(crate) use request::validate_file_scan;
pub use work_unit::file_scan_work_unit;
pub(crate) use work_unit::split_scan_by_file;
