//! Integration coverage for Parquet scan behavior through the generic scan contract.

mod bytes_read;
mod dictionary;
mod row_group;
mod runtime_filter;
mod support;

use support::{scan_over, write_parquet};
