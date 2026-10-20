use super::*;

mod bytes_read;
mod dictionary;
mod row_group;
mod runtime_filter;
#[path = "tests/reader_operator.rs"]
mod reader_operator;
mod support;

use support::{
    ArrayTypeSink, Int64Collector, KeepIds, scan_over, write_multi_row_group_parquet,
    write_parquet, write_string_parquet,
};
