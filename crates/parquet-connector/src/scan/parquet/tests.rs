use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[path = "tests/metrics.rs"]
mod metrics;
#[path = "tests/page_header.rs"]
mod page_header;
#[path = "tests/projection.rs"]
mod projection;
#[path = "tests/page_selection_io.rs"]
mod page_selection_io;
#[path = "tests/partitioning.rs"]
mod partitioning;
#[path = "tests/partitioning_support.rs"]
mod partitioning_support;
