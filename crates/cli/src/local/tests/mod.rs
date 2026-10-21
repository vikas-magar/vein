use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeListArray, Float32Array, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

use super::*;

mod support;
use support::{
    contains_sort, create_flights_table, find_file_scan, materialized_result_column,
    parquet_catalog, parquet_catalog_with_runtime, write_parquet_table,
};

mod query_execution;
mod runtime_synopsis;
mod adaptive;
mod projection;
mod runtime_statistics;
mod scalar_functions;
mod writes;
mod external_statistics;
