use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeListArray, Float32Array, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use plan::LogicalPlan;

use super::query_context::query_statistics_context;
use super::*;

mod support;
use support::{
    contains_sort, create_flights_table, find_file_scan, materialized_result_column,
    parquet_catalog, parquet_catalog_with_runtime, write_parquet_batch,
};

mod query_statistics;
mod query_execution;
mod runtime_synopsis;
mod adaptive;
mod projection;
mod runtime_statistics;
mod scalar_functions;
mod writes;
mod external_statistics;
