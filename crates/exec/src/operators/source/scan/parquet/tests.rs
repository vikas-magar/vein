use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use parquet::DecodeResult;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::sync::Arc;

#[path = "tests/metrics.rs"]
mod metrics;
#[path = "tests/page_header.rs"]
mod page_header;
#[path = "tests/projection.rs"]
mod projection;
