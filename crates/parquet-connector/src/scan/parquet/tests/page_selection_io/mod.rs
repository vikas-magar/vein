use std::ops::Range;
use std::sync::{Arc, Mutex};

use arrow::array::{AsArray, Int32Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetRecordBatchStreamBuilder, RowGroupSelection,
};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;


mod fixture;
mod selection;
mod runtime;
use fixture::indexed_file;
