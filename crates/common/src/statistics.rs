//! Format-neutral, streaming statistics over borrowed Arrow batches.
//!
//! This module owns algorithms, not file readers. Any source that produces
//! Arrow arrays can feed batches to the collector. Values remain in their input
//! buffers; collection reads borrowed scalars and retains only bounded synopsis
//! state.

/// Generic streaming analysis orchestration and source/publisher contracts.
pub mod analysis;
/// Shared, query-time statistics observations collected from borrowed batches.
pub mod runtime;
/// One query-scoped view of source metadata and lazily collected runtime feedback.
pub mod context;
/// Query-lifetime state shared by task-local execution decisions.
pub mod query_state;
/// Compact, payload-safe formatting for query statistics diagnostics.
pub mod logging;

use arrow::array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use catalog::{
    ByteLengthStatistics, ColumnGroupStatistics, ColumnStatistics, HistogramBucket,
    HistogramSample, HistogramStatistics, MostCommonValue, MostCommonValuesStatistics,
    NumericStatistics, SerializedSketch, TableStatistics,
};
use error::VeinError;
use types::LogicalLiteral;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::Result;

mod accumulators;
mod input;
pub use input::{SelectedRecordBatch, StatisticsInput};
pub use accumulators::StatisticsCollector;
pub use runtime::{RuntimeStatisticsObservation, RuntimeStatisticsStore};
pub use context::{
    OperatorCounter, OperatorStatisticsHandle, OperatorStatisticsIdentity, OperatorStatisticsSnapshot,
    QuerySourceStatistics, QueryStatisticsContext, QueryStatisticsSnapshot,
};
pub use query_state::QueryStateRegistry;
pub use logging::TableStatisticsLog;

/// Unambiguous synopsis key for a column in a query-local intermediate relation.
/// Display names can repeat after joins; bound identities cannot alias inputs.
pub fn intermediate_column_key(id: types::ColumnId) -> String { id.0.to_string() }
#[cfg(test)]
mod tests;

mod encoding;
mod sketch;
pub use encoding::{ArrowScalarEncoder, ArrowValueEncoder};
pub use sketch::{HyperLogLog, MergeableDistinctSketch};
use encoding::{canonical_float, stable_hash, type_tag};

/// Fingerprint a logical schema independently of the physical file format.
///
/// Names, order, nullability, and logical types are part of the identity. A
/// changed schema safely invalidates synopses collected for the prior layout.
pub fn schema_fingerprint(schema: &types::VeinSchema) -> String {
    let mut identity = String::from("vein-logical-schema-v1;");
    for field in &schema.fields {
        identity.push_str(&field.name.len().to_string());
        identity.push(':');
        identity.push_str(&field.name);
        identity.push('|');
        identity.push_str(&format!("{:?}", field.data_type));
        identity.push('|');
        identity.push(if field.nullable { '1' } else { '0' });
        identity.push(';');
    }
    format!("{:016x}", stable_hash(0xfe, identity.as_bytes()))
}

/// Stable non-cryptographic key for adapter-owned statistics sidecars.
///
/// Persistent consumers must store and validate the original identifier too;
/// this compact key is for path selection, not an identity proof.
pub fn stable_identifier_hash(identifier: &str) -> u64 {
    stable_hash(0xfc, identifier.as_bytes())
}

/// Configuration shared by the statistics collectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticsConfig {
    /// Maximum number of rows requested from an analysis source per batch.
    pub scan_batch_size: usize,
    /// HyperLogLog precision. More registers reduce error and increase memory.
    pub distinct_precision: u8,
    /// Maximum string bound retained as an optimizer literal. Longer values
    /// keep length statistics but make the column bounds explicitly unknown.
    pub max_bound_value_bytes: usize,
    /// Maximum candidate count in each mergeable frequent-value synopsis.
    pub most_common_capacity: usize,
    /// Maximum uniformly sampled values retained for each ordered column.
    pub histogram_sample_size: usize,
    /// Maximum equal-depth histogram buckets emitted per ordered column.
    pub histogram_bucket_count: usize,
    /// Maximum owned scalar bytes retained by frequency and histogram synopses.
    pub max_synopsis_value_bytes: usize,
    /// Confidence level in parts per million for the histogram rank-error bound.
    pub histogram_confidence_ppm: u32,
    /// Explicit multi-column groups to analyze. Empty by default to keep cost
    /// proportional to selected groups instead of generating a quadratic set.
    pub multi_column_sets: Vec<Vec<String>>,
}

impl Default for StatisticsConfig {
    fn default() -> Self {
        Self {
            scan_batch_size: 8192,
            distinct_precision: 12,
            max_bound_value_bytes: 4096,
            most_common_capacity: 16,
            histogram_sample_size: 256,
            histogram_bucket_count: 32,
            max_synopsis_value_bytes: 256,
            histogram_confidence_ppm: 950_000,
            multi_column_sets: Vec::new(),
        }
    }
}
