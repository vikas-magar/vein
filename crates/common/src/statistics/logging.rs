//! Bounded, human-readable summaries for query statistics diagnostics.
//!
//! This deliberately omits mergeable sketch payload bytes and histogram sample
//! priorities. It reports the synopsis configuration and estimates needed to
//! understand planning, without logging opaque or row-like internal state.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use catalog::{ColumnStatistics, TableStatistics};
use super::runtime::{runtime_metric_availability, RUNTIME_STATISTIC_METRICS};

/// Formats a table statistics snapshot without exposing raw synopsis payloads.
pub struct TableStatisticsLog<'a>(&'a TableStatistics);

impl<'a> TableStatisticsLog<'a> {
    /// Wraps an existing snapshot by reference; formatting does not clone it.
    pub fn new(statistics: &'a TableStatistics) -> Self {
        Self(statistics)
    }
}

impl Display for TableStatisticsLog<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let statistics = self.0;
        write!(
            f,
            "rows={:?} bytes={:?} files={:?} row_groups={:?} compressed_bytes={:?} uncompressed_bytes={:?}",
            statistics.num_rows,
            statistics.total_bytes,
            statistics.file_count,
            statistics.row_group_count,
            statistics.compressed_bytes,
            statistics.uncompressed_bytes,
        )?;

        let mut columns = statistics.column_statistics.iter().collect::<Vec<_>>();
        columns.sort_unstable_by_key(|(left, _)| *left);
        write!(f, " columns={{")?;
        for (index, (name, column)) in columns.into_iter().enumerate() {
            if index > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{name}: ")?;
            write_column(f, column)?;
        }
        write!(f, "}} metric_coverage={{")?;
        let mut metric_coverage = [0usize; RUNTIME_STATISTIC_METRICS.len()];
        for column in statistics.column_statistics.values() {
            for (count, available) in metric_coverage
                .iter_mut()
                .zip(runtime_metric_availability(column))
            {
                *count += usize::from(available);
            }
        }
        for (index, metric) in RUNTIME_STATISTIC_METRICS.iter().enumerate() {
            if index > 0 {
                write!(f, ",")?;
            }
            write!(
                f,
                "{metric}:{}/{}",
                metric_coverage[index],
                statistics.column_statistics.len()
            )?;
        }
        write!(f, "}} column_groups=[")?;
        for (index, group) in statistics.column_groups.iter().enumerate() {
            if index > 0 {
                write!(f, "; ")?;
            }
            write!(
                f,
                "columns={:?}, ndv={:?}, ndv_error={:?}, sketch={}",
                group.columns,
                group.distinct_count,
                group.distinct_count_relative_error,
                sketch_summary(group.distinct_sketch.as_ref()),
            )?;
        }
        write!(f, "] data_units={}", data_unit_summary(statistics))
    }
}

fn write_column(f: &mut Formatter<'_>, column: &ColumnStatistics) -> fmt::Result {
    let mut separator = "";
    macro_rules! field {
        ($name:literal, $value:expr) => {
            if let Some(value) = $value {
                write!(f, "{}{}={value:?}", separator, $name)?;
                separator = ",";
            }
        };
    }
    write!(f, "{{")?;
    field!("nulls", column.null_count);
    field!("ndv", column.distinct_count);
    field!("ndv_error", column.distinct_count_relative_error);
    field!("nans", column.nan_count);
    if let (Some(min), Some(max)) = (&column.min_value, &column.max_value) {
        write!(f, "{separator}range={min:?}..{max:?}")?;
        separator = ",";
    }
    field!("bounds_complete", column.min_max_complete);
    if let Some(numeric) = &column.numeric {
        write!(f, "{separator}numeric={numeric:?}")?;
        separator = ",";
    }
    if let Some(lengths) = &column.byte_length {
        write!(f, "{separator}byte_length={lengths:?}")?;
        separator = ",";
    }
    if let Some(sketch) = &column.distinct_sketch {
        write!(f, "{separator}sketch={}", sketch_summary(Some(sketch)))?;
        separator = ",";
    }

    if let Some(mcv) = &column.most_common_values {
        let top_candidate = mcv
            .candidates
            .first()
            .map(|candidate| format!("{:?}:{}", candidate.value, candidate.lower_count));
        write!(
            f,
            "{separator}mcv={}/{}, analyzed={}, error={}, top={:?}",
            mcv.candidates.len(),
            mcv.capacity,
            mcv.analyzed_count,
            mcv.error_bound,
            top_candidate,
        )?;
        separator = ",";
    }

    if let Some(histogram) = &column.histogram {
        write!(
            f,
            "{separator}hist=population:{},sample:{}/{},buckets:{}/{},confidence_ppm:{},rank_error:{:?},range:{:?}..{:?}",
            histogram.population_count,
            histogram.sample_count,
            histogram.sample_capacity,
            histogram.buckets.len(),
            histogram.bucket_count,
            histogram.confidence_ppm,
            histogram.rank_error_bound,
            histogram.buckets.first().map(|bucket| &bucket.lower_bound),
            histogram.buckets.last().map(|bucket| &bucket.upper_bound),
        )?;
    }
    write!(f, "}}")
}

fn sketch_summary(sketch: Option<&catalog::SerializedSketch>) -> String {
    sketch.map_or_else(
        || "unavailable".to_owned(),
        |sketch| {
            format!(
                "{}@{}(parameter_bytes={},state_bytes={})",
                sketch.algorithm,
                sketch.version,
                sketch.parameters.len(),
                sketch.payload.len(),
            )
        },
    )
}

fn data_unit_summary(statistics: &TableStatistics) -> String {
    let mut kinds = BTreeMap::<String, usize>::new();
    let mut units_with_column_stats = 0usize;
    let mut units_with_storage_stats = 0usize;
    let mut bloom_columns = 0usize;
    let mut dictionary_columns = 0usize;
    let mut column_index_columns = 0usize;
    let mut offset_index_columns = 0usize;
    for unit in &statistics.data_units {
        *kinds.entry(format!("{:?}", unit.kind)).or_default() += 1;
        units_with_column_stats += usize::from(!unit.column_statistics.is_empty());
        units_with_storage_stats += usize::from(!unit.column_storage.is_empty());
        for column in unit.column_storage.values() {
            bloom_columns += usize::from(column.has_bloom_filter == Some(true));
            dictionary_columns += usize::from(column.has_dictionary_page == Some(true));
            column_index_columns += usize::from(column.has_column_index == Some(true));
            offset_index_columns += usize::from(column.has_offset_index == Some(true));
        }
    }
    format!(
        "count={}, kinds={:?}, units_with_column_stats={}, units_with_storage_stats={}, bloom_columns={}, dictionary_columns={}, column_index_columns={}, offset_index_columns={}",
        statistics.data_units.len(),
        kinds,
        units_with_column_stats,
        units_with_storage_stats,
        bloom_columns,
        dictionary_columns,
        column_index_columns,
        offset_index_columns,
    )
}

#[cfg(test)]
mod tests {
    use catalog::{
        ColumnStatistics, HistogramBucket, HistogramSample, HistogramStatistics,
        MostCommonValue, MostCommonValuesStatistics, SerializedSketch, TableStatistics,
    };
    use types::LogicalLiteral;

    use super::TableStatisticsLog;

    #[test]
    fn summary_reports_synopsis_values_without_dumping_sketch_or_sample_payloads() {
        let statistics = TableStatistics {
            num_rows: Some(4),
            column_statistics: [(
                "key".to_owned(),
                ColumnStatistics {
                    distinct_count: Some(3),
                    distinct_sketch: Some(SerializedSketch {
                        algorithm: "hll".to_owned(),
                        version: 1,
                        parameters: vec![1, 2],
                        payload: vec![9; 128],
                    }),
                    most_common_values: Some(MostCommonValuesStatistics {
                        capacity: 2,
                        analyzed_count: 4,
                        error_bound: 1,
                        candidates: vec![MostCommonValue {
                            value: LogicalLiteral::Int64(7),
                            hash: 42,
                            lower_count: 2,
                        }],
                    }),
                    histogram: Some(HistogramStatistics {
                        population_count: 4,
                        sample_count: 1,
                        sample_capacity: 8,
                        bucket_count: 1,
                        confidence_ppm: 950_000,
                        samples: vec![HistogramSample {
                            priority: 123456,
                            tie_breaker: 654321,
                            value: LogicalLiteral::Int64(7),
                        }],
                        buckets: vec![HistogramBucket {
                            lower_bound: LogicalLiteral::Int64(7),
                            upper_bound: LogicalLiteral::Int64(7),
                            sample_count: 1,
                            estimated_count: 4,
                        }],
                        rank_error_bound: Some(0.1),
                    }),
                    ..ColumnStatistics::default()
                },
            )]
                .into_iter()
                .collect(),
            ..TableStatistics::default()
        };

        let summary = TableStatisticsLog::new(&statistics).to_string();
        assert!(summary.contains("ndv=3"));
        assert!(summary.contains("hll@1(parameter_bytes=2,state_bytes=128)"));
        assert!(summary.contains("mcv=1/2, analyzed=4, error=1"));
        assert!(summary.contains("hist=population:4,sample:1/8,buckets:1/1"));
        assert!(summary.contains("range:Some(Int64(7))..Some(Int64(7))"));
        assert!(!summary.contains("123456"));
        assert!(!summary.contains("654321"));
        assert!(!summary.contains("[9, 9, 9"));
    }
}
