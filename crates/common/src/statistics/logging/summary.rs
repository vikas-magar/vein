//! Bounded, privacy-safe summaries for query statistics diagnostics.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use crate::statistics::runtime::{RUNTIME_STATISTIC_METRICS, runtime_metric_availability};
use catalog::{ColumnStatistics, TableStatistics};

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
    write!(
        f,
        "{separator}bounds_available={}",
        column.min_value.is_some() && column.max_value.is_some()
    )?;
    separator = ",";
    field!("bounds_complete", column.min_max_complete);
    write!(f, "{separator}numeric_available={}", column.numeric.is_some())?;
    separator = ",";
    write!(
        f,
        "{separator}byte_length_available={}",
        column.byte_length.is_some()
    )?;
    separator = ",";
    if let Some(sketch) = &column.distinct_sketch {
        write!(f, "{separator}sketch={}", sketch_summary(Some(sketch)))?;
        separator = ",";
    }

    if let Some(mcv) = &column.most_common_values {
        write!(
            f,
            "{separator}mcv={}/{}, analyzed={}, error={}",
            mcv.candidates.len(),
            mcv.capacity,
            mcv.analyzed_count,
            mcv.error_bound,
        )?;
        separator = ",";
    }

    if let Some(histogram) = &column.histogram {
        write!(
            f,
            "{separator}hist=population:{},sample:{}/{},buckets:{}/{},confidence_ppm:{},rank_error:{:?}",
            histogram.population_count,
            histogram.sample_count,
            histogram.sample_capacity,
            histogram.buckets.len(),
            histogram.bucket_count,
            histogram.confidence_ppm,
            histogram.rank_error_bound,
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
