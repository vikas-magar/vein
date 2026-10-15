//! Standard table metadata contract shared by every catalog.
//!
//! `TableMetadata` is what catalogs hand to the binder, the cost-based
//! optimizer, and the physical planner: schema, storage layout, physical
//! format, partitioning, clustering/sort ordering, statistics, and extensible
//! properties. Nothing here knows about a specific catalog implementation, so a
//! local file catalog, Iceberg, and Delta all describe a table the same way.

use std::collections::HashMap;
use std::sync::Arc;

use types::{LogicalLiteral, ResolvedColumnRef, VeinSchemaRef};

use crate::snapshot::CatalogSnapshotToken;

/// The kind of table represented in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TableType {
    /// A persistent managed base table.
    #[default]
    Base,
    /// A virtual view defined by a query.
    View,
    /// A temporary session-scoped table.
    Temporary,
    /// An unmanaged external table pointing to external storage paths.
    External,
}

/// The physical storage format of a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TableFormat {
    /// Apache Parquet columnar format.
    Parquet,
    /// Comma-separated values format.
    Csv,
    /// JSON text format.
    Json,
    /// Apache Arrow IPC / Feather streaming format.
    Arrow,
    /// Apache Iceberg open table format.
    Iceberg,
    /// Delta Lake open table format.
    Delta,
    /// Custom or external table format.
    Custom(String),
}

impl TableFormat {
    /// Returns the canonical string representation of this format.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Arrow => "arrow",
            Self::Iceberg => "iceberg",
            Self::Delta => "delta",
            Self::Custom(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for TableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for TableFormat {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "parquet" => Self::Parquet,
            "csv" => Self::Csv,
            "json" => Self::Json,
            "arrow" | "feather" => Self::Arrow,
            "iceberg" => Self::Iceberg,
            "delta" => Self::Delta,
            other => Self::Custom(other.to_string()),
        }
    }
}

impl From<String> for TableFormat {
    fn from(s: String) -> Self {
        TableFormat::from(s.as_str())
    }
}

/// Statistics for an individual column, used by the optimizer for selectivity estimation,
/// join ordering, and min/max data skipping (zone maps).
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct ColumnStatistics {
    /// Number of null values in this column.
    pub null_count: Option<u64>,
    /// Number of distinct values (NDV) in this column for join cardinality estimation.
    pub distinct_count: Option<u64>,
    /// Mergeable state used to estimate NDV across files and data units.
    ///
    /// A catalog must not add local NDV estimates. It may merge compatible
    /// synopsis states and then recompute the table-level estimate.
    pub distinct_sketch: Option<SerializedSketch>,
    /// Relative standard error of distinct_count when it is approximate.
    pub distinct_count_relative_error: Option<f64>,
    /// Number of floating-point NaN values when this column has been analyzed.
    pub nan_count: Option<u64>,
    /// Minimum scalar value in this column for min/max data skipping.
    pub min_value: Option<LogicalLiteral>,
    /// Maximum scalar value in this column for min/max data skipping.
    pub max_value: Option<LogicalLiteral>,
    /// Indicates whether both observed scalar bounds are complete and exact.
    /// `false` means one or more values could not be represented safely.
    pub min_max_complete: Option<bool>,
    /// Approximate numeric moments collected from non-null finite numeric values.
    pub numeric: Option<NumericStatistics>,
    /// Exact byte-length summaries for variable-length values.
    pub byte_length: Option<ByteLengthStatistics>,
    /// Bounded Misra-Gries heavy-hitter candidates.
    #[serde(default)]
    pub most_common_values: Option<MostCommonValuesStatistics>,
    /// Bounded sampled equi-depth histogram for ordered scalar values.
    #[serde(default)]
    pub histogram: Option<HistogramStatistics>,
}

impl ColumnStatistics {
    /// Checks internal count, bound, and synopsis relationships against the
    /// table row count. This validation is shared by catalog loading and
    /// append-time restoration, so a consumer never accepts a contradictory
    /// partial summary as table-wide statistics.
    pub fn is_consistent_with_rows(&self, rows: u64) -> bool {
        let Some(nulls) = self.null_count else {
            return false;
        };
        if nulls > rows {
            return false;
        }
        let non_nulls = rows - nulls;
        if self.distinct_count.is_some_and(|count| count > non_nulls)
            || (self.distinct_count.is_none() && self.distinct_count_relative_error.is_some())
            || self.distinct_count_relative_error.is_some_and(|error| {
            !error.is_finite() || !(0.0..=1.0).contains(&error)
        })
            || self.nan_count.is_some_and(|count| count > non_nulls)
        {
            return false;
        }

        match (&self.min_value, &self.max_value, self.min_max_complete) {
            (None, None, Some(true))
            if non_nulls == 0 || self.nan_count == Some(non_nulls) => {}
            (None, None, Some(false) | None) => {}
            (Some(_), Some(_), Some(false)) => return false,
            (Some(min), Some(max), _) if min.try_cmp(max).is_some_and(|order| !order.is_gt()) => {}
            _ => return false,
        }

        if let Some(numeric) = &self.numeric {
            if numeric.finite_count.checked_add(numeric.non_finite_count) != Some(non_nulls)
                || numeric.sum.is_some_and(|sum| !sum.is_finite())
                || numeric.mean.is_some_and(|mean| !mean.is_finite())
                || numeric.m2.is_some_and(|m2| !m2.is_finite() || m2 < 0.0)
                || self.nan_count.is_some_and(|count| count > numeric.non_finite_count)
                || (numeric.finite_count == 0
                && (numeric.sum != Some(0.0)
                || numeric.mean.is_some()
                || numeric.m2.is_some()))
                || (numeric.finite_count > 0
                && (numeric.mean.is_none() || numeric.m2.is_none()))
            {
                return false;
            }
        }

        if let Some(lengths) = &self.byte_length {
            if lengths.value_count != non_nulls
                || (lengths.value_count == 0
                && (lengths.total_bytes != Some(0)
                || lengths.min_bytes.is_some()
                || lengths.max_bytes.is_some()))
                || (lengths.value_count > 0
                && (lengths.min_bytes.is_none() || lengths.max_bytes.is_none()))
                || lengths
                .min_bytes
                .zip(lengths.max_bytes)
                .is_some_and(|(min, max)| min > max)
                || lengths
                .total_bytes
                .zip(lengths.min_bytes)
                .is_some_and(|(total, min)| {
                    lengths.value_count > 0
                        && min
                        .checked_mul(lengths.value_count)
                        .is_none_or(|lower| total < lower)
                })
                || lengths
                .total_bytes
                .zip(lengths.max_bytes)
                .is_some_and(|(total, max)| {
                    lengths.value_count > 0
                        && max
                        .checked_mul(lengths.value_count)
                        .is_some_and(|upper| total > upper)
                })
            {
                return false;
            }
        }

        if let Some(most_common) = &self.most_common_values {
            let lower_count_sum = most_common
                .candidates
                .iter()
                .try_fold(0_u64, |total, candidate| total.checked_add(candidate.lower_count));
            let has_duplicate_candidates = most_common.candidates.iter().enumerate().any(|(index, candidate)| {
                most_common.candidates[..index].iter().any(|prior| {
                    prior.hash == candidate.hash
                        && prior.value.try_cmp(&candidate.value).is_some_and(|order| order.is_eq())
                })
            });
            if most_common.capacity == 0
                || most_common.candidates.len() > most_common.capacity
                || most_common.analyzed_count > non_nulls
                || self.nan_count.is_some_and(|nans| {
                    most_common.analyzed_count != non_nulls.saturating_sub(nans)
                })
                || most_common.error_bound > most_common.analyzed_count
                || lower_count_sum.is_none_or(|sum| sum > most_common.analyzed_count)
                || has_duplicate_candidates
                || most_common.candidates.windows(2).any(|pair| {
                    pair[0].lower_count < pair[1].lower_count
                })
                || most_common.candidates.iter().any(|candidate| {
                    candidate.lower_count == 0 || candidate.lower_count > most_common.analyzed_count
                })
            {
                return false;
            }
        }

        if let Some(histogram) = &self.histogram {
            let estimated_rows = histogram.buckets.iter().try_fold(0_u64, |total, bucket| {
                total.checked_add(bucket.estimated_count)
            });
            let sample_capacity = u64::try_from(histogram.sample_capacity).ok();
            let expected_sample_count = sample_capacity.map(|capacity| histogram.population_count.min(capacity));
            let expected_bucket_count = histogram.bucket_count.min(histogram.sample_count as usize);
            let samples_ordered = histogram.samples.windows(2).all(|pair| {
                pair[0].value.try_cmp(&pair[1].value).is_some_and(|order| !order.is_gt())
            });
            let priorities_unique = histogram.samples.iter().enumerate().all(|(index, sample)| {
                !histogram.samples[..index].iter().any(|prior| {
                    (prior.priority, prior.tie_breaker) == (sample.priority, sample.tie_breaker)
                })
            });
            let buckets_match_sample = histogram.buckets.iter().enumerate().all(|(bucket_index, bucket)| {
                let sample_count = histogram.sample_count as usize;
                if sample_count == 0 || expected_bucket_count == 0 {
                    return false;
                }
                let start = bucket_index * sample_count / expected_bucket_count;
                let end = (bucket_index + 1) * sample_count / expected_bucket_count;
                let Some(first) = histogram.samples.get(start) else { return false };
                let Some(last) = histogram.samples.get(end - 1) else { return false };
                let expected_estimated = ((histogram.population_count as u128 * end as u128)
                    / sample_count as u128)
                    .saturating_sub((histogram.population_count as u128 * start as u128)
                        / sample_count as u128) as u64;
                bucket.sample_count == (end - start) as u64
                    && bucket.estimated_count == expected_estimated
                    && bucket.lower_bound.try_cmp(&first.value).is_some_and(|order| order.is_eq())
                    && bucket.upper_bound.try_cmp(&last.value).is_some_and(|order| order.is_eq())
            });
            let expected_rank_error = (histogram.sample_count > 0).then(|| {
                let confidence = f64::from(histogram.confidence_ppm) / 1_000_000.0;
                ((2.0 / (1.0 - confidence)).ln()
                    / (2.0 * histogram.sample_count as f64))
                    .sqrt()
                    .min(1.0)
            });
            if histogram.population_count > non_nulls
                || histogram.sample_capacity == 0
                || histogram.bucket_count == 0
                || !(1..1_000_000).contains(&histogram.confidence_ppm)
                || histogram.samples.len() > histogram.sample_capacity
                || histogram.samples.len() as u64 != histogram.sample_count
                || histogram.sample_count > histogram.population_count
                || expected_sample_count != Some(histogram.sample_count)
                || histogram.buckets.is_empty() != (histogram.population_count == 0)
                || histogram.buckets.len() != expected_bucket_count
                || estimated_rows != Some(histogram.population_count)
                || !samples_ordered
                || !priorities_unique
                || !buckets_match_sample
                || histogram.rank_error_bound != expected_rank_error
                || histogram.rank_error_bound.is_some_and(|error| {
                    !error.is_finite() || !(0.0..=1.0).contains(&error)
                })
            {
                return false;
            }
        }

        true
    }
}

/// One frequent-value candidate and its guaranteed observed lower frequency.
///
/// The value is retained only when it fits the configured synopsis value budget.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MostCommonValue {
    /// Owned, bounded candidate value.
    pub value: LogicalLiteral,
    /// Stable typed value hash used to find candidates without copying row data.
    pub hash: u64,
    /// Lower bound on the candidate's true frequency.
    pub lower_count: u64,
}

/// Bounded mergeable frequent-value synopsis for a column.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct MostCommonValuesStatistics {
    /// Maximum number of candidate values retained.
    pub capacity: usize,
    /// Non-null rows offered to this synopsis.
    pub analyzed_count: u64,
    /// Total Misra-Gries decrement count; bounds per-candidate undercount.
    pub error_bound: u64,
    /// Candidate values ordered by decreasing lower frequency.
    pub candidates: Vec<MostCommonValue>,
}

/// One bounded sample value retained by the mergeable histogram synopsis.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HistogramSample {
    /// Independent random priority used to retain a uniform bottom-k sample.
    pub priority: u64,
    /// Independent tie breaker so equal priorities remain totally ordered.
    #[serde(default)]
    pub tie_breaker: u64,
    /// Owned scalar value sampled from the input.
    pub value: LogicalLiteral,
}

/// One equal-depth histogram bucket.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HistogramBucket {
    /// Smallest sampled value assigned to this bucket.
    pub lower_bound: LogicalLiteral,
    /// Largest sampled value assigned to this bucket.
    pub upper_bound: LogicalLiteral,
    /// Number of sampled observations assigned to this bucket.
    pub sample_count: u64,
    /// Population count estimated from the uniform sample.
    pub estimated_count: u64,
}

/// Mergeable bounded sample and equi-depth histogram for ordered values.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct HistogramStatistics {
    /// Non-null, non-NaN population represented by this synopsis.
    pub population_count: u64,
    /// Number of retained sample values.
    pub sample_count: u64,
    /// Maximum number of values retained in the reservoir.
    pub sample_capacity: usize,
    /// Maximum number of equal-depth buckets derived from the sample.
    #[serde(default)]
    pub bucket_count: usize,
    /// Confidence level in parts per million for the rank-error bound.
    #[serde(default)]
    pub confidence_ppm: u32,
    /// Uniform bottom-k sample required to merge append and parallel partitions.
    pub samples: Vec<HistogramSample>,
    /// Equi-depth buckets derived from `samples`.
    pub buckets: Vec<HistogramBucket>,
    /// Dvoretzky-Kiefer-Wolfowitz 95% rank-error bound for the sample.
    pub rank_error_bound: Option<f64>,
}

/// Joint distinct-value synopsis for a configured set of columns.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct ColumnGroupStatistics {
    /// Column names in the canonical schema order used to hash the tuple.
    pub columns: Vec<String>,
    /// Estimated distinct tuples, including tuples with null components.
    pub distinct_count: Option<u64>,
    /// Relative standard error when the estimate is approximate.
    pub distinct_count_relative_error: Option<f64>,
    /// Mergeable joint NDV sketch.
    pub distinct_sketch: Option<SerializedSketch>,
}

/// Mergeable finite-number moments represented with floating-point arithmetic.
///
/// These values support cost models and diagnostics; they are approximate and
/// must not be used as proof for row pruning or semantic rewrites.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct NumericStatistics {
    /// Number of finite, non-null numeric inputs included in these moments.
    pub finite_count: u64,
    /// Number of non-null numeric values excluded from moments because they
    /// were NaN or infinite.
    pub non_finite_count: u64,
    /// Approximate sum, or `None` if it overflowed.
    pub sum: Option<f64>,
    /// Approximate arithmetic mean for the included values.
    pub mean: Option<f64>,
    /// Approximate sum of squared deviations from the mean (Welford M2).
    pub m2: Option<f64>,
}

/// Exact byte-length summaries for strings or binary values.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct ByteLengthStatistics {
    /// Number of non-null values measured.
    pub value_count: u64,
    /// Sum of value lengths, or `None` if the sum overflowed.
    pub total_bytes: Option<u64>,
    /// Smallest measured value length in bytes.
    pub min_bytes: Option<u64>,
    /// Largest measured value length in bytes.
    pub max_bytes: Option<u64>,
    /// Whether any UTF-8 string contained non-ASCII characters; `None` for
    /// binary data or when the source does not report this property.
    pub contains_non_ascii: Option<bool>,
}

/// Versioned, format-neutral state for a mergeable statistical synopsis.
///
/// The algorithm, version, and parameters identify the decoder. Unknown
/// synopsis kinds remain safely ignorable by readers; payload is owned by the
/// synopsis implementation. This keeps catalog metadata independent of Arrow
/// readers and storage formats.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct SerializedSketch {
    /// Stable synopsis algorithm identifier.
    pub algorithm: String,
    /// Version of the serialized state format.
    pub version: u16,
    /// Algorithm-specific configuration bytes.
    pub parameters: Vec<u8>,
    /// Algorithm-specific state bytes.
    pub payload: Vec<u8>,
}

/// Physical metadata for one column inside a data unit.
///
/// Every value is scoped to the containing [`DataUnitStatistics`]. It describes
/// storage layout and does not imply anything about the logical table as a whole.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct ColumnStorageStatistics {
    /// Number of physical values recorded in the column chunk, when provided.
    pub num_values: Option<u64>,
    /// Compressed bytes for this column chunk, when provided.
    pub compressed_bytes: Option<u64>,
    /// Uncompressed bytes for this column chunk, when provided.
    pub uncompressed_bytes: Option<u64>,
    /// Offset of the first data page in the containing file or object.
    pub data_page_offset: Option<u64>,
    /// Offset of the dictionary page in the containing file or object.
    pub dictionary_page_offset: Option<u64>,
    /// Offset of the legacy index page, when present.
    pub index_page_offset: Option<u64>,
    /// Offset of the bloom filter, when present.
    pub bloom_filter_offset: Option<u64>,
    /// Offset of the column index, when present.
    pub column_index_offset: Option<u64>,
    /// Offset of the offset index, when present.
    pub offset_index_offset: Option<u64>,
    /// Encoded byte-array bytes before compression, when reported.
    pub unencoded_byte_array_bytes: Option<u64>,
    /// Storage codec name, when provided by the format.
    pub compression: Option<String>,
    /// Encodings used by this data unit.
    pub encodings: Vec<String>,
    /// Whether the footer reports statistics for this column chunk.
    pub has_statistics: Option<bool>,
    /// Whether the footer reports a dictionary page.
    pub has_dictionary_page: Option<bool>,
    /// Whether the footer reports a bloom filter.
    pub has_bloom_filter: Option<bool>,
    /// Whether the footer reports a column index.
    pub has_column_index: Option<bool>,
    /// Whether the footer reports an offset index.
    pub has_offset_index: Option<bool>,
    /// Counts of values by repetition level for nested columns, when reported.
    pub repetition_level_histogram: Option<Vec<i64>>,
    /// Counts of values by definition level for nested columns, when reported.
    pub definition_level_histogram: Option<Vec<i64>>,
}

/// Kind of independently addressable data unit described by statistics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum StatisticsUnitKind {
    /// A physical file or object.
    File,
    /// A row group or equivalent storage-format scan unit.
    RowGroup,
    /// A logical partition, such as a directory or manifest partition.
    Partition,
    /// A smaller physical unit such as a page or block.
    Page,
    /// A catalog-specific unit not covered by the common variants.
    Custom(String),
}

/// Statistics and storage facts for one file, row group, partition, or other unit.
///
/// `parent` is an index into the containing [`TableStatistics::data_units`] list.
/// Parents precede their children. `ordinal` is the unit's stable position among
/// siblings. Column statistics are always local to this unit; in particular, a
/// row group's distinct count is not a table-level distinct count.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DataUnitStatistics {
    /// Unit kind in the generic catalog contract.
    pub kind: StatisticsUnitKind,
    /// Index of this unit's parent in [`TableStatistics::data_units`].
    pub parent: Option<usize>,
    /// Stable ordinal among units with the same parent and kind.
    pub ordinal: usize,
    /// Rows in this unit, when known.
    pub num_rows: Option<u64>,
    /// Physical bytes occupied by the complete unit, when known.
    pub physical_bytes: Option<u64>,
    /// Compressed payload bytes, when known.
    pub compressed_bytes: Option<u64>,
    /// Uncompressed payload bytes, when known.
    pub uncompressed_bytes: Option<u64>,
    /// Column value statistics scoped to this unit and keyed by column name.
    pub column_statistics: HashMap<String, ColumnStatistics>,
    /// Per-column storage facts scoped to this unit and keyed by column name.
    pub column_storage: HashMap<String, ColumnStorageStatistics>,
}

impl DataUnitStatistics {
    /// Creates an empty statistics record for one addressable data unit.
    pub fn new(kind: StatisticsUnitKind, ordinal: usize) -> Self {
        Self {
            kind,
            parent: None,
            ordinal,
            num_rows: None,
            physical_bytes: None,
            compressed_bytes: None,
            uncompressed_bytes: None,
            column_statistics: HashMap::new(),
            column_storage: HashMap::new(),
        }
    }
}

/// Aggregate statistics for a table, consumed by the Cost-Based Optimizer (CBO).
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct TableStatistics {
    /// Total estimated or exact row count.
    pub num_rows: Option<u64>,
    /// Total estimated or exact physical storage size in bytes.
    pub total_bytes: Option<u64>,
    /// Number of physical files or objects in the table snapshot.
    pub file_count: Option<u64>,
    /// Number of row groups or equivalent units in the table snapshot.
    pub row_group_count: Option<u64>,
    /// Total compressed column payload bytes, when known.
    pub compressed_bytes: Option<u64>,
    /// Total uncompressed column payload bytes, when known.
    pub uncompressed_bytes: Option<u64>,
    /// Per-column statistics keyed by column name.
    pub column_statistics: HashMap<String, ColumnStatistics>,
    /// Joint statistics for configured column sets.
    #[serde(default)]
    pub column_groups: Vec<ColumnGroupStatistics>,
    /// File, row-group, partition, or other scoped statistics in parent-first order.
    pub data_units: Vec<DataUnitStatistics>,
}

/// Opaque identity for one source object included in analyzed statistics.
///
/// The identifier is a stable catalog URI or path. Version is supplied by the
/// storage adapter and may represent an ETag, generation, or local file
/// metadata fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StatisticsSourceIdentity {
    /// Stable source object identifier.
    pub identifier: String,
    /// Adapter-provided version token.
    pub version: String,
}

/// Table-level statistics analyzed from values for one source snapshot.
///
/// This value is independent of the source format. Catalog adapters validate
/// its revision, schema fingerprint, and source identities before publishing it
/// to the optimizer.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AnalyzedTableStatistics {
    /// Catalog revision the synopsis describes.
    pub source_revision: u64,
    /// Stable logical schema fingerprint.
    pub schema_fingerprint: String,
    /// Source identities used to reject stale analyzed statistics.
    pub source_identities: Vec<StatisticsSourceIdentity>,
    /// Mergeable table and column summaries.
    pub statistics: TableStatistics,
}

/// One column of a table's physical sort or clustering order.
///
/// The order is recorded against resolved columns rather than planner
/// expressions: clustering is a property of the stored data, and keeping the
/// expression tree out of the contract lets a catalog describe layout without
/// depending on the planner.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ColumnSortKey {
    /// The sorted column.
    pub column: ResolvedColumnRef,
    /// Whether the column is sorted ascending (`true`) or descending (`false`).
    pub ascending: bool,
    /// Whether nulls sort before (`true`) or after (`false`) the non-null values.
    pub nulls_first: bool,
}

/// Authoritative metadata for a table in the catalog.
///
/// Encapsulates all relational, physical, partitioning, and statistical properties
/// needed for query analysis, logical optimization, and physical plan generation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableMetadata {
    /// The unique or qualified name of the table.
    pub name: String,
    /// The authoritative schema of the table.
    pub schema: VeinSchemaRef,
    /// The table kind (Base, View, Temporary, External).
    pub table_type: TableType,
    /// Storage root location or base URI (e.g. "vfs://input", "s3://bucket/path").
    pub location: Option<String>,
    /// Physical table format (e.g. `TableFormat::Exo`, `TableFormat::Parquet`).
    pub format: Option<TableFormat>,
    /// Explicit physical file paths or partition URIs to scan.
    pub files: Vec<String>,
    /// Partition column names used for partition pruning in the optimizer.
    pub partition_columns: Vec<String>,
    /// Physical sort or clustering order for sort elimination and streaming execution.
    pub sort_order: Vec<ColumnSortKey>,
    /// Statistical metadata for Cost-Based Optimization (CBO).
    pub statistics: Option<TableStatistics>,
    /// Catalog snapshot pinned for planning and execution.
    pub catalog_snapshot: Option<CatalogSnapshotToken>,
    /// Extensible key-value properties (e.g. compression codec, snapshot IDs, options).
    pub properties: HashMap<String, String>,
    /// Optional human-readable table description or comment.
    pub comment: Option<String>,
}

/// Shared reference to a [`TableMetadata`].
pub type TableMetadataRef = Arc<TableMetadata>;

impl TableMetadata {
    /// Creates a new `TableMetadata` with default properties for a given table name and schema.
    pub fn new(name: impl Into<String>, schema: VeinSchemaRef) -> Self {
        Self {
            name: name.into(),
            schema,
            table_type: TableType::Base,
            location: None,
            format: None,
            files: Vec::new(),
            partition_columns: Vec::new(),
            sort_order: Vec::new(),
            statistics: None,
            catalog_snapshot: None,
            properties: HashMap::new(),
            comment: None,
        }
    }

    /// Pins the table to a catalog snapshot for planning and execution.
    pub fn with_catalog_snapshot(mut self, snapshot: CatalogSnapshotToken) -> Self {
        self.catalog_snapshot = Some(snapshot);
        self
    }

    /// Sets the table type.
    pub fn with_table_type(mut self, table_type: TableType) -> Self {
        self.table_type = table_type;
        self
    }

    /// Sets the physical storage location.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Sets the physical storage format.
    pub fn with_format(mut self, format: impl Into<TableFormat>) -> Self {
        self.format = Some(format.into());
        self
    }

    /// Sets the physical files or partitions.
    pub fn with_files(mut self, files: Vec<String>) -> Self {
        self.files = files;
        self
    }

    /// Sets the partition columns for partition pruning.
    pub fn with_partition_columns(mut self, columns: Vec<String>) -> Self {
        self.partition_columns = columns;
        self
    }

    /// Sets the physical sort order for sort elimination.
    pub fn with_sort_order(mut self, sort_order: Vec<ColumnSortKey>) -> Self {
        self.sort_order = sort_order;
        self
    }

    /// Sets table statistics for Cost-Based Optimization.
    pub fn with_statistics(mut self, statistics: TableStatistics) -> Self {
        self.statistics = Some(statistics);
        self
    }

    /// Adds a key-value property.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Sets the table comment.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }
}
