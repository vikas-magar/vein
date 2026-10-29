use super::*;

/// A source predicate plan evaluated before projected rows are materialized.
///
/// Stages are applied in order, allowing a selective safe predicate to reduce
/// the input to later stages without materializing conjunction arrays.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPredicate {
    /// Number of logical conjuncts in the source predicate.
    pub predicate_count: usize,
    /// Ordered predicate stages. Each stage only reads its own required columns.
    pub stages: Vec<ScanPredicateStage>,
    /// Format-neutral necessary conditions for skipping data units from exact
    /// source statistics. Unsupported logical expressions remain `Unknown`.
    #[serde(default)]
    pub statistics: Vec<StatisticsPredicate>,
}

/// One compiled conjunction evaluated as a source-level selection stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPredicateStage {
    /// Number of logical conjuncts combined in this stage.
    pub predicate_count: usize,
    /// Whether evaluating this stage in a different order is semantics-safe.
    /// A missing value defaults to false so older serialized plans stay conservative.
    #[serde(default)]
    pub reorder_safe: bool,
    /// Stage input columns in the order addressed by its expression tree.
    pub input: VeinSchemaRef,
    /// Boolean expression evaluated for one source batch.
    pub predicate: PhysicalExpr,
}

/// Self-contained file scan instruction for an Executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileScanTask {
    /// List of URIs/Paths to the files.
    pub files: Vec<String>,
    /// Storage snapshots aligned with `files`; empty when unavailable.
    /// These snapshots let scan providers reuse catalog metadata without
    /// issuing another storage stat request or reconstructing object identity.
    #[serde(default)]
    pub storage_metadata: Vec<StorageFileMetadata>,
    /// Identifier of the registered reader for these files.
    pub data_file_format: DataFileFormatId,
    /// Reader-specific settings. The registered provider validates and interprets these values.
    #[serde(default)]
    pub format_options: Vec<KeyValue>,
    /// Best-effort output encoding requests addressed to projected columns.
    #[serde(default)]
    pub column_encoding_hints: Vec<ColumnEncodingHint>,
    /// Optional byte range within the file.
    pub byte_range: Option<Range<u64>>,
    /// Optional range of reader-defined, independently schedulable units in a
    /// single file. For Parquet this addresses row groups; other scan adapters
    /// may define their own unit ordinals. `None` means scan the whole file.
    #[serde(default)]
    pub scan_unit_range: Option<Range<usize>>,
    /// The projected columns, in the engine's own schema vocabulary.
    ///
    /// Refcounted so splitting one file scan into per-file tasks copies a
    /// handle rather than the schema.
    pub projected: VeinSchemaRef,
    /// Push-down predicate the source evaluates before materializing rows.
    ///
    /// Refcounted for the same reason as `projected`: splitting a scan into
    /// per-file tasks must not duplicate the predicate. `None` means the source
    /// returns every projected row and the residual program filters them, so a
    /// source that ignores this only forgoes an optimization.
    pub pushdown: Option<Arc<ScanPredicate>>,
    /// Task identifier for reporting.
    pub task_id: TaskId,
    /// Catalog snapshot pinned by the planning session.
    pub catalog_snapshot: Option<CatalogSnapshotToken>,
    /// Runtime synopsis collection context, when the planner identified a table scan.
    #[serde(default)]
    pub runtime_statistics: Option<RuntimeStatisticsScan>,
}

/// Stable identity and coverage information for lazy runtime statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStatisticsScan {
    /// Identity shared by all independently scheduled pieces of this scan.
    pub scan_id: u128,
    /// Source revision key used to prevent cross-snapshot statistics reuse.
    pub table_key: String,
    /// Exact row count from the catalog, when available.
    pub expected_rows: Option<u64>,
    /// Whether the plan can prove complete source-row coverage. Column
    /// coverage is independent: projected-column synopses may still be useful.
    pub row_coverage_candidate: bool,
    /// Ordinal of this independently scheduled work unit.
    pub partition_index: usize,
    /// Number of work units that must finish before the synopsis is finalized.
    pub partition_count: usize,
}

/// Read from an infinite stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSource {
    /// E.g., Kafka topic name.
    pub topic: String,
}

/// Source of data for a stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
// Physical plan nodes retain this enum behind an Arc. Keeping the file task
// inline avoids a second allocation for its already heap-backed metadata.
#[allow(clippy::large_enum_variant)]
pub enum DataSource {
    /// Read from static storage (e.g., Parquet).
    File(FileScanTask),
    /// Read from an infinite event stream (e.g., Kafka).
    Stream(StreamSource),
    /// A source that generates data directly (e.g. VALUES).
    Memory,
    /// A query-local relation produced by an earlier adaptive stage.
    ///
    /// The identifier is resolved by the in-process executor registry. The
    /// relation payload itself is deliberately not serialized into the plan.
    Intermediate(IntermediateSourceId),
}
