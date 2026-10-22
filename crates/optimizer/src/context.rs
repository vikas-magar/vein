//! Shared optimizer configuration, context, and execution diagnostics.

use std::collections::HashMap;
use std::sync::Arc;

use crate::estimation::{CardinalityEstimator, NodeEstimate, StatisticsEstimator};
use common::statistics::QueryStatisticsContext;
use plan::{TableMetadata, TableStatistics};

mod diagnostics;
mod predicates;
pub use predicates::{PredicateLocation, PredicatePlacement, PredicateRetention};
pub(crate) use predicates::PredicateLineage;
mod filter_placement;
pub(crate) use filter_placement::FilterPlacementVisibility;
pub use diagnostics::{
    JoinCardinalityAssumption, JoinCostBreakdown, JoinOrderAlternative, JoinOrderDecision,
    JoinOrderOutcome, OptimizationDiagnostics, OptimizerPhase, RuleExecution,
};

/// Optimizer execution configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerConfig {
    /// Maximum number of complete rule sweeps before optimization fails.
    pub max_sweeps: usize,
    /// Bounds for statistics-driven join-order search.
    pub join_order: JoinOrderConfig,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_sweeps: 8,
            join_order: JoinOrderConfig::default(),
        }
    }
}

/// Resource limits for exact join-order enumeration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOrderConfig {
    /// Maximum relations for exact search; larger safe regions use bounded greedy search.
    pub max_relations: usize,
    /// Maximum number of subset splits examined by one region.
    pub max_candidate_splits: usize,
}

impl Default for JoinOrderConfig {
    fn default() -> Self {
        Self {
            max_relations: 8,
            max_candidate_splits: 10_000,
        }
    }
}

/// Freshness supplied by the statistics owner, independently of its estimates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsFreshness {
    /// The provider validated the snapshot against the current table revision.
    Current,
    /// Catalog statistics do not carry a freshness guarantee.
    Unverified,
    /// The provider knows the snapshot no longer describes the table.
    Stale,
}

/// Borrowed statistics with explicit freshness; no metadata or bounds are cloned.
#[derive(Debug, Clone, Copy)]
pub struct StatisticsSnapshot<'a> {
    /// Statistics owned by the catalog or provider.
    pub statistics: &'a TableStatistics,
    /// The provider's freshness assessment.
    pub freshness: StatisticsFreshness,
}

/// Supplies borrowed table statistics to optimizer models.
pub trait StatisticsProvider: Send + Sync {
    /// Returns statistics for a table when available.
    fn table_statistics<'a>(&'a self, table: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>>;
    /// Complete statistics for a query-local checkpoint. Column keys are
    /// query-local ColumnIds rather than potentially ambiguous display names.
    fn intermediate_statistics(&self, _source_id: u128) -> Option<&TableStatistics> { None }
}

/// Reads statistics directly from catalog table metadata.
#[derive(Debug, Default)]
pub struct CatalogStatisticsProvider;

impl StatisticsProvider for CatalogStatisticsProvider {
    fn table_statistics<'a>(&'a self, table: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>> {
        table
            .statistics
            .as_ref()
            .map(|statistics| StatisticsSnapshot {
                statistics,
                freshness: StatisticsFreshness::Unverified,
            })
    }
}

/// Reads the source snapshot selected for one query, then falls back to the
/// metadata attached to the bound table when that source was not registered.
#[derive(Clone)]
pub struct QueryStatisticsProvider {
    context: Arc<QueryStatisticsContext>,
    runtime_statistics: HashMap<String, Arc<TableStatistics>>,
    intermediates: HashMap<u128, Arc<TableStatistics>>,
}

impl QueryStatisticsProvider {
    /// Creates an optimizer provider backed by a shared query statistics context.
    pub fn new(context: Arc<QueryStatisticsContext>) -> Self {
        let snapshot = context.snapshot();
        let mut runtime_statistics = HashMap::new();
        for source in context.sources() {
            let observations = snapshot.scan_statistics(&source.source_key);
            let complete_observations = observations
                .iter()
                .filter(|observation| observation.complete_table_snapshot)
                .count();
            let runtime = snapshot.complete_scan_statistics(&source.source_key);
            let merged = runtime.as_ref().map(|runtime| {
                Arc::new(merge_runtime_statistics(
                    source.statistics.as_deref(),
                    runtime.as_ref(),
                ))
            });
            let used_statistics = merged.as_deref().or(source.statistics.as_deref());

            tracing::info!(
                target: "vein.statistics",
                event = "optimizer_statistics_snapshot",
                table = %source.table_name,
                source_key = %source.source_key,
                source_statistics_available = source.statistics.is_some(),
                source_rows = ?source.statistics.as_deref().and_then(|statistics| statistics.num_rows),
                runtime_scan_observations = observations.len(),
                complete_runtime_observations = complete_observations,
                runtime_snapshot_accepted = runtime.is_some(),
                optimizer_statistics_origin = if merged.is_some() { "runtime_merged" } else { "pinned_source" },
                runtime_rows = ?runtime.as_deref().and_then(|statistics| statistics.num_rows),
                optimizer_rows = ?used_statistics.and_then(|statistics| statistics.num_rows),
                optimizer_synopsis_columns = used_statistics.map_or(0, |statistics| statistics.column_statistics.len()),
                optimizer_ndv_columns = used_statistics.map_or(0, |statistics| {
                    statistics.column_statistics.values().filter(|column| column.distinct_count.is_some()).count()
                }),
                optimizer_mcv_columns = used_statistics.map_or(0, |statistics| {
                    statistics.column_statistics.values().filter(|column| column.most_common_values.is_some()).count()
                }),
                optimizer_histogram_columns = used_statistics.map_or(0, |statistics| {
                    statistics.column_statistics.values().filter(|column| column.histogram.is_some()).count()
                }),
                "optimizer statistics snapshot"
            );

            if let Some(merged) = merged {
                runtime_statistics.insert(source.source_key.clone(), merged);
            }
        }
        Self {
            context,
            runtime_statistics,
            intermediates: snapshot.intermediates,
        }
    }
}

impl StatisticsProvider for QueryStatisticsProvider {
    fn intermediate_statistics(&self, source_id: u128) -> Option<&TableStatistics> {
        self.intermediates.get(&source_id).map(Arc::as_ref)
    }
    fn table_statistics<'a>(&'a self, table: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>> {
        let source_key = match table.catalog_snapshot.as_ref() {
            Some(snapshot) => {
                common::statistics::runtime::table_statistics_key(snapshot)
            }
            None => table.name.clone(),
        };
        self.runtime_statistics
            .get(&source_key)
            .map(|statistics| StatisticsSnapshot {
                statistics: statistics.as_ref(),
                freshness: StatisticsFreshness::Current,
            })
            .or_else(|| {
                self.context
                    .source_statistics(&source_key)
                    .map(|statistics| StatisticsSnapshot {
                        statistics,
                        freshness: StatisticsFreshness::Current,
                    })
            })
            .or_else(|| CatalogStatisticsProvider.table_statistics(table))
    }
}

/// Adds only query-local synopses whose scan coverage was proven complete.
/// Catalog values remain preferred when present, except row count which is
/// exact in a completed scan of the pinned source revision.
fn merge_runtime_statistics(
    catalog: Option<&TableStatistics>,
    runtime: &TableStatistics,
) -> TableStatistics {
    let mut merged = catalog.cloned().unwrap_or_default();
    if runtime.num_rows.is_some() {
        merged.num_rows = runtime.num_rows;
    }
    for (name, runtime_column) in &runtime.column_statistics {
        let column = merged.column_statistics.entry(name.clone()).or_default();
        macro_rules! fill_missing {
            ($($field:ident),+ $(,)?) => {
                $(if column.$field.is_none() {
                    column.$field.clone_from(&runtime_column.$field);
                })+
            };
        }
        fill_missing!(
            null_count,
            distinct_count,
            distinct_sketch,
            distinct_count_relative_error,
            nan_count,
            min_value,
            max_value,
            min_max_complete,
            numeric,
            byte_length,
            most_common_values,
            histogram,
        );
    }
    if merged.column_groups.is_empty() {
        merged.column_groups.clone_from(&runtime.column_groups);
    }
    merged
}

/// Shared context supplied to optimizer rules.
#[derive(Clone)]
pub struct OptimizerContext {
    config: Arc<OptimizerConfig>,
    statistics: Arc<dyn StatisticsProvider>,
}

impl OptimizerContext {
    /// Borrows the complete population measured for a query-local checkpoint.
    pub fn intermediate_statistics(&self, source_id: u128) -> Option<&TableStatistics> {
        self.statistics.intermediate_statistics(source_id)
    }
    /// Creates a context using catalog-provided table statistics.
    pub fn new(config: OptimizerConfig) -> Self {
        Self::with_statistics_provider(config, Arc::new(CatalogStatisticsProvider))
    }

    /// Creates a context with a custom statistics provider.
    pub fn with_statistics_provider(
        config: OptimizerConfig,
        statistics: Arc<dyn StatisticsProvider>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            statistics,
        }
    }

    /// Returns the optimizer configuration.
    pub fn config(&self) -> &OptimizerConfig {
        self.config.as_ref()
    }

    /// Returns statistics for a table when available.
    pub fn table_statistics<'a>(
        &'a self,
        table: &'a TableMetadata,
    ) -> Option<StatisticsSnapshot<'a>> {
        self.statistics.table_statistics(table)
    }

    /// Estimates the authoritative logical tree using the shared provider.
    pub fn estimate_plan(&self, plan: &plan::LogicalPlan) -> Vec<NodeEstimate> {
        StatisticsEstimator::new(self.statistics.as_ref()).estimate_plan(plan)
    }

    pub(crate) fn estimate_relation<'a>(
        &'a self,
        plan: &'a plan::LogicalPlan,
    ) -> crate::estimation::RelationEstimate<'a> {
        StatisticsEstimator::new(self.statistics.as_ref()).estimate_relation(plan)
    }
}

impl Default for OptimizerContext {
    fn default() -> Self {
        Self::new(OptimizerConfig::default())
    }
}

#[cfg(test)]
mod query_statistics_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use catalog::{CatalogSnapshotToken, ColumnStatistics, TableStatistics};
    use common::statistics::{
        QuerySourceStatistics, QueryStatisticsContext, RuntimeStatisticsObservation,
    };
    use types::VeinSchema;

    use super::{QueryStatisticsProvider, StatisticsFreshness, StatisticsProvider};

    #[test]
    fn revision_mismatch_does_not_use_same_name_runtime_statistics() {
        let context = Arc::new(QueryStatisticsContext::new([QuerySourceStatistics {
            source_key: "old-revision".into(),
            table_name: "orders".into(),
            statistics: Some(Arc::new(TableStatistics {
                num_rows: Some(1),
                ..TableStatistics::default()
            })),
        }]));
        let provider = QueryStatisticsProvider::new(context);
        let mut table = catalog::TableMetadata::new("orders", Arc::new(VeinSchema::empty()))
            .with_statistics(TableStatistics {
                num_rows: Some(100),
                ..TableStatistics::default()
            });
        table.catalog_snapshot = Some(CatalogSnapshotToken::new(
            "orders",
            "catalog-v2",
            "new-revision",
            1,
        ));

        let snapshot = provider
            .table_statistics(&table)
            .expect("catalog statistics remain available as fallback");
        assert_eq!(snapshot.statistics.num_rows, Some(100));
        assert_eq!(snapshot.freshness, StatisticsFreshness::Unverified);
    }

    #[test]
    fn complete_query_runtime_synopses_feed_optimizer_statistics() {
        let snapshot = CatalogSnapshotToken::new("orders", "catalog-v1", "revision-1", 1);
        let source_key = common::statistics::runtime::table_statistics_key(&snapshot);
        let context = Arc::new(QueryStatisticsContext::new([QuerySourceStatistics {
            source_key: source_key.clone(),
            table_name: "orders".into(),
            statistics: Some(Arc::new(TableStatistics {
                num_rows: Some(100),
                column_statistics: HashMap::from([(
                    "o_orderkey".into(),
                    ColumnStatistics::default(),
                )]),
                ..TableStatistics::default()
            })),
        }]));
        context.record_scan(&RuntimeStatisticsObservation {
            table_key: source_key,
            statistics: Arc::new(TableStatistics {
                num_rows: Some(5),
                column_statistics: HashMap::from([(
                    "o_orderkey".into(),
                    ColumnStatistics {
                        distinct_count: Some(5),
                        ..ColumnStatistics::default()
                    },
                )]),
                ..TableStatistics::default()
            }),
            complete_table_snapshot: true,
            expected_rows: Some(100),
        });
        let provider = QueryStatisticsProvider::new(context);
        let mut table = catalog::TableMetadata::new("orders", Arc::new(VeinSchema::empty()))
            .with_statistics(TableStatistics {
                num_rows: Some(100),
                ..TableStatistics::default()
            });
        table.catalog_snapshot = Some(snapshot);

        let statistics = provider
            .table_statistics(&table)
            .expect("complete query synopsis is available to the optimizer");
        assert_eq!(statistics.freshness, StatisticsFreshness::Current);
        assert_eq!(statistics.statistics.num_rows, Some(5));
        assert_eq!(
            statistics.statistics.column_statistics["o_orderkey"].distinct_count,
            Some(5)
        );
    }

    #[test]
    fn partial_query_runtime_synopses_do_not_replace_source_statistics() {
        let snapshot = CatalogSnapshotToken::new("orders", "catalog-v1", "revision-1", 1);
        let source_key = common::statistics::runtime::table_statistics_key(&snapshot);
        let context = Arc::new(QueryStatisticsContext::new([QuerySourceStatistics {
            source_key,
            table_name: "orders".into(),
            statistics: Some(Arc::new(TableStatistics {
                num_rows: Some(100),
                ..TableStatistics::default()
            })),
        }]));
        context.record_scan(&RuntimeStatisticsObservation {
            table_key: common::statistics::runtime::table_statistics_key(&snapshot),
            statistics: Arc::new(TableStatistics {
                num_rows: Some(5),
                ..TableStatistics::default()
            }),
            complete_table_snapshot: false,
            expected_rows: Some(100),
        });
        let provider = QueryStatisticsProvider::new(context);
        let mut table = catalog::TableMetadata::new("orders", Arc::new(VeinSchema::empty()))
            .with_statistics(TableStatistics {
                num_rows: Some(100),
                ..TableStatistics::default()
            });
        table.catalog_snapshot = Some(snapshot);

        let statistics = provider
            .table_statistics(&table)
            .expect("pinned source statistics remain available");
        assert_eq!(statistics.freshness, StatisticsFreshness::Current);
        assert_eq!(statistics.statistics.num_rows, Some(100));
    }
}
