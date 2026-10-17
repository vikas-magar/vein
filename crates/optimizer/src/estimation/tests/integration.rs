use super::*;
use crate::{OptimizerPipeline, OptimizerContext};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingProvider {
    calls: AtomicUsize,
    statistics: TableStatistics,
}
impl StatisticsProvider for CountingProvider {
    fn table_statistics<'a>(&'a self, _: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Some(StatisticsSnapshot { statistics: &self.statistics, freshness: StatisticsFreshness::Current })
    }
}

#[test]
fn pipeline_uses_injected_provider_once_per_scan_and_preserves_the_plan() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        statistics: TableStatistics { num_rows: Some(321), ..Default::default() },
    });
    let pipeline = OptimizerPipeline::new(vec![]).with_statistics_provider(provider.clone());
    let plan = scan(42, Some(100), Some(10), Some(0));
    let LogicalPlan::Scan { table: original, .. } = &plan else { unreachable!(); };
    let original = original.clone();
    let (optimized, diagnostics) = pipeline.optimize_with_diagnostics(plan.clone()).unwrap();
    assert_eq!(optimized, plan);
    let LogicalPlan::Scan { table, .. } = optimized else { unreachable!(); };
    assert!(Arc::ptr_eq(&original, &table));
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    assert_eq!(diagnostics.cardinalities.len(), 1);
    assert_eq!(diagnostics.cardinalities[0].rows.value(), Some(321.0));
}

#[test]
fn resolved_external_scan_uses_the_shared_table_statistics_provider() {
    let provider = CountingProvider {
        calls: AtomicUsize::new(0),
        statistics: TableStatistics { num_rows: Some(321), ..Default::default() },
    };
    let LogicalPlan::Scan { table, schema, .. } = scan(42, Some(100), Some(10), Some(0)) else {
        unreachable!();
    };
    let external = LogicalPlan::ExternalScan {
        url: "file:///source".into(),
        options: vec![],
        files: vec![],
        source_metadata: Some(table),
        schema,
    };

    let estimate = StatisticsEstimator::new(&provider).estimate_plan(&external);
    assert_eq!(estimate.last().unwrap().rows.value(), Some(321.0));
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);

    let LogicalPlan::ExternalScan { schema, .. } = external else { unreachable!(); };
    let unresolved = LogicalPlan::ExternalScan {
        url: "s3://unresolved/source".into(),
        options: vec![],
        files: vec![],
        source_metadata: None,
        schema,
    };
    let estimate = StatisticsEstimator::new(&CatalogStatisticsProvider).estimate_plan(&unresolved);
    assert_eq!(estimate.last().unwrap().rows.value(), None);
    assert_eq!(estimate.last().unwrap().rows.source, EstimateSource::MissingStatistics);
}

struct CustomSelectivity;
impl SelectivityEstimator for CustomSelectivity {
    fn estimate(&self, _: &LogicalExpr, _: &dyn ColumnStatisticsLookup) -> PredicateEstimate {
        PredicateEstimate::model(Some(0.25), Some(0.75))
    }
}

#[test]
fn static_and_dynamic_strategies_share_the_same_plan_walker() {
    let plan = scan(1, Some(100), Some(10), Some(0));
    let c = plan.schema().fields[0].clone();
    let plan = filter(plan, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(3)));
    let estimate = StatisticsEstimator::with_selectivity(&CatalogStatisticsProvider, CustomSelectivity)
        .estimate_plan(&plan);
    assert_eq!(estimate.last().unwrap().rows.value(), Some(25.0));
    let model: &dyn SelectivityEstimator = &CustomSelectivity;
    assert_eq!(StatisticsEstimator::with_selectivity(&CatalogStatisticsProvider, model)
                   .estimate_plan(&plan), estimate);
    assert_eq!(OptimizerContext::default().estimate_plan(&plan).last().unwrap().rows.value(), Some(10.0));
}

#[test]
fn unknown_statistics_never_rewrite_the_plan_or_prune_rows() {
    let plan = scan(1, None, None, None);
    let c = plan.schema().fields[0].clone();
    let plan = filter(plan, predicate(&c, BinaryOp::Eq, LogicalLiteral::Int64(3)));
    let (optimized, diagnostics) = OptimizerPipeline::new(vec![])
        .optimize_with_diagnostics(plan.clone()).unwrap();
    assert_eq!(optimized, plan);
    assert_eq!(diagnostics.cardinalities.last().unwrap().rows.value(), None);
}

#[test]
fn statistical_zero_is_not_reported_as_a_logical_proof() {
    let plan = join(scan(1, Some(0), Some(0), Some(0)),
                    scan(2, Some(20), Some(10), Some(0)), JoinType::Cross, false);
    assert_eq!(root(&plan).rows.confidence, EstimateConfidence::Statistics);
}

#[test]
fn child_summary_iterator_keeps_nested_siblings_in_order() {
    let left = join(scan(1, Some(2), None, None), scan(2, Some(3), None, None), JoinType::Cross, false);
    let right = join(scan(3, Some(5), None, None), scan(4, Some(7), None, None), JoinType::Cross, false);
    let plan = join(left, right, JoinType::Cross, false);
    let estimates = StatisticsEstimator::new(&CatalogStatisticsProvider).estimate_plan(&plan);
    let rows: Vec<_> = estimates.iter().map(|node| node.rows.value()).collect();
    assert_eq!(rows, vec![Some(2.0), Some(3.0), Some(6.0), Some(5.0), Some(7.0), Some(35.0), Some(210.0)]);
}

#[test]
fn plan_walker_handles_deep_trees_without_recursive_estimation() {
    let mut plan = scan(1, Some(100), Some(10), Some(0));
    for _ in 0..512 {
        let schema = Box::new(plan.schema().clone());
        plan = LogicalPlan::Sort { input: Box::new(plan), sort_keys: vec![], schema };
    }
    let estimates = StatisticsEstimator::new(&CatalogStatisticsProvider).estimate_plan(&plan);
    assert_eq!(estimates.len(), 513);
    assert_eq!(estimates.last().unwrap().rows.value(), Some(100.0));
}
