use super::*;
use common::statistics::{OperatorStatisticsIdentity, QueryStatisticsContext};

#[tokio::test]
async fn parquet_scan_applies_runtime_filter_before_emitting_projected_rows() {
    let dir = tempfile::tempdir().expect("temporary data root");
    let path = write_parquet(dir.path(), "part-0.parquet", 64);
    let mut scan = scan_over(dir.path(), &[("part-0.parquet", path)]);
    scan.projected = Arc::new(types::VeinSchema::new(vec![Arc::new(
        types::ResolvedColumn::new(
            types::ColumnId(9),
            "id",
            None,
            types::VeinDataType::Int64,
        ),
    )]));
    let statistics_context = Arc::new(QueryStatisticsContext::new([]));
    let statistics = statistics_context.register_operator(
        OperatorStatisticsIdentity {
            task_id: 12,
            node_id: 4,
        },
        "join",
    );
    scan.runtime_filters = RuntimeFilterSet::with_statistics(vec![Arc::new(KeepIds {
        column: types::ColumnId(9),
        ids: Arc::from([3, 7, 42]),
    })], statistics);
    let mut sink = Int64Collector::default();

    scan.execute(&mut sink)
        .await
        .expect("apply runtime filter during Parquet scan");

    assert_eq!(sink.0, vec![3, 7, 42].into_iter().filter(|id| *id < 64).collect::<Vec<_>>());
    assert!(scan.runtime_filters.rows_seen() >= 64);
    assert!(scan.runtime_filters.rows_kept() < scan.runtime_filters.rows_seen());
    assert_eq!(
        statistics_context.operator_statistics()[0].metrics["runtime_filter_rows_seen"],
        scan.runtime_filters.rows_seen(),
    );
}
