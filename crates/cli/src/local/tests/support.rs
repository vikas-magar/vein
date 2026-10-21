use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::statistics::RuntimeStatisticsStore;
use exec::pipeline::DataChunkView;
use parquet_connector::catalog::ParquetCatalog;

pub(super) fn write_parquet_batch(
    root: &Path,
    table: &str,
    mode: &str,
    schema: Arc<Schema>,
    batch: &RecordBatch,
) {
    crate::test_support::write_parquet_table(root, table, mode, schema, batch);
}

pub(super) fn parquet_catalog(root: impl Into<PathBuf>) -> ParquetCatalog {
    crate::test_support::parquet_catalog(root)
}

pub(super) fn parquet_catalog_with_runtime(
    root: impl Into<PathBuf>,
    runtime_statistics: RuntimeStatisticsStore,
) -> ParquetCatalog {
    crate::test_support::parquet_catalog(root).with_runtime_statistics_store(runtime_statistics)
}

pub(super) fn create_flights_table(root: &Path) -> (PathBuf, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    for (mode, values) in [
        ("create", vec![1, 4]),
        ("append", vec![5, 2, 3]),
    ] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
            .expect("valid batch");
        write_parquet_batch(root, "flights", mode, Arc::clone(&schema), &batch);
    }
    (root.join("flights"), schema)
}

pub(super) fn materialized_result_column(view: &DataChunkView, column: usize) -> arrow::array::ArrayRef {
    view.materialize_column(column)
        .expect("materialize result column for assertion")
}

pub(super) fn contains_sort(node: &common::protocol::PhysicalPlanNode) -> bool {
    use common::protocol::PhysicalPlanOperator;
    match &node.operator {
        PhysicalPlanOperator::Sort { .. } => true,
        PhysicalPlanOperator::Transform { input, .. }
        | PhysicalPlanOperator::Aggregate { input, .. }
        | PhysicalPlanOperator::Distinct { input, .. }
        | PhysicalPlanOperator::Limit { input, .. }
        | PhysicalPlanOperator::Sink { input, .. } => contains_sort(input),
        PhysicalPlanOperator::Join { probe, build, .. } => {
            contains_sort(probe) || contains_sort(build)
        }
        PhysicalPlanOperator::Pipeline { .. } => false,
    }
}

pub(super) fn find_file_scan(
    node: &common::protocol::PhysicalPlanNode,
) -> Option<&common::protocol::FileScanTask> {
    use common::protocol::{DataSource, PhysicalPlanOperator};
    match &node.operator {
        PhysicalPlanOperator::Pipeline { source, .. } => match source.as_ref() {
            DataSource::File(scan) => Some(scan),
            DataSource::Memory | DataSource::Stream(_) | DataSource::Intermediate(_) => None,
        },
        PhysicalPlanOperator::Transform { input, .. }
        | PhysicalPlanOperator::Aggregate { input, .. }
        | PhysicalPlanOperator::Distinct { input, .. }
        | PhysicalPlanOperator::Sort { input, .. }
        | PhysicalPlanOperator::Limit { input, .. }
        | PhysicalPlanOperator::Sink { input, .. } => find_file_scan(input),
        PhysicalPlanOperator::Join { probe, build, .. } => {
            find_file_scan(probe).or_else(|| find_file_scan(build))
        }
    }
}
