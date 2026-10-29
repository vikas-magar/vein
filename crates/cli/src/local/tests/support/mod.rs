use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::protocol::{DataSource, FileScanTask, PhysicalPlanNode, PhysicalPlanOperator};
use common::statistics::RuntimeStatisticsStore;
use exec::pipeline::DataChunkView;
use parquet_connector::catalog::ParquetCatalog;

mod fixtures;
#[cfg(test)]
mod tests;

pub(super) use fixtures::{parquet_catalog, write_parquet_table};

pub(super) fn parquet_catalog_with_runtime(
    root: impl Into<PathBuf>,
    runtime_statistics: RuntimeStatisticsStore,
) -> ParquetCatalog {
    parquet_catalog(root).with_runtime_statistics_store(runtime_statistics)
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
        write_parquet_table(root, "flights", mode, Arc::clone(&schema), &batch);
    }
    (root.join("flights"), schema)
}

pub(super) fn materialized_result_column(view: &DataChunkView, column: usize) -> arrow::array::ArrayRef {
    view.materialize_column(column)
        .expect("materialize result column for assertion")
}

pub(super) fn contains_sort(node: &PhysicalPlanNode) -> bool {
    find_first_plan_node(node, &|node| {
        matches!(&node.operator, PhysicalPlanOperator::Sort { .. })
    })
        .is_some()
}

pub(super) fn find_file_scan(node: &PhysicalPlanNode) -> Option<&FileScanTask> {
    let matching_node = find_first_plan_node(node, &|node| match &node.operator {
        PhysicalPlanOperator::Pipeline { source, .. } => {
            matches!(source.as_ref(), DataSource::File(_))
        }
        _ => false,
    })?;
    let PhysicalPlanOperator::Pipeline { source, .. } = &matching_node.operator else {
        return None;
    };
    let DataSource::File(scan) = source.as_ref() else {
        return None;
    };
    Some(scan)
}

fn find_first_plan_node<'a>(
    node: &'a PhysicalPlanNode,
    predicate: &impl Fn(&PhysicalPlanNode) -> bool,
) -> Option<&'a PhysicalPlanNode> {
    if predicate(node) {
        return Some(node);
    }

    node.operator.inputs()
        .find_map(|child| find_first_plan_node(child, predicate))
}
