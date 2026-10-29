use super::support::{scan_config, scan_with_config, write_multi_row_group_parquet, KeepIds};
use std::sync::Arc;
use futures::{StreamExt, TryStreamExt};
use exec::operators::source::scan::ScanSourceConfig;
use exec::runtime::filter::RuntimeFilterSet;
use types::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

fn projected(mut config: ScanSourceConfig) -> ScanSourceConfig {
    config.projected = Arc::new(VeinSchema::new(vec![Arc::new(
        ResolvedColumn::new(ColumnId(0), "id", None, VeinDataType::Int64),
    )]));
    config
}

#[tokio::test]
async fn dropping_after_one_batch_does_not_open_the_next_file() {
    let directory = tempfile::tempdir().unwrap();
    let first = write_multi_row_group_parquet(directory.path(), "first.parquet");
    let missing = directory.path().join("must-not-be-opened.parquet");
    let config = projected(scan_config(directory.path(), &[
        ("first.parquet", first), ("must-not-be-opened.parquet", missing),
    ]));
    let mut cursor = scan_with_config(config).into_cursor();
    let output = cursor.next().await.unwrap().unwrap();
    assert_eq!(output.num_rows(), 2);
    let pointer = output.resolve(0, 0).unwrap().unwrap().0.to_data().buffers()[0].as_ptr();
    drop(cursor);
    assert_eq!(output.resolve(0, 0).unwrap().unwrap().0.to_data().buffers()[0].as_ptr(), pointer);
    assert_eq!(output.num_rows(), 2, "returned views survive early source disposal");
}

#[tokio::test]
async fn incremental_cursor_preserves_assigned_ranges_and_runtime_filters() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_multi_row_group_parquet(directory.path(), "part.parquet");
    let mut config = projected(scan_config(directory.path(), &[("part.parquet", path)]));
    config.files[0].scan_unit_range = Some(1..2);
    config.runtime_filters = RuntimeFilterSet::new(vec![Arc::new(KeepIds {
        column: ColumnId(0),
        ids: Arc::from([3]),
    })]);
    let output = scan_with_config(config).into_cursor().try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(output.iter().map(|view| view.num_rows()).sum::<usize>(), 1);
    let (array, row) = output[0].resolve(0, 0).unwrap().unwrap();
    assert_eq!(array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(row), 3);
}
