use std::fs;

use plan::CatalogProvider;

use super::support::{catalog, write_parquet_with_row_groups};

#[tokio::test]
async fn combines_footer_statistics_at_file_row_group_and_table_scopes() {
    let root = tempfile::tempdir().expect("temporary root");
    let table = root.path().join("measurements");
    fs::create_dir_all(&table).expect("create table directory");
    write_parquet_with_row_groups(
        &table.join("a.parquet"),
        vec![Some(1), None, Some(4), Some(5)],
    );
    write_parquet_with_row_groups(&table.join("b.parquet"), vec![Some(8), Some(9)]);

    let metadata = catalog(root.path())
        .get_table("measurements")
        .await
        .expect("resolve table");
    let statistics = metadata.statistics.as_ref().expect("footer statistics");
    assert_eq!(statistics.num_rows, Some(6));
    assert_eq!(statistics.file_count, Some(2));
    assert_eq!(statistics.row_group_count, Some(3));
    assert!(statistics.total_bytes.is_some_and(|bytes| bytes > 0));
    assert!(statistics.compressed_bytes.is_some_and(|bytes| bytes > 0));
    assert!(statistics.uncompressed_bytes.is_some_and(|bytes| bytes > 0));
    assert_eq!(statistics.data_units.len(), 5);
    assert_eq!(statistics.data_units[0].kind, catalog::StatisticsUnitKind::File);
    assert_eq!(statistics.data_units[1].parent, Some(0));
    assert_eq!(statistics.data_units[3].kind, catalog::StatisticsUnitKind::File);
    assert_eq!(statistics.data_units[4].parent, Some(3));

    let column = &statistics.column_statistics["value"];
    assert_eq!(column.null_count, Some(1));
    assert_eq!(column.min_value, Some(plan::LogicalLiteral::Int64(1)));
    assert_eq!(column.max_value, Some(plan::LogicalLiteral::Int64(9)));
    assert_eq!(column.distinct_count, None);
}
