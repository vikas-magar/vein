use super::*;

#[tokio::test]
async fn parquet_scan_reads_only_its_assigned_row_group_range() {
    let dir = tempfile::tempdir().expect("temporary data root");
    let path = write_multi_row_group_parquet(dir.path(), "part-0.parquet");
    let mut scan = scan_over(dir.path(), &[("part-0.parquet", path)]);
    scan.projected = Arc::new(types::VeinSchema::new(vec![Arc::new(
        types::ResolvedColumn::new(
            types::ColumnId(0),
            "id",
            None,
            types::VeinDataType::Int64,
        ),
    )]));
    scan.files[0].scan_unit_range = Some(1..2);
    let mut sink = Int64Collector::default();

    scan.execute(&mut sink)
        .await
        .expect("scan assigned row-group range");

    assert_eq!(sink.0, vec![2, 3]);
}
