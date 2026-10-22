use super::*;

#[tokio::test]
async fn reader_does_not_request_skipped_data_page_bytes() {
    let (builder, requested_ranges, locations, row_count) = indexed_file().await;
    let skipped = &locations[0];
    let skipped_start = usize::try_from(skipped.offset).unwrap();
    let skipped_end = skipped_start + usize::try_from(skipped.compressed_page_size).unwrap();
    let skipped_rows = usize::try_from(locations[1].first_row_index).unwrap();
    let selection = RowSelection::from(vec![
        RowSelector::skip(skipped_rows),
        RowSelector::select(row_count - skipped_rows),
    ]);
    let row_group = RowGroupSelection::new(0, Some(selection)).with_page_locations(0, locations);

    requested_ranges.lock().unwrap().clear();
    let stream = builder
        .with_row_group_selections(vec![row_group])
        .build()
        .unwrap();
    let batches = stream.try_collect::<Vec<_>>().await.unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| batch.column(0).as_primitive::<arrow::datatypes::Int32Type>().values())
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(values, (skipped_rows as i32..row_count as i32).collect::<Vec<_>>());

    let requests = requested_ranges.lock().unwrap();
    assert!(requests.iter().all(|range| {
        range.end <= skipped_start || range.start >= skipped_end
    }), "skipped page bytes were requested: {requests:?}");
}
