use super::*;

#[test]
fn dictionary_pages_prune_absent_values_and_keep_present_values() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("dictionary_filter.parquet");
    write_dictionary_row_groups(&path);

    let bytes = Bytes::from(std::fs::read(&path).expect("read test Parquet bytes"));
    let file_length = bytes.len() as u64;
    let file = std::fs::File::open(&path).expect("open dictionary Parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read Parquet footer");
    assert_eq!(builder.metadata().num_row_groups(), 2);

    let predicate = dictionary_equality("shipmode", LogicalLiteral::String("SHIP".into()));
    let probes = dictionary_predicates(std::slice::from_ref(&predicate));
    let columns = bloom_filter_columns(
        builder.schema().as_ref(),
        builder.metadata(),
        std::slice::from_ref(&predicate),
    );
    let column = columns
        .iter()
        .find(|column| column.name == "shipmode")
        .expect("resolve dictionary column");

    let first_chunk = builder.metadata().row_group(0).column(0);
    let second_chunk = builder.metadata().row_group(1).column(0);
    assert!(is_fully_dictionary_encoded(first_chunk));
    assert!(is_fully_dictionary_encoded(second_chunk));
    let first_matches = dictionary_page_predicate_matches(
        dictionary_page_bytes(&bytes, file_length, first_chunk),
        file_length,
        2,
        first_chunk,
        column,
        &probes,
    );
    let second_matches = dictionary_page_predicate_matches(
        dictionary_page_bytes(&bytes, file_length, second_chunk),
        file_length,
        2,
        second_chunk,
        column,
        &probes,
    );
    assert_eq!(first_matches, vec![Some(false)]);
    assert_eq!(second_matches, vec![Some(true)]);

    let id_predicate = dictionary_equality("id", LogicalLiteral::Int64(30));
    let id_probes = dictionary_predicates(std::slice::from_ref(&id_predicate));
    let id_column = bloom_filter_columns(
        builder.schema().as_ref(),
        builder.metadata(),
        std::slice::from_ref(&id_predicate),
    )
        .into_iter()
        .next()
        .expect("resolve Int64 dictionary column");
    let first_id_chunk = builder.metadata().row_group(0).column(1);
    let second_id_chunk = builder.metadata().row_group(1).column(1);
    assert_eq!(
        dictionary_page_predicate_matches(
            dictionary_page_bytes(&bytes, file_length, first_id_chunk),
            file_length,
            2,
            first_id_chunk,
            &id_column,
            &id_probes,
        ),
        vec![Some(false)]
    );
    assert_eq!(
        dictionary_page_predicate_matches(
            dictionary_page_bytes(&bytes, file_length, second_id_chunk),
            file_length,
            2,
            second_id_chunk,
            &id_column,
            &id_probes,
        ),
        vec![Some(true)]
    );

    let incompatible_predicate =
        dictionary_equality("shipmode", LogicalLiteral::Int64(30));
    let incompatible_probes =
        dictionary_predicates(std::slice::from_ref(&incompatible_predicate));
    assert_eq!(
        dictionary_page_predicate_matches(
            dictionary_page_bytes(&bytes, file_length, first_chunk),
            file_length,
            2,
            first_chunk,
            column,
            &incompatible_probes,
        ),
        vec![None],
        "a type mismatch must retain the row group"
    );

    let membership = HashMap::from([
        (0, first_matches),
        (1, second_matches),
    ]);
    let pruning = prune_row_groups_with_dictionary(
        builder.metadata(),
        vec![
            RowGroupSelection::new(0, None),
            RowGroupSelection::new(1, None),
        ],
        &membership,
    );
    assert_eq!(pruning.row_groups_skipped, 1);
    assert_eq!(pruning.rows_skipped, 2);
    assert_eq!(pruning.probes, 2);
    assert_eq!(pruning.selections.len(), 1);
    assert_eq!(pruning.selections[0].row_group_index(), 1);
}

#[test]
fn dictionary_page_pruning_fails_open_for_chunks_without_dictionary_encoding() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("plain_column.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("shipmode", DataType::Utf8, false)]));
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .build();
    let file = std::fs::File::create(&path).expect("create plain Parquet file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .expect("create plain writer");
    writer
        .write(
            &RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["MAIL", "RAIL"]))],
            )
                .expect("valid plain batch"),
        )
        .expect("write plain values");
    writer.close().expect("finish plain Parquet file");
    let file = std::fs::File::open(&path).expect("open plain Parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read plain footer");
    let chunk = builder.metadata().row_group(0).column(0);

    assert!(!is_fully_dictionary_encoded(chunk));
    assert!(dictionary_page_range(chunk, 1024).is_none());
}
