use super::*;

#[test]
fn dictionary_predicates_extract_equality_and_safe_in_lists_only() {
    let equality = dictionary_equality("shipmode", LogicalLiteral::String("MAIL".into()));
    let in_list = StatisticsPredicate::Or(vec![
        dictionary_equality("shipmode", LogicalLiteral::String("MAIL".into())),
        dictionary_equality("shipmode", LogicalLiteral::String("SHIP".into())),
    ]);
    let mixed_or = StatisticsPredicate::Or(vec![
        equality.clone(),
        StatisticsPredicate::Unknown,
    ]);
    let other_column = StatisticsPredicate::Or(vec![
        equality.clone(),
        dictionary_equality("other", LogicalLiteral::String("SHIP".into())),
    ]);
    let predicates = dictionary_predicates(&[
        StatisticsPredicate::And(vec![equality, in_list]),
        mixed_or,
        other_column,
    ]);

    assert_eq!(predicates.len(), 2);
    assert_eq!(predicates[0].index, 0);
    assert_eq!(predicates[0].candidates, vec![LogicalLiteral::String("MAIL".into())]);
    assert_eq!(predicates[1].index, 1);
    assert_eq!(predicates[1].candidates.len(), 2);
}

#[test]
fn mixed_dictionary_chunk_still_supports_dictionary_page_membership() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("mixed_dictionary_filter.parquet");
    write_dictionary_row_groups(&path);

    let bytes = Bytes::from(std::fs::read(&path).expect("read test Parquet bytes"));
    let file_length = u64::try_from(bytes.len()).expect("file length fits u64");
    let file = std::fs::File::open(&path).expect("open dictionary Parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read Parquet footer");
    let original_chunk = builder.metadata().row_group(0).column(0);
    let mixed_chunk = original_chunk
        .clone()
        .into_builder()
        .set_page_encoding_stats(vec![
            parquet::file::metadata::PageEncodingStats {
                page_type: parquet::basic::PageType::DATA_PAGE,
                encoding: parquet::basic::Encoding::RLE_DICTIONARY,
                count: 1,
            },
            parquet::file::metadata::PageEncodingStats {
                page_type: parquet::basic::PageType::DATA_PAGE,
                encoding: parquet::basic::Encoding::PLAIN,
                count: 1,
            },
        ])
        .build()
        .expect("build mixed encoding metadata");

    assert!(!is_fully_dictionary_encoded(&mixed_chunk));
    assert!(dictionary_page_range(&mixed_chunk, file_length).is_some());

    let predicate = dictionary_equality("shipmode", LogicalLiteral::String("SHIP".into()));
    let predicates = dictionary_predicates(std::slice::from_ref(&predicate));
    let column = bloom_filter_columns(
        builder.schema().as_ref(),
        builder.metadata(),
        std::slice::from_ref(&predicate),
    )
        .into_iter()
        .next()
        .expect("resolve dictionary column");
    let membership = dictionary_page_predicate_matches(
        dictionary_page_bytes(&bytes, file_length, &mixed_chunk),
        file_length,
        2,
        &mixed_chunk,
        &column,
        &predicates,
    );

    assert_eq!(membership, vec![Some(false)]);
}
