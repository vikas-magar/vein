use super::ColumnChunkData;
use bytes::Bytes;
use std::ops::Range;

#[test]
fn sparse_ranges_fetch_only_uncovered_bytes() {
    let chunk = ColumnChunkData::sparse(
        32,
        vec![
            (10, Bytes::from_static(b"abcd")),
            (20, Bytes::from_static(b"wxyz")),
        ],
    );

    assert_eq!(
        chunk.missing_ranges(vec![8..24]),
        vec![8..10, 14..20]
    );
}

#[test]
fn sparse_ranges_coalesce_adjacent_pages_without_copying_payloads() {
    let first = Bytes::from_static(b"abcd");
    let second = Bytes::from_static(b"efgh");
    let chunk = ColumnChunkData::sparse(32, vec![(10, first.clone()), (14, second.clone())]);

    assert_eq!(chunk.missing_ranges(vec![10..18]), Vec::<Range<u64>>::new());
    assert_eq!(chunk.get(12).unwrap().as_ref(), b"cd");
    assert_eq!(chunk.get(14).unwrap().as_ptr(), second.as_ptr());
}

#[test]
fn incremental_merge_keeps_existing_duplicate_and_shares_bytes() {
    let existing = Bytes::from_static(b"abcd");
    let added = Bytes::from_static(b"efgh");
    let chunk = ColumnChunkData::sparse(32, vec![(10, existing.clone())]);
    let merged = chunk
        .merge_sparse_pages(vec![
            (10, Bytes::from_static(b"xxxx")),
            (14, added.clone()),
        ])
        .unwrap();

    assert_eq!(merged.get(10).unwrap().as_ptr(), existing.as_ptr());
    assert_eq!(merged.get(14).unwrap().as_ptr(), added.as_ptr());
    assert_eq!(merged.missing_ranges(vec![10..18]), Vec::<Range<u64>>::new());
}

#[test]
fn dense_chunk_does_not_request_ranges_again() {
    let data = Bytes::from_static(b"column bytes");
    let chunk = ColumnChunkData::dense(100, data.clone());

    assert!(!chunk.can_fetch_more());
    assert!(chunk.missing_ranges(vec![100..112]).is_empty());
    assert_eq!(chunk.get(104).unwrap().as_ptr(), data.slice(4..).as_ptr());
}
