use std::sync::Arc;

use crate::runtime::filter::RuntimeFilterSet;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use common::arrow::SelectionVector;

use super::*;

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn chunk(column: ArrayRef) -> DataChunkView {
    view(vec![column])
}

fn view(columns: Vec<ArrayRef>) -> DataChunkView {
    let rows = columns.first().map_or(0, |column| column.len());
    DataChunkView::from_arrays(columns, rows, SelectionVector::new_dense(rows))
        .expect("valid test view")
}

fn hashes_for_view(view: &DataChunkView, columns: Vec<usize>) -> Vec<u64> {
    let keys = ChunkKeys::resolve_view(&JoinKeys::new(columns), view)
        .expect("resolve view join keys")
        .0;
    (0..view.num_rows())
        .filter_map(|row| {
            keys.hash_and_single_i64(row, view)
                .expect("hash key row")
                .map(|(hash, _)| hash)
        })
        .collect()
}

fn query_memory(limit: usize) -> QueryMemoryTracker {
    QueryMemoryTracker::new(limit, common::cancellation::CancellationToken::new())
}

#[test]
fn bloom_runtime_filter_preserves_composite_join_keys_and_null_semantics() {
    let build = vec![
        Arc::new(Int64Array::from(vec![Some(5), Some(8), Some(5)])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])) as ArrayRef,
    ];
    let build = view(build);
    let hashes = hashes_for_view(&build, vec![0, 1]);
    let memory = query_memory(1 << 20);
    let columns = [ColumnId(11), ColumnId(12)];
    let filters = JoinBloomFilterFactory::default()
        .build_filters(RuntimeFilterBuildInput {
            probe_columns: &columns,
            build_hashes: &hashes,
            build_key_bounds: Vec::new(),
            memory: &memory,
        })
        .expect("build runtime filter");
    assert_eq!(filters.len(), 1);

    let probe = view(vec![
        Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(8), None])) as ArrayRef,
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("missing"),
            Some("b"),
            Some("a"),
        ])) as ArrayRef,
    ]);
    let selection = filters[0]
        .evaluate(&probe, &[0, 1])
        .expect("evaluate composite runtime filter");

    assert!(
        selection.value(0),
        "every build key must survive the filter"
    );
    assert!(
        selection.value(2),
        "every build key must survive the filter"
    );
    assert!(!selection.value(3), "null join keys never match");
    assert!(
        filters[0].memory_bytes() > 0,
        "filter storage is accounted while retained"
    );
    drop(filters);
    assert_eq!(memory.used(), 0, "filter reservation releases on drop");
}

#[test]
fn bloom_runtime_filter_declines_optional_allocation_when_budget_is_exhausted() {
    let hashes = [fnv1a(&7_i64.to_ne_bytes(), FNV_OFFSET)];
    let memory = query_memory(0);
    let columns = [ColumnId(7)];

    let filters = JoinBloomFilterFactory::default()
        .build_filters(RuntimeFilterBuildInput {
            probe_columns: &columns,
            build_hashes: &hashes,
            build_key_bounds: Vec::new(),
            memory: &memory,
        })
        .expect("optional filter allocation failure must fall back");

    assert!(filters.is_empty());
    assert_eq!(memory.used(), 0);
}

#[test]
fn row_key_encoding_hashes_composite_values_and_skips_null_keys() {
    let single_i64_view = view(vec![Arc::new(Int64Array::from(vec![Some(3), None])) as ArrayRef]);
    let single_i64 = ChunkKeys::resolve_view(&JoinKeys::new(vec![0]), &single_i64_view)
        .expect("resolve single Int64 key")
        .0;
    let expected_i64_hash = fnv1a(&3_i64.to_ne_bytes(), FNV_OFFSET);
    assert_eq!(
        single_i64.hash_and_single_i64(0, &single_i64_view).unwrap(),
        Some((expected_i64_hash, Some(3))),
        "the Int64 scalar is read for hashing without retaining a duplicate"
    );
    assert_eq!(single_i64.hash_and_single_i64(1, &single_i64_view).unwrap(), None);

    let composite_view = view(vec![
        Arc::new(Int64Array::from(vec![Some(3), Some(3), None])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("north"), None, Some("north")])) as ArrayRef,
    ]);
    let composite = ChunkKeys::resolve_view(&JoinKeys::new(vec![0, 1]), &composite_view)
        .expect("resolve composite key")
        .0;
    let expected_composite_hash = fnv1a(b"north", fnv1a(&3_i64.to_ne_bytes(), FNV_OFFSET));
    assert_eq!(
        composite.hash_and_single_i64(0, &composite_view).unwrap(),
        Some((expected_composite_hash, None)),
        "composite keys preserve column order in the canonical hash"
    );
    assert_eq!(
        composite.hash_and_single_i64(1, &composite_view).unwrap(),
        None,
        "a null in any key column skips the row"
    );
    assert_eq!(
        composite.hash_and_single_i64(2, &composite_view).unwrap(),
        None,
        "null leading key columns skip the row"
    );
}

#[test]
fn spill_hashes_use_the_same_composite_key_encoding_as_join_indexing() {
    let keys = JoinKeys::new(vec![0, 1]);
    let build = crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![Some(3), Some(3), None])) as ArrayRef,
        Arc::new(StringArray::from(vec![
            Some("north"),
            Some("south"),
            Some("north"),
        ])) as ArrayRef,
    ]);
    let probe = crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![Some(3)])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("north")])) as ArrayRef,
    ]);

    let build_hashes = HashTable::hashes_for_view(&build, &keys).expect("build hashes");
    let probe_hashes = HashTable::hashes_for_view(&probe, &keys).expect("probe hashes");

    assert_eq!(build_hashes[0], probe_hashes[0]);
    assert_ne!(build_hashes[0], build_hashes[1]);
    assert_eq!(
        build_hashes[2], FNV_OFFSET,
        "null keys still receive a partition hash"
    );
}

#[test]
fn sparse_composite_join_keys_stay_reference_backed_through_table_merge_and_probe() {
    use common::arrow::SelectionVector;

    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(8)]));
    let strings: ArrayRef = Arc::new(StringArray::from(vec!["A", "B", "C"]));
    let payload = ints(&[70, 0, 80]);
    let make_view = |selection| {
        crate::pipeline::test_support::selected_view(
            vec![
                Arc::clone(&numbers),
                Arc::clone(&strings),
                Arc::clone(&payload),
            ],
            selection,
        )
    };
    let mut left = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
    left.insert(make_view(SelectionVector::new_dense(3)))
        .unwrap();
    let (shared, shared_us) = left.build_key_materialization();
    assert_eq!(shared, MaterializationReport::default());
    let mut right = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
    let sparse = make_view(SelectionVector::new_sparse(vec![2, 1, 0]));
    right.insert(sparse.clone()).unwrap();
    let (gathered, gathered_us) = right.build_key_materialization();
    assert_eq!(gathered, MaterializationReport::default());
    for (column, expected) in [(&numbers, 0), (&strings, 1)] {
        assert!(
            Arc::ptr_eq(
                &right
                    .build_chunk(0)
                    .source_arrays_for_column(expected)
                    .unwrap()[0],
                column
            ),
            "sparse join-key access retains source Arrow arrays"
        );
    }
    left.merge(right).unwrap();
    let mut expected = shared;
    expected.accumulate(gathered);
    assert_eq!(
        left.build_key_materialization(),
        (expected, shared_us.saturating_add(gathered_us))
    );
    for index in 0..2 {
        assert!(Arc::ptr_eq(
            left.build_chunk(index).resolve(2, 0).unwrap().unwrap().0,
            &payload
        ));
    }

    let before = left.build_key_materialization();
    let mut matches = Vec::new();
    let (report, _) = left.probe(&sparse, &mut matches).unwrap();
    assert_eq!(report, MaterializationReport::default());
    matches.sort_unstable();
    assert_eq!(
        matches,
        vec![(0, (0, 2)), (0, (1, 0)), (2, (0, 0)), (2, (1, 2))]
    );
    assert_eq!(
        left.build_key_materialization(),
        before,
        "probing must not mutate the shared build table"
    );
    matches.clear();
    let (report, _) = left
        .probe(&make_view(SelectionVector::new_dense(3)), &mut matches)
        .unwrap();
    assert_eq!(report, shared);
    let mut incompatible = HashTable::new(JoinKeys::new(vec![2]), JoinKeys::new(vec![2]));
    incompatible
        .insert(make_view(SelectionVector::new_dense(3)))
        .unwrap();
    assert!(left.merge(incompatible).is_err());
    assert_eq!(left.build_key_materialization(), before);
}

#[test]
fn key_reports_handle_empty_batches_and_keyless_joins() {
    use common::arrow::SelectionVector;

    let empty = crate::pipeline::test_support::selected_view(
        vec![ints(&[1])],
        SelectionVector::new_sparse(vec![]),
    );
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(empty.clone()).unwrap();
    let expected = MaterializationReport::default();
    assert_eq!(table.build_key_materialization().0, expected);
    let mut matches = Vec::new();
    assert_eq!(table.probe(&empty, &mut matches).unwrap().0, expected);
    assert!(matches.is_empty());

    let mut keyless = HashTable::new(JoinKeys::new(vec![]), JoinKeys::new(vec![]));
    let view = chunk(ints(&[1, 2]));
    keyless.insert(view.clone()).unwrap();
    assert_eq!(
        keyless.build_key_materialization().0,
        MaterializationReport::default()
    );
    assert_eq!(
        keyless.probe(&view, &mut matches).unwrap().0,
        MaterializationReport::default()
    );
    assert_eq!(matches.len(), 4);
}

/// A probe row finds exactly the build rows whose key equals it.
#[test]
fn probe_finds_equal_keys() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(chunk(ints(&[10, 20, 30]))).expect("insert");
    table.insert(chunk(ints(&[20, 40]))).expect("insert");

    let mut pairs = Vec::new();
    table
        .probe(&chunk(ints(&[20, 30, 50])), &mut pairs)
        .expect("probe");

    let mut matches: Vec<(u32, u32, u32)> = pairs
        .iter()
        .map(|(probe_row, (build_chunk, build_row))| (*probe_row, *build_chunk, *build_row))
        .collect();
    matches.sort();
    // Probe row 0 (key 20) matches chunk 0 row 1 and chunk 1 row 0; probe
    // row 1 (key 30) matches chunk 0 row 2; probe row 2 (50) matches nothing.
    assert_eq!(matches, vec![(0, 0, 1), (0, 1, 0), (1, 0, 2)]);
}

#[test]
fn insert_rejects_key_types_without_a_hash_join_representation() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    let unsupported: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));

    let error = table
        .insert(chunk(unsupported))
        .expect_err("unsupported key representations must fail before probing");

    assert!(error.to_string().contains("not supported by hash join"));
    assert!(error.to_string().contains("Boolean"));
    assert_eq!(
        table.build_key_materialization(),
        (MaterializationReport::default(), 0)
    );
}

/// A null key never matches, on either side of the join.
#[test]
fn null_keys_never_match() {
    let build: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None]));
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![build]))
        .expect("insert");

    let probe: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(10)]));
    let mut pairs = Vec::new();
    table
        .probe(
            &crate::pipeline::test_support::dense_view(vec![probe]),
            &mut pairs,
        )
        .expect("probe");

    assert_eq!(pairs.len(), 1, "only the non-null key may match");
    assert_eq!(pairs[0].0, 1, "and it is probe row 1");
}

#[test]
fn runtime_key_bounds_track_each_composite_component_and_ignore_null_rows() {
    let mut table = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            Arc::new(Int64Array::from(vec![Some(5), None, Some(8)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("m"), Some("z"), Some("a")])) as ArrayRef,
        ]))
        .expect("insert composite build rows");

    assert_eq!(
        table.runtime_key_bounds().expect("snapshot typed bounds"),
        vec![
            Some((LogicalLiteral::Int64(5), LogicalLiteral::Int64(8))),
            Some((
                LogicalLiteral::String("a".into()),
                LogicalLiteral::String("m".into()),
            )),
        ],
        "only fully non-null build keys can match an equijoin probe"
    );
}

#[test]
fn range_filter_prunes_from_statistics_without_binding_a_row_mask() {
    let memory = query_memory(1 << 20);
    let filter = JoinKeyRangeFilter::try_new(
        &[ColumnId(7)],
        vec![Some((LogicalLiteral::Int64(2), LogicalLiteral::Int64(5)))],
        &memory,
    )
    .expect("create bounded filter")
    .expect("range bounds are available");
    let filters = RuntimeFilterSet::new(vec![Arc::new(filter)]);
    let schema = types::VeinSchema::new(vec![Arc::new(types::ResolvedColumn::new(
        ColumnId(7),
        "id",
        None,
        types::VeinDataType::Int64,
    ))]);

    assert!(!filters.has_row_filters());
    assert!(filters.bind(&schema).is_none());
    assert_eq!(
        filters.statistics_predicates(&schema),
        vec![
            catalog::StatisticsPredicate::Comparison {
                column: "id".into(),
                comparison: catalog::StatisticsComparison::GreaterOrEqual,
                literal: LogicalLiteral::Int64(2),
            },
            catalog::StatisticsPredicate::Comparison {
                column: "id".into(),
                comparison: catalog::StatisticsComparison::LessOrEqual,
                literal: LogicalLiteral::Int64(5),
            },
        ]
    );
    assert!(filters.memory_bytes() > 0);
    drop(filters);
    assert_eq!(memory.used(), 0, "range filter accounting releases on drop");
}

#[test]
fn merging_hash_tables_combines_and_rebases_runtime_key_bounds() {
    let keys = JoinKeys::new(vec![0, 1]);
    let mut left = HashTable::new(keys.clone(), keys.clone());
    let mut right = HashTable::new(keys.clone(), keys);
    left.insert(crate::pipeline::test_support::dense_view(vec![
        ints(&[5, 7]),
        Arc::new(StringArray::from(vec!["m", "z"])) as ArrayRef,
    ]))
    .expect("insert first worker rows");
    right
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[1, 9]),
            Arc::new(StringArray::from(vec!["a", "y"])) as ArrayRef,
        ]))
        .expect("insert second worker rows");

    left.merge(right).expect("combine worker hash tables");

    assert_eq!(
        left.runtime_key_bounds().expect("snapshot merged bounds"),
        vec![
            Some((LogicalLiteral::Int64(1), LogicalLiteral::Int64(9))),
            Some((
                LogicalLiteral::String("a".into()),
                LogicalLiteral::String("z".into()),
            )),
        ],
        "merged endpoints must resolve into the destination's rebased chunks"
    );
}

/// The probe reports what it matched, and the table reports every build row:
/// an outer join subtracts one from the other.
#[test]
fn the_probe_reports_its_matches_and_the_table_every_row() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(chunk(ints(&[10, 20]))).expect("insert");

    let mut pairs = Vec::new();
    table.probe(&chunk(ints(&[10])), &mut pairs).expect("probe");

    assert_eq!(pairs, vec![(0, (0, 0))]);
    assert_eq!(table.all_build_rows(), vec![(0, 0), (0, 1)]);
}

/// Several key columns must all agree, and strings compare by value.
#[test]
fn multi_column_keys_and_strings_match() {
    let mut table = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[1, 1]),
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ]))
        .expect("insert");

    let mut pairs = Vec::new();
    table
        .probe(
            &crate::pipeline::test_support::dense_view(vec![
                ints(&[1]),
                Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
            ]),
            &mut pairs,
        )
        .expect("probe");
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].1, (0, 1));

    let mut none = Vec::new();
    table
        .probe(
            &crate::pipeline::test_support::dense_view(vec![
                ints(&[1]),
                Arc::new(StringArray::from(vec!["c"])) as ArrayRef,
            ]),
            &mut none,
        )
        .expect("probe");
    assert!(none.is_empty(), "one differing column must not match");
}

/// The build columns are retained, not copied: the table holds the caller's
/// `ArrayRef`, so the data exists once.
#[test]
fn build_columns_are_retained_not_copied() {
    let key: ArrayRef = ints(&[1, 2, 3]);
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![Arc::clone(
            &key,
        )]))
        .expect("insert");
    let (retained, _) = table
        .build_chunk(0)
        .resolve(0, 0)
        .expect("resolve row")
        .unwrap();
    assert!(Arc::ptr_eq(retained, &key));
}

/// The bucket table grows with the build side, so probing stays short-chained
/// rather than degrading into a linear scan.
#[test]
fn the_table_grows_with_the_build_side() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    let values: Vec<i64> = (0..20_000).collect();
    table.insert(chunk(ints(&values))).expect("insert");
    assert!(
        table.buckets.len() > INITIAL_BUCKETS,
        "buckets must grow past the initial size"
    );
    assert!(
        table.buckets.len().is_power_of_two(),
        "the bucketing mask needs a power-of-two bucket count"
    );

    let mut pairs = Vec::new();
    table
        .probe(&chunk(ints(&[19_999, 0, 500])), &mut pairs)
        .expect("probe");
    assert_eq!(pairs.len(), 3, "every key must still be found after growth");
}

/// The key need not be at the same column position on both sides: the probe
/// is resolved with the probe's positions, not the build's.
#[test]
fn the_two_sides_may_key_on_different_columns() {
    // Build columns are [payload, key]; the probe's only column is the key.
    let mut table = HashTable::new(JoinKeys::new(vec![1]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[100, 200]),
            ints(&[7, 8]),
        ]))
        .expect("insert");

    let mut pairs = Vec::new();
    table.probe(&chunk(ints(&[8])), &mut pairs).expect("probe");

    assert_eq!(pairs.len(), 1, "key 8 must match the build's second column");
    assert_eq!(pairs[0].1, (0, 1));
}

/// Two tables built independently can be combined, which is what a parallel
/// build produces: every row of both sides is still found, and the rebased
/// chunk references resolve to the right data.
#[test]
fn merged_tables_find_every_row_from_both() {
    let mut left = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    left.insert(chunk(ints(&[10, 20]))).expect("insert");
    let mut right = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    right.insert(chunk(ints(&[30]))).expect("insert");
    // A null key on the merged-in side must survive the rebase too.
    let with_null: ArrayRef = Arc::new(Int64Array::from(vec![Some(40), None]));
    right
        .insert(crate::pipeline::test_support::dense_view(vec![with_null]))
        .expect("insert");

    left.merge(right).expect("merge");
    assert!(
        left.buckets.len().is_power_of_two(),
        "a merge must leave the bucket count a power of two"
    );

    let mut pairs = Vec::new();
    left.probe(&chunk(ints(&[10, 30, 40, 99])), &mut pairs)
        .expect("probe");
    let mut matches: Vec<(u32, u32, u32)> = pairs
        .iter()
        .map(|(probe, (build_chunk, build_row))| (*probe, *build_chunk, *build_row))
        .collect();
    matches.sort();
    assert_eq!(
        matches,
        vec![
            // 10 is in `left`'s own chunk 0; the merged side contributes two
            // chunks, so 30 stays chunk 1 and 40 lands in chunk 2.
            (0, 0, 0),
            (1, 1, 0),
            (2, 2, 0),
        ],
        "every key must be found across both tables"
    );
    assert_eq!(
        left.all_build_rows(),
        vec![(0, 0), (0, 1), (1, 0), (2, 0), (2, 1)],
        "every row of both tables must survive the rebase, null-keyed included"
    );
}

#[test]
fn composite_keys_match_only_when_every_key_column_matches() {
    let mut table = HashTable::new(JoinKeys::new(vec![0, 1]), JoinKeys::new(vec![0, 1]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[1, 1, 2]),
            ints(&[10, 20, 10]),
        ]))
        .expect("insert composite build keys");

    let probe = crate::pipeline::test_support::dense_view(vec![
        ints(&[1, 1, 2, 2]),
        ints(&[20, 99, 10, 11]),
    ]);
    let mut pairs = Vec::new();
    table
        .probe(&probe, &mut pairs)
        .expect("probe composite keys");

    let mut matches: Vec<(u32, u32)> = pairs
        .iter()
        .map(|(probe_row, (_, build_row))| (*probe_row, *build_row))
        .collect();
    matches.sort();
    assert_eq!(
        matches,
        vec![(0, 1), (2, 2)],
        "matching just one component must not produce a join match"
    );
}

/// Merging tables built on different keys is refused rather than comparing
/// the wrong columns.
#[test]
fn merging_different_keys_is_refused() {
    let mut left = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    let right = HashTable::new(JoinKeys::new(vec![1]), JoinKeys::new(vec![1]));
    assert!(left.merge(right).is_err());
}

#[test]
fn build_index_respects_budget_and_releases_reservation_on_error() {
    let cancel = common::cancellation::CancellationToken::new();
    let initial_bytes = INITIAL_BUCKETS * size_of::<u32>() + size_of::<BuildKeyBounds>();
    let budget = QueryMemoryTracker::new(initial_bytes, cancel);
    let mut table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        budget.clone(),
    )
    .expect("initial bucket table fits the configured budget");
    assert_eq!(budget.used(), initial_bytes);

    let error = table
        .insert(chunk(ints(&[1])))
        .expect_err("the row index must not exceed the budget");
    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    drop(table);
    assert_eq!(
        budget.used(),
        0,
        "failed build state releases all reservations"
    );
}

#[test]
fn retained_build_arrays_are_charged_once_across_views_and_live_through_outputs() {
    let source = ints(&[10, 20]);
    let source_bytes = source.get_array_memory_size();
    let tracker = QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let mut table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        tracker.clone(),
    )
    .expect("create budgeted table");
    let initial_bytes = tracker.used();

    for _ in 0..2 {
        let view = chunk(Arc::clone(&source));
        table.insert(view).expect("retain build view");
    }
    let after_build = tracker.used();
    assert!(
        after_build >= initial_bytes + source_bytes,
        "retained Arrow source array must be included in query memory"
    );

    let output = DataChunkView::compose_rows(&[table.build_chunk(0)], &[(0, 1)], 1)
        .expect("output keeps the source array reference");
    drop(table);
    assert!(
        tracker.used() >= source_bytes,
        "the result view keeps the source-array lease alive after the hash table drops"
    );
    assert_eq!(
        output.resolve(0, 0).expect("resolve output").unwrap().1,
        1,
        "the output maps to the requested source row"
    );
    drop(output);
    assert_eq!(
        tracker.used(),
        0,
        "the last retaining view releases all state"
    );
}

#[test]
fn equal_sized_distinct_build_arrays_each_consume_their_own_budget() {
    let shared_tracker =
        QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let mut shared_table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        shared_tracker.clone(),
    )
    .expect("create shared-array table");
    let source = ints(&[10, 20]);
    let array_bytes = source.get_array_memory_size();
    let initial_shared_bytes = shared_tracker.used();

    for _ in 0..2 {
        shared_table
            .insert(chunk(Arc::clone(&source)))
            .expect("insert shared array");
    }
    let shared_bytes = shared_tracker.used() - initial_shared_bytes;

    let distinct_tracker =
        QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let mut distinct_table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        distinct_tracker.clone(),
    )
    .expect("create distinct-array table");
    let initial_distinct_bytes = distinct_tracker.used();
    for _ in 0..2 {
        distinct_table
            .insert(chunk(ints(&[10, 20])))
            .expect("insert distinct array");
    }
    let distinct_bytes = distinct_tracker.used() - initial_distinct_bytes;

    assert_eq!(
        distinct_bytes - shared_bytes,
        array_bytes,
        "only the distinct source array adds another retained-array reservation"
    );
}

#[test]
fn retained_build_array_budget_failure_happens_before_retaining_the_view() {
    let source = ints(&[10, 20]);
    let source_bytes = source.get_array_memory_size();
    let initial_buckets = INITIAL_BUCKETS * size_of::<u32>() + size_of::<BuildKeyBounds>();
    let tracker = QueryMemoryTracker::new(
        initial_buckets + source_bytes - 1,
        common::cancellation::CancellationToken::new(),
    );
    let mut table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        tracker.clone(),
    )
    .expect("initial buckets fit the budget");
    let view = chunk(source);

    let error = table
        .insert(view)
        .expect_err("the retained source array must respect the query budget");
    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    assert_eq!(
        tracker.used(),
        initial_buckets,
        "failed insertion releases its lease"
    );
    drop(table);
    assert_eq!(tracker.used(), 0);
}

#[test]
fn row_index_budget_failure_does_not_partially_insert_a_build_batch() {
    let first = ints(&[10]);
    let second = ints(&[20]);
    let initial_bytes = INITIAL_BUCKETS * size_of::<u32>() + size_of::<BuildKeyBounds>();
    let sizing_tracker =
        QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut sizing_table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        sizing_tracker.clone(),
    )
    .expect("create sizing table");
    let initial_usage = sizing_tracker.used();
    sizing_table
        .insert(chunk(Arc::clone(&first)))
        .expect("insert sizing row");
    let per_batch_bytes = sizing_tracker.used() - initial_usage;
    drop(sizing_table);

    let tracker = QueryMemoryTracker::new(
        initial_bytes + per_batch_bytes * 2 - 1,
        common::cancellation::CancellationToken::new(),
    );
    let mut table = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        tracker.clone(),
    )
    .expect("initial buckets fit the budget");
    table
        .insert(chunk(Arc::clone(&first)))
        .expect("first build batch fits");
    let used_before = tracker.used();

    let error = table
        .insert(chunk(Arc::clone(&second)))
        .expect_err("second row index exceeds the remaining budget");

    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    assert_eq!(
        table.len(),
        1,
        "failed insertion leaves the row count intact"
    );
    assert_eq!(
        table.chunk_count(),
        1,
        "failed insertion retains no partial chunk"
    );
    assert_eq!(
        tracker.used(),
        used_before,
        "failed batch releases its array lease"
    );
    let mut matches = Vec::new();
    table
        .probe(&chunk(ints(&[10, 20])), &mut matches)
        .expect("existing build rows remain probeable");
    assert_eq!(matches.len(), 1);
    assert_eq!(table.build_row(matches[0].1.0), (0, 0));
}

#[test]
fn merging_budgeted_tables_releases_the_replaced_bucket_allocation() {
    let tracker = QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let mut left = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        tracker.clone(),
    )
    .expect("create left table");
    let mut right = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        tracker.clone(),
    )
    .expect("create right table");
    left.insert(chunk(ints(&[1]))).expect("insert left row");
    right.insert(chunk(ints(&[2]))).expect("insert right row");
    let before_merge = tracker.used();

    left.merge(right).expect("merge tables sharing one tracker");

    assert_eq!(
        tracker.used(),
        before_merge - INITIAL_BUCKETS * size_of::<u32>() - size_of::<BuildKeyBounds>(),
        "merge replaces the second bucket array and key-bounds vector"
    );
    drop(left);
    assert_eq!(tracker.used(), 0);
}

#[test]
fn merging_tables_from_independent_budgets_transfers_accounting() {
    let left_tracker =
        QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let right_tracker =
        QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let mut left = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        left_tracker.clone(),
    )
    .expect("create left table");
    let mut right = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        right_tracker.clone(),
    )
    .expect("create right table");
    left.insert(chunk(ints(&[10]))).expect("insert left row");
    right.insert(chunk(ints(&[20]))).expect("insert right row");
    let left_before = left_tracker.used();
    let right_before = right_tracker.used();

    left.merge(right)
        .expect("merge independently budgeted tables");

    assert_eq!(
        right_tracker.used(),
        0,
        "source reservation was transferred"
    );
    assert_eq!(
        left_tracker.used(),
        left_before + right_before
            - INITIAL_BUCKETS * size_of::<u32>()
            - size_of::<BuildKeyBounds>(),
        "destination owns merged state with one bucket array and one key-bounds vector"
    );
    let mut pairs = Vec::new();
    left.probe(&chunk(ints(&[10, 20])), &mut pairs)
        .expect("probe merged table");
    assert_eq!(pairs.len(), 2);
    drop(left);
    assert_eq!(
        left_tracker.used(),
        0,
        "merged reservation releases with table"
    );
}

#[test]
fn merged_build_array_lease_moves_to_destination_and_outlives_the_table() {
    let left_tracker =
        QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let right_tracker =
        QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let mut left = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        left_tracker.clone(),
    )
    .expect("create destination table");
    let mut right = HashTable::with_memory_tracker(
        JoinKeys::new(vec![0]),
        JoinKeys::new(vec![0]),
        right_tracker.clone(),
    )
    .expect("create source table");
    let source = ints(&[20, 30]);
    let source_bytes = source.get_array_memory_size();
    right
        .insert(chunk(Arc::clone(&source)))
        .expect("retain source build array");

    left.merge(right).expect("transfer source table accounting");

    assert_eq!(
        right_tracker.used(),
        0,
        "source budget releases after merge"
    );
    assert!(
        left_tracker.used() >= source_bytes,
        "destination budget includes the merged build array"
    );
    let output = DataChunkView::compose_rows(&[left.build_chunk(0)], &[(0, 1)], 1)
        .expect("compose a view over the merged array");
    drop(left);

    assert!(
        left_tracker.used() >= source_bytes,
        "output view keeps the destination lease alive after table drop"
    );
    assert_eq!(output.resolve(0, 0).unwrap().unwrap().1, 1);
    drop(output);
    assert_eq!(
        left_tracker.used(),
        0,
        "final output drop releases destination lease"
    );
}
