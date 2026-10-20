    use super::*;
    use arrow::array::{Array, Int64Array};

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn int64_mapping(lengths: &[usize], rows: &[Option<SourceRow>]) -> Arc<RowMapping> {
        Arc::new(
            RowMapping::new(Arc::from(lengths.to_vec()), rows.iter().copied())
                .expect("valid row mapping"),
        )
    }

    fn view_column(arrays: Vec<ArrayRef>, mapping: Arc<RowMapping>) -> ViewColumn {
        ViewColumn::new(arrays, DataType::Int64, mapping).expect("valid view column")
    }

    fn int_at(view: &DataChunkView, column: usize, row: usize) -> Option<i64> {
        let (array, physical_row) = view.resolve(column, row).expect("resolve view row")?;
        Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 view value")
                .value(physical_row),
        )
    }

    #[test]
    fn resolves_rows_from_the_same_and_different_source_chunks_without_copying() {
        let first = ints(&[10, 20]);
        let second = ints(&[30, 40]);
        let mapping = int64_mapping(
            &[2, 2],
            &[
                Some(SourceRow::new(0, 1).unwrap()),
                Some(SourceRow::new(1, 0).unwrap()),
                Some(SourceRow::new(1, 1).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![view_column(
                vec![Arc::clone(&first), Arc::clone(&second)],
                mapping,
            )],
            SelectionVector::new_dense(3),
        )
        .expect("valid view");

        assert_eq!(
            (0..3).map(|row| int_at(&view, 0, row)).collect::<Vec<_>>(),
            [Some(20), Some(30), Some(40)]
        );
        let (resolved, row) = view.resolve(0, 1).unwrap().unwrap();
        assert!(Arc::ptr_eq(resolved, &second));
        assert_eq!(row, 0);
        assert_eq!(
            view.resolve(0, 0)
                .unwrap()
                .unwrap()
                .0
                .as_ref()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ptr(),
            first
                .as_ref()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ptr(),
            "view resolution must retain the source value buffer"
        );
    }

    #[test]
    fn repeated_rows_and_duplicate_projection_columns_share_the_same_references() {
        let input = ints(&[7, 8]);
        let mapping = int64_mapping(
            &[2],
            &[
                Some(SourceRow::new(0, 1).unwrap()),
                Some(SourceRow::new(0, 1).unwrap()),
                Some(SourceRow::new(0, 0).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![Arc::clone(&input)], mapping)],
            SelectionVector::new_dense(3),
        )
        .expect("valid repeated-row view");
        let projected = view.project(&[0, 0]).expect("duplicate projection");

        assert_eq!(
            (0..3)
                .map(|row| int_at(&projected, 0, row))
                .collect::<Vec<_>>(),
            [Some(8), Some(8), Some(7)]
        );
        assert!(Arc::ptr_eq(
            projected.columns[0].row_mapping(),
            projected.columns[1].row_mapping()
        ));
        assert!(Arc::ptr_eq(
            &projected.columns[0].arrays[0],
            &projected.columns[1].arrays[0]
        ));
        assert!(Arc::ptr_eq(&projected.columns[0].arrays[0], &input));
    }

    #[test]
    fn outer_join_null_extension_uses_mapping_metadata_without_null_arrays() {
        let input = ints(&[11, 22]);
        let mapping = int64_mapping(&[2], &[Some(SourceRow::new(0, 1).unwrap()), None]);
        let view = DataChunkView::new(
            2,
            vec![view_column(vec![Arc::clone(&input)], mapping)],
            SelectionVector::new_dense(2),
        )
        .expect("valid outer join view");

        assert_eq!(int_at(&view, 0, 0), Some(22));
        assert!(view.is_null(0, 1).expect("null extension"));
        assert_eq!(view.columns[0].source_arrays().len(), 1);
        assert!(Arc::ptr_eq(&view.columns[0].source_arrays()[0], &input));
    }

    #[test]
    fn empty_input_and_zero_column_views_preserve_their_row_count() {
        let empty_mapping = int64_mapping(&[], &[]);
        let empty = DataChunkView::new(
            0,
            vec![view_column(Vec::new(), empty_mapping)],
            SelectionVector::new_dense(0),
        )
        .expect("typed empty view");
        let zero_columns = DataChunkView::new(3, Vec::new(), SelectionVector::new_dense(3))
            .expect("zero-column view");

        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.num_columns(), 1);
        assert_eq!(zero_columns.num_rows(), 3);
        assert_eq!(zero_columns.num_columns(), 0);
    }

    #[test]
    fn composing_join_views_flattens_row_maps_and_keeps_leaf_buffers_alive() {
        let probe = ints(&[1, 2]);
        let build = ints(&[10, 20]);
        let probe_map = int64_mapping(
            &[2],
            &[
                Some(SourceRow::new(0, 0).unwrap()),
                Some(SourceRow::new(0, 1).unwrap()),
            ],
        );
        let build_map = int64_mapping(
            &[2],
            &[
                Some(SourceRow::new(0, 0).unwrap()),
                Some(SourceRow::new(0, 1).unwrap()),
            ],
        );
        let left = DataChunkView::new(
            2,
            vec![view_column(vec![Arc::clone(&probe)], probe_map)],
            SelectionVector::new_dense(2),
        )
        .unwrap();
        let right = DataChunkView::new(
            2,
            vec![view_column(vec![Arc::clone(&build)], build_map)],
            SelectionVector::new_dense(2),
        )
        .unwrap();
        let joined = DataChunkView::combine(
            &[
                ViewInputRows {
                    view: &left,
                    rows: &[Some(1), None],
                },
                ViewInputRows {
                    view: &right,
                    rows: &[Some(0), Some(1)],
                },
            ],
            &[
                ViewColumnRef {
                    input: 0,
                    column: 0,
                },
                ViewColumnRef {
                    input: 1,
                    column: 0,
                },
            ],
        )
        .expect("join two source views");
        let chained = DataChunkView::combine(
            &[ViewInputRows {
                view: &joined,
                rows: &[Some(1), Some(0)],
            }],
            &[
                ViewColumnRef {
                    input: 0,
                    column: 0,
                },
                ViewColumnRef {
                    input: 0,
                    column: 1,
                },
            ],
        )
        .expect("compose a chained join projection");

        assert_eq!(int_at(&chained, 0, 0), None);
        assert_eq!(int_at(&chained, 1, 0), Some(20));
        assert_eq!(int_at(&chained, 0, 1), Some(2));
        assert_eq!(int_at(&chained, 1, 1), Some(10));
        assert!(Arc::ptr_eq(
            chained.resolve(0, 1).unwrap().unwrap().0,
            &probe
        ));
        assert!(Arc::ptr_eq(
            chained.resolve(1, 0).unwrap().unwrap().0,
            &build
        ));
    }

    #[test]
    fn view_selection_changes_active_rows_without_changing_source_arrays() {
        let input = ints(&[3, 5, 7]);
        let mapping = Arc::new(RowMapping::identity(Arc::from(vec![3]), 0, 3).unwrap());
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![Arc::clone(&input)], mapping)],
            SelectionVector::new_dense(3),
        )
        .unwrap()
        .with_selection(SelectionVector::new_sparse(vec![2, 0]))
        .expect("select view rows");

        assert_eq!(int_at(&view, 0, 0), Some(7));
        assert_eq!(int_at(&view, 0, 1), Some(3));
        assert!(Arc::ptr_eq(
            view.columns[0].source_arrays().first().unwrap(),
            &input
        ));
    }

    #[test]
    fn compacting_a_dense_view_keeps_its_existing_row_mapping() {
        let input = ints(&[3, 5, 7]);
        let mapping = Arc::new(RowMapping::identity(Arc::from(vec![3]), 0, 3).unwrap());
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![Arc::clone(&input)], Arc::clone(&mapping))],
            SelectionVector::new_dense(3),
        )
        .unwrap();

        let compacted = view.compact_active_rows().unwrap();
        assert!(Arc::ptr_eq(compacted.columns[0].row_mapping(), &mapping));
        assert!(Arc::ptr_eq(
            compacted.columns[0].source_arrays().first().unwrap(),
            &input
        ));
    }

    #[test]
    fn materializing_contiguous_view_rows_returns_a_zero_copy_slice() {
        let input = ints(&[10, 20, 30, 40]);
        let input_values = input
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ptr();
        let mapping = Arc::new(RowMapping::identity(Arc::from(vec![4]), 0, 4).unwrap());
        let view = DataChunkView::new(
            4,
            vec![view_column(vec![Arc::clone(&input)], mapping)],
            SelectionVector::new_sparse(vec![1, 2]),
        )
        .unwrap();

        let output = view.materialize_column(0).unwrap();
        let output = output.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(output.values().as_ref(), &[20, 30]);
        assert_eq!(output.values().as_ptr(), input_values.wrapping_add(1));
    }

    #[test]
    fn materializing_mapped_view_rows_gathers_values_and_null_extensions() {
        let first = ints(&[10, 20]);
        let second = ints(&[30, 40]);
        let mapping = int64_mapping(
            &[2, 2],
            &[
                Some(SourceRow::new(0, 1).unwrap()),
                None,
                Some(SourceRow::new(1, 0).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![first, second], mapping)],
            SelectionVector::new_dense(3),
        )
        .unwrap();

        let output = view.materialize_column(0).unwrap();
        let output = output.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(output, &Int64Array::from(vec![Some(20), None, Some(30)]));
    }

    #[test]
    fn selected_view_preserves_selection_and_arrow_buffer_identity() {
        let input = ints(&[10, 20, 30]);
        let view = crate::pipeline::test_support::selected_view(
            vec![Arc::clone(&input)],
            SelectionVector::new_sparse(vec![2, 0]),
        );

        assert_eq!(view.num_rows(), 2);
        assert_eq!(int_at(&view, 0, 0), Some(30));
        assert_eq!(int_at(&view, 0, 1), Some(10));
        assert!(Arc::ptr_eq(
            view.columns[0].source_arrays().first().unwrap(),
            &input
        ));
    }

    #[test]
    fn wrapping_a_record_batch_preserves_arrow_array_identity() {
        let input = ints(&[10, 20, 30]);
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "id",
                DataType::Int64,
                false,
            )])),
            vec![Arc::clone(&input)],
        )
        .expect("valid Arrow batch");

        let view = DataChunkView::from_record_batch(batch).expect("wrap batch as a view");

        assert_eq!(view.num_rows(), 3);
        assert_eq!(int_at(&view, 0, 1), Some(20));
        assert!(Arc::ptr_eq(
            view.columns[0].source_arrays().first().unwrap(),
            &input
        ));
    }

    #[test]
    fn arrow_boundary_materializes_mapped_and_null_extended_rows() {
        let first = ints(&[10, 20]);
        let second = ints(&[30, 40]);
        let mapping = int64_mapping(
            &[2, 2],
            &[
                Some(SourceRow::new(0, 1).unwrap()),
                None,
                Some(SourceRow::new(1, 0).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![first, second], mapping)],
            SelectionVector::new_dense(3),
        )
        .expect("joined view");

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, true),
        ]));
        let (batch, _) = view
            .to_record_batch_with_report(schema)
            .expect("materialize at Arrow boundary");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert_eq!(values, &Int64Array::from(vec![Some(20), None, Some(30)]));
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn materialization_report_distinguishes_shared_buffers_and_null_extended_gathers() {
        let source = ints(&[10, 20, 30]);
        let shared = int64_mapping(
            &[3],
            &[
                Some(SourceRow::new(0, 0).unwrap()),
                Some(SourceRow::new(0, 1).unwrap()),
                Some(SourceRow::new(0, 2).unwrap()),
            ],
        );
        let gathered = int64_mapping(
            &[3],
            &[
                Some(SourceRow::new(0, 2).unwrap()),
                None,
                Some(SourceRow::new(0, 0).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![
                view_column(vec![Arc::clone(&source)], shared),
                view_column(vec![Arc::clone(&source)], gathered),
            ],
            SelectionVector::new_dense(3),
        )
        .unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("shared", DataType::Int64, false),
            arrow::datatypes::Field::new("gathered", DataType::Int64, true),
        ]));
        let (batch, report) = view.to_record_batch_with_report(schema).unwrap();
        let mut column_reports = MaterializationReport::default();
        for column in 0..view.num_columns() {
            let (array, report) = view.materialize_column_with_report(column).unwrap();
            assert_eq!(array.to_data(), batch.column(column).to_data());
            column_reports.accumulate(report);
        }
        assert_eq!(column_reports, report);
        assert!(
            view.materialize_column_with_report(view.num_columns())
                .is_err()
        );
        let shared = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let gathered = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let original = source.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(shared.values().as_ptr(), original.values().as_ptr());
        assert_eq!(gathered, &Int64Array::from(vec![Some(30), None, Some(10)]));
        assert_eq!(report.shared_columns, 1);
        assert_eq!(report.gathered_columns, 1);
        assert_eq!(report.empty_columns, 0);
        assert_eq!(
            report.gathered_output_buffer_bytes,
            gathered.get_buffer_memory_size()
        );
        assert!(report.gathered_output_buffer_bytes > 0);
    }

    #[test]
    fn materialization_report_handles_empty_and_zero_column_batches() {
        let empty = crate::pipeline::test_support::selected_view(
            vec![ints(&[10, 20])],
            SelectionVector::new_sparse(vec![]),
        );
        let empty_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
        ]));
        let (batch, report) = empty.to_record_batch_with_report(empty_schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
        assert_eq!(
            report,
            MaterializationReport {
                empty_columns: 1,
                ..Default::default()
            }
        );

        let view = DataChunkView::new(3, vec![], SelectionVector::new_dense(3)).unwrap();
        let (batch, report) = view
            .to_record_batch_with_report(Arc::new(arrow::datatypes::Schema::empty()))
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(report, MaterializationReport::default());
    }

    #[test]
    fn composing_reordered_rows_across_views_keeps_original_arrays() {
        let first = ints(&[10, 99, 20]);
        let second = ints(&[30, 40]);
        let first_view = crate::pipeline::test_support::selected_view(
            vec![Arc::clone(&first)],
            SelectionVector::new_sparse(vec![2, 0]),
        );
        let second_view = crate::pipeline::test_support::selected_view(
            vec![Arc::clone(&second)],
            SelectionVector::new_dense(2),
        );

        let output = DataChunkView::compose_rows(
            &[&first_view, &second_view],
            &[(1, 1), (0, 0), (0, 1), (1, 0)],
            1,
        )
        .expect("compose reordered rows");

        assert_eq!(
            (0..output.num_rows())
                .map(|row| int_at(&output, 0, row))
                .collect::<Vec<_>>(),
            vec![Some(40), Some(20), Some(10), Some(30)],
        );
        for row in 0..output.num_rows() {
            let (array, _) = output.resolve(0, row).unwrap().unwrap();
            assert!(Arc::ptr_eq(array, &first) || Arc::ptr_eq(array, &second));
        }
    }

    #[test]
    fn composing_rows_preserves_null_extensions_and_present_source_arrays() {
        let null_extended_values = ints(&[10, 20]);
        let present_values = ints(&[30, 40]);
        let null_extended = DataChunkView::new(
            2,
            vec![view_column(
                vec![Arc::clone(&null_extended_values)],
                int64_mapping(&[2], &[Some(SourceRow::new(0, 1).unwrap()), None]),
            )],
            SelectionVector::new_dense(2),
        )
        .expect("outer-join view with a null-extended row");
        let present = crate::pipeline::test_support::dense_view(vec![Arc::clone(&present_values)]);

        let output = DataChunkView::compose_rows(&[&null_extended, &present], &[(0, 1), (1, 0)], 1)
            .expect("compose a null extension and a present row");

        assert!(output.is_null(0, 0).expect("preserved null extension"));
        assert_eq!(int_at(&output, 0, 1), Some(30));
        let (array, _) = output.resolve(0, 1).expect("resolve present row").unwrap();
        assert!(Arc::ptr_eq(array, &present_values));
        assert!(Arc::ptr_eq(
            output.columns[0].source_arrays().first().unwrap(),
            &present_values
        ));
    }

    #[test]
    fn wrapping_rejects_selection_or_columns_outside_the_physical_rows() {
        assert!(
            DataChunkView::from_arrays(
                vec![ints(&[1, 2]), ints(&[3])],
                2,
                SelectionVector::new_dense(2),
            )
            .is_err()
        );
        assert!(
            DataChunkView::from_selected_arrays(
                vec![ints(&[1, 2])],
                SelectionVector::new_sparse(vec![2]),
            )
            .is_err()
        );
    }

    #[test]
    fn source_array_footprint_deduplicates_array_handles_within_a_view() {
        let shared = ints(&[1, 2, 3]);
        let view = crate::pipeline::test_support::dense_view(vec![
            Arc::clone(&shared),
            Arc::clone(&shared),
        ]);

        let (array_count, bytes) = view.source_array_footprint();

        assert_eq!(array_count, 1);
        assert_eq!(bytes, shared.get_array_memory_size());
    }

    #[test]
    fn invalid_source_rows_and_selection_indices_are_rejected() {
        assert!(
            RowMapping::new(Arc::from(vec![2]), [Some(SourceRow::new(0, 2).unwrap())],).is_err()
        );

        let mapping = int64_mapping(&[2], &[Some(SourceRow::new(0, 0).unwrap())]);
        let column = view_column(vec![ints(&[1, 2])], mapping);
        assert!(
            DataChunkView::new(1, vec![column], SelectionVector::new_sparse(vec![1]),).is_err()
        );
    }
