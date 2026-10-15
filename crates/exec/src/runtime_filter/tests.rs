use super::*;
use arrow::array::Int64Array;
use types::{ResolvedColumn, VeinDataType};

#[derive(Debug)]
struct KeepValues {
    columns: Vec<ColumnId>,
    expected: i64,
}

impl RuntimeFilter for KeepValues {
    fn name(&self) -> &'static str {
        "test_keep_values"
    }

    fn required_columns(&self) -> &[ColumnId] {
        &self.columns
    }

    fn memory_bytes(&self) -> usize {
        0
    }

    fn evaluate(
        &self,
        batch: &RecordBatch,
        column_positions: &[usize],
    ) -> Result<BooleanArray> {
        let values = batch
            .column(column_positions[0])
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| VeinError::internal("test key must be Int64"))?;
        let mut output = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            output.append_value(values.is_valid(row) && values.value(row) == self.expected);
        }
        Ok(output.finish())
    }
}

fn schema() -> VeinSchema {
    VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(
            ColumnId(7),
            "id",
            None,
            VeinDataType::Int64,
        )),
        Arc::new(ResolvedColumn::new(
            ColumnId(8),
            "label",
            None,
            VeinDataType::Utf8,
        )),
    ])
}

#[test]
fn binding_maps_filter_columns_by_id_and_preserves_stable_order() {
    let filters: Vec<Arc<dyn RuntimeFilter>> = vec![Arc::new(KeepValues {
        columns: vec![ColumnId(7)],
        expected: 2,
    })];
    let set = RuntimeFilterSet::new(filters);
    let binding = set.bind(&schema()).expect("filter columns bind");

    assert_eq!(binding.columns().len(), 1);
    assert_eq!(binding.columns()[0].id, ColumnId(7));
}

#[test]
fn binding_uses_scan_order_without_changing_filter_key_positions() {
    let set = RuntimeFilterSet::new(vec![
        Arc::new(KeepValues {
            columns: vec![ColumnId(8)],
            expected: 20,
        }),
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        }),
    ]);
    let binding = set.bind(&schema()).expect("filter columns bind");
    let bound_ids = binding
        .columns()
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    assert_eq!(bound_ids, vec![ColumnId(7), ColumnId(8)]);

    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>,
        ),
        (
            "label",
            Arc::new(Int64Array::from(vec![10, 20, 30])) as Arc<dyn arrow::array::Array>,
        ),
    ])
        .expect("test batch");
    let mask = binding.evaluate(&batch).expect("evaluate bound filters");

    assert_eq!(mask, BooleanArray::from(vec![false, true, false]));
}

#[test]
fn missing_filter_column_declines_pushdown() {
    let set = RuntimeFilterSet::new(vec![Arc::new(KeepValues {
        columns: vec![ColumnId(99)],
        expected: 2,
    })]);

    assert!(set.bind(&schema()).is_none());
}

#[test]
fn combined_runtime_filters_return_a_selection_and_account_rows() {
    let set = RuntimeFilterSet::new(vec![
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        }),
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        }),
    ]);
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>,
    )])
        .expect("test batch");
    let binding = set.bind(&schema()).expect("filter columns bind");
    let mask = binding.evaluate(&batch).expect("combine filters");

    assert_eq!(mask, BooleanArray::from(vec![false, true, false]));
    assert_eq!(set.rows_seen(), 3);
    assert_eq!(set.rows_kept(), 1);
}

#[test]
fn empty_filter_set_does_not_bind_or_allocate_a_mask() {
    assert!(RuntimeFilterSet::empty().bind(&schema()).is_none());
}
