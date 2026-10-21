use super::*;
use common::arrow::SelectionVector;

#[test]
fn policy_selected_filter_set_changes_probe_selection_without_copying_filter_state() {
    let candidates: Vec<Arc<dyn RuntimeFilter>> = vec![
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 1,
        }),
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        }),
    ];
    let selected = RuntimeFilterSet::from_selection(&candidates, &[1])
        .expect("valid adaptive filter decision");
    let view = int64_view(vec![vec![1, 2, 3]]);
    let binding = selected.bind(&schema()).expect("selected filter binds");
    let mask = binding.evaluate(&view).expect("evaluate selected filter");

    assert_eq!(selected.names(), vec!["test_keep_values"]);
    assert_eq!(mask, BooleanArray::from(vec![false, true, false]));
}

#[test]
fn runtime_filter_evaluates_only_active_rows_of_sparse_view() {
    let set = RuntimeFilterSet::new(vec![Arc::new(KeepValues {
        columns: vec![ColumnId(7)],
        expected: 4,
    })]);
    let view = int64_view_with_selection(
        vec![vec![1, 2, 3, 4]],
        SelectionVector::new_sparse(vec![1, 3]),
    );
    let binding = set.bind(&schema()).expect("filter binds to view columns");

    assert_eq!(binding.evaluate(&view).expect("evaluate active view rows"),
        BooleanArray::from(vec![false, true]));
}

#[test]
fn policy_selection_rejects_duplicate_and_out_of_range_candidates() {
    let candidates: Vec<Arc<dyn RuntimeFilter>> = vec![Arc::new(KeepValues {
        columns: vec![ColumnId(7)],
        expected: 1,
    })];

    assert!(RuntimeFilterSet::from_selection(&candidates, &[0, 0]).is_err());
    assert!(RuntimeFilterSet::from_selection(&candidates, &[1]).is_err());
}
