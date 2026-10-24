//! Scalar row semantics through the binder, optimizer, planner and real joins.
use super::*;

#[test]
fn inequality_scalar_rows_preserve_duplicates_nulls_and_empty_matches() {
    let rows = execute_result(vec![
        ("outer_rows", vec![("outer_key", vec![Some(0), Some(1), Some(1), None])]),
        ("inner_rows", vec![("inner_key", vec![Some(0), Some(4), None])]),
    ], "FROM outer_rows |> SELECT outer_key,
        (FROM inner_rows |> WHERE inner_key < outer_rows.outer_key |> SELECT inner_key) AS value",
        false).unwrap();
    assert_eq!(rows, vec![vec![None, None], vec![Some(0), None],
        vec![Some(1), Some(0)], vec![Some(1), Some(0)]]);
}

#[test]
fn equality_keys_and_non_equality_correlations_are_both_enforced() {
    let rows = execute_result(vec![
        ("outer_rows", vec![
            ("outer_key", vec![Some(1), Some(2), None]),
            ("bound", vec![Some(2), Some(3), Some(4)]),
        ]),
        ("inner_rows", vec![
            ("inner_key", vec![Some(1), Some(1), Some(2)]),
            ("amount", vec![Some(1), Some(4), Some(2)]),
        ]),
    ], "FROM outer_rows |> SELECT outer_key,
        (FROM inner_rows
         |> WHERE inner_key = outer_rows.outer_key AND amount < outer_rows.bound
         |> SELECT amount) AS value", false).unwrap();
    assert_eq!(rows, vec![vec![None, None], vec![Some(1), Some(1)], vec![Some(2), Some(2)]]);
}

#[test]
fn scalar_row_residual_preserves_disjunction_and_outer_null_predicates() {
    let rows = execute_result(vec![
        ("outer_rows", vec![("outer_key", vec![Some(0), Some(1), None])]),
        ("inner_rows", vec![("inner_key", vec![Some(0), Some(4), None])]),
    ], "FROM outer_rows |> SELECT outer_key,
        (FROM inner_rows
         |> WHERE inner_key < outer_rows.outer_key
             OR (outer_rows.outer_key IS NULL AND inner_key = 0)
         |> SELECT inner_key) AS value", false).unwrap();
    assert_eq!(rows, vec![vec![None, Some(0)], vec![Some(0), None], vec![Some(1), Some(0)]]);
}

#[test]
fn scalar_row_inequality_reports_multiple_matches() {
    let error = execute_result(vec![
        ("outer_rows", vec![("outer_key", vec![Some(2)])]),
        ("inner_rows", vec![("inner_key", vec![Some(0), Some(1)])]),
    ], "FROM outer_rows |> SELECT outer_key,
        (FROM inner_rows |> WHERE inner_key < outer_rows.outer_key |> SELECT inner_key) AS value",
        false).unwrap_err();
    assert!(error.to_string().contains("scalar subquery returned more than one row"), "{error}");
}

#[test]
fn scalar_row_inequality_over_empty_build_returns_null() {
    let rows = execute_result(vec![
        ("outer_rows", vec![("outer_key", vec![Some(2), None])]),
        ("inner_rows", vec![("inner_key", Vec::new())]),
    ], "FROM outer_rows |> SELECT outer_key,
        (FROM inner_rows |> WHERE inner_key < outer_rows.outer_key |> SELECT inner_key) AS value",
        false).unwrap();
    assert_eq!(rows, vec![vec![None, None], vec![Some(2), None]]);
}
