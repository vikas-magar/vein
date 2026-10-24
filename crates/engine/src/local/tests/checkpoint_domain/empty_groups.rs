//! Correlated empty-group defaults must survive domain restriction and left joins.
use super::*;

fn tables() -> Vec<(&'static str, fixture::Columns)> {
    vec![
        ("outer_rows", vec![("outer_key", vec![Some(1), Some(1), Some(2), Some(4), None])]),
        ("inner_rows", vec![
            ("inner_key", vec![Some(1), Some(1), Some(1), Some(2), None, Some(3)]),
            ("amount", vec![Some(10), None, Some(10), None, Some(999), Some(400)]),
        ]),
    ]
}

#[test]
fn correlated_count_preserves_duplicates_null_keys_and_empty_groups() {
    for (aggregate, counts) in [
        ("count(*)", [3, 1, 0, 0]),
        ("count(amount)", [2, 0, 0, 0]),
        ("count(DISTINCT amount)", [1, 0, 0, 0]),
    ] {
        let query = format!("FROM outer_rows
            |> WHERE outer_key IS NULL OR outer_key >= 0
            |> SELECT outer_key,
              (FROM inner_rows |> WHERE inner_key = outer_rows.outer_key
               |> AGGREGATE {aggregate}) AS total");
        let rows = execute(tables(), &query);
        assert_eq!(rows, vec![
            vec![None, Some(counts[3])],
            vec![Some(1), Some(counts[0])], vec![Some(1), Some(counts[0])],
            vec![Some(2), Some(counts[1])], vec![Some(4), Some(counts[2])],
        ], "{aggregate}");
    }
}

#[test]
fn scalar_arithmetic_observes_count_zero_after_null_extension() {
    let rows = execute(tables(), "FROM outer_rows
        |> WHERE outer_key IS NULL OR outer_key >= 0
        |> SELECT outer_key,
          (FROM inner_rows |> WHERE inner_key = outer_rows.outer_key
           |> AGGREGATE count(*) + 1) AS total");
    assert_eq!(rows, vec![
        vec![None, Some(1)], vec![Some(1), Some(4)], vec![Some(1), Some(4)],
        vec![Some(2), Some(2)], vec![Some(4), Some(1)],
    ]);
}

#[test]
fn a_scalar_case_is_evaluated_for_a_missing_sum_group() {
    let rows = execute(tables(), "FROM outer_rows
        |> WHERE outer_key IS NULL OR outer_key >= 0
        |> SELECT outer_key,
          (FROM inner_rows |> WHERE inner_key = outer_rows.outer_key
           |> AGGREGATE CASE WHEN sum(amount) IS NULL THEN 5 ELSE 1 END) AS total");
    assert_eq!(rows, vec![
        vec![None, Some(5)], vec![Some(1), Some(1)], vec![Some(1), Some(1)],
        vec![Some(2), Some(5)], vec![Some(4), Some(5)],
    ]);
}

#[test]
fn filtering_for_count_zero_keeps_unmatched_outer_rows() {
    let rows = execute(tables(), "FROM outer_rows
        |> WHERE outer_key IS NULL OR outer_key >= 0
        |> SELECT outer_key,
          (FROM inner_rows |> WHERE inner_key = outer_rows.outer_key
           |> AGGREGATE count(*)) AS total
        |> WHERE total = 0");
    assert_eq!(rows, vec![vec![None, Some(0)], vec![Some(4), Some(0)]]);
}
