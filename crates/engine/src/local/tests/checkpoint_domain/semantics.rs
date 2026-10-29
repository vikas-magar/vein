use super::*;

#[test]
fn a_normal_inner_join_build_does_not_replace_the_domain_checkpoint() {
    let rows = execute(vec![
        ("outer_rows", vec![("outer_key", vec![Some(1), Some(1), Some(2), None])]),
        ("inner_rows", vec![
            ("inner_key", vec![Some(1), Some(1), Some(1), Some(3), None, Some(4)]),
            ("amount", vec![Some(10), Some(20), Some(20), Some(300), Some(999), Some(400)]),
        ]),
    ], "FROM outer_rows
        |> WHERE outer_key IS NULL OR outer_key >= 0
        |> SELECT outer_key,
           (FROM inner_rows |> WHERE inner_key = outer_rows.outer_key |> AGGREGATE sum(amount)) AS total
        |> WHERE total > 0");
    assert_eq!(rows, vec![vec![Some(1), Some(50)], vec![Some(1), Some(50)]]);
}

#[test]
fn duplicates_null_keys_and_missing_groups_keep_scalar_sum_semantics() {
    let rows = execute(vec![
        ("outer_rows", vec![("outer_key", vec![Some(1), Some(1), Some(2), None])]),
        ("inner_rows", vec![
            ("inner_key", vec![Some(1), Some(1), Some(1), Some(3), None, Some(4)]),
            ("amount", vec![Some(10), Some(20), Some(20), Some(300), Some(999), Some(400)]),
        ]),
    ], "FROM outer_rows
        |> WHERE outer_key IS NULL OR outer_key >= 0
        |> SELECT outer_key,
           (FROM inner_rows |> WHERE inner_key = outer_rows.outer_key |> AGGREGATE sum(amount)) AS total");
    assert_eq!(rows, vec![
        vec![None, None], vec![Some(1), Some(50)], vec![Some(1), Some(50)], vec![Some(2), None],
    ]);
}

#[test]
fn composite_domains_do_not_admit_crossed_key_tuples() {
    let rows = execute(vec![
        ("outer_rows", vec![
            ("outer_key", vec![Some(1), Some(2), Some(1), None]),
            ("outer_other", vec![Some(10), Some(20), Some(10), Some(10)]),
        ]),
        ("inner_rows", vec![
            ("inner_key", vec![Some(1), Some(2), Some(1), Some(2), None, Some(1)]),
            ("inner_other", vec![Some(10), Some(20), Some(20), Some(10), Some(10), Some(10)]),
            ("amount", vec![Some(5), Some(7), Some(100), Some(200), Some(999), Some(6)]),
        ]),
    ], "FROM outer_rows
        |> WHERE outer_key IS NULL OR outer_key >= 0
        |> SELECT outer_key, outer_other,
           (FROM inner_rows
            |> WHERE inner_key = outer_rows.outer_key AND inner_other = outer_rows.outer_other
            |> AGGREGATE sum(amount)) AS total");
    assert_eq!(rows, vec![
        vec![None, Some(10), None],
        vec![Some(1), Some(10), Some(11)], vec![Some(1), Some(10), Some(11)],
        vec![Some(2), Some(20), Some(7)],
    ]);
}
