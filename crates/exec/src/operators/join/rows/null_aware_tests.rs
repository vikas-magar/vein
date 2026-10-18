use super::{OutputRow, null_aware_anti_rows};

fn probe_rows(rows: Vec<OutputRow>) -> Vec<u32> {
    rows.into_iter().filter_map(|row| row.probe).collect()
}

#[test]
fn not_in_empty_build_keeps_all_probe_rows_including_nulls() {
    let result = null_aware_anti_rows(0..3, &[], &[1], true, false);
    assert_eq!(probe_rows(result), [0, 1, 2]);
}

#[test]
fn not_in_nonempty_null_free_build_excludes_matches_and_null_probes() {
    let matches = [(0, (0, 0)), (0, (0, 1))];
    let result = null_aware_anti_rows(0..4, &matches, &[1], false, false);
    assert_eq!(probe_rows(result), [2, 3]);
}

#[test]
fn not_in_with_a_null_build_key_has_no_true_rows() {
    let matches = [(0, (0, 0))];
    let result = null_aware_anti_rows(0..4, &matches, &[1], false, true);
    assert!(result.is_empty());
}
