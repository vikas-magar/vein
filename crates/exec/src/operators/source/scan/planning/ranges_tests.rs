use super::coalesce_scan_units;

#[test]
fn empty_scan_units_produce_no_ranges() {
    assert!(coalesce_scan_units([], None).is_empty());
}

#[test]
fn adjacent_units_share_one_unbounded_range() {
    assert_eq!(coalesce_scan_units([(2, 10), (3, 20), (4, 30)], None), [2..5]);
}

#[test]
fn byte_budget_splits_only_between_adjacent_units() {
    assert_eq!(
        coalesce_scan_units([(0, 6), (1, 6), (2, 3), (5, 4)], Some(10)),
        [0..2, 2..3, 5..6]
    );
}
