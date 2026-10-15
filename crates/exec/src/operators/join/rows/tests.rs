use super::*;

/// Probe rows 0 and 1 both matched; probe row 2 did not.
fn matches() -> Vec<(u32, (u32, u32))> {
    vec![(0, (0, 5)), (1, (0, 6))]
}

/// Build rows 5 and 6 matched; build row 7 did not.
fn unmatched() -> Vec<(u32, u32)> {
    vec![(0, 7)]
}

fn probe_rows() -> std::ops::Range<u32> {
    0..3
}

#[test]
fn inner_keeps_only_matches() {
    let rows = output_rows(JoinType::Inner, probe_rows(), &matches(), &unmatched());
    assert_eq!(
        rows,
        vec![
            OutputRow { probe: Some(0), build: Some((0, 5)) },
            OutputRow { probe: Some(1), build: Some((0, 6)) },
        ]
    );
}

/// A left join keeps the unmatched probe row, with a null build side.
#[test]
fn left_adds_unmatched_probe_rows() {
    let rows = output_rows(JoinType::Left, probe_rows(), &matches(), &unmatched());
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.last(),
        Some(&OutputRow { probe: Some(2), build: None }),
        "the unmatched probe row must be emitted with no build side"
    );
}

/// A right join keeps the unmatched build row, with a null probe side.
#[test]
fn right_adds_unmatched_build_rows() {
    let rows = output_rows(JoinType::Right, probe_rows(), &matches(), &unmatched());
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.last(),
        Some(&OutputRow { probe: None, build: Some((0, 7)) }),
        "the unmatched build row must be emitted with no probe side"
    );
}

/// A full join emits both kinds of unmatched row, and duplicates no match.
#[test]
fn full_adds_both_and_duplicates_nothing() {
    let rows = output_rows(JoinType::Full, probe_rows(), &matches(), &unmatched());
    assert_eq!(rows.len(), 4, "two matches, one per unmatched side");
    assert_eq!(
        rows.iter().filter(|row| row.probe.is_none()).count(),
        1
    );
    assert_eq!(rows.iter().filter(|row| row.build.is_none()).count(), 1);
}

/// A probe row with a null key matches nothing, so an outer join emits it.
#[test]
fn a_null_keyed_probe_row_is_unmatched_but_still_emitted() {
    let rows = output_rows(JoinType::Left, 0..2, &[], &[]);
    assert_eq!(
        rows,
        vec![
            OutputRow { probe: Some(0), build: None },
            OutputRow { probe: Some(1), build: None },
        ]
    );
}

#[test]
fn semi_emits_each_matched_probe_once_even_for_duplicate_inner_matches() {
    let rows = output_rows(
        JoinType::Semi,
        0..3,
        &[(0, (0, 4)), (0, (0, 5)), (2, (1, 6))],
        &[],
    );
    assert_eq!(
        rows,
        vec![
            OutputRow { probe: Some(0), build: None },
            OutputRow { probe: Some(2), build: None },
        ]
    );
}

#[test]
fn anti_emits_unmatched_and_null_keyed_probe_rows_once() {
    // A NULL key creates no hash match, so it remains eligible for NOT EXISTS.
    let rows = output_rows(JoinType::Anti, 0..3, &[(0, (0, 4)), (0, (0, 5))], &[]);
    assert_eq!(
        rows,
        vec![
            OutputRow { probe: Some(1), build: None },
            OutputRow { probe: Some(2), build: None },
        ]
    );
}

#[test]
fn semi_and_anti_preserve_probe_order_and_ignore_matches_outside_the_input_selection() {
    let probes = [7, 2, 9];
    let matches = [(2, (0, 0)), (2, (0, 1)), (8, (0, 2))];

    assert_eq!(
        existence_probe_rows(JoinType::Semi, probes.into_iter(), &matches),
        vec![2]
    );
    assert_eq!(
        existence_probe_rows(JoinType::Anti, probes.into_iter(), &matches),
        vec![7, 9]
    );
    assert_eq!(existence_probe_rows(JoinType::Semi, probes.into_iter(), &[]), Vec::<u32>::new());
    assert_eq!(existence_probe_rows(JoinType::Anti, probes.into_iter(), &[]), probes);
}

#[test]
fn cross_join_emits_every_pair_and_no_unmatched_rows() {
    let matches = [
        (0, (0, 0)),
        (0, (0, 1)),
        (1, (0, 0)),
        (1, (0, 1)),
    ];
    let rows = output_rows(JoinType::Cross, 0..2, &matches, &[(0, 2)]);

    assert_eq!(
        rows,
        vec![
            OutputRow { probe: Some(0), build: Some((0, 0)) },
            OutputRow { probe: Some(0), build: Some((0, 1)) },
            OutputRow { probe: Some(1), build: Some((0, 0)) },
            OutputRow { probe: Some(1), build: Some((0, 1)) },
        ],
        "CROSS JOIN emits the product and does not append unmatched outer rows"
    );
}

#[test]
fn inner_join_does_not_iterate_probe_rows_that_only_outer_joins_need() {
    #[derive(Clone)]
    struct UnvisitedProbeRows;

    impl Iterator for UnvisitedProbeRows {
        type Item = u32;

        fn next(&mut self) -> Option<Self::Item> {
            panic!("inner join must not enumerate unmatched probe rows");
        }
    }

    let matches = [(1, (0, 5))];
    let rows = output_rows(JoinType::Inner, UnvisitedProbeRows, &matches, &[]);

    assert_eq!(rows, vec![OutputRow { probe: Some(1), build: Some((0, 5)) }]);
}
