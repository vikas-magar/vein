//! Which output rows a join produces.
//!
//! Matching produces `(probe row, build entry)` pairs; the join type decides
//! what happens to rows that did *not* match. Keeping that decision here — pure
//! logic over row references, with no Arrow in sight — is what lets the gather
//! step stay a mechanical `take` over a list of references, and lets each join
//! type be tested without building any data.

/// The join flavours a hash join implements.
///
/// CROSS has no key and emits every probe/build pair. It shares row assembly
/// with the keyed join operator, while the planner/runtime preserve its
/// distinct semantics and enforce that it carries no hash keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Only matched rows.
    Inner,
    /// Matched rows, plus every unmatched probe row with a null build side.
    Left,
    /// Matched rows, plus every unmatched build row with a null probe side.
    Right,
    /// Both of the above.
    Full,
    /// Each matching probe row once, without build-side columns.
    Semi,
    /// Each unmatched probe row once, without build-side columns.
    Anti,
    /// Every probe row paired with every build row.
    Cross,
}

/// One output row, as references into the probe chunk and the retained build
/// chunks.
///
/// `probe` is a physical row in the probe chunk; `build` is `(chunk, row)` in
/// the table's retained chunks. `None` on either side means that side is null
/// for this row, which the gather turns into a typed null column entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputRow {
    /// Probe row, when the output row has one.
    pub probe: Option<u32>,
    /// Build row, when the output row has one.
    pub build: Option<(u32, u32)>,
}

/// The output rows for `join_type`.
///
/// `probe_rows` are the rows of the probe chunk that were considered — including
/// ones with a null key, which never match and which an outer join still has to
/// emit. `matches` is what the table produced, resolved to `(chunk, row)`.
/// `unmatched_build` is what the table reported for the build side.
pub fn output_rows(
    join_type: JoinType,
    probe_rows: &[u32],
    matches: &[(u32, (u32, u32))],
    unmatched_build: &[(u32, u32)],
) -> Vec<OutputRow> {
    let mut rows = Vec::new();

    if matches!(join_type, JoinType::Semi | JoinType::Anti) {
        rows.extend(
            existence_probe_rows(join_type, probe_rows, matches)
                .into_iter()
                .map(|probe| OutputRow {
                    probe: Some(probe),
                    build: None,
                }),
        );
        return rows;
    }

    // Inner and outer join flavours emit each accepted match pair.
    rows.extend(matches.iter().map(|(probe, build)| OutputRow {
        probe: Some(*probe),
        build: Some(*build),
    }));

    if matches!(join_type, JoinType::Left | JoinType::Full) {
        let matched: std::collections::HashSet<u32> =
            matches.iter().map(|(probe, _)| *probe).collect();
        rows.extend(
            probe_rows
                .iter()
                .filter(|probe| !matched.contains(probe))
                .map(|probe| OutputRow {
                    probe: Some(*probe),
                    build: None,
                }),
        );
    }

    if matches!(join_type, JoinType::Right | JoinType::Full) {
        rows.extend(unmatched_build.iter().map(|build| OutputRow {
            probe: None,
            build: Some(*build),
        }));
    }

    rows
}

/// Returns selected physical probe rows for a semi/anti join. Match pairs may
/// contain multiple build rows for the same probe row; existence only records
/// whether at least one accepted pair survived residual filtering.
pub fn existence_probe_rows(
    join_type: JoinType,
    probe_rows: &[u32],
    matches: &[(u32, (u32, u32))],
) -> Vec<u32> {
    debug_assert!(matches!(join_type, JoinType::Semi | JoinType::Anti));
    let matched: std::collections::HashSet<u32> =
        matches.iter().map(|(probe, _)| *probe).collect();
    probe_rows
        .iter()
        .filter_map(|probe| {
            let has_match = matched.contains(probe);
            (has_match == (join_type == JoinType::Semi)).then_some(*probe)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Probe rows 0 and 1 both matched; probe row 2 did not.
    fn matches() -> Vec<(u32, (u32, u32))> {
        vec![(0, (0, 5)), (1, (0, 6))]
    }

    /// Build rows 5 and 6 matched; build row 7 did not.
    fn unmatched() -> Vec<(u32, u32)> {
        vec![(0, 7)]
    }

    fn probe_rows() -> Vec<u32> {
        vec![0, 1, 2]
    }

    #[test]
    fn inner_keeps_only_matches() {
        let rows = output_rows(JoinType::Inner, &probe_rows(), &matches(), &unmatched());
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
        let rows = output_rows(JoinType::Left, &probe_rows(), &matches(), &unmatched());
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
        let rows = output_rows(JoinType::Right, &probe_rows(), &matches(), &unmatched());
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
        let rows = output_rows(JoinType::Full, &probe_rows(), &matches(), &unmatched());
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
        let rows = output_rows(JoinType::Left, &[0, 1], &[], &[]);
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
            &[0, 1, 2],
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
        let rows = output_rows(JoinType::Anti, &[0, 1, 2], &[(0, (0, 4)), (0, (0, 5))], &[]);
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
            existence_probe_rows(JoinType::Semi, &probes, &matches),
            vec![2]
        );
        assert_eq!(
            existence_probe_rows(JoinType::Anti, &probes, &matches),
            vec![7, 9]
        );
        assert_eq!(existence_probe_rows(JoinType::Semi, &probes, &[]), Vec::<u32>::new());
        assert_eq!(existence_probe_rows(JoinType::Anti, &probes, &[]), probes);
    }

    #[test]
    fn cross_join_emits_every_pair_and_no_unmatched_rows() {
        let matches = [
            (0, (0, 0)),
            (0, (0, 1)),
            (1, (0, 0)),
            (1, (0, 1)),
        ];
        let rows = output_rows(JoinType::Cross, &[0, 1], &matches, &[(0, 2)]);

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
}
