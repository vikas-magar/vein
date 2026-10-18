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
    /// `NOT IN` anti join, with NULL handling supplied by the hash table.
    NullAwareAnti,
    /// One build row per probe row, or a cardinality error for multiple matches.
    Single,
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
/// `probe_rows` lazily yields the probe rows considered, including rows with a
/// null key that an outer join still has to emit. The iterator is only consumed
/// for join kinds that need unmatched probe rows. `matches` is what the table
/// produced, resolved to `(chunk, row)`.
/// `unmatched_build` is what the table reported for the build side.
pub fn output_rows(
    join_type: JoinType,
    probe_rows: impl Iterator<Item=u32> + Clone,
    matches: &[(u32, (u32, u32))],
    unmatched_build: &[(u32, u32)],
) -> Vec<OutputRow> {
    let mut rows = if matches!(join_type, JoinType::Inner | JoinType::Cross) {
        Vec::with_capacity(matches.len())
    } else {
        Vec::new()
    };

    if matches!(join_type, JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti) {
        let existence_type = if join_type == JoinType::NullAwareAnti {
            JoinType::Anti
        } else {
            join_type
        };
        rows.extend(
            existence_probe_rows(existence_type, probe_rows, matches)
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

    if matches!(join_type, JoinType::Left | JoinType::Full | JoinType::Single) {
        let matched: std::collections::HashSet<u32> =
            matches.iter().map(|(probe, _)| *probe).collect();
        rows.extend(
            probe_rows
                .filter(|probe| !matched.contains(probe))
                .map(|probe| OutputRow {
                    probe: Some(probe),
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
    probe_rows: impl Iterator<Item=u32>,
    matches: &[(u32, (u32, u32))],
) -> Vec<u32> {
    debug_assert!(matches!(join_type, JoinType::Semi | JoinType::Anti));
    let matched: std::collections::HashSet<u32> =
        matches.iter().map(|(probe, _)| *probe).collect();
    probe_rows
        .filter(|probe| matched.contains(probe) == (join_type == JoinType::Semi))
        .collect()
}

/// Applies SQL `NOT IN` truth semantics to an anti-join's row references.
/// Empty build input makes even NULL probes true; otherwise NULL probes are
/// unknown, and any NULL build key makes every non-matching probe unknown.
pub fn null_aware_anti_rows(
    probe_rows: impl Iterator<Item = u32>,
    matches: &[(u32, (u32, u32))],
    null_probe_rows: &[u32],
    build_is_empty: bool,
    build_has_null: bool,
) -> Vec<OutputRow> {
    if build_has_null {
        return Vec::new();
    }

    let matched: std::collections::HashSet<u32> =
        matches.iter().map(|(probe, _)| *probe).collect();
    probe_rows
        .filter(|probe| {
            !matched.contains(probe)
                && (build_is_empty || null_probe_rows.binary_search(probe).is_err())
        })
        .map(|probe| OutputRow {
            probe: Some(probe),
            build: None,
        })
        .collect()
}

#[cfg(test)]
mod tests;
#[cfg(test)]
#[path = "rows/null_aware_tests.rs"]
mod null_aware_tests;
