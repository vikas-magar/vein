//! Storage work granularity is independent of execution parallelism.

use std::ops::Range;

/// Coalesce adjacent groups up to a byte budget while leaving independently
/// claimable work in the shared queue. A group is indivisible. With no budget,
/// one worker reuses a reader for each contiguous run of candidates. Ordinals
/// must be sorted and unique; rejected groups are never bridged.
pub(super) fn row_group_ranges(
    groups: impl IntoIterator<Item=(usize, u64)>,
    target_bytes: Option<u64>,
) -> Vec<Range<usize>> {
    let mut ranges: Vec<Range<usize>> = Vec::new();
    let mut weight = 0_u64;
    for (group, bytes) in groups {
        if let Some(last) = ranges.last_mut() {
            if last.end == group && target_bytes.is_none_or(|target| weight.saturating_add(bytes.max(1)) <= target.max(1)) {
                last.end = group + 1;
                weight = weight.saturating_add(bytes.max(1));
                continue;
            }
        }
        ranges.push(group..group + 1);
        weight = bytes.max(1);
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallel_work_can_exceed_worker_count() {
        for workers in [2_usize, 3, 8, 32] {
            let target = 85_u64.div_ceil(workers.saturating_mul(2) as u64);
            let ranges = row_group_ranges((0..85).map(|group| (group, 1)), Some(target));
            assert!(ranges.len() > workers);
            assert!(ranges.iter().all(|range| range.len() <= target as usize));
            assert_eq!(ranges.into_iter().flatten().collect::<Vec<_>>(), (0..85).collect::<Vec<_>>());
        }
    }

    #[test]
    fn single_worker_coalesces_only_contiguous_candidates() {
        assert_eq!(row_group_ranges([0, 1, 4, 5, 8].map(|group| (group, 1)), None), vec![0..2, 4..6, 8..9]);
    }

    #[test]
    fn rejected_groups_remain_excluded_at_any_parallelism() {
        for budget in [None, Some(1), Some(8)] {
            let ranges = row_group_ranges([1, 4, 8].map(|group| (group, 1)), budget);
            assert_eq!(ranges.into_iter().flatten().collect::<Vec<_>>(), vec![1, 4, 8]);
            assert!(row_group_ranges([], budget).is_empty());
        }
    }

    #[test]
    fn oversized_groups_and_zero_weights_do_not_lose_candidates() {
        let ranges = row_group_ranges([(0, 1), (1, 100), (2, 0), (3, 1)], Some(5));
        assert_eq!(ranges, vec![0..1, 1..2, 2..4]);
        assert_eq!(row_group_ranges([(0, 0), (1, 0)], Some(0)), vec![0..1, 1..2]);
    }
}
