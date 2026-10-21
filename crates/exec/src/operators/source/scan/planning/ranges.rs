use std::ops::Range;

/// Coalesces ordered unit indexes into adjacent byte-bounded ranges.
pub fn coalesce_scan_units(
    groups: impl IntoIterator<Item=(usize, u64)>,
    target_bytes: Option<u64>,
) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = None;
    let mut end = 0;
    let mut bytes = 0_u64;

    for (index, weight) in groups {
        if start.is_some_and(|_| index != end) {
            if let Some(range_start) = start.take() {
                ranges.push(range_start..end);
            }
            bytes = 0;
        }
        start.get_or_insert(index);
        end = index.saturating_add(1);
        bytes = bytes.saturating_add(weight.max(1));
        if target_bytes.is_some_and(|target| bytes >= target) {
            if let Some(range_start) = start.take() {
                ranges.push(range_start..end);
            }
            bytes = 0;
        }
    }

    if let Some(start) = start {
        ranges.push(start..end);
    }
    ranges
}

#[cfg(test)]
#[path = "ranges_tests.rs"]
mod tests;
