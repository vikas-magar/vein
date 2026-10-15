//! Page-header audit configuration and safe page-selection helpers.

use parquet::arrow::arrow_reader::RowGroupSelection;

use super::page_header_reader::HeaderPageLocations;

pub(super) fn page_header_audit_enabled() -> bool {
    std::env::var("VEIN_PARQUET_PAGE_HEADER_AUDIT")
        .ok()
        .is_some_and(|value| {
            let value = value.trim();
            value == "1"
                || value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("yes")
                || value.eq_ignore_ascii_case("on")
        })
}

#[derive(Debug, Default)]
pub(super) struct PagePayloadPruningMetrics {
    pub(super) columns_with_locations: u64,
    pub(super) data_pages_available: u64,
    pub(super) data_pages_selected: u64,
    pub(super) data_pages_skipped: u64,
    pub(super) selected_payload_bytes: u64,
    pub(super) skipped_payload_bytes: u64,
}

pub(super) fn attach_header_page_locations(
    selections: Vec<RowGroupSelection>,
    locations_by_row_group: &HeaderPageLocations,
) -> Vec<RowGroupSelection> {
    selections
        .into_iter()
        .map(|mut selection| {
            if let Some(columns) = locations_by_row_group.get(&selection.row_group_index()) {
                for (column_index, locations) in columns {
                    selection = selection.with_page_locations(*column_index, locations.clone());
                }
            }
            selection
        })
        .collect()
}

pub(super) fn page_payload_pruning_metrics(
    selections: &[RowGroupSelection],
    locations_by_row_group: &HeaderPageLocations,
) -> PagePayloadPruningMetrics {
    let mut metrics = PagePayloadPruningMetrics::default();
    for selection in selections {
        let Some(columns) = locations_by_row_group.get(&selection.row_group_index()) else {
            continue;
        };
        for locations in columns.values() {
            if locations.is_empty() {
                continue;
            }
            metrics.columns_with_locations = metrics.columns_with_locations.saturating_add(1);
            metrics.data_pages_available = metrics
                .data_pages_available
                .saturating_add(u64::try_from(locations.len()).unwrap_or(u64::MAX));
            let total_bytes = locations.iter().fold(0_u64, |total, page| {
                total.saturating_add(u64::try_from(page.compressed_page_size).unwrap_or(0))
            });
            let selected_ranges = selection
                .selection()
                .map(|rows| rows.scan_ranges(locations));
            let selected_pages = locations
                .iter()
                .filter(|page| {
                    selected_ranges.as_ref().is_none_or(|ranges| {
                        ranges.iter().any(|range| {
                            u64::try_from(page.offset).ok().is_some_and(|offset| {
                                offset >= range.start
                                    && offset
                                    .checked_add(
                                        u64::try_from(page.compressed_page_size).unwrap_or(0),
                                    )
                                    .is_some_and(|end| end <= range.end)
                            })
                        })
                    })
                })
                .collect::<Vec<_>>();
            let selected_bytes = selected_pages.iter().fold(0_u64, |total, page| {
                total.saturating_add(u64::try_from(page.compressed_page_size).unwrap_or(0))
            });
            metrics.data_pages_selected = metrics
                .data_pages_selected
                .saturating_add(u64::try_from(selected_pages.len()).unwrap_or(u64::MAX));
            metrics.data_pages_skipped = metrics
                .data_pages_skipped
                .saturating_add(u64::try_from(locations.len() - selected_pages.len()).unwrap_or(u64::MAX));
            metrics.selected_payload_bytes = metrics
                .selected_payload_bytes
                .saturating_add(selected_bytes.min(total_bytes));
            metrics.skipped_payload_bytes = metrics
                .skipped_payload_bytes
                .saturating_add(total_bytes.saturating_sub(selected_bytes));
        }
    }
    metrics
}
