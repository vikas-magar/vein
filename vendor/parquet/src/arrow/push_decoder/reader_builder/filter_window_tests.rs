use super::FilterWindowProgress;
use crate::arrow::arrow_reader::RowSelection;

fn selection(ranges: &[std::ops::Range<usize>], rows: usize) -> RowSelection {
    RowSelection::from_consecutive_ranges(ranges.iter().cloned(), rows)
}

#[test]
fn windows_preserve_absolute_row_ranges_and_accumulate_results() {
    let mut progress = FilterWindowProgress::new(10, 4, None);
    let first = progress.next_window().unwrap();
    let second = progress.next_window().unwrap();
    let third = progress.next_window().unwrap();

    assert_eq!(first, selection(&[0..4], 10));
    assert_eq!(second, selection(&[4..8], 10));
    assert_eq!(third, selection(&[8..10], 10));
    assert!(progress.next_window().is_none());

    progress.record_completed_window(&selection(&[1..3], 10));
    progress.record_completed_window(&selection(&[5..6], 10));
    assert_eq!(progress.selected_rows(), 3);
    assert_eq!(progress.output_selection(), selection(&[1..3, 5..6], 10));
}

#[test]
fn input_selection_skips_empty_windows_without_losing_row_positions() {
    let input = selection(&[1..2, 6..9], 10);
    let mut progress = FilterWindowProgress::new(10, 3, Some(input));

    assert_eq!(progress.next_window(), Some(selection(&[1..2], 10)));
    assert_eq!(progress.next_window(), Some(selection(&[6..9], 10)));
    assert!(progress.next_window().is_none());
}

#[test]
fn completed_count_supports_limit_driven_window_cancellation() {
    let mut progress = FilterWindowProgress::new(8, 4, None);
    let first = progress.next_window().unwrap();
    progress.record_completed_window(&selection(&[0..3], 8));
    assert_eq!(progress.selected_rows(), 3);

    progress.stop_after_current_window();
    assert!(progress.next_window().is_none());
    assert_eq!(progress.output_selection(), selection(&[0..3], 8));
    assert_eq!(first.total_row_count(), 8);
}

#[test]
fn empty_row_group_has_a_well_formed_empty_selection() {
    let progress = FilterWindowProgress::new(0, 4, None);
    assert_eq!(
        progress.output_selection(),
        RowSelection::from_consecutive_ranges(std::iter::empty(), 0)
    );
}
