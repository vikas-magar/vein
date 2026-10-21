use super::*;

fn policy() -> RowGroupBatchSizePolicy {
    RowGroupBatchSizePolicy::default()
}

#[test]
fn derives_default_from_largest_assigned_row_group() {
    let selected = policy().select([8_192, 64_000, 32_000], None);

    assert_eq!(selected.rows, 64_000);
    assert_eq!(selected.source, BatchSizeSource::RowGroupMetadata);
    assert_eq!(selected.largest_assigned_row_group, Some(64_000));
    assert!(!selected.invalid_override);
}

#[test]
fn caps_default_for_oversized_row_groups() {
    let selected = policy().select([200_000], None);

    assert_eq!(selected.rows, MAX_DEFAULT_BATCH_ROWS);
    assert_eq!(selected.largest_assigned_row_group, Some(200_000));
}

#[test]
fn uses_fallback_when_no_valid_row_group_metadata_exists() {
    let selected = policy().select([-1, 0], None);

    assert_eq!(selected.rows, FALLBACK_BATCH_ROWS);
    assert_eq!(selected.source, BatchSizeSource::Fallback);
    assert_eq!(selected.largest_assigned_row_group, None);
}

#[test]
fn valid_override_takes_precedence_over_metadata() {
    let selected = policy().select([64_000], Some(" 32768 "));

    assert_eq!(selected.rows, 32_768);
    assert_eq!(selected.source, BatchSizeSource::EnvironmentOverride);
    assert!(!selected.invalid_override);
}

#[test]
fn invalid_or_zero_override_falls_back_to_metadata() {
    for value in ["invalid", "0"] {
        let selected = policy().select([64_000], Some(value));

        assert_eq!(selected.rows, 64_000);
        assert_eq!(selected.source, BatchSizeSource::RowGroupMetadata);
        assert!(selected.invalid_override);
    }
}
