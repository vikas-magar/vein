use super::{ColumnEncodingHint, ColumnEncodingId};
#[test]
fn encoding_hints_keep_projected_position_and_open_encoding_id() {
    let hint = ColumnEncodingHint::dictionary(7);
    assert_eq!(hint.column_index, 7);
    assert_eq!(hint.encoding.as_str(), "arrow_dictionary");

    let custom = ColumnEncodingId::new("connector:delta_encoded");
    assert_eq!(custom.as_str(), "connector:delta_encoded");
}
