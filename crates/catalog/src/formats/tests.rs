use super::{DataFileFormatId, TableProviderId};

#[test]
fn identifiers_accept_registered_values_without_closed_enum_variants() {
    let file_format = DataFileFormatId::new("vendor.columnar.v2");
    let provider = TableProviderId::new("vendor.snapshot-table.v1");

    assert_eq!(file_format.as_str(), "vendor.columnar.v2");
    assert_eq!(provider.to_string(), "vendor.snapshot-table.v1");
}

#[test]
fn identifiers_clone_and_serialize_as_their_stable_string() {
    let file_format = DataFileFormatId::new("parquet");
    let cloned = file_format.clone();

    assert_eq!(cloned, file_format);
    assert_eq!(
        serde_json::to_string(&cloned).expect("serialize format identifier"),
        "\"parquet\""
    );
}
