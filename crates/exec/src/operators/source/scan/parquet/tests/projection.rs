use super::*;

#[test]
fn dictionary_schema_hint_changes_only_requested_utf8_fields() {
    let metadata =
        std::collections::HashMap::from([("source".to_string(), "test".to_string())]);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("status", DataType::Utf8, true),
            Field::new("orderkey", DataType::Int64, false),
        ],
        metadata,
    ));
    let projected = VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(
            ColumnId(0),
            "status",
            None,
            VeinDataType::Utf8,
        )),
        Arc::new(ResolvedColumn::new(
            ColumnId(1),
            "orderkey",
            None,
            VeinDataType::Int64,
        )),
    ]);

    let hinted = dictionary_schema_hint(&schema, &projected, &[0]).expect("valid hint");

    assert_eq!(
        hinted.field(0).data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    );
    assert!(hinted.field(0).is_nullable());
    assert_eq!(hinted.field(1).data_type(), &DataType::Int64);
    assert_eq!(hinted.metadata()["source"], "test");
}

#[test]
fn dictionary_schema_hint_rejects_non_utf8_and_out_of_range_fields() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let projected = VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
        ColumnId(0),
        "id",
        None,
        VeinDataType::Int64,
    ))]);

    assert!(dictionary_schema_hint(&schema, &projected, &[0]).is_err());
    assert!(dictionary_schema_hint(&schema, &projected, &[1]).is_err());
}
