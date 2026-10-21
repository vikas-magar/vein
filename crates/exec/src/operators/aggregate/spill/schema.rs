use super::*;

impl AggregateSpillSchema {
    pub(in crate::operators::aggregate) fn try_new(
        key_types: Vec<VeinDataType>,
        grouping_sets: Vec<Vec<usize>>,
    ) -> Result<Self> {
        for grouping_set in &grouping_sets {
            if grouping_set.iter().any(|&index| index >= key_types.len()) {
                return Err(VeinError::internal(
                    "aggregate spill grouping set references an unknown key",
                ));
            }
        }

        let mut fields = Vec::with_capacity(key_types.len().saturating_add(3));
        fields.push(Field::new(GROUPING_SET_ID_FIELD, DataType::UInt64, false));
        fields.push(Field::new(HASH_FIELD, DataType::UInt64, false));
        fields.extend(key_types.iter().enumerate().map(|(index, data_type)| {
            Field::new(format!("key_{index}"), data_type.to_arrow(), true)
        }));
        fields.push(Field::new(STATE_FIELD, DataType::Binary, false));

        let mut metadata = HashMap::new();
        metadata.insert(
            FORMAT_METADATA_KEY.to_owned(),
            FORMAT_METADATA_VALUE.to_owned(),
        );
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        Ok(Self {
            key_types,
            grouping_sets,
            schema,
        })
    }
}
