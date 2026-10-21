use super::*;

fn assert_spill_key_hash_matches_batch(array: ArrayRef) {
    let key = GroupKeyColumn::new(&array).expect("supported grouping key");
    let selection = SelectionVector::new_dense(array.len());
    let hash_builder = DefaultHashBuilder::default();
    let grouping_set_count = 2;
    let mut batch_hashers = (0..array.len() * grouping_set_count)
        .map(|_| hash_builder.build_hasher())
        .collect::<Vec<_>>();
    for (index, hasher) in batch_hashers.iter_mut().enumerate() {
        (index % grouping_set_count).hash(hasher);
    }
    key.hash_batch(0, &selection, &[0, 1], grouping_set_count, &mut batch_hashers);

    for row in 0..array.len() {
        for set_id in 0..grouping_set_count {
            let mut spill_hasher = hash_builder.build_hasher();
            set_id.hash(&mut spill_hasher);
            key.hash_row(0, row, &mut spill_hasher);
            assert_eq!(
                spill_hasher.finish(),
                batch_hashers[row * grouping_set_count + set_id].finish(),
                "hash differs at row {row}, grouping set {set_id}"
            );
        }
    }
}

#[test]
fn spill_key_hash_matches_live_hash_for_typed_null_and_dictionary_keys() {
    assert_spill_key_hash_matches_batch(Arc::new(Int64Array::from(vec![
        Some(-7), None, Some(42), Some(-7),
    ])));
    assert_spill_key_hash_matches_batch(Arc::new(UInt32Array::from(vec![
        Some(7), None, Some(u32::MAX), Some(7),
    ])));
    assert_spill_key_hash_matches_batch(Arc::new(StringArray::from(vec![
        Some("alpha"), None, Some("beta"), Some("alpha"),
    ])));

    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), None, Some(1), Some(2)]),
        Arc::new(StringArray::from(vec![Some("alpha"), Some("beta"), None])),
    )
        .expect("valid dictionary keys");
    assert_spill_key_hash_matches_batch(Arc::new(dictionary));
}
