//! Aggregate key types regression coverage.

use super::*;

macro_rules! assert_unsigned_group_key_layout {
    ($array:ty, $data_type:expr) => {{
        let plan = AggregatePlan {
            groups: 1,
            estimated_groups: None,
            grouping_sets: None,
            emit_empty_group_on_empty_input: true,
            aggs: vec![AggOpCode::Sum],
            aggregate_input_types: vec![VeinDataType::Int64],
            types: vec![$data_type, VeinDataType::Int64],
        };
        let mut sink = AggregateSink::new(plan).unwrap();

        sink.accumulate(crate::pipeline::test_support::dense_view(vec![
            Arc::new(<$array>::from(vec![Some(2), None])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ]))
        .unwrap();
        sink.accumulate(crate::pipeline::test_support::dense_view(vec![
            Arc::new(<$array>::from(vec![Some(2), Some(5)])),
            Arc::new(Int64Array::from(vec![3, 4])),
        ]))
        .unwrap();

        let output = sink.finish().unwrap();
        let keys = crate::pipeline::test_support::materialized_column::<$array>(&output[0], 0);
        let sums = crate::pipeline::test_support::materialized_column::<Int64Array>(&output[0], 1);

        assert_eq!(keys.len(), 3);
        assert_eq!(keys.value(0), 2);
        assert!(keys.is_null(1));
        assert_eq!(keys.value(2), 5);
        assert_eq!(sums.values(), &[4, 2, 4]);
    }};
}

#[test]
fn unsigned_group_keys_preserve_width_nulls_and_cross_batch_groups() {
    assert_unsigned_group_key_layout!(UInt8Array, VeinDataType::UInt8);
    assert_unsigned_group_key_layout!(UInt16Array, VeinDataType::UInt16);
    assert_unsigned_group_key_layout!(UInt32Array, VeinDataType::UInt32);
    assert_unsigned_group_key_layout!(UInt64Array, VeinDataType::UInt64);
}

#[test]
fn dictionary_utf8_grouping_handles_nulls_unused_values_and_batch_local_ids() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Utf8, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(plan).unwrap();
    let first_dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), Some(1), Some(0), None, Some(2), Some(3)]),
        Arc::new(StringArray::from(vec![
            Some("O"),
            Some("F"),
            None,
            Some("O"),
            Some("unused"),
        ])),
    )
        .unwrap();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(first_dictionary),
        Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
    ]))
        .unwrap();

    let second_dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), Some(1)]),
        Arc::new(StringArray::from(vec![Some("F"), Some("O")])),
    )
        .unwrap();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(second_dictionary),
        Arc::new(Int64Array::from(vec![60, 70])),
    ]))
        .unwrap();

    let output = sink.finish().unwrap();
    let keys = crate::pipeline::test_support::materialized_column::<StringArray>(&output[0], 0);
    let sums = crate::pipeline::test_support::materialized_column::<Int64Array>(&output[0], 1);
    let actual = (0..output[0].num_rows())
        .map(|row| {
            (
                (!keys.is_null(row)).then(|| keys.value(row)),
                sums.value(row),
            )
        })
        .collect::<std::collections::HashMap<_, _>>();

    let metrics = sink.metrics();
    assert_eq!(actual.get(&Some("O")), Some(&170));
    assert_eq!(actual.get(&Some("F")), Some(&80));
    assert_eq!(actual.get(&None), Some(&90));
    assert_eq!(
        actual.len(),
        3,
        "unused dictionary values must not form groups"
    );
    assert_eq!(metrics.input_rows, 8);
    assert_eq!(
        metrics.grouping_probes,
        8,
        "view-backed keys probe input rows while preserving NULL grouping semantics"
    );
    assert_eq!(metrics.new_groups, 3);
    assert_eq!(metrics.string_key_bytes_copied, 0);
}
