//! Narrow signed integer grouping coverage.

use super::*;

fn assert_grouped(keys: ArrayRef, key_type: VeinDataType) {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![key_type, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(plan).unwrap();
    sink.accumulate(DataChunk::from_vectors(vec![
        keys,
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
    ]))
        .unwrap();

    let output = sink.finish().unwrap();
    let keys = arrow::compute::cast(output[0].column(0), &arrow::datatypes::DataType::Int64)
        .unwrap();
    let keys = keys.as_any().downcast_ref::<Int64Array>().unwrap();
    let sums = output[0].column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    let actual = (0..keys.len())
        .map(|row| (keys.is_valid(row).then(|| keys.value(row)), sums.value(row)))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        actual,
        std::collections::BTreeMap::from([(None, 2), (Some(-2), 5), (Some(4), 3)])
    );
}

#[test]
fn int8_and_int16_group_keys_preserve_values_and_nulls() {
    assert_grouped(
        Arc::new(Int8Array::from(vec![Some(-2), None, Some(4), Some(-2)])),
        VeinDataType::Int8,
    );
    assert_grouped(
        Arc::new(Int16Array::from(vec![Some(-2), None, Some(4), Some(-2)])),
        VeinDataType::Int16,
    );
}
