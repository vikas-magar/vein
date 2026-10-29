//! Typed aggregate fixtures and exact assertions for native wide sums.
use super::*;

pub(super) fn inferred_results(mut plan: AggregatePlan) -> AggregatePlan {
    let registry = FunctionRegistry::with_builtins();
    plan.types.truncate(plan.groups as usize);
    for (operation, input) in plan.aggs.iter().zip(&plan.aggregate_input_types) {
        plan.types.push(registry.aggregate_function(operation.name()).unwrap()
            .return_type(std::slice::from_ref(input)).unwrap());
    }
    plan
}

pub(super) struct Int128Column(Decimal256Array);

impl Int128Column {
    pub(super) fn value(&self, row: usize) -> i128 {
        types::wide_integer::decode(self.0.value(row)).unwrap()
    }
    pub(super) fn values(&self) -> Vec<i128> {
        (0..self.len()).map(|row| self.value(row)).collect()
    }
    pub(super) fn len(&self) -> usize { self.0.len() }
}

pub(super) fn int128_column(view: &DataChunkView, column: usize) -> Int128Column {
    let values = materialized_column::<Decimal256Array>(view, column);
    assert_eq!(values.data_type(), &types::wide_integer::INT128_STORAGE);
    Int128Column(values)
}
