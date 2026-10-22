use std::sync::Arc;

use arrow::array::BooleanArray;
use common::Result;
use exec::pipeline::DataChunkView;
use exec::runtime::filter::{RuntimeFilter, RuntimeFilterSet};
use types::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

use super::AdaptiveFilterOrder;

#[derive(Debug)]
struct TestFilter;

impl RuntimeFilter for TestFilter {
    fn name(&self) -> &'static str { "identity_test" }
    fn required_columns(&self) -> &[ColumnId] { &[] }
    fn memory_bytes(&self) -> usize { 0 }
    fn evaluate(&self, view: &DataChunkView, _: &[usize]) -> Result<BooleanArray> {
        Ok(BooleanArray::from(vec![true; view.num_rows()]))
    }
}

fn filters() -> RuntimeFilterSet {
    RuntimeFilterSet::new(vec![Arc::new(TestFilter), Arc::new(TestFilter)])
}

#[test]
fn runtime_filter_clones_reuse_policy_but_replaced_evaluators_do_not() {
    let schema = VeinSchema::empty();
    let original = filters();
    let first = AdaptiveFilterOrder::for_scan(None, 2, None, &schema, &original).unwrap();
    let same = AdaptiveFilterOrder::for_scan(Some(Arc::clone(&first)), 2,
                                             None, &schema, &original.clone()).unwrap();
    assert!(Arc::ptr_eq(&first, &same));
    let replaced = AdaptiveFilterOrder::for_scan(Some(Arc::clone(&first)), 2,
                                                 None, &schema, &filters()).unwrap();
    assert!(!Arc::ptr_eq(&first, &replaced));
}

#[test]
fn changing_column_bindings_invalidates_policy_even_at_the_same_stage_count() {
    let runtime_filters = filters();
    let first = AdaptiveFilterOrder::for_scan(None, 2, None,
                                              &VeinSchema::empty(), &runtime_filters).unwrap();
    let changed = VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
        ColumnId(0), "other", None, VeinDataType::Int64,
    ))]);
    let replaced = AdaptiveFilterOrder::for_scan(Some(Arc::clone(&first)), 2,
                                                 None, &changed, &runtime_filters).unwrap();
    assert!(!Arc::ptr_eq(&first, &replaced));
}
