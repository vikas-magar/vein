use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use common::arrow::SelectionVector;
use common::protocol::{LimitSpec, SortKey, SortPlan};

use super::*;
use crate::pipeline::DataChunkView;

#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<DataChunkView>>>);

impl PipelineNode<DataChunkView> for Capture {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.lock().unwrap().push(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn collect_values(views: &[DataChunkView]) -> Vec<Option<i64>> {
    views
        .iter()
        .flat_map(|view| {
            (0..view.num_rows()).map(|row| {
                let (array, index) = view.resolve(0, row).unwrap()?;
                let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
                values.is_valid(index).then(|| values.value(index))
            })
        })
        .collect()
}

#[test]
fn full_sort_and_top_n_keep_sparse_source_buffers() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![9, 4, 7, 2, 5]));
    for limit in [None, Some(LimitSpec { offset: 0, count: 3 })] {
        let expects_top_n = limit.is_some();
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut sort = SortTransform::with_limit(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            None,
            limit,
            Box::new(Capture(Arc::clone(&output))),
        );
        let input = crate::pipeline::test_support::selected_view(
            vec![Arc::clone(&source)],
            SelectionVector::new_sparse(vec![4, 1, 3, 2]),
        );
        sort.push(input).unwrap();
        sort.flush().unwrap();

        let output = output.lock().unwrap();
        assert_eq!(
            collect_values(&output),
            if expects_top_n {
                vec![Some(2), Some(4), Some(5)]
            } else {
                vec![Some(2), Some(4), Some(5), Some(7)]
            }
        );
        assert_eq!(sort.metrics.key_materialization, MaterializationReport::default());
        assert!(sort.metrics.row_comparisons > 0);
        for view in output.iter() {
            for row in 0..view.num_rows() {
                let (array, _) = view.resolve(0, row).unwrap().unwrap();
                assert!(Arc::ptr_eq(array, &source));
            }
        }
    }
}

#[test]
fn comparator_orders_null_extended_rows_without_building_null_arrays() {
    for (nulls_first, expected) in [
        (true, vec![None, Some(1), Some(2)]),
        (false, vec![Some(1), Some(2), None]),
    ] {
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut sort = SortTransform::new(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first,
                }],
            },
            Box::new(Capture(Arc::clone(&output))),
        );
        let values: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));
        sort.push(
            crate::pipeline::test_support::dense_view(vec![values]),
        )
            .unwrap();
        sort.push(DataChunkView::nulls(&[DataType::Int64], 1).unwrap()).unwrap();
        sort.flush().unwrap();

        let output = output.lock().unwrap();
        assert_eq!(collect_values(&output), expected);
        assert_eq!(sort.metrics.key_materialization, MaterializationReport::default());
    }
}
