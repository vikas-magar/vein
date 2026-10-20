use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
use arrow_schema::{DataType, Field, Fields};

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{ArrayReader, StructArrayReader};
use crate::arrow::array_reader::test_util::make_int32_page_reader;
use crate::arrow::arrow_reader::{ArrowPredicateFn, RowSelection};

use super::{PredicateOptions, ReadPlanBuilder};

fn int_reader(rows: usize) -> Box<dyn ArrayReader> {
    let values = (0..rows as i32).collect::<Vec<_>>();
    let levels = vec![0; rows];
    let leaf = make_int32_page_reader(&values, &levels, &levels, 0, 0, None);
    let data_type = DataType::Struct(Fields::from(vec![Field::new("value", DataType::Int32, false)]));
    Box::new(StructArrayReader::new(data_type, vec![leaf], 0, 0, false, None))
}

#[test]
fn sequential_predicate_selections_match_the_composed_filter_result() {
    let mut first = ArrowPredicateFn::new(ProjectionMask::all(), |batch| {
        assert_eq!(batch.num_rows(), 6);
        Ok(BooleanArray::from(vec![true, false, true, true, false, true]))
    });
    let first_plan = ReadPlanBuilder::new(16)
        .with_predicate_options(PredicateOptions::new(int_reader(6), &mut first))
        .expect("first selection stage");

    let mut second = ArrowPredicateFn::new(ProjectionMask::all(), |batch| {
        assert_eq!(batch.num_rows(), 4);
        Ok(BooleanArray::from(vec![true, false, true, false]))
    });
    let plan = first_plan
        .with_predicate_options(PredicateOptions::new(int_reader(6), &mut second))
        .expect("second selection stage");

    let expected = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
        true, false, false, true, false, false,
    ]));
    assert_eq!(plan.selection(), Some(&expected));
}
