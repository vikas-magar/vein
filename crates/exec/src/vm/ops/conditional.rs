//! Vector kernels for SQL CASE and IN/NOT IN expressions.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, new_null_array};
use arrow::compute::kernels::{boolean, zip};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use crate::vm::register::RegValue;

/// Evaluates SQL IN/NOT IN while preserving nullable comparison semantics.
pub fn eval_in_list(
    value: &RegValue,
    candidates: &[RegValue],
    rows: usize,
    negated: bool,
) -> Result<ArrayRef> {
    let mut result = BooleanArray::from(vec![Some(false); rows]);
    for candidate in candidates {
        // Arrow represents an untyped NULL literal with NullArray. Comparison
        // kernels cannot compare that physical type to a typed value, while SQL
        // defines every equality involving NULL as unknown.
        let equality = if value.data_type() == &DataType::Null
            || candidate.data_type() == &DataType::Null
        {
            BooleanArray::from(vec![None; rows])
        } else {
            arrow::compute::kernels::cmp::eq(value, candidate)
                .map_err(|e| VeinError::internal(format!("Vector VM IN equality error: {e}")))?
        };
        result = boolean::or_kleene(&result, &equality)
            .map_err(|e| VeinError::internal(format!("Vector VM IN combine error: {e}")))?;
    }
    if negated {
        result = boolean::not(&result)
            .map_err(|e| VeinError::internal(format!("Vector VM NOT IN error: {e}")))?;
    }
    Ok(Arc::new(result))
}

/// Evaluates a searched CASE expression. Inputs remain Arrow arrays/scalars; only
/// the selected output values are assembled into the result array.
pub fn eval_case(
    conditions: &[RegValue],
    results: &[RegValue],
    otherwise: Option<&RegValue>,
    return_type: &DataType,
    rows: usize,
) -> Result<ArrayRef> {
    if conditions.len() != results.len() {
        return Err(VeinError::internal(
            "CASE condition/result arm count mismatch",
        ));
    }

    let mut output = otherwise
        .cloned()
        .unwrap_or_else(|| RegValue::Array(new_null_array(return_type, rows)));

    for (condition, result) in conditions.iter().zip(results).rev() {
        let condition_array = if condition.data_type() == &DataType::Null {
            // A bare NULL has Arrow's Null type, but SQL permits it as a WHEN
            // condition and considers it non-true, just like NULL::BOOLEAN.
            BooleanArray::from(vec![None; rows])
        } else if condition.is_scalar() {
            let array = condition.array();
            let boolean = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| VeinError::internal("CASE WHEN condition must be boolean"))?;
            let scalar = boolean.value(0);
            BooleanArray::from(vec![
                if boolean.is_null(0) {
                    None
                } else {
                    Some(scalar)
                };
                rows
            ])
        } else {
            condition
                .array()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| VeinError::internal("CASE WHEN condition must be boolean"))?
        };
        output = RegValue::Array(
            zip::zip(&condition_array, result, &output)
                .map_err(|e| VeinError::internal(format!("Vector VM CASE zip error: {e}")))?,
        );
    }

    if output.data_type() != return_type {
        return Err(VeinError::internal(format!(
            "CASE result type mismatch: expected {return_type}, got {}",
            output.data_type()
        )));
    }
    Ok(output.array().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    fn array(value: impl Array + 'static) -> RegValue {
        RegValue::Array(Arc::new(value))
    }

    fn scalar(value: impl Array + 'static) -> RegValue {
        RegValue::Scalar(Arc::new(value))
    }

    #[test]
    fn in_list_obeys_sql_null_and_negation_rules() {
        let values = array(Int64Array::from(vec![Some(1), Some(2), None, Some(4)]));
        let candidates = vec![
            scalar(Int64Array::from(vec![2])),
            scalar(Int64Array::from(vec![3])),
            scalar(Int64Array::from(vec![None])),
        ];

        let result = eval_in_list(&values, &candidates, 4, false).unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result,
            &BooleanArray::from(vec![None, Some(true), None, None])
        );

        let result = eval_in_list(&values, &candidates, 4, true).unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result,
            &BooleanArray::from(vec![None, Some(false), None, None])
        );
    }

    #[test]
    fn empty_in_candidate_set_returns_identity_values() {
        let values = array(Int64Array::from(vec![Some(1), None]));
        let matched = eval_in_list(&values, &[], 2, false).unwrap();
        assert_eq!(
            matched.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![false, false])
        );
        let unmatched = eval_in_list(&values, &[], 2, true).unwrap();
        assert_eq!(
            unmatched.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![true, true])
        );
    }

    #[test]
    fn case_uses_first_true_arm_and_treats_null_when_as_false() {
        let conditions = vec![
            array(BooleanArray::from(vec![Some(true), Some(false), None])),
            array(BooleanArray::from(vec![Some(true), Some(true), Some(true)])),
        ];
        let results = vec![
            array(StringArray::from(vec![
                Some("first"),
                Some("first"),
                Some("first"),
            ])),
            array(StringArray::from(vec![
                Some("second"),
                Some("second"),
                Some("second"),
            ])),
        ];
        let otherwise = scalar(StringArray::from(vec![Some("else")]));

        let result =
            eval_case(&conditions, &results, Some(&otherwise), &DataType::Utf8, 3).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            result,
            &StringArray::from(vec![Some("first"), Some("second"), Some("second")])
        );
    }

    #[test]
    fn case_without_else_returns_null_of_bound_result_type() {
        let conditions = vec![array(BooleanArray::from(vec![false, false]))];
        let results = vec![array(Int64Array::from(vec![10, 20]))];
        let result = eval_case(&conditions, &results, None, &DataType::Int64, 2).unwrap();
        assert_eq!(
            result.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![None, None])
        );
    }

    #[test]
    fn case_treats_untyped_null_when_as_not_true() {
        let conditions = vec![scalar(arrow::array::NullArray::new(1))];
        let results = vec![scalar(Int64Array::from(vec![7]))];
        let otherwise = scalar(Int64Array::from(vec![9]));
        let result = eval_case(
            &conditions,
            &results,
            Some(&otherwise),
            &DataType::Int64,
            2,
        )
        .unwrap();
        assert_eq!(
            result.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![9, 9])
        );
    }
}
