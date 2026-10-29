use super::model::{Sources, rows};
use super::*;

#[test]
fn preserves_bag_semantics_for_duplicates_nulls_and_empty_inputs() {
    let populations = [
        vec![], vec![None], vec![Some(1)], vec![Some(2)],
        vec![Some(1), Some(1), None], vec![Some(1), Some(2), Some(2)],
    ];
    for probe_key in [1, 2] {
        let original = candidate(probe_key);
        let optimized = rewrite(original.clone());
        for a in &populations {
            for b in &populations {
                for c in &populations {
                    let sources = Sources::from([(1, a.clone()), (2, b.clone()), (3, c.clone())]);
                    assert_eq!(rows(&optimized, &sources), rows(&original, &sources),
                               "probe={probe_key} inputs={sources:?}");
                }
            }
        }
    }
}

#[test]
fn preserves_residual_semantics_without_multiplying_build_duplicates() {
    let mut original = candidate(1);
    let LogicalPlan::Join { on, .. } = &mut original else { unreachable!() };
    *on = Some(binary(on.take().unwrap(), BinaryOp::And,
                      binary(column(1), BinaryOp::Leq, column(3))));
    let sources = Sources::from([
        (1, vec![Some(1), Some(1), Some(2), None]),
        (2, vec![Some(1), Some(1), Some(2), None]),
        (3, vec![Some(1), Some(1), None]),
    ]);
    let expected = rows(&original, &sources);
    assert_eq!(expected.len(), 4, "semi build duplicates must not duplicate probe rows");
    assert_eq!(rows(&rewrite(original), &sources), expected);
}
