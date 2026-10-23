use super::*;

#[test]
fn reduction_recomputes_null_extension_without_losing_intrinsic_nullability() {
    for intrinsic_nullable in [false, true] {
        for (original, expected, predicate, expected_nulls) in [
            (JoinType::Left, JoinType::Inner, E::IsNotNull { expr: Box::new(column(2)) }, [false, intrinsic_nullable]),
            (JoinType::Right, JoinType::Inner, E::IsNotNull { expr: Box::new(column(1)) }, [false, intrinsic_nullable]),
            (JoinType::Full, JoinType::Left, E::IsNotNull { expr: Box::new(column(1)) }, [false, true]),
            (JoinType::Full, JoinType::Right, E::IsNotNull { expr: Box::new(column(2)) }, [true, intrinsic_nullable]),
        ] {
            let mut plan = filtered(predicate);
            let LogicalPlan::Filter { input, .. } = &mut plan else { unreachable!() };
            let LogicalPlan::Join { join_type, right, schema, .. } = input.as_mut() else { unreachable!() };
            *join_type = original;
            let LogicalPlan::Scan { schema: right_schema, .. } = right.as_mut() else { unreachable!() };
            Arc::make_mut(&mut right_schema.fields[0]).nullable = intrinsic_nullable;
            for (index, field) in schema.fields.iter_mut().enumerate() {
                Arc::make_mut(field).nullable = if index == 0 {
                    matches!(original, JoinType::Right | JoinType::Full)
                } else {
                    intrinsic_nullable || matches!(original, JoinType::Left | JoinType::Full)
                };
            }
            let result = rule().apply(&plan, &OptimizerContext::default()).unwrap().unwrap();
            assert_eq!(result.schema().fields.iter().map(|field| field.nullable).collect::<Vec<_>>(), expected_nulls);
            let LogicalPlan::Filter { input, .. } = result else { unreachable!() };
            assert!(matches!(input.as_ref(), LogicalPlan::Join { join_type, .. } if *join_type == expected));
        }
    }
}
