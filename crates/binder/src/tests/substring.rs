use super::{bind, bind_err};
use plan::{LogicalExpr, LogicalPlan, VeinDataType};

#[tokio::test]
async fn substring_binds_two_and_three_argument_forms() {
    for (query, expected_args) in [
        ("FROM users |> SELECT substring(name, 1)", 2),
        ("FROM users |> SELECT substring(name, 1, 2)", 3),
    ] {
        let plan = bind(query).await;
        let LogicalPlan::Project { exprs, schema, .. } = plan else {
            panic!("expected a projection for {query}");
        };
        assert!(matches!(
            &exprs[0],
            LogicalExpr::FunctionCall { name, args, return_type, .. }
                if name.eq_ignore_ascii_case("substring")
                    && args.len() == expected_args
                    && *return_type == VeinDataType::Utf8
        ));
        assert_eq!(schema.fields[0].data_type, VeinDataType::Utf8);
    }
}

#[tokio::test]
async fn substring_rejects_invalid_arity_and_argument_types() {
    for (query, message) in [
        ("FROM users |> SELECT substring(name)", "expects between 2 and 3"),
        (
            "FROM users |> SELECT substring(name, 1, 2, 3)",
            "expects between 2 and 3",
        ),
        (
            "FROM users |> SELECT substring(age, 1, 2)",
            "argument 1 expects Utf8",
        ),
        (
            "FROM users |> SELECT substring(name, 1, '2')",
            "argument 3 expects Int64",
        ),
    ] {
        let error = bind_err(query).await;
        assert!(error.to_string().contains(message), "{error} for {query}");
    }
}
