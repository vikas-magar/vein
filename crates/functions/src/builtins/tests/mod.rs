use crate::{FunctionCatalog, FunctionKind, FunctionRegistry};

#[test]
fn builtins_share_one_case_insensitive_metadata_catalog() {
    let registry = FunctionRegistry::with_builtins();
    assert_eq!(registry.resolve("year").unwrap().kind, FunctionKind::TemporalExtract);
    assert_eq!(registry.resolve("SUM").unwrap().kind, FunctionKind::Aggregate);
    assert_eq!(registry.resolve("vector_distance").unwrap().kind, FunctionKind::Scalar);
    assert_eq!(registry.resolve("shift").unwrap().kind, FunctionKind::TemporalShift);
    assert_eq!(registry.resolve("embed").unwrap().kind, FunctionKind::AiOperator);
}
