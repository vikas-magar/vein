use super::*;
use plan::LogicalLiteral;
use plan::{ColumnId, VeinDataType};

fn call() -> LogicalExpr {
    LogicalExpr::WindowCall {
        name: "sum".to_string(),
        args: vec![LogicalExpr::Literal(LogicalLiteral::Int64(1))],
        is_aggregate: true,
        distinct: false,
        partition_by: vec![],
        order_by: vec![],
        frame: None,
        return_type: VeinDataType::Int64,
    }
}

fn col(id: usize) -> LogicalExpr {
    LogicalExpr::Column(std::sync::Arc::new(ResolvedColumn::new(
        ColumnId(id),
        format!("c{id}"),
        None,
        VeinDataType::Int64,
    )))
}

#[test]
fn lifts_and_replaces() {
    let catalog = crate::testing::TestCatalog::default();
    let registry = functions::FunctionRegistry::with_builtins();
    let mut ctx = crate::context::BinderContext::new(&catalog, &registry);
    let mut scopes = ScopeStack::new();
    let items = vec![(call(), Some("total".to_string()))];
    let (plain, windows) = split_window_items(items, &mut ctx, &mut scopes).unwrap();

    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].output_column.name, "total");
    assert_eq!(plain.len(), 1);
    assert!(
        matches!(&plain[0].0, LogicalExpr::Column(c) if c.id == windows[0].output_column.id)
    );
}

#[test]
fn keeps_plain_items_untouched() {
    let catalog = crate::testing::TestCatalog::default();
    let registry = functions::FunctionRegistry::with_builtins();
    let mut ctx = crate::context::BinderContext::new(&catalog, &registry);
    let mut scopes = ScopeStack::new();
    let items = vec![(col(7), None), (call(), None), (col(8), None)];
    let (plain, windows) = split_window_items(items, &mut ctx, &mut scopes).unwrap();

    assert_eq!(plain.len(), 3);
    assert_eq!(windows.len(), 1);
    assert_eq!(plain[0].0, col(7));
    assert_eq!(plain[2].0, col(8));
}
