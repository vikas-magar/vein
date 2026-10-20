use super::reorder_by_identity;

#[test]
fn applies_multiple_orders_by_stable_identity() {
    let mut items = vec!["zero", "one", "two"];
    let mut current_order = vec![0, 1, 2];

    assert!(reorder_by_identity(&mut items, &mut current_order, &[2, 0, 1]));
    assert_eq!(items, ["two", "zero", "one"]);
    assert_eq!(current_order, [2, 0, 1]);

    assert!(reorder_by_identity(&mut items, &mut current_order, &[1, 2, 0]));
    assert_eq!(items, ["one", "two", "zero"]);
    assert_eq!(current_order, [1, 2, 0]);
}

#[test]
fn rejects_invalid_orders_without_changing_items_or_identity_map() {
    let mut items = vec!["zero", "one", "two"];
    let mut current_order = vec![0, 1, 2];

    assert!(!reorder_by_identity(&mut items, &mut current_order, &[1, 1, 2]));
    assert!(!reorder_by_identity(&mut items, &mut current_order, &[0, 1]));
    assert_eq!(items, ["zero", "one", "two"]);
    assert_eq!(current_order, [0, 1, 2]);
}

#[test]
fn keeps_items_when_requested_order_is_current_order() {
    let mut items = vec!["zero", "one"];
    let mut current_order = vec![0, 1];

    assert!(!reorder_by_identity(&mut items, &mut current_order, &[0, 1]));
    assert_eq!(items, ["zero", "one"]);
}
