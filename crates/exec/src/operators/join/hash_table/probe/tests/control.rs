use super::*;
use super::super::control::WorkBudget;

#[test]
fn control_polling_has_a_bounded_work_interval_for_every_batch_geometry() {
    for rows in [0, 1, 7, 1024, 65_536, usize::MAX] {
        let mut budget = WorkBudget::for_batch(rows);
        let quantum = rows.clamp(1, 1024);
        for work in 0..quantum * 3 {
            assert_eq!(budget.should_poll(), work % quantum == 0);
        }
    }
}

#[test]
fn large_probe_batches_do_not_poll_downstream_for_every_row() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7)])).unwrap();
    let input = view(vec![Some(7); 20_000]);
    let mut cursor = HashProbe::new(&table, &input, false).unwrap();
    let mut batch = Vec::new();
    let limit = cursor.reserve_candidates(&mut batch).unwrap();
    let mut calls = 0;
    assert!(cursor.next_batch(&mut batch, limit, || { calls += 1; false }).unwrap());
    assert_eq!(batch.len(), 20_000);
    assert!(calls < 100, "control checks must follow the work quantum: {calls}");
    assert_eq!(cursor.metrics.control_polls, calls);
}

#[test]
fn batched_polling_stops_an_adversarial_chain_within_its_work_bound() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7); 10_000])).unwrap();
    table.buckets.fill(9_999);
    let input = view(vec![Some(9)]);
    let mut cursor = HashProbe::new(&table, &input, false).unwrap();
    let mut batch = Vec::new();
    cursor.reserve_candidates(&mut batch).unwrap();
    let mut polls = 0;
    assert!(!cursor.next_batch(&mut batch, 10_000, || { polls += 1; polls >= 2 }).unwrap());
    assert!(cursor.examined <= 1024);
    assert!(batch.is_empty());
}
