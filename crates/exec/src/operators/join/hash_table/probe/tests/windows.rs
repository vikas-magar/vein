use super::*;

#[test]
fn existence_retries_hash_each_row_once_across_multiple_windows() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7); 3])).unwrap();
    let input = view(vec![Some(7); 2500]);
    let mut cursor = HashProbe::new(&table, &input, true).unwrap();
    let mut batch = Vec::new();
    let limit = cursor.reserve_candidates(&mut batch).unwrap();
    let mut seen = vec![0; input.num_rows()];
    while cursor.next_batch(&mut batch, limit, || false).unwrap() {
        for &(row, _) in &batch {
            seen[row as usize] += 1;
            if seen[row as usize] == 3 { cursor.accept(row as usize); }
        }
    }
    assert!(seen.iter().all(|&count| count == 3));
    assert_eq!(cursor.metrics.hash_rows, 2500);
    assert_eq!(cursor.metrics.bucket_lookups, 2500);
    assert_eq!(cursor.metrics.hash_windows, 3);
    assert_eq!(cursor.examined, 7500);
}

#[test]
fn batch_hashing_preserves_duplicate_order_across_window_and_candidate_boundaries() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table.insert(view(vec![Some(7), Some(7)])).unwrap();
    let input = view((0..2100).map(|row| if row % 11 == 0 { None } else { Some(7) }).collect());
    let mut cursor = HashProbe::new(&table, &input, false).unwrap();
    let mut batch = Vec::new();
    cursor.reserve_candidates(&mut batch).unwrap();
    let mut actual = Vec::new();
    while cursor.next_batch(&mut batch, 17, || false).unwrap() { actual.extend_from_slice(&batch); }
    let expected = (0..2100).filter(|row| row % 11 != 0)
        .flat_map(|row| [(row, (0, 1)), (row, (0, 0))]).collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(cursor.metrics.hash_rows, 2100);
    assert_eq!(cursor.metrics.hash_windows, 3);
}
