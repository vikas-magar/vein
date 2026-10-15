use super::{GroupIndex, GroupIndexEntry, ProbeResolution};
#[test]
fn scalar_resolution_reuses_same_batch_groups_and_resolves_hash_collisions() {
    let hashes = [7, 7, 7];
    let keys = [10, 20, 20];
    let mut index = GroupIndex::default();
    let mut slots = Vec::new();
    let mut retained = Vec::<i64>::new();

    let metrics = index
        .resolve_rows(&hashes, &mut slots, |row, candidate| match candidate {
            Some(slot) if retained[slot] == keys[row] => Ok(ProbeResolution::Match(slot)),
            Some(_) => Ok(ProbeResolution::Retry),
            None => {
                let slot = retained.len();
                retained.push(keys[row]);
                Ok(ProbeResolution::Insert(slot))
            }
        })
        .unwrap();

    assert_eq!(slots, [0, 1, 1]);
    assert_eq!(index.len(), 2);
    assert_eq!(metrics.capacity_growth_events, 1);
    assert_eq!(metrics.entries_at_capacity_growth, 0);
    assert!(metrics.capacity >= index.len());
    let first_capacity = metrics.capacity;

    let metrics = index
        .resolve_rows(&hashes, &mut slots, |row, candidate| match candidate {
            Some(slot) if retained[slot] == keys[row] => Ok(ProbeResolution::Match(slot)),
            Some(_) => Ok(ProbeResolution::Retry),
            None => unreachable!("all keys were inserted by the first batch"),
        })
        .unwrap();
    assert_eq!(metrics.capacity_growth_events, 0);
    assert_eq!(metrics.capacity, first_capacity);
    assert_eq!(metrics.capacity, index.entries.capacity());
}

#[test]
fn repeated_rows_only_grow_for_the_distinct_group() {
    let hashes = vec![7; 4096];
    let mut index = GroupIndex::default();
    let mut slots = Vec::new();

    let metrics = index
        .resolve_rows(&hashes, &mut slots, |_, candidate| {
            Ok(candidate.map_or(ProbeResolution::Insert(0), ProbeResolution::Match))
        })
        .unwrap();

    assert_eq!(slots.len(), hashes.len());
    assert!(slots.iter().all(|&slot| slot == 0));
    assert_eq!(index.len(), 1);
    assert!(metrics.capacity < hashes.len() / 4);
    assert_eq!(metrics.capacity_growth_events, 1);
    assert_eq!(metrics.entries_at_capacity_growth, 0);

    let metrics = index
        .resolve_rows(&hashes, &mut slots, |_, candidate| {
            Ok(candidate.map_or(ProbeResolution::Insert(0), ProbeResolution::Match))
        })
        .unwrap();

    assert!(slots.iter().all(|&slot| slot == 0));
    assert_eq!(index.len(), 1);
    assert_eq!(metrics.capacity_growth_events, 0);
    assert!(metrics.capacity < hashes.len() / 4);
}

#[test]
fn later_batches_find_groups_inserted_by_earlier_batches() {
    let mut index = GroupIndex::default();
    let mut slots = Vec::new();

    index
        .resolve_rows(&[11], &mut slots, |_, candidate| {
            Ok(candidate.map_or(ProbeResolution::Insert(4), ProbeResolution::Match))
        })
        .unwrap();
    assert_eq!(slots, [4]);

    index
        .resolve_rows(&[11], &mut slots, |_, candidate| {
            Ok(candidate.map_or(ProbeResolution::Insert(9), ProbeResolution::Match))
        })
        .unwrap();
    assert_eq!(slots, [4]);
    assert_eq!(index.len(), 1);
}

#[test]
fn explicit_insert_is_findable() {
    let mut index = GroupIndex::default();
    index
        .insert_unique(GroupIndexEntry { hash: 23, slot: 6 })
        .unwrap();
    assert_eq!(index.find(23, |_| true).unwrap().slot, 6);
}

#[test]
fn reserve_allocates_capacity_without_inserting_entries() {
    let mut index = GroupIndex::default();

    index.reserve(4096).expect("reserve group index");

    assert_eq!(index.entries.len(), 0);
    assert!(index.entries.capacity() >= 4096);
}
