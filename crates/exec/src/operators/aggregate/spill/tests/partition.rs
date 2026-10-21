use super::*;

#[test]
fn aggregate_spill_partitions_runs_by_saved_hash_and_removes_inputs_after_success() {
    let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
        .expect("create spill schema");
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let key: ArrayRef = Arc::new(Int64Array::from_iter_values(0..8));
    let groups = (0..8).map(|row| {
        sample_group(
            0,
            Some((Arc::clone(&key), row)),
            i64::try_from(row).expect("hash fits i64"),
        )
    });
    let source = managed_run(&layout, &tracker, groups);
    let manager = tracker.spill_manager();
    let children = layout
        .partition_runs(std::slice::from_ref(&source), &tracker, 0)
        .expect("partition by low hash bit");

    assert!(
        manager.exists(&source),
        "source remains live until caller commits replacement"
    );
    drop(source);
    assert_eq!(
        tracker.spill_bytes_used(),
        children.iter().flatten().map(SpillRun::bytes).sum::<u64>()
    );
    let hashes = children
        .into_iter()
        .enumerate()
        .flat_map(|(partition, path)| {
            let Some(path) = path else {
                return Vec::new();
            };
            let mut reader = layout
                .open_reader(path.open().expect("open child run"))
                .expect("validate child run");
            let mut hashes = Vec::new();
            while let Some(batch) = reader.next_batch().expect("read child run") {
                hashes.extend(batch.into_iter().map(|group| {
                    assert_eq!(group.hash & 1, partition as u64);
                    group.hash
                }));
            }
            hashes
        })
        .collect::<Vec<_>>();
    let mut sorted_hashes = hashes;
    sorted_hashes.sort_unstable();
    assert_eq!(sorted_hashes, (0..8).collect::<Vec<_>>());
}

#[test]
fn aggregate_spill_partition_removes_unused_empty_child_file() {
    let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
        .expect("create spill schema");
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let key: ArrayRef = Arc::new(Int64Array::from_iter_values(0..4));
    let groups = (0..4).map(|row| {
        sample_group(
            0,
            Some((Arc::clone(&key), row)),
            i64::try_from(row * 2).expect("even test hash fits i64"),
        )
    });
    let source = managed_run(&layout, &tracker, groups);
    let manager = tracker.spill_manager();
    let directory = manager.local_directory().expect("spill directory");

    let children = layout
        .partition_runs(std::slice::from_ref(&source), &tracker, 0)
        .expect("partition groups with an unused child");

    assert_eq!(tracker.used(), 0, "partition output releases decoded key leases");
    assert!(children[0].is_some());
    assert!(children[1].is_none());
    drop(source);
    let remaining_files = std::fs::read_dir(directory)
        .expect("read spill directory")
        .count();
    assert_eq!(remaining_files, 1, "only the referenced child run remains");
}

#[test]
fn canceled_aggregate_partition_preserves_source_run_for_query_cleanup() {
    let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
        .expect("create spill schema");
    let cancel = common::cancellation::CancellationToken::new();
    let tracker = QueryMemoryTracker::unlimited(cancel.clone());
    let key: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let source = managed_run(&layout, &tracker, [sample_group(0, Some((key, 0)), 7)]);
    let manager = tracker.spill_manager();
    let directory = manager.local_directory().expect("spill directory");

    cancel.cancel();
    assert!(matches!(
        layout.partition_runs(std::slice::from_ref(&source), &tracker, 0),
        Err(VeinError::Cancelled)
    ));
    assert!(
        manager.exists(&source),
        "failed partition leaves source data intact"
    );

    drop(source);
    drop(tracker);
    drop(manager);
    assert!(
        !directory.exists(),
        "query cleanup removes runs after cancellation"
    );
}
