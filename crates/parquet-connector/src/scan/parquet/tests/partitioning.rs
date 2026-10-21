use std::sync::Arc;

use catalog::MetadataStore;
use common::cancellation::CancellationToken;
use common::protocol::DataSource;
use exec::memory::QueryMemoryTracker;
use exec::operators::join::JoinBloomFilterFactory;
use exec::runtime::filter::{RuntimeFilterBuildInput, RuntimeFilterFactory, RuntimeFilterSet};
use types::{ColumnId, LogicalLiteral, ResolvedColumn, VeinDataType, VeinSchema};

use super::partitioning_support::{cache_footer, file_scan, plan, storage_snapshot, write_row_groups};

#[test]
fn provider_partitions_cached_footer_by_available_parallelism() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    let footer = write_row_groups(&path, 5);
    let snapshot = storage_snapshot(&path);
    let metadata = MetadataStore::new();
    cache_footer(&metadata, &snapshot, footer);
    let scan = file_scan(vec![path.clone()], vec![snapshot], Arc::new(VeinSchema::empty()));
    std::fs::remove_file(&path).expect("force planning to use the cached footer");

    let work = plan(&scan, &metadata, 3, None).expect("plan from cached footer");
    let ranges = row_group_ranges(&work);
    assert_eq!(ranges.len(), 5);
    assert_eq!(ranges.first().map(|range| range.start), Some(0));
    assert_eq!(ranges.last().map(|range| range.end), Some(5));
    assert!(ranges.windows(2).all(|pair| pair[0].end == pair[1].start));
}

#[test]
fn runtime_join_key_bounds_prune_row_groups_before_scheduling() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("runtime_bounds.parquet");
    let footer = write_row_groups(&path, 5);
    let snapshot = storage_snapshot(&path);
    let metadata = MetadataStore::new();
    cache_footer(&metadata, &snapshot, footer);
    let projection = Arc::new(VeinSchema::new(vec![Arc::new(ResolvedColumn::new(
        ColumnId(70), "id", None, VeinDataType::Int64,
    ))]));
    let scan = file_scan(vec![path], vec![snapshot], projection);
    let tracker = QueryMemoryTracker::new(1 << 20, CancellationToken::new());
    let filters = JoinBloomFilterFactory::default()
        .build_filters(RuntimeFilterBuildInput {
            probe_columns: &[ColumnId(70)],
            build_hashes: &[123_u64],
            build_key_bounds: vec![Some((LogicalLiteral::Int64(2), LogicalLiteral::Int64(3)))],
            memory: &tracker,
        })
        .expect("build runtime join filters");
    let filters = RuntimeFilterSet::new(filters);

    let work = plan(&scan, &metadata, 3, Some(&filters)).expect("prune using runtime bounds");
    assert_eq!(row_group_ranges(&work), vec![1..2]);
}

#[test]
fn planned_work_covers_every_row_group_once_across_files() {
    for (file_count, groups_per_file, workers) in [(2, 5, 3), (5, 17, 8), (7, 3, 4)] {
        assert_all_row_groups_covered(file_count, groups_per_file, workers);
    }
}

fn assert_all_row_groups_covered(file_count: usize, groups: usize, workers: usize) {
    let dir = tempfile::tempdir().expect("temporary directory");
    let metadata = MetadataStore::new();
    let mut paths = Vec::with_capacity(file_count);
    let mut snapshots = Vec::with_capacity(file_count);
    for index in 0..file_count {
        let path = dir.path().join(format!("part-{index}.parquet"));
        let footer = write_row_groups(&path, groups);
        let snapshot = storage_snapshot(&path);
        cache_footer(&metadata, &snapshot, footer);
        paths.push(path);
        snapshots.push(snapshot);
    }
    let scan = file_scan(paths.clone(), snapshots, Arc::new(VeinSchema::empty()));
    let work = plan(&scan, &metadata, workers, None).expect("partition cached footers");
    assert!(work.len() > workers);
    for path in paths {
        let mut assigned = row_groups_for_file(&work, &path);
        assigned.sort_unstable();
        assert_eq!(assigned, (0..groups).collect::<Vec<_>>());
    }
}

fn row_groups_for_file(work: &[DataSource], path: &std::path::Path) -> Vec<usize> {
    let file = path.display().to_string();
    work.iter()
        .filter_map(|source| match source {
            DataSource::File(task) if task.files == [file.clone()] => {
                task.scan_unit_range.clone()
            }
            _ => None,
        })
        .flatten()
        .collect()
}

fn row_group_ranges(work: &[DataSource]) -> Vec<std::ops::Range<usize>> {
    work.iter()
        .map(|source| match source {
            DataSource::File(task) => task.scan_unit_range.clone().expect("provider range"),
            _ => panic!("file provider must return file work units"),
        })
        .collect()
}
