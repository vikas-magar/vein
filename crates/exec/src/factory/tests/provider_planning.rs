use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::factory::tests::support::{file_scan, partition};
use crate::operators::source::scan::{
    ScanPlanningRequest, ScanProvider, ScanProviderRegistry, ScanRequest,
};
use crate::runtime::filter::RuntimeFilterSet;
use catalog::{DataFileFormatId, MetadataStore};
use common::Result;
use common::protocol::DataSource;

struct UnevenUnitProvider {
    format: DataFileFormatId,
    planning_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ScanProvider for UnevenUnitProvider {
    fn format_id(&self) -> &DataFileFormatId {
        &self.format
    }

    fn plan_work_units(&self, request: ScanPlanningRequest<'_>) -> Result<Vec<DataSource>> {
        assert_eq!(request.max_parallelism, 4);
        assert!(request.runtime_filters.is_some());
        self.planning_calls.fetch_add(1, Ordering::Relaxed);
        let mut work = Vec::new();
        for (file, ranges) in [
            ("a.data", vec![0..1, 1..4]),
            ("b.data", std::iter::once(0..2).collect()),
        ] {
            for range in ranges {
                let mut scan = request.scan.clone();
                scan.files = vec![file.to_owned()];
                scan.scan_unit_range = Some(range);
                work.push(DataSource::File(scan));
            }
        }
        Ok(work)
    }

    fn open<'a>(&'a self, _request: ScanRequest<'a>) -> crate::operators::source::scan::ScanCursor<'a> {
        Box::pin(futures::stream::empty())
    }
}

#[test]
fn generic_factory_dispatches_uneven_units_to_the_registered_provider_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let format = DataFileFormatId::new("test_format");
    let mut providers = ScanProviderRegistry::empty();
    providers
        .register(UnevenUnitProvider {
            format: format.clone(),
            planning_calls: Arc::clone(&calls),
        })
        .expect("register provider");
    let scan = file_scan(&format.to_string(), vec!["a.data".into(), "b.data".into()], vec![]);
    let runtime_filters = RuntimeFilterSet::empty();

    let planned = partition(
        &scan,
        &providers,
        &MetadataStore::new(),
        4,
        Some(&runtime_filters),
    )
        .expect("registered provider plans opaque scan units");
    let mut units = planned
        .iter()
        .map(|source| match source {
            DataSource::File(scan) => (scan.files[0].clone(), scan.scan_unit_range.clone().unwrap()),
            _ => panic!("provider returned a non-file unit"),
        })
        .collect::<Vec<_>>();
    units.sort_by_key(|(file, range)| (file.clone(), range.start));

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(units, vec![
        ("a.data".to_owned(), 0..1),
        ("a.data".to_owned(), 1..4),
        ("b.data".to_owned(), 0..2),
    ]);
}
