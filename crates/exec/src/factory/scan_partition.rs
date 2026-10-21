use crate::operators::source::scan::{
    ScanPlanningRequest, ScanProviderRegistry, validate_file_scan,
};
use crate::runtime::filter::RuntimeFilterSet;
use crate::runtime::split_scan_by_file;
use catalog::MetadataStore;
use common::Result;
use common::protocol::DataSource;

pub(super) fn partition_source(
    source: &DataSource,
    providers: &ScanProviderRegistry,
    metadata_store: &MetadataStore,
    max_parallelism: usize,
    runtime_filters: Option<&RuntimeFilterSet>,
) -> Result<Vec<DataSource>> {
    let mut partitions = match source {
        DataSource::File(scan) => {
            validate_file_scan(scan)?;
            if scan.scan_unit_range.is_some() {
                vec![source.clone()]
            } else {
                let provider = providers.resolve(&scan.data_file_format)?;
                provider.plan_work_units(ScanPlanningRequest {
                    scan,
                    metadata_store,
                    max_parallelism,
                    runtime_filters,
                })?
            }
        }
        _ => split_scan_by_file(source),
    };

    let partition_count = partitions.len();
    for (partition_index, partition) in partitions.iter_mut().enumerate() {
        let DataSource::File(scan) = partition else {
            continue;
        };
        if let Some(statistics) = &mut scan.runtime_statistics {
            statistics.partition_index = partition_index;
            statistics.partition_count = partition_count;
        }
    }
    Ok(partitions)
}
