use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use catalog::DataFileFormatId;
use common::{Result, protocol::DataSource};

use crate::operators::source::scan::{
    ScanPlanningRequest, ScanProvider, ScanRequest, file_scan_work_unit,
};
use crate::pipeline::{DataChunkView, PipelineNode};

pub(super) type Visits = Arc<Mutex<HashMap<u64, Vec<usize>>>>;

pub(super) struct StatefulProvider {
    pub(super) format: DataFileFormatId,
    pub(super) units: usize,
    pub(super) visits: Visits,
}

#[async_trait::async_trait]
impl ScanProvider for StatefulProvider {
    fn format_id(&self) -> &DataFileFormatId {
        &self.format
    }

    fn plan_work_units(&self, request: ScanPlanningRequest<'_>) -> Result<Vec<DataSource>> {
        Ok((0..self.units).map(|unit| {
            file_scan_work_unit(request.scan, 0, Some(unit..unit + 1))
        }).collect())
    }

    async fn execute(&self, request: ScanRequest<'_>, next: &mut dyn PipelineNode) -> Result<()> {
        assert!(request.query_statistics.is_some(), "every scheduling path retains query context");
        let count = request.worker_state.with::<usize, _>(|count| {
            *count += 1;
            *count
        })?;
        self.visits.lock().unwrap().entry(request.worker_state.diagnostic_id())
            .or_default().push(count);
        let view = DataChunkView::from_dense_arrays(vec![Arc::new(Int64Array::from(vec![1]))])?
            .with_schema(Arc::new(request.projected.to_arrow()))?;
        next.push(view).await
    }
}
