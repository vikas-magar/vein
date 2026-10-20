use bytes::Bytes;
use futures::future::BoxFuture;
use ::opendal::Operator;
use parquet::errors::{ParquetError, Result};

use super::ParquetRangeSource;

pub(super) struct OpenDalRangeSource {
    operator: Operator,
    path: String,
}

impl OpenDalRangeSource {
    pub(super) fn new(operator: &Operator, path: &str) -> Self {
        Self {
            operator: operator.clone(),
            path: path.to_owned(),
        }
    }
}

impl ParquetRangeSource for OpenDalRangeSource {
    fn read_range(&self, range: std::ops::Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            self.operator
                .read_with(&self.path)
                .range(range.start..range.end)
                .await
                .map_err(|error| ParquetError::General(error.to_string()))
                .map(|data| data.to_bytes())
        })
    }

    fn representation(&self) -> &'static str {
        "opendal_range_buffer"
    }
}
