use crate::pipeline::{DataChunk, PipelineNode};
use bytes::Bytes;
use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;
use futures::StreamExt;
use futures::future::BoxFuture;
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

/// An adapter to use OpenDAL's Operator as a Parquet AsyncFileReader
struct OpendalParquetReader {
    operator: Operator,
    path: String,
    _size: u64,
}

impl OpendalParquetReader {
    async fn new(operator: &Operator, path: &str) -> Result<Self> {
        let meta = operator
            .stat(path)
            .await
            .map_err(|e| VeinError::internal(format!("Failed to stat file {}: {}", path, e)))?;

        let _size = meta.content_length();

        Ok(Self {
            operator: operator.clone(),
            path: path.to_string(),
            _size,
        })
    }
}

impl AsyncFileReader for OpendalParquetReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            let data = self
                .operator
                .read_with(&self.path)
                .range(range.start..range.end)
                .await
                .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
            Ok(Bytes::from(data.to_vec()))
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        Box::pin(async move {
            let mut result = Vec::with_capacity(ranges.len());
            for range in ranges {
                let data = self
                    .operator
                    .read_with(&self.path)
                    .range(range.start..range.end)
                    .await
                    .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
                result.push(bytes::Bytes::from(data.to_vec()));
            }
            Ok(result)
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<
        'a,
        parquet::errors::Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        Box::pin(async move {
            let size = self._size;
            let reader = parquet::file::metadata::ParquetMetaDataReader::new();
            let meta = reader.load_and_finish(self, size).await?;
            Ok(std::sync::Arc::new(meta))
        })
    }
}

/// Executes a scan over a list of Parquet files
pub async fn execute_parquet_scan(
    operator: &Operator,
    paths: &[String],
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    for path in paths {
        tracing::info!(path = %path, format = "parquet", "Starting Parquet file scan");

        let reader = OpendalParquetReader::new(operator, path).await?;

        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| VeinError::internal(format!("Failed to build parquet stream: {}", e)))?;

        let mut stream = builder
            .build()
            .map_err(|e| VeinError::internal(format!("Failed to build parquet stream: {}", e)))?;

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| VeinError::internal(format!("Parquet read error: {}", e)))?;
            total_rows += batch.num_rows();

            tracing::trace!(rows = batch.num_rows(), "Pushing batch down pipeline");
            let num_rows = batch.num_rows();
            let vectors = batch.columns().to_vec();
            let address_columns = vec![None; vectors.len()];
            let chunk = DataChunk {
                vectors,
                selection: SelectionVector::new_dense(num_rows),
                address_columns,
            };
            next.push(chunk)?;
        }

        tracing::info!(path = %path, rows_processed = total_rows, "Finished scanning Parquet file");
    }

    Ok(())
}
