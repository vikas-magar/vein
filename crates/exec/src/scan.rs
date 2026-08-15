use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use error::{Result, VeinError};

/// A `SourceStage` that reads a local Parquet file and produces `RecordBatch`es.
pub struct ParquetScanStage {
    file_path: String,
    projection: Option<Vec<i32>>,
    reader: Option<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>>,
    finished: bool,
}

impl ParquetScanStage {
    /// Create a new Parquet scan stage for the given file.
    ///
    /// `projection` is optional column indices to read (projection pushdown).
    pub fn new(file_path: impl Into<String>, projection: Option<Vec<i32>>) -> Self {
        Self {
            file_path: file_path.into(),
            projection,
            reader: None,
            finished: false,
        }
    }
}

impl ParquetScanStage {
    /// Initialize the scan stage by opening the parquet file and building the reader.
    pub async fn init(&mut self) -> Result<()> {
        let path = self.file_path.clone();
        let projection = self.projection.clone();

        let reader = tokio::task::spawn_blocking(move || -> Result<_> {
            let file = File::open(Path::new(&path))
                .map_err(|e| VeinError::internal(format!("open {path}: {e}")))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| VeinError::internal(format!("parquet builder: {e}")))?;

            let arrow_schema = builder.schema().clone();

            let reader = if let Some(ref cols) = projection {
                // build a projected schema and a column mask
                let projected_fields: Vec<_> = cols
                    .iter()
                    .filter_map(|&i| {
                        if (i as usize) < arrow_schema.fields().len() {
                            Some(arrow_schema.field(i as usize).clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let projected_schema = Arc::new(Schema::new(projected_fields));
                let mask = parquet::arrow::ProjectionMask::leaves(
                    builder.metadata().file_metadata().schema_descr(),
                    cols.iter().map(|&i| i as usize),
                );
                let reader = builder
                    .with_projection(mask)
                    .build()
                    .map_err(|e| VeinError::internal(format!("build reader: {e}")))?;
                let batches: Vec<RecordBatch> = reader
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| VeinError::internal(format!("read batches: {e}")))?;
                // re-wrap with projected schema
                let batches: Vec<RecordBatch> = batches
                    .into_iter()
                    .map(|b| {
                        RecordBatch::try_new(projected_schema.clone(), b.columns().to_vec())
                            .map_err(|e| VeinError::internal(format!("re-wrap: {e}")))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Box::new(batches.into_iter().map(Ok))
                    as Box<dyn Iterator<Item = Result<RecordBatch>> + Send>
            } else {
                let reader = builder
                    .build()
                    .map_err(|e| VeinError::internal(format!("build reader: {e}")))?;
                let batches: Vec<RecordBatch> = reader
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| VeinError::internal(format!("read batches: {e}")))?;
                Box::new(batches.into_iter().map(Ok))
                    as Box<dyn Iterator<Item = Result<RecordBatch>> + Send>
            };

            Ok(reader)
        })
        .await
        .map_err(|e| VeinError::internal(format!("spawn_blocking: {e}")))??;

        self.reader = Some(reader);
        Ok(())
    }

    /// Produce the next RecordBatch from the parquet file.
    pub async fn produce(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        match &mut self.reader {
            Some(iter) => match iter.next() {
                Some(Ok(batch)) => Ok(Some(batch)),
                Some(Err(e)) => {
                    self.finished = true;
                    Err(e)
                }
                None => {
                    self.finished = true;
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::ArrowWriter;

    fn write_test_parquet(path: &Path, rows: i32) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        let ids: Vec<i32> = (0..rows).collect();
        let names: Vec<String> = (0..rows).map(|i| format!("row_{i}")).collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_parquet_scan_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        write_test_parquet(&path, 10);

        let mut stage = ParquetScanStage::new(path.to_string_lossy(), None);
        stage.init().await.unwrap();

        let mut count = 0;
        while let Some(batch) = stage.produce().await.unwrap() {
            count += batch.num_rows();
        }
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_parquet_scan_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_proj.parquet");
        write_test_parquet(&path, 5);

        // Project only column 0 (id)
        let mut stage = ParquetScanStage::new(path.to_string_lossy(), Some(vec![0]));
        stage.init().await.unwrap();

        let batch = stage.produce().await.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.column(0).data_type(), &DataType::Int32);
    }

    #[tokio::test]
    async fn test_parquet_scan_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let file = File::create(&path).unwrap();
        let writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.close().unwrap();

        let mut stage = ParquetScanStage::new(path.to_string_lossy(), None);
        stage.init().await.unwrap();
        let result = stage.produce().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_parquet_scan_file_not_found() {
        let mut stage = ParquetScanStage::new("/nonexistent/file.parquet", None);
        let result = stage.init().await;
        assert!(result.is_err());
    }
}
