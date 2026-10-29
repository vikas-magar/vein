use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use opendal::{ErrorKind, Operator, Result};

use super::{FileMetadata, FileRangeReader, FileStorage};

#[derive(Debug, Default)]
struct RecordingReader {
    calls: Mutex<Vec<(String, u64, usize)>>,
}

impl FileRangeReader for RecordingReader {
    fn read_into(&self, path: &str, offset: u64, output: &mut [u8]) -> Result<()> {
        self.calls.lock().unwrap().push((path.to_owned(), offset, output.len()));
        let start = offset as usize;
        output.copy_from_slice(&b"0123456789"[start..start + output.len()]);
        Ok(())
    }
}

fn operator() -> (Operator, Arc<RecordingReader>) {
    let reader = Arc::new(RecordingReader::default());
    let files = Arc::new(BTreeMap::from([
        ("part/a.parquet".into(), FileMetadata { size: 10, version: "snapshot-a".into() }),
        ("partsupp/b.parquet".into(), FileMetadata { size: 10, version: "snapshot-b".into() }),
    ]));
    (Operator::new(FileStorage::new("test".into(), files, Arc::clone(&reader))).unwrap(), reader)
}

#[tokio::test]
async fn registration_stat_and_listing_never_read_payloads() {
    let (operator, reader) = operator();
    let metadata = operator.stat("part/a.parquet").await.unwrap();
    assert_eq!(metadata.content_length(), 10);
    assert_eq!(metadata.version(), Some("snapshot-a"));
    let paths = operator.lister_with("/").recursive(true).await.unwrap()
        .map_ok(|entry| entry.path().to_owned()).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(paths, ["part/a.parquet", "partsupp/b.parquet"]);
    assert!(reader.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn range_reads_fetch_only_requested_bytes_and_reject_writes() {
    let (operator, reader) = operator();
    assert_eq!(operator.read_with("part/a.parquet").range(3..6).await.unwrap().to_bytes().as_ref(), b"345");
    assert_eq!(reader.calls.lock().unwrap().as_slice(), &[("part/a.parquet".into(), 3, 3)]);
    assert_eq!(operator.write("part/a.parquet", "x").await.unwrap_err().kind(), ErrorKind::Unsupported);
    assert_eq!(operator.stat("missing").await.unwrap_err().kind(), ErrorKind::NotFound);
}
