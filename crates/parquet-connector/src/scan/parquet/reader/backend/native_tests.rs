use std::io::Write;

use super::super::ParquetRangeSource;
use super::NativeRangeSource;

#[tokio::test]
async fn mapped_ranges_lend_file_bytes_and_keep_mapping_alive() {
    let mut file = tempfile::NamedTempFile::new().expect("temporary file");
    file.write_all(b"0123456789").expect("write source bytes");
    let source = NativeRangeSource::open(file.path(), 10)
        .await
        .expect("open local reader");
    #[cfg(unix)]
    let original = source.mapped.as_ref().map(|bytes| bytes.as_ptr());

    let first = source.read_range(2..6).await.expect("read first range");
    let second = source.read_range(7..10).await.expect("read second range");
    assert_eq!(first.as_ref(), b"2345");
    assert_eq!(second.as_ref(), b"789");
    #[cfg(unix)]
    {
        let base = original.expect("Unix source uses a file mapping");
        assert_eq!(first.as_ptr(), base.wrapping_add(2));
        assert_eq!(second.as_ptr(), base.wrapping_add(7));
        assert_eq!(source.representation(), "read_only_mmap");
    }

    drop(source);
    assert_eq!(first.as_ref(), b"2345", "range owns the mapping lifetime");
}

#[tokio::test]
async fn rejects_invalid_ranges_and_stale_file_size() {
    let mut file = tempfile::NamedTempFile::new().expect("temporary file");
    file.write_all(b"abc").expect("write source bytes");
    let source = NativeRangeSource::open(file.path(), 3)
        .await
        .expect("open local reader");

    assert!(source.read_range(2..4).await.is_err());
    assert!(NativeRangeSource::open(file.path(), 4).await.is_err());
}

#[tokio::test]
async fn empty_ranges_return_empty_bytes_without_mapping_empty_files() {
    let file = tempfile::NamedTempFile::new().expect("temporary file");
    let source = NativeRangeSource::open(file.path(), 0)
        .await
        .expect("open empty local reader");

    assert!(source.read_range(0..0).await.unwrap().is_empty());
    assert!(source.read_range(0..1).await.is_err());
}
