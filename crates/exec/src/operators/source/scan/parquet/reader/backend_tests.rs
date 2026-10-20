use std::io::Write;

use super::ParquetIoBackend;

fn local_operator(root: &std::path::Path) -> opendal::Operator {
    opendal::Operator::new(opendal::services::Fs::default().root(&root.to_string_lossy()))
        .expect("local filesystem operator")
}

#[test]
fn backend_names_parse_without_case_sensitivity() {
    assert_eq!(ParquetIoBackend::parse("native"), Some(ParquetIoBackend::Native));
    assert_eq!(ParquetIoBackend::parse("AUTO"), Some(ParquetIoBackend::Auto));
    assert_eq!(ParquetIoBackend::parse("OPENdal"), Some(ParquetIoBackend::OpenDal));
    assert_eq!(ParquetIoBackend::parse("unknown"), None);
}

#[test]
fn automatic_backend_prefers_local_mapping() {
    assert_eq!(ParquetIoBackend::default(), ParquetIoBackend::Auto);
}

#[tokio::test]
async fn automatic_backend_uses_zero_copy_source_for_local_files() {
    let mut file = tempfile::NamedTempFile::new().expect("temporary file");
    file.write_all(b"local parquet bytes").expect("write source");
    let size = file.as_file().metadata().unwrap().len();
    let root = file.path().parent().unwrap();
    let source = ParquetIoBackend::Auto
        .open_source(&local_operator(root), "unused-for-local", file.path(), size)
        .await
        .expect("open automatic source");

    #[cfg(unix)]
    assert_eq!(source.representation(), "read_only_mmap");
    let bytes = source.read_range(6..12).await.expect("read local range");
    assert_eq!(bytes.as_ref(), b"parque");
}

#[tokio::test]
async fn automatic_backend_falls_back_to_opendal_for_non_local_paths() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut stored = std::fs::File::create(directory.path().join("remote.parquet"))
        .expect("create OpenDAL source");
    stored.write_all(b"remote bytes").expect("write OpenDAL source");
    let local_only = directory.path().join("missing-local-file.parquet");
    let source = ParquetIoBackend::Auto
        .open_source(
            &local_operator(directory.path()),
            "remote.parquet",
            &local_only,
            12,
        )
        .await
        .expect("fallback to OpenDAL");

    assert_eq!(source.representation(), "opendal_range_buffer");
    assert_eq!(
        source.read_range(0..6).await.unwrap().as_ref(),
        b"remote"
    );
}
