use super::*;
use crate::operators::sink::DiscardSink;

/// [`Source::bytes_read`] feeds the runtime's task metrics, so a scan that read
/// storage must never report zero, and reading an extra file must report more
/// bytes than reading one.
#[tokio::test]
async fn scan_reports_bytes_read_from_storage() {
    let dir = tempfile::tempdir().expect("temporary data root");
    let small = write_parquet(dir.path(), "part-0.parquet", 1_024);
    let large = write_parquet(dir.path(), "part-1.parquet", 8_192);

    let mut one = scan_over(dir.path(), &[("part-0.parquet", small.clone())]);
    one.execute(&mut DiscardSink::new())
        .await
        .expect("scan one file");
    let one_bytes = one.bytes_read();
    assert!(
        one_bytes > 0,
        "a completed scan must report the bytes it read, not the default zero"
    );

    let mut both = scan_over(
        dir.path(),
        &[("part-0.parquet", small), ("part-1.parquet", large)],
    );
    both.execute(&mut DiscardSink::new())
        .await
        .expect("scan two files");
    let both_bytes = both.bytes_read();
    assert!(
        both_bytes > one_bytes,
        "bytes read must accumulate across files: {both_bytes} <= {one_bytes}"
    );
}
