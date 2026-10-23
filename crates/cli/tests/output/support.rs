use std::io::Read;
use std::process::{Child, Output};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

pub(super) fn scan_fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("temporary fixture root");
    let directory = root.path().join("events");
    std::fs::create_dir(&directory).expect("fixture table directory");
    let file = std::fs::File::create(directory.join("part.parquet")).expect("fixture file");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![3, 1, 2]))],
    ).expect("fixture batch");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("fixture writer");
    writer.write(&batch).expect("write fixture");
    writer.close().expect("close fixture");
    root
}

/// Drain both pipes concurrently so logging volume cannot deadlock the test itself.
pub(super) fn finish_with_timeout(mut child: Child) -> Output {
    fn drain(mut input: impl Read + Send + 'static) -> thread::JoinHandle<Vec<u8>> {
        thread::spawn(move || {
            let mut bytes = Vec::new();
            input.read_to_end(&mut bytes).expect("read child output");
            bytes
        })
    }
    let stdout = drain(child.stdout.take().expect("stdout pipe"));
    let stderr = drain(child.stderr.take().expect("stderr pipe"));
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll CLI") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout.join();
            let diagnostics = stderr.join().expect("stderr reader");
            panic!("CLI timed out: {}", String::from_utf8_lossy(&diagnostics));
        }
        thread::sleep(Duration::from_millis(10));
    };
    Output {
        status,
        stdout: stdout.join().expect("stdout reader"),
        stderr: stderr.join().expect("stderr reader"),
    }
}
