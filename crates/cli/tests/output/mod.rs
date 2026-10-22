//! Process-level checks for result and diagnostic stream isolation.

mod support;

use std::process::{Command, Stdio};

#[test]
fn jsonl_results_complete_with_worker_logs_on_stderr() {
    for format in ["pretty", "json"] {
        let root = support::scan_fixture();
        let child = Command::new(env!("CARGO_BIN_EXE_cli"))
            .arg("FROM events |> WHERE id > 0 |> SELECT id |> ORDER id")
            .env("VEIN_DATA_DIR", root.path())
            .env("VEIN_OUTPUT_FORMAT", "jsonl")
            .env("VEIN_LOG_FORMAT", format)
            .env("VEIN_LOG_ANSI", "false")
            .env("RUST_LOG", "warn,vein.audit=info")
            .env_remove("VEIN_AUDIT_LOG_PATH")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("launch CLI");
        let output = support::finish_with_timeout(child);
        let stderr = String::from_utf8(output.stderr).expect("UTF-8 diagnostics");
        assert!(output.status.success(), "{format}: {stderr}");
        assert!(stderr.contains("logging_initialized"), "{stderr}");
        assert!(stderr.contains("parquet_io_backend_selected"), "{stderr}");
        assert!(stderr.contains("tokio-rt-worker"), "{stderr}");
        assert!(stderr.contains("query_finished"), "{stderr}");
        let stdout = String::from_utf8(output.stdout).expect("UTF-8 results");
        let lines = stdout.lines().map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .unwrap_or_else(|error| panic!("invalid JSONL: {error}: {line}"))
        }).collect::<Vec<_>>();
        assert_eq!(lines.len(), 4, "{stdout}");
        assert_eq!(lines[0]["kind"], "schema");
        for (line, id) in lines[1..].iter().zip(1..=3) {
            assert_eq!(line["vein_col_0"], id);
        }
    }
}
