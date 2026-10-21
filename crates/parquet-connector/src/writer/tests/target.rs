use super::support::{storage, target};
use common::protocol::KeyValue;
use exec::operators::sink::WriteStorage;

#[test]
fn validates_parquet_targets_and_rejects_unsupported_or_ambiguous_options() {
    let root = tempfile::tempdir().expect("temporary root");
    let storage = storage(root.path());
    for (destination, options, expected) in [
        ("../events", vec![], "single logical table name"),
        ("events", vec![("mode".into(), "merge".into())], "mode must be create, append, or overwrite"),
        ("events", vec![("format".into(), "csv".into())], "supports only format='parquet'"),
        ("events", vec![("compression".into(), "zstd".into())], "does not support option 'compression'"),
        ("events", vec![("mode".into(), "append".into()), ("MODE".into(), "create".into())], "duplicate WRITE option"),
        ("events", vec![("partition_id".into(), "0".into())], "reserved for the execution engine"),
    ] {
        let error = storage.validate_target(destination, &options).expect_err("invalid target must fail preflight");
        assert!(error.to_string().contains(expected), "unexpected error: {error}");
    }
}

#[test]
fn begin_write_revalidates_options_and_accepts_dispatch_partition_metadata() {
    let root = tempfile::tempdir().expect("temporary root");
    let storage = storage(root.path());
    let mut supported = target(None);
    supported.options.push(KeyValue::new("partition_id", "0"));
    assert!(storage.begin_write(&supported).is_ok());

    let mut unsupported = target(None);
    unsupported.options.push(KeyValue::new("compression", "zstd"));
    let error = match storage.begin_write(&unsupported) {
        Err(error) => error,
        Ok(_) => panic!("unsupported option must fail"),
    };
    assert!(error.to_string().contains("does not support option 'compression'"));
}
