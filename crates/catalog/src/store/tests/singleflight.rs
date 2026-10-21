use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use storage::{
    StorageFileMetadata, StorageLocation, StorageObjectIdentity, StorageObjectPath,
    StorageObjectVersion,
};

use super::super::{MetadataKey, MetadataKind, MetadataStore};

#[derive(Debug, PartialEq)]
struct Parsed(u64);

fn shared_key() -> MetadataKey {
    let metadata = StorageFileMetadata {
        identity: StorageObjectIdentity::new(
            StorageLocation::uri("memory://test"),
            StorageObjectPath::new("part.parquet"),
        ),
        size_bytes: 128,
        version: StorageObjectVersion {
            etag: Some(Arc::from("version-1")),
            ..StorageObjectVersion::default()
        },
    };
    MetadataKey::from_storage(MetadataKind::new("test.footer"), &metadata).unwrap()
}

#[test]
fn concurrent_misses_parse_once_and_release_the_flight_entry() {
    let store = MetadataStore::new();
    let parses = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(8));
    let workers = (0..8)
        .map(|_| {
            let store = store.clone();
            let parses = Arc::clone(&parses);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                store
                    .get_or_parse(shared_key(), || {
                        parses.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_millis(20));
                        Ok::<_, ()>(Arc::new(Parsed(42)))
                    })
                    .expect("parse succeeds")
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        let (value, _) = worker.join().expect("worker completes");
        assert_eq!(*value, Parsed(42));
    }
    assert_eq!(parses.load(Ordering::Relaxed), 1);
}

#[test]
fn failed_parse_releases_flight_for_a_later_retry() {
    let store = MetadataStore::new();
    let key = shared_key();
    assert!(store.get_or_parse::<Parsed, _>(key.clone(), || Err("parse failed")).is_err());

    let (value, hit) = store
        .get_or_parse(key, || Ok::<_, &'static str>(Arc::new(Parsed(7))))
        .expect("retry succeeds");
    assert!(!hit);
    assert_eq!(*value, Parsed(7));
}
