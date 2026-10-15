use std::io::Write;

use super::*;

/// A format-owned payload: the store never names it, which is the point.
#[derive(Debug, PartialEq)]
struct RowCount(u64);

const TEST_KIND: MetadataKind = MetadataKind::new("test.footer");
const OTHER_KIND: MetadataKind = MetadataKind::new("test.manifest");

fn write_file(path: &Path, contents: &[u8]) {
    let mut file = std::fs::File::create(path).expect("create file");
    file.write_all(contents).expect("write file");
}

fn key(path: &Path) -> MetadataKey {
    MetadataKey::for_file(TEST_KIND, path).expect("file exists")
}

/// The scan builds its key with [`MetadataKey::for_canonical_file`] from a
/// path the catalog resolved, so a resolved path must key the same entry as
/// the unresolved spelling it was resolved from. If these ever disagree, the
/// catalog's parsed footers stop reaching execution and every scan silently
/// re-parses what planning already read.
#[test]
fn resolved_and_unresolved_spellings_name_the_same_file() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    write_file(&path, b"metadata");

    let canonical = std::fs::canonicalize(&path).expect("resolve file");
    let resolved =
        MetadataKey::for_canonical_file(TEST_KIND, &canonical).expect("file exists");

    assert_eq!(resolved, key(&path));
}

#[test]
fn misses_files_it_has_not_seen() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    write_file(&path, b"metadata");

    let store = MetadataStore::new();
    assert!(store.get::<RowCount>(&key(&path)).is_none());
    assert!(store.is_empty());
}

#[test]
fn serves_what_it_was_given() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    write_file(&path, b"metadata");

    let store = MetadataStore::new();
    store.insert(key(&path), Arc::new(RowCount(7)));

    let cached = store.get::<RowCount>(&key(&path)).expect("cached payload");
    assert_eq!(*cached, RowCount(7));
    assert_eq!(store.len(), 1);
}

#[test]
fn distinguishes_kinds_and_payload_types() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    write_file(&path, b"metadata");

    let store = MetadataStore::new();
    store.insert(key(&path), Arc::new(RowCount(7)));
    store.insert(
        MetadataKey::for_file(OTHER_KIND, &path).expect("file exists"),
        Arc::new("manifest".to_string()),
    );

    assert_eq!(
        *store.get::<RowCount>(&key(&path)).expect("footer"),
        RowCount(7)
    );
    assert!(
        store
            .get::<RowCount>(&MetadataKey::for_file(OTHER_KIND, &path).expect("file exists"))
            .is_none(),
        "a different kind must not answer for the footer payload type"
    );
    assert_eq!(store.len(), 2);
}

#[test]
fn a_rewritten_file_invalidates_its_entry() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    write_file(&path, b"metadata");

    let store = MetadataStore::new();
    store.insert(key(&path), Arc::new(RowCount(7)));

    // A different size means the entry cannot be mistaken for the
    // replacement even at coarse modification-time granularity.
    write_file(&path, b"a longer metadata block");
    assert!(store.get::<RowCount>(&key(&path)).is_none());
}

#[test]
fn keeps_one_entry_per_file_and_kind() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    write_file(&path, b"metadata");

    let store = MetadataStore::new();
    store.insert(key(&path), Arc::new(RowCount(1)));
    store.insert(key(&path), Arc::new(RowCount(2)));

    assert_eq!(store.len(), 1);
    assert_eq!(*store.get::<RowCount>(&key(&path)).expect("footer"), RowCount(2));
}

#[test]
fn evicts_the_oldest_entry_past_capacity() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let store = MetadataStore::with_capacity(2);
    let paths: Vec<PathBuf> = (0..3)
        .map(|index| {
            let path = dir.path().join(format!("part-{index}.parquet"));
            write_file(&path, format!("metadata-{index}").as_bytes());
            path
        })
        .collect();

    for (index, path) in paths.iter().enumerate() {
        store.insert(key(path), Arc::new(RowCount(index as u64)));
    }

    assert_eq!(store.len(), 2);
    assert!(store.get::<RowCount>(&key(&paths[0])).is_none(), "oldest evicted");
    assert!(store.get::<RowCount>(&key(&paths[1])).is_some());
    assert!(store.get::<RowCount>(&key(&paths[2])).is_some());
}

#[test]
fn get_or_parse_parses_once() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    write_file(&path, b"metadata");

    let store = MetadataStore::new();
    let mut parses = 0;
    for expected_hit in [false, true, true] {
        let (payload, hit) = store
            .get_or_parse(key(&path), || {
                parses += 1;
                Ok::<_, ()>(Arc::new(RowCount(7)))
            })
            .expect("parse succeeds");
        assert_eq!(hit, expected_hit);
        assert_eq!(*payload, RowCount(7));
    }
    assert_eq!(parses, 1, "the second and third lookups reuse the parsed value");
}

#[test]
fn clear_drops_every_entry() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("part.parquet");
    write_file(&path, b"metadata");

    let store = MetadataStore::new();
    store.insert(key(&path), Arc::new(RowCount(7)));
    store.clear();

    assert!(store.is_empty());
    assert!(store.get::<RowCount>(&key(&path)).is_none());
}
