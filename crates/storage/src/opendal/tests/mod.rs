#[cfg(feature = "opendal-fs")]
mod filesystem;

use crate::{ReadStorage, ReadStorageFactory, StorageLocation, StorageObjectPath};
use super::OpenDalReadStorage;
use opendal::{Operator, services::Memory};

#[tokio::test]
async fn configured_operator_reuses_the_same_adapter_for_non_filesystem_storage() {
    let operator = Operator::new(Memory::default()).unwrap();
    operator.write("table/file", "0123456789").await.unwrap();
    let location = StorageLocation::uri("test://namespace");
    let adapter = OpenDalReadStorage::new(location.clone(), operator);
    let storage = adapter.open(&location).unwrap();
    let path = StorageObjectPath::new("table/file");
    assert_eq!(storage.read_range(&path, 3..7).await.unwrap().as_ref(), b"3456");
    assert_eq!(storage.stat(&path).await.unwrap().identity.location(), &location);
    assert_eq!(adapter.read_optional(&StorageObjectPath::new("missing")).await.unwrap(), None);
    assert!(adapter.open(&StorageLocation::uri("test://other")).is_err());
}
